# Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from abc import ABC, abstractmethod
from threading import Lock, Timer
from typing import Optional, Dict, List

from google.protobuf import text_format
from sortedcontainers import SortedDict, SortedSet
from th2_common.schema.event.event_batch_router import EventBatchRouter
from th2_grpc_common.common_pb2 import EventStatus, Event, EventBatch, EventID, Message
from th2_grpc_util.util_pb2 import CompareMessageVsMessageRequest, ComparisonSettings, \
    CompareMessageVsMessageTask, CompareMessageVsMessageResult

from th2_check2_recon.common import EventUtils, MessageComponent, VerificationComponent
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupType


logger = logging.getLogger(__name__)


class AbstractService(ABC):

    @abstractmethod
    def stop(self):
        pass


class EventsBatchCollector(AbstractService):

    def __init__(self, event_router, max_batch_size: int, timeout: int) -> None:
        self.max_batch_size = max_batch_size
        self.timeout = timeout  # Send interval

        self.__batches = {}  # {event_id: (event_batch, batch_timer)}
        self.__lock = Lock()
        self.__event_router: EventBatchRouter = event_router

    def put_event(self, event: Event):
        with self.__lock:
            try:
                event_batch, batch_timer = self.__batches[event.parent_id.id]
            except KeyError:
                event_batch = EventBatch(parent_event_id=event.parent_id)
                batch_timer = self._create_timer(event_batch)
                self.__batches[event.parent_id.id] = (event_batch, batch_timer)

            event_batch.events.append(event)
            if len(event_batch.events) == self.max_batch_size:
                del self.__batches[event.parent_id.id]
                batch_timer.cancel()
                self._send_batch(event_batch)

    def _timer_handle(self, batch: EventBatch):
        with self.__lock:
            if batch.parent_event_id.id in self.__batches:
                del self.__batches[batch.parent_event_id.id]
            else:
                return
        self._send_batch(batch)

    def _send_batch(self, batch: EventBatch):
        try:
            self.__event_router.send(batch)
        except Exception as e:
            logger.exception(F"Error while send EventBatch: {e}")

    def _create_timer(self, batch: EventBatch):
        timer = Timer(self.timeout, self._timer_handle, [batch])
        timer.start()
        return timer

    def stop(self):
        with self.__lock:
            for id in self.__batches.keys():
                event_batch, batch_timer = self.__batches[id]
                batch_timer.cancel()
        with self.__lock:
            for id in self.__batches.keys():
                event_batch, batch_timer = self.__batches[id]
                self._send_batch(event_batch)
            self.__batches.clear()


class EventStore(AbstractService):
    MATCHED_FAILED = 'Matched failed'
    MATCHED_PASSED = 'Matched passed'
    MATCHED_OUT_OF_TIMEOUT = 'Matched out of timeout'
    NO_MATCH_WITHIN_TIMEOUT = 'No match within timeout'
    NO_MATCH = 'No match'
    ERRORS = 'Errors'

    def __init__(self, event_router: EventBatchRouter, report_name: str, event_batch_max_size: int,
                 event_batch_send_interval: int) -> None:
        self.__event_router = event_router
        self.__events_batch_collector = EventsBatchCollector(event_router, event_batch_max_size,
                                                             event_batch_send_interval)
        self.__group_event_by_rule_id = dict()

        self.root_event: Event = EventUtils.create_event(name='Recon: ' + report_name)
        logger.debug('Created root report Event for Recon: %s',
                     text_format.MessageToString(self.root_event, as_one_line=True))
        self.send_parent_event(self.root_event)

    def send_event(self, event: Event, rule_event_id: EventID, group_event_name: str):
        try:
            if not self.__group_event_by_rule_id.__contains__(rule_event_id.id):
                self.__group_event_by_rule_id[rule_event_id.id] = dict()
            if not self.__group_event_by_rule_id[rule_event_id.id].__contains__(group_event_name):
                group_event = EventUtils.create_event(parent_id=rule_event_id,
                                                      name=group_event_name)
                logger.debug(f"Create group Event '%s' for rule Event '%s'", group_event_name, rule_event_id)
                self.__group_event_by_rule_id[rule_event_id.id][group_event_name] = group_event
                self.send_parent_event(group_event)

            group_event = self.__group_event_by_rule_id[rule_event_id.id][group_event_name]
            event.id.CopyFrom(EventUtils.create_event_id())
            event.parent_id.CopyFrom(group_event.id)
            self.__events_batch_collector.put_event(event)
        except Exception:
            logger.exception(f'Error while sending event')

    def send_parent_event(self, event: Event):
        event_batch = EventBatch()
        event_batch.events.append(event)
        self.__event_router.send(event_batch)

    def store_no_match_within_timeout(self, rule_event_id: EventID, recon_message: ReconMessage,
                                      actual_timestamp: int, timeout: int):
        name = f'{recon_message.get_all_info()}'

        units = ['sec', 'ms', 'mcs', 'ns']
        factor_units = [1_000_000_000, 1_000_000, 1_000, 1]
        units_idx = 0
        while actual_timestamp % factor_units[units_idx] != 0 \
                or recon_message.timestamp % factor_units[units_idx] != 0 \
                or timeout % factor_units[units_idx] != 0:
            units_idx += 1

        unit = units[units_idx]
        actual_timestamp = int(actual_timestamp / factor_units[units_idx])
        message_timestamp = int(recon_message.timestamp / factor_units[units_idx])
        timeout = int(timeout / factor_units[units_idx])

        event_message = f"Timestamp of the last received message: '{actual_timestamp:,}' {unit}\n" \
                        f"Timestamp this message: '{message_timestamp:,}' {unit}\n" \
                        f"Timeout: '{timeout:,}' {unit}"
        body = EventUtils.create_event_body(MessageComponent(event_message))
        attached_message_ids = self._get_attached_message_ids(recon_message)
        event = EventUtils.create_event(name=name,
                                        body=body,
                                        attached_message_ids=attached_message_ids)
        logger.debug("Create '%s' Event for rule Event '%s'", self.NO_MATCH_WITHIN_TIMEOUT, rule_event_id)
        self.send_event(event, rule_event_id, self.NO_MATCH_WITHIN_TIMEOUT)

    def store_no_match(self, rule_event_id: EventID, message: ReconMessage, event_message: str):
        name = f"Remove {message.get_all_info()}"
        event_message += f"\n Message {'not' if not message.is_matched else ''} matched"
        body = EventUtils.create_event_body(MessageComponent(event_message))
        attached_message_ids = self._get_attached_message_ids(message)
        event = EventUtils.create_event(name=name,
                                        body=body,
                                        status=EventStatus.SUCCESS if message.is_matched else EventStatus.FAILED,
                                        attached_message_ids=attached_message_ids)
        logger.debug("Create '%s' Event for rule Event '%s'", self.NO_MATCH, rule_event_id)
        self.send_event(event, rule_event_id, self.NO_MATCH)

    def store_error(self, rule_event_id: EventID, event_name: str, error_message: str,
                    messages: List[ReconMessage] = None):
        body = EventUtils.create_event_body(MessageComponent(error_message))
        attached_message_ids = self._get_attached_message_ids(*messages)
        event = EventUtils.create_event(name=event_name,
                                        status=EventStatus.FAILED,
                                        attached_message_ids=attached_message_ids,
                                        body=body)
        logger.debug("Create '%s' Event for rule Event '%s'", self.ERRORS, rule_event_id)
        self.send_event(event, rule_event_id, self.ERRORS)

    def store_matched_out_of_timeout(self, rule_event_id: EventID, check_event: Event):
        self.send_event(check_event, rule_event_id, self.MATCHED_OUT_OF_TIMEOUT)

    def store_matched(self, rule_event_id: EventID, check_event: Event):
        if check_event.status == EventStatus.SUCCESS:
            self.send_event(check_event, rule_event_id, self.MATCHED_PASSED)
        else:
            self.send_event(check_event, rule_event_id, self.MATCHED_FAILED)

    def stop(self):
        self.__events_batch_collector.stop()

    def _get_attached_message_ids(self, *recon_msgs):
        try:
            return [message.proto_message.metadata.id for message in recon_msgs]
        except AttributeError:
            return []


class MessageComparator(AbstractService):

    def __init__(self, comparator_service) -> None:
        self.__comparator_service = comparator_service

    def compare(self, expected: Message, actual: Message,
                settings: ComparisonSettings) -> CompareMessageVsMessageResult:
        return self.comparing(expected, actual, settings)

    def comparing(self, expected: Message, actual: Message,
                  settings: ComparisonSettings) -> CompareMessageVsMessageResult:
        request = CompareMessageVsMessageRequest(
            comparison_tasks=[CompareMessageVsMessageTask(first=expected, second=actual,
                                                          settings=settings)])
        try:
            compare_response = self.__comparator_service.compareMessageVsMessage(request)
            for compare_result in compare_response.comparison_results:  # TODO  fix it
                return compare_result
        except Exception:
            logger.exception(f'Error while comparing. CompareMessageVsMessageRequest: {request}')

    def compare_messages(self, messages: [ReconMessage],
                         ignore_fields: [str] = None) -> Optional[VerificationComponent]:
        if len(messages) != 2:
            logger.exception(f'The number of messages to compare must be 2.')
            return
        settings = ComparisonSettings()
        if ignore_fields is not None:
            settings.ignore_fields.extend(ignore_fields)

        compare_result = self.compare(messages[0].proto_message, messages[1].proto_message, settings)
        return VerificationComponent(compare_result.comparison_result)

    def stop(self):
        pass


class MessageGroup:

    def __init__(self, id: str, capacity: int, type: {MessageGroupType}, event_store: EventStore,
                 parent_event: Event) -> None:
        self.id = id
        self.capacity = capacity
        self.size = 0
        self.type: {MessageGroupType} = type
        self.__parent_event: Event = parent_event
        self.__event_store = event_store

        self.is_cleanable = True
        self.__data: Dict[int, List[ReconMessage]] = dict()  # {ReconMessage.hash: [ReconMessage]}
        self.__hash_by_timestamp: Dict[int, List[int]] = SortedDict()
        self.__timestamp_within_timeout = SortedSet()

    def get(self, hash_of_message: int) -> List[ReconMessage]:
        return self.__data[hash_of_message]

    def put(self, message: ReconMessage):
        if self.size < self.capacity:
            if message.hash in self.__data and MessageGroupType.single in self.type:
                self.__remove_same_hash_messages(message.hash)
            self.__data.setdefault(message.hash, []).append(message)
            self.__hash_by_timestamp.setdefault(message.timestamp, []).append(message.hash)
            self.__timestamp_within_timeout.add(message.timestamp)
            self.size += 1
        else:
            self.__remove_oldest_message()
            self.put(message)

    def __remove_same_hash_messages(self, same_hash: int):
        cause = f"The message was deleted because a new one came with the same hash '{same_hash}' " \
                f"in message group '{self.id}'"

        for same_hash_message in self.__data[same_hash]:
            self.__event_store.store_no_match(rule_event_id=self.__parent_event.id,
                                              message=same_hash_message,
                                              event_message=cause)
            timestamp = same_hash_message.timestamp
            self.__hash_by_timestamp[timestamp].remove(same_hash)
            if len(self.__hash_by_timestamp[timestamp]) == 0:
                del self.__hash_by_timestamp[timestamp]

            if timestamp in self.__timestamp_within_timeout and timestamp not in self.__hash_by_timestamp:
                self.__timestamp_within_timeout.remove(timestamp)

            self.size -= 1
        del self.__data[same_hash]

    def __remove_oldest_message(self, cause: str = None):
        oldest_timestamp = next(iter(self.__hash_by_timestamp))
        oldest_hash = self.__hash_by_timestamp[oldest_timestamp][0]
        oldest_message = self.__data[oldest_hash][0]

        if cause is None:
            cause = f"The message was deleted because there was no free space in the message group '{self.id}'"
        self.__event_store.store_no_match(rule_event_id=self.__parent_event.id,
                                          message=oldest_message,
                                          event_message=cause)

        self.__data[oldest_hash].remove(oldest_message)
        if len(self.__data[oldest_hash]) == 0:
            del self.__data[oldest_hash]

        self.__hash_by_timestamp[oldest_timestamp].remove(oldest_hash)
        if len(self.__hash_by_timestamp[oldest_timestamp]) == 0:
            del self.__hash_by_timestamp[oldest_timestamp]

        if oldest_timestamp in self.__timestamp_within_timeout and oldest_timestamp not in self.__hash_by_timestamp:
            self.__timestamp_within_timeout.remove(oldest_timestamp)

        self.size -= 1

    def check_messages_out_of_timeout(self, lower_bound_timestamp: int):
        timestamp_out_of_timeout = []
        for timestamp in self.__timestamp_within_timeout:
            if timestamp < lower_bound_timestamp:
                timestamp_out_of_timeout.append(timestamp)
            else:
                break

        for timestamp in timestamp_out_of_timeout:
            self.__timestamp_within_timeout.remove(timestamp)
            for hash_of_message in self.__hash_by_timestamp[timestamp]:
                for message in self.__data[hash_of_message]:
                    message.is_ouf_of_timeout = True

    def clear(self):
        cause = "The message was deleted because the Recon stopped"
        while len(self.__data) != 0:
            self.__remove_oldest_message(cause)
