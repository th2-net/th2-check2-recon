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
from typing import Optional

import sortedcollections
from th2_common.schema.event.event_batch_router import EventBatchRouter
from th2_grpc_common.common_pb2 import EventStatus, Event, EventBatch, EventID, Message
from th2_grpc_util.util_pb2 import CompareMessageVsMessageRequest, ComparisonSettings, \
    CompareMessageVsMessageTask, CompareMessageVsMessageResult

from th2_check2_recon.common import EventUtils, MessageComponent, MessageUtils, VerificationComponent
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupType

logger = logging.getLogger()


class AbstractService(ABC):

    @abstractmethod
    def stop(self):
        pass


class EventsBatchCollector(AbstractService):

    def __init__(self, event_router, max_batch_size: int, timeout: int) -> None:
        self.max_batch_size = max_batch_size
        self.timeout = timeout

        self.__batches = {}
        self.__lock = Lock()
        self.__event_router: EventBatchRouter = event_router

    def put_event(self, event: Event):
        with self.__lock:
            if event.parent_id.id in self.__batches:
                event_batch, batch_timer = self.__batches.get(event.parent_id.id)
            else:
                event_batch = EventBatch(parent_event_id=event.parent_id)
                batch_timer = self._create_timer(event_batch)
                self.__batches[event.parent_id.id] = (event_batch, batch_timer)
            event_batch.events.append(event)
            if len(event_batch.events) == self.max_batch_size:
                self.__batches.pop(event.parent_id.id)
                batch_timer.cancel()
            else:
                return

        self._send_batch(event_batch)

    def _timer_handle(self, batch: EventBatch):
        with self.__lock:
            if batch.parent_event_id.id in self.__batches:
                self.__batches.pop(batch.parent_event_id.id)
            else:
                return
        self._send_batch(batch)

    def _send_batch(self, batch: EventBatch):
        try:
            self.__event_router.send(batch)
        except Exception:
            logger.exception("Error while send EventBatch")

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
        self.event_router = event_router
        self.__events_batch_collector = EventsBatchCollector(event_router, event_batch_max_size,
                                                             event_batch_send_interval)
        self.__group_event_by_rule_id = dict()


        self.root_event: Event = EventUtils.create_event(name='Recon: ' + report_name)
        logger.info(f'Created root report Event for Recon: {self.root_event}')
        self.send_parent_event(self.root_event)

    def send_event(self, event: Event, rule_event_id: EventID, group_event_name: str):
        try:
            if not self.__group_event_by_rule_id.__contains__(rule_event_id.id):
                self.__group_event_by_rule_id[rule_event_id.id] = dict()
            if not self.__group_event_by_rule_id[rule_event_id.id].__contains__(group_event_name):
                group_event = EventUtils.create_event(parent_id=rule_event_id,
                                                      name=group_event_name)
                logger.info(f"Create group Event '{group_event_name}' for rule Event '{rule_event_id}'")
                self.__group_event_by_rule_id[rule_event_id.id][group_event_name] = group_event
                self.send_parent_event(group_event)

            group_event = self.__group_event_by_rule_id[rule_event_id.id][group_event_name]
            event.id.CopyFrom(EventUtils.new_event_id())
            event.parent_id.CopyFrom(group_event.id)
            self.__events_batch_collector.put_event(event)
        except Exception:
            logger.exception(f'Error while sending event')

    def send_parent_event(self, event: Event):
        event_batch = EventBatch()
        event_batch.events.append(event)
        self.event_router.send(event_batch)

    def store_no_match_within_timeout(self, rule_event_id: EventID, recon_message: ReconMessage,
                                      actual_timestamp: int, timeout: int):
        name = f'{recon_message.get_all_info()}'
        message_timestamp = MessageUtils.get_timestamp_ns(recon_message.proto_message)

        units = ['sec', 'ms', 'mcs', 'ns']
        factor_units = [1_000_000_000, 1_000_000, 1_000, 1]
        units_idx = 0
        while actual_timestamp % factor_units[units_idx] != 0 \
                or message_timestamp % factor_units[units_idx] != 0 \
                or timeout % factor_units[units_idx] != 0:
            units_idx += 1

        unit = units[units_idx]
        actual_timestamp = int(actual_timestamp / factor_units[units_idx])
        message_timestamp = int(message_timestamp / factor_units[units_idx])
        timeout = int(timeout / factor_units[units_idx])

        event_message = f"Timestamp of the last received message: '{actual_timestamp:,}' {unit}\n" \
                        f"Timestamp this message: '{message_timestamp:,}' {unit}\n" \
                        f"Timeout: '{timeout:,}' {unit}"
        body = EventUtils.create_event_body(MessageComponent(event_message))
        attached_message_ids = [recon_message.proto_message.metadata.id]
        event = EventUtils.create_event(name=name,
                                        body=body,
                                        attached_message_ids=attached_message_ids)
        logger.info(f"Create '{self.NO_MATCH_WITHIN_TIMEOUT}' Event for rule Event '{rule_event_id}'")
        self.send_event(event, rule_event_id, self.NO_MATCH_WITHIN_TIMEOUT)

    def store_no_match(self, rule_event_id: EventID, message: ReconMessage, event_message: str):
        name = f"Remove '{message.proto_message.metadata.message_type}' {message.get_all_info()}"
        event_message += f"\n Message {'not' if not message.is_matched else ''} matched"
        body = EventUtils.create_event_body(MessageComponent(event_message))
        attached_message_ids = [message.proto_message.metadata.id]
        event = EventUtils.create_event(name=name,
                                        body=body,
                                        status=EventStatus.SUCCESS if message.is_matched else EventStatus.FAILED,
                                        attached_message_ids=attached_message_ids)
        logger.info(f"Create '{self.NO_MATCH}' Event for rule Event '{rule_event_id}'")
        self.send_event(event, rule_event_id, self.NO_MATCH)

    def store_error(self, rule_event_id: EventID, event_name: str, error_message: str,
                    messages: [ReconMessage] = None):
        body = EventUtils.create_event_body(MessageComponent(error_message))
        attached_message_ids = [message.proto_message.metadata.id for message in messages]
        event = EventUtils.create_event(name=event_name,
                                        status=EventStatus.FAILED,
                                        attached_message_ids=attached_message_ids,
                                        body=body)
        logger.info(f"Create '{self.ERRORS}' Event for rule Event '{rule_event_id}'")
        self.send_event(event, rule_event_id, self.ERRORS)

    def store_matched_out_of_timeout(self, rule_event_id: EventID, check_event: Event,
                                     min_time, max_time):
        self.send_event(check_event, rule_event_id, self.MATCHED_OUT_OF_TIMEOUT)

    def store_matched(self, rule_event_id: EventID, check_event: Event):
        if check_event.status == EventStatus.SUCCESS:
            self.send_event(check_event, rule_event_id, self.MATCHED_PASSED)
        else:
            self.send_event(check_event, rule_event_id, self.MATCHED_FAILED)

    def stop(self):
        self.__events_batch_collector.stop()


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


class Cache(AbstractService):

    def __init__(self, message_group_types: dict, cache_size: int, event_store: EventStore,
                 rule_event: Event) -> None:
        self.capacity = cache_size
        self.event_store = event_store
        self.rule_event = rule_event

        self.message_groups = [Cache.MessageGroup(id=message_group_id,
                                                  capacity=cache_size,
                                                  type=message_group_types[message_group_id],
                                                  event_store=event_store,
                                                  rule_event=self.rule_event)
                               for message_group_id in message_group_types.keys()]
        multi_cnt = 0
        for group in self.message_groups:
            if group.type == MessageGroupType.multi:
                multi_cnt += 1

        for group in self.message_groups:
            if multi_cnt == 1:
                if group.type == MessageGroupType.single:
                    group.is_cleanable = False
            elif multi_cnt > 1:
                group.is_cleanable = False

    def index_of_group(self, group_id: str) -> int:
        for i in range(len(self.message_groups)):
            if self.message_groups[i].id == group_id:
                return i
        return -1

    def stop(self):
        for group in self.message_groups:
            group.clear()

    class MessageGroup:

        def __init__(self, id: str, capacity: int, type: MessageGroupType, event_store: EventStore,
                     rule_event: Event) -> None:
            self.id = id
            self.capacity = capacity
            self.size = 0
            self.type = type
            self.event_store = event_store
            self.rule_event = rule_event

            self.is_cleanable = True
            self.data = dict()
            self.hash_by_sorted_timestamp = sortedcollections.SortedDict()

        def put(self, message: ReconMessage):
            if self.size < self.capacity:
                if self.contains(message.hash) and self.type == MessageGroupType.single:
                    cause = f"The message was deleted because a new one came with the same hash '{message.hash}' " \
                            f"in message group '{self.id}'"
                    self.remove(message.hash, cause)

                if not self.data.__contains__(message.hash):
                    self.data[message.hash] = list()
                self.data[message.hash].append(message)

                timestamp = MessageUtils.get_timestamp_ns(message.proto_message)
                if not self.hash_by_sorted_timestamp.__contains__(timestamp):
                    self.hash_by_sorted_timestamp[timestamp] = list()
                self.hash_by_sorted_timestamp[timestamp].append(message.hash)

                self.size += 1
            else:
                timestamp_for_remove = self.hash_by_sorted_timestamp.keys().__iter__().__next__()
                hash_for_remove = self.hash_by_sorted_timestamp[timestamp_for_remove].__iter__().__next__()
                cause = f"The message was deleted because there was no free space in the message group '{self.id}'"
                self.remove(hash_for_remove, cause, remove_all=False)
                self.put(message)

        def get(self, hash_of_message: int) -> list:
            return self.data[hash_of_message]

        def contains(self, hash_of_message: int) -> bool:
            return self.data.__contains__(hash_of_message)

        def check_no_match_within_timeout(self, actual_timestamp: int, timeout: int):
            lower_bound_timestamp = actual_timestamp - timeout
            old_timestamps = []
            if lower_bound_timestamp is None:
                old_timestamps.append(self.hash_by_sorted_timestamp.keys().__iter__().__next__())
            else:
                for timestamp in self.hash_by_sorted_timestamp.keys():
                    if timestamp < lower_bound_timestamp:
                        old_timestamps.append(timestamp)
            for old_timestamp in old_timestamps:
                old_hash = self.hash_by_sorted_timestamp[old_timestamp].__iter__().__next__()
                for recon_message in self.data[old_hash]:
                    if not recon_message.is_matched and not recon_message.is_check_no_match_within_timeout:
                        recon_message.is_check_no_match_within_timeout = True
                        self.event_store.store_no_match_within_timeout(self.rule_event.id, recon_message,
                                                                       actual_timestamp, timeout)

        def remove(self, hash_of_message: int, cause="", remove_all=True):
            message_for_remove = None
            if remove_all:
                for message in self.data[hash_of_message]:
                    message_for_remove: ReconMessage = message
                    timestamp_for_remove = MessageUtils.get_timestamp_ns(message_for_remove.proto_message)
                    self.hash_by_sorted_timestamp[timestamp_for_remove].remove(hash_of_message)
                    if len(self.hash_by_sorted_timestamp[timestamp_for_remove]) == 0:
                        self.hash_by_sorted_timestamp.pop(timestamp_for_remove)
                    self.size -= 1
                self.data.pop(hash_of_message)
            else:
                message_for_remove: ReconMessage = self.data[hash_of_message].__iter__().__next__()
                timestamp_for_remove = MessageUtils.get_timestamp_ns(message_for_remove.proto_message)

                self.data[hash_of_message].remove(message_for_remove)
                if len(self.data[hash_of_message]) == 0:
                    self.data.pop(hash_of_message)

                self.hash_by_sorted_timestamp[timestamp_for_remove].remove(hash_of_message)
                if len(self.hash_by_sorted_timestamp[timestamp_for_remove]) == 0:
                    self.hash_by_sorted_timestamp.pop(timestamp_for_remove)
                self.size -= 1

            if len(cause) != 0:
                self.event_store.store_no_match(rule_event_id=self.rule_event.id,
                                                message=message_for_remove,
                                                event_message=cause)

        def clear(self):
            cause = "The message was deleted because the Recon stopped"
            while len(self.data) != 0:
                hash_for_remove = self.data.keys().__iter__().__next__()
                while self.data.__contains__(hash_for_remove) and len(self.data[hash_for_remove]) != 0:
                    self.remove(hash_for_remove, cause, remove_all=False)
