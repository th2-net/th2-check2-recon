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

import collections
import datetime
import enum
import itertools
import logging
from abc import ABC, abstractmethod
from threading import Lock, Timer
from typing import Optional, Dict, List

import sortedcollections
from th2_common.schema.event.event_batch_router import EventBatchRouter
from th2_common_utils import dict_to_message
from th2_grpc_common.common_pb2 import EventStatus, Event, EventBatch, EventID, Message, MessageID, ConnectionID, \
    Direction
from th2_grpc_util.util_pb2 import CompareMessageVsMessageRequest, ComparisonSettings, \
    CompareMessageVsMessageTask, CompareMessageVsMessageResult

from th2_check2_recon.common import EventUtils, MessageComponent, VerificationComponent
from th2_check2_recon.reconcommon import ReconMessage, MessageGroupDescription

logger = logging.getLogger(__name__)


class AbstractService(ABC):

    @abstractmethod
    def stop(self):
        pass


class EventSender:

    def __init__(self,
                 event_router: EventBatchRouter,
                 event_batch_max_size: int,
                 event_batch_sending_interval: int):
        self.event_router = event_router
        self.max_batch_size = event_batch_max_size
        self.sending_interval = event_batch_sending_interval

        self._event_batches_to_send = {}  # {event_id: (event_batch, batch_timer)}
        self._lock = Lock()

    def send_event(self, event: Event):
        try:
            self.event_router.send(EventBatch(events=[event]))
        except Exception as e:
            logger.exception(f'Could not send event: {e}')

    def send_event_batch(self, event_batch: EventBatch):
        try:
            self.event_router.send(event_batch)
        except Exception as e:
            logger.exception(f'Could not send event batch: {e}')

    def put_event_in_batch(self, event: Event):
        with self._lock:
            if event.parent_id.id in self._event_batches_to_send:
                event_batch, batch_timer = self._event_batches_to_send[event.parent_id.id]
            else:
                event_batch = EventBatch(parent_event_id=event.parent_id)
                batch_timer = self._create_timer(event_batch)
                self._event_batches_to_send[event.parent_id.id] = (event_batch, batch_timer)

            event_batch.events.append(event)
            if len(event_batch.events) == self.max_batch_size:
                del self._event_batches_to_send[event.parent_id.id]
                batch_timer.cancel()
                self.send_event_batch(event_batch)

    def _create_timer(self, batch: EventBatch):
        timer = Timer(self.sending_interval, self._timer_handle, [batch])
        timer.start()
        return timer

    def _timer_handle(self, batch: EventBatch):
        with self._lock:
            if batch.parent_event_id.id in self._event_batches_to_send:
                del self._event_batches_to_send[batch.parent_event_id.id]
            else:
                return
        self.send_event_batch(batch)

    def stop(self):
        with self._lock:
            for _, batch_timer in self._event_batches_to_send.values():
                batch_timer.cancel()
        with self._lock:
            for event_batch, _ in self._event_batches_to_send.values():
                self.send_event_batch(event_batch)
            self._event_batches_to_send.clear()


class EventStore(AbstractService):
    class EventPack(str, enum.Enum):
        FAILED_MATCHES = 'Failed matches'
        PASSED_MATCHES = 'Passed matches'
        OUT_OF_TIMEOUT_MATCHES = 'Out of timeout matches'
        NO_MATCH_WITHIN_TIMEOUT = 'No match within timeout'
        IGNORED_NO_MATCH_WITHIN_TIMEOUT = 'Ignored no match within timeout'
        REMOVED_MESSAGES = 'Removed messages'
        ERRORS = 'Errors'

    class EventCollection:

        def __init__(self, parent_event_id: EventID):
            self.parent_event_id = parent_event_id
            self.event_pack_ids: Dict[str, Optional[EventID]] = {
                EventStore.EventPack.FAILED_MATCHES: None,
                EventStore.EventPack.IGNORED_NO_MATCH_WITHIN_TIMEOUT: None,
                EventStore.EventPack.PASSED_MATCHES: None,
                EventStore.EventPack.OUT_OF_TIMEOUT_MATCHES: None,
                EventStore.EventPack.NO_MATCH_WITHIN_TIMEOUT: None,
                EventStore.EventPack.REMOVED_MESSAGES: None,
                EventStore.EventPack.ERRORS: None
            }

        def get_event_pack_id(self, event_pack_name: 'EventStore.EventPack'):
            return self.event_pack_ids[event_pack_name]

        def create_pack_event(self, event_pack_name: 'EventStore.EventPack') -> Event:
            event = EventUtils.create_event(name=event_pack_name,
                                            parent_id=self.parent_event_id,
                                            type=EventUtils.EventType.STATUS)
            self.event_pack_ids[event_pack_name] = event.id
            return event

    def __init__(self,
                 recon_name: str,
                 event_router: EventBatchRouter,
                 event_batch_max_size: int,
                 event_batch_sending_interval: int) -> None:
        self.event_sender = EventSender(event_router=event_router,
                                        event_batch_max_size=event_batch_max_size,
                                        event_batch_sending_interval=event_batch_sending_interval)
        self.recon_event_id = self.put_recon_event(recon_name=recon_name)
        self.shared_groups_root_event_id = self.put_shared_groups_root_event()
        self.shared_group_events = {}
        self.event_tree: Dict[str, EventStore.EventCollection] = {}

    def put_recon_event(self, recon_name) -> EventID:
        recon_event = EventUtils.create_event(name=f'Recon: {recon_name}',
                                              type=EventUtils.EventType.ROOT)
        self.event_sender.put_event_in_batch(event=recon_event)
        return recon_event.id

    def put_shared_groups_root_event(self) -> EventID:
        shared_root_event_id: Event = EventUtils.create_event(parent_id=self.recon_event_id,
                                                              name='Shared messages',
                                                              type=EventUtils.EventType.EVENT)
        self.event_sender.put_event_in_batch(event=shared_root_event_id)
        return shared_root_event_id.id

    def put_shared_group_event(self, shared_group_name: str) -> EventID:
        shared_group_event: Event = EventUtils.create_event(parent_id=self.shared_groups_root_event_id,
                                                            name=f'Group: {shared_group_name}',
                                                            type=EventUtils.EventType.EVENT)
        self.event_tree[shared_group_event.id.id] = EventStore.EventCollection(shared_group_event.id)
        self.event_sender.put_event_in_batch(event=shared_group_event)
        return shared_group_event.id

    def put_rule_event(self, rule_name, rule_description) -> EventID:
        rule_event: Event = EventUtils.create_event(
            name=rule_name,
            parent_id=self.recon_event_id,
            body=EventUtils.create_event_body(MessageComponent(message=rule_description)),
            type=EventUtils.EventType.RULE
        )
        self.event_tree[rule_event.id.id] = EventStore.EventCollection(rule_event.id)
        self.event_sender.put_event_in_batch(event=rule_event)
        return rule_event.id

    def put_event_in_group_pack(self,
                                event: Event,
                                rule_event_id: EventID,
                                event_pack_name: EventPack,
                                shared_group_name: Optional[str] = None) -> None:
        if shared_group_name is not None:
            if shared_group_name in self.shared_group_events:
                event_id = self.shared_group_events[shared_group_name]
            else:
                event_id = self.put_shared_group_event(shared_group_name)
                self.shared_group_events[shared_group_name] = event_id
        else:
            event_id = rule_event_id

        event_pack_id = self.event_tree[event_id.id].get_event_pack_id(event_pack_name)
        if event_pack_id is None:
            pack_event = self.event_tree[event_id.id].create_pack_event(event_pack_name)
            event_pack_id = pack_event.id
            self.event_sender.put_event_in_batch(event=pack_event)
        event.parent_id.CopyFrom(event_pack_id)
        self.event_sender.put_event_in_batch(event=event)

    def create_and_send_single_message_event(self,
                                             recon_message: ReconMessage,
                                             event_name: str,
                                             event_message: str,
                                             event_status: EventStatus,
                                             rule_event_id: EventID,
                                             event_pack_name: EventPack):
        event = EventUtils.create_event(name=event_name,
                                        body=EventUtils.create_event_body(MessageComponent(event_message)),
                                        attached_message_ids=self._get_attached_message_ids(recon_message),
                                        status=event_status,
                                        type=EventUtils.EventType.EVENT)
        self.put_event_in_group_pack(event=event,
                                     rule_event_id=rule_event_id,
                                     event_pack_name=event_pack_name,
                                     shared_group_name=recon_message.group_name if recon_message._shared else None)

    def store_no_match_within_timeout_event(self,
                                            rule_event_id: EventID,
                                            recon_message: ReconMessage,
                                            actual_timestamp: datetime.datetime,
                                            timeout: int,
                                            event_pack_name: 'EventStore.EventPack'):
        event_message = f"Timestamp of the last received message: {actual_timestamp}\n" \
                        f"Timestamp this message: {recon_message.timestamp}\n" \
                        f"Timeout: {timeout / 10 ** 9}"
        event_status = EventStatus.SUCCESS if event_pack_name == EventStore.EventPack.IGNORED_NO_MATCH_WITHIN_TIMEOUT \
            else EventStatus.FAILED
        self.create_and_send_single_message_event(recon_message=recon_message,
                                                  event_name=f'{recon_message.all_info}',
                                                  event_message=event_message,
                                                  event_status=event_status,
                                                  rule_event_id=rule_event_id,
                                                  event_pack_name=event_pack_name)

    def store_removed_message_event(self,
                                    rule_event_id: EventID,
                                    recon_message: ReconMessage,
                                    event_message: str):
        event_message += f"\n Message {'not' if not recon_message._is_matched else ''} matched"
        event_status = EventStatus.SUCCESS if recon_message._is_matched else EventStatus.FAILED
        self.create_and_send_single_message_event(recon_message=recon_message,
                                                  event_name=f"Remove {recon_message.all_info}",
                                                  event_message=event_message,
                                                  event_status=event_status,
                                                  rule_event_id=rule_event_id,
                                                  event_pack_name=EventStore.EventPack.REMOVED_MESSAGES)

    def store_error(self, rule_event_id: EventID, event_name: str, error_message: str,
                    messages: List[ReconMessage] = None):
        body = EventUtils.create_event_body(MessageComponent(error_message))
        attached_message_ids = self._get_attached_message_ids(*messages)
        event = EventUtils.create_event(name=event_name,
                                        status=EventStatus.FAILED,
                                        attached_message_ids=attached_message_ids,
                                        body=body,
                                        type=EventUtils.EventType.EVENT)
        self.put_event_in_group_pack(event=event,
                                     rule_event_id=rule_event_id,
                                     event_pack_name=EventStore.EventPack.ERRORS)

    def store_matched_out_of_timeout(self, rule_event_id: EventID, check_event: Event):
        self.put_event_in_group_pack(event=check_event,
                                     rule_event_id=rule_event_id,
                                     event_pack_name=EventStore.EventPack.OUT_OF_TIMEOUT_MATCHES)

    def store_matched(self, rule_event_id: EventID, check_event: Event):
        if check_event.status == EventStatus.SUCCESS:
            self.put_event_in_group_pack(event=check_event,
                                         rule_event_id=rule_event_id,
                                         event_pack_name=EventStore.EventPack.PASSED_MATCHES)
        else:
            self.put_event_in_group_pack(event=check_event,
                                         rule_event_id=rule_event_id,
                                         event_pack_name=EventStore.EventPack.FAILED_MATCHES)

    def stop(self):
        self.event_sender.stop()

    @staticmethod
    def _get_attached_message_ids(*recon_msgs: ReconMessage):
        try:
            return [
                MessageID(
                    connection_id=ConnectionID(session_alias=message.proto_message['metadata']['session_alias'],
                                               session_group=message.proto_message['metadata']['session_group']),
                    direction=getattr(Direction, message.proto_message['metadata']['direction']),
                    sequence=message.proto_message['metadata']['sequence'],
                    subsequence=message.proto_message['metadata']['subsequence']
                )
                for message in recon_msgs
            ]
        except KeyError:
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
        except Exception as e:
            logger.exception(f'Error while comparing: {e}.\nCompareMessageVsMessageRequest: {request}')

    def compare_messages(self, messages: [ReconMessage],
                         ignore_fields: [str] = None) -> Optional[VerificationComponent]:
        if len(messages) != 2:
            logger.exception(f'The number of messages to compare must be 2.')
            return
        settings = ComparisonSettings()
        if ignore_fields is not None:
            settings.ignore_fields.extend(ignore_fields)

        compare_result = self.compare(dict_to_message(messages[0].proto_message['fields']),
                                      dict_to_message(messages[1].proto_message['fields']), settings)
        return VerificationComponent(compare_result.comparison_result)

    def stop(self):
        pass


class Cache(AbstractService):
    __slots__ = 'rule', 'capacity', 'event_store', 'rule_event', '_message_groups'

    def __init__(self, rule, cache_size) -> None:
        self.rule = rule
        self.capacity = cache_size
        self.event_store = self.rule.event_store
        self.rule_event_id = self.rule.rule_event_id

        self._message_groups = {}

        for group_name, group_description in self.rule.description_of_groups().items():
            if group_description.shared:
                if group_name not in self.rule.recon.shared_message_groups:
                    self.rule.recon.shared_message_groups[group_name] = Cache.MessageGroup(
                        group_name=group_name,
                        group_description=group_description,
                        capacity=cache_size,
                        event_store=self.event_store,
                        parent_event_id=self.rule_event_id
                    )
                self._message_groups[group_name] = self.rule.recon.shared_message_groups[group_name]
            else:
                self._message_groups[group_name] = Cache.MessageGroup(group_name=group_name,
                                                                      group_description=group_description,
                                                                      capacity=cache_size,
                                                                      event_store=self.event_store,
                                                                      parent_event_id=self.rule_event_id)

        has_multi = any(group.group_description.multi for group in self._message_groups.values())
        if has_multi:
            for group in self.message_groups.values():
                if group.group_description.single:
                    group.is_cleanable = False

    @property
    def message_groups(self):
        return self._message_groups

    def stop(self):
        for group in self.message_groups.values():
            group.clear()

    class MessageGroup:

        __slots__ = ('name', 'capacity', 'size', 'group_description', 'event_store', 'parent_event_id',
                     'is_cleanable', 'data', 'hash_by_sorted_timestamp')

        def __init__(self,
                     group_name: str,
                     capacity: int,
                     group_description: MessageGroupDescription,
                     event_store: EventStore,
                     parent_event_id: EventID) -> None:
            self.name = group_name
            self.capacity = capacity
            self.size = 0
            self.group_description = group_description
            self.event_store = event_store
            self.parent_event_id: EventID = parent_event_id

            self.is_cleanable = True
            self.data: Dict[int, List[ReconMessage]] = collections.defaultdict(
                list)  # {ReconMessage.hash: [ReconMessage]}
            self.hash_by_sorted_timestamp: Dict[
                int, List[int]] = sortedcollections.SortedDict()  # {timestamp: [ReconMessage.hash]}

        def put(self, message: ReconMessage):
            if self.size < self.capacity:
                if message.hash in self.data and self.group_description.single:
                    if self.group_description.shared:
                        return
                    cause = f"The message was deleted because a new one came with the same hash '{message.hash}' " \
                            f"in message group '{self.name}'"
                    self.remove(message.hash, cause)

                self.data[message.hash].append(message)
                self.hash_by_sorted_timestamp.setdefault(message.timestamp_ns, []).append(message.hash)
                self.size += 1
            else:
                timestamp_for_remove = next(iter(self.hash_by_sorted_timestamp.keys()))
                hash_for_remove = self.hash_by_sorted_timestamp[timestamp_for_remove][0]
                cause = f"The message was deleted because there was no free space in the message group '{self.name}'"
                self.remove(hash_for_remove, cause, remove_all=False)
                self.put(message)

        def check_no_match_within_timeout(self,
                                          actual_timestamp: datetime.datetime,
                                          actual_timestamp_ns: int,
                                          timeout: int):
            lower_bound_timestamp = actual_timestamp_ns - timeout
            for timestamp, hashes in self.hash_by_sorted_timestamp.items():
                if timestamp < lower_bound_timestamp:
                    old_hash = hashes[0]
                    for recon_message in self.data[old_hash]:
                        if not recon_message._is_matched and not recon_message._was_checked_no_match_within_timeout:
                            recon_message._was_checked_no_match_within_timeout = True
                            if self.group_description.ignore_no_match:
                                self.event_store.store_no_match_within_timeout_event(
                                    rule_event_id=self.parent_event_id,
                                    recon_message=recon_message,
                                    actual_timestamp=actual_timestamp,
                                    timeout=timeout,
                                    event_pack_name=EventStore.EventPack.IGNORED_NO_MATCH_WITHIN_TIMEOUT
                                )
                                self.remove(hash_of_message=old_hash)
                            else:
                                self.event_store.store_no_match_within_timeout_event(
                                    rule_event_id=self.parent_event_id,
                                    recon_message=recon_message,
                                    actual_timestamp=actual_timestamp,
                                    timeout=timeout,
                                    event_pack_name=EventStore.EventPack.NO_MATCH_WITHIN_TIMEOUT
                                )
                else:
                    break

        def remove(self, hash_of_message: int, cause=None, remove_all=True):
            message_for_remove = None
            if remove_all:
                for message_for_remove in self.data[hash_of_message]:
                    timestamp_for_remove = message_for_remove.timestamp_ns
                    self.hash_by_sorted_timestamp[timestamp_for_remove].remove(hash_of_message)
                    if len(self.hash_by_sorted_timestamp[timestamp_for_remove]) == 0:
                        del self.hash_by_sorted_timestamp[timestamp_for_remove]
                    self.size -= 1
                del self.data[hash_of_message]
            else:
                message_for_remove = self.data[hash_of_message][0]
                timestamp_for_remove = message_for_remove.timestamp_ns

                self.data[hash_of_message].remove(message_for_remove)
                if len(self.data[hash_of_message]) == 0:
                    del self.data[hash_of_message]

                self.hash_by_sorted_timestamp[timestamp_for_remove].remove(hash_of_message)
                if len(self.hash_by_sorted_timestamp[timestamp_for_remove]) == 0:
                    del self.hash_by_sorted_timestamp[timestamp_for_remove]
                self.size -= 1

            if cause is not None:
                self.event_store.store_removed_message_event(rule_event_id=self.parent_event_id,
                                                             recon_message=message_for_remove,
                                                             event_message=cause)

        def clear(self):
            cause = "The message was deleted because the Recon stopped"
            while len(self.data) != 0:
                hash_for_remove = next(iter(self.data.keys()))
                while hash_for_remove in self.data and len(self.data[hash_for_remove]) != 0:
                    self.remove(hash_for_remove, cause, remove_all=False)

        def wipe(self):
            self.data.clear()
            self.hash_by_sorted_timestamp.clear()
            self.size = 0

        def clear_out_of_timeout(self, clean_timestamp):
            hashes_to_removal = []
            for timestamp, hashes in self.hash_by_sorted_timestamp.items():
                if timestamp < clean_timestamp:
                    hashes_to_removal.append(hashes)
                else:
                    break
            for hash_to_remove in itertools.chain.from_iterable(hashes_to_removal):
                self.remove(hash_to_remove, remove_all=False)

        def __iter__(self):
            """Generator that yields all messages in the group"""
            for recon_mgs_lst in self.data.values():
                for msg in recon_mgs_lst:
                    yield msg

        def __contains__(self, hash_of_message: int):
            return hash_of_message in self.data
