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

import time
import logging
import traceback
from abc import abstractmethod

from th2_common.schema.message.message_listener import MessageListener
from th2_grpc_common.common_pb2 import Event, Message, MessageBatch

from th2_check2_recon.common import EventUtils, MessageComponent, MessageUtils
from th2_check2_recon.reconcommon import ReconMessage
from th2_check2_recon.services import EventStore, MessageComparator, Cache

logger = logging.getLogger()


class MessageHandler(MessageListener):

    def __init__(self, rule) -> None:
        self.__rule = rule

    def handler(self, attributes: tuple, message_batch: MessageBatch):
        try:
            for message in message_batch.messages:
                start_time = time.time_ns()
                self.__rule.process(message, attributes)
                logger.info(f"  Processed '{message.metadata.message_type}'"
                            f" id='{MessageUtils.str_message_id(message)}'"
                            f" in {(time.time_ns() - start_time) / 1_000_000} ms")

            logger.info(f"  Cache size '{self.__rule.get_name()}': {self.__rule.log_groups_size()}.")
        except Exception:
            logger.exception(f'An error occurred while processing the received message. Body: {message_batch}')


class Rule:

    def __init__(self, event_store: EventStore, message_comparator: MessageComparator,
                 cache_size: int, match_timeout: int, configuration) -> None:
        logger.info(f"Rule '{self.get_name()}' initializing...")
        self.event_store = event_store
        self.message_comparator = message_comparator
        self.match_timeout = match_timeout
        self.configure(configuration)

        self.rule_event: Event = \
            EventUtils.create_event(name=self.get_name(),
                                    parent_id=event_store.root_event.id,
                                    body=EventUtils.create_event_body(MessageComponent(message=self.get_description())))
        logger.info(f"Created report Event for Rule '{self.get_name()}': {self.rule_event}")
        self.event_store.send_parent_event(self.rule_event)

        self.__cache = Cache(self.description_of_groups(), cache_size, event_store, self.rule_event)
        logger.info(f"Rule '{self.get_name()}' initialized")

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_description(self) -> str:
        pass

    @abstractmethod
    def get_attributes(self) -> [list]:
        pass

    @abstractmethod
    def description_of_groups(self) -> dict:
        """
            Return dictionary whose key is 'group_id', and value is 'type'.
            Type can be MessageGroupType.single or MessageGroupType.multi .
        """
        pass

    @abstractmethod
    def group(self, message: ReconMessage, attributes: tuple):
        pass

    @abstractmethod
    def hash(self, message: ReconMessage, attributes: tuple):
        pass

    @abstractmethod
    def check(self, messages: [ReconMessage]) -> Event:
        pass

    def configure(self, configuration):
        pass

    def get_listener(self) -> MessageListener:
        return MessageHandler(self)

    def process(self, proto_message: Message, attributes: tuple):
        message = ReconMessage(proto_message=proto_message)
        self.check_no_match_within_timeout(MessageUtils.get_timestamp_ns(proto_message))

        self.group(message, attributes)
        if message.group_id is None:
            logger.info(f"RULE '{self.get_name()}': Ignored  {message.get_all_info()}")
            return

        self.hash(message, attributes)

        index_of_main_group = self.__cache.index_of_group(message.group_id)
        if index_of_main_group == -1:
            logger.info(f"RULE '{self.get_name()}': Ignored because group '{message.group_id}' don't exist. "
                        f"{message.get_all_info()}")
            return
        else:
            logger.info(f"RULE '{self.get_name()}': Received {message.get_all_info()}")

        group_indices = []
        group_sizes = []

        for index_of_compared_group in range(len(self.__cache.message_groups)):
            if index_of_compared_group == index_of_main_group:
                continue
            compared_group: Cache.MessageGroup = self.__cache.message_groups[index_of_compared_group]
            if not compared_group.contains(message.hash):
                break
            group_indices.append(0)
            group_sizes.append(len(compared_group.get(message.hash)))

        if len(group_indices) != len(self.__cache.message_groups) - 1:
            self.__cache.message_groups[index_of_main_group].put(message)
            return

        group_indices[-1] = -1
        while self.__increment_indices(group_sizes, group_indices):
            matched_messages = [message]
            for i in range(len(group_indices)):
                index_of_compared_group = i if i < index_of_main_group else i + 1
                matched_messages.append(
                    self.__cache.message_groups[index_of_compared_group].data[message.hash][group_indices[i]])
            self.__check_and_store_event(matched_messages)

        self.__cache.message_groups[index_of_main_group].put(message)
        for group in self.__cache.message_groups:
            if group.is_cleanable:
                group.remove(message.hash)

    def __check_and_store_event(self, matched_messages: list):
        for msg in matched_messages:
            msg.is_matched = True
        try:
            check_event: Event = self.check(matched_messages)
        except Exception:
            logger.exception(f"RULE '{self.get_name()}': An exception was caught while running 'check'")
            self.event_store.store_error(rule_event_id=self.rule_event.id,
                                         event_name="An exception was caught while running 'check'",
                                         error_message=traceback.format_exc(),
                                         messages=matched_messages)
            return

        if check_event is None:
            return

        max_timestamp_msg = max(matched_messages, key=lambda m: MessageUtils.get_timestamp_ns(m.proto_message))
        min_timestamp_msg = min(matched_messages, key=lambda m: MessageUtils.get_timestamp_ns(m.proto_message))

        max_timestamp = MessageUtils.get_timestamp_ns(max_timestamp_msg.proto_message)
        min_timestamp = MessageUtils.get_timestamp_ns(min_timestamp_msg.proto_message)

        if max_timestamp - min_timestamp > self.match_timeout:
            self.event_store.store_matched_out_of_timeout(rule_event_id=self.rule_event.id,
                                                          check_event=check_event,
                                                          min_time=min_timestamp,
                                                          max_time=max_timestamp)
        else:
            self.event_store.store_matched(rule_event_id=self.rule_event.id,
                                           check_event=check_event)

    def log_groups_size(self):
        res = ""
        for group in self.__cache.message_groups:
            res += f"'{group.id}': {group.size} msg, "
        res = "[" + res.strip(" ,") + "]"
        return res

    def cache_size(self):
        res = 0
        for group in self.__cache.message_groups:
            res += group.size
        return res

    def check_no_match_within_timeout(self, actual_time: int):
        for group in self.__cache.message_groups:
            group.check_no_match_within_timeout(actual_time, self.match_timeout)

    def stop(self):
        self.__cache.stop()

    @staticmethod
    def __increment_indices(sizes: list, indices: list) -> bool:
        indices[-1] += 1
        for i in range(len(sizes) - 1, -1, -1):
            if indices[i] == sizes[i]:
                if i == 0:
                    return False
                indices[i] = 0
                indices[i - 1] += 1
            else:
                break
        return True
