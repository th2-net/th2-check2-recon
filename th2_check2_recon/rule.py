# Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import traceback
from abc import abstractmethod
from typing import List, Optional, Dict

from th2_grpc_common.common_pb2 import Event

from th2_check2_recon.common import EventUtils, MessageComponent
from th2_check2_recon.handler import MessageHandler, AbstractHandler
from th2_check2_recon.recon import Recon
from th2_check2_recon.reconcommon import ReconMessage, _get_msg_timestamp
from th2_check2_recon.services import Cache

logger = logging.getLogger()


class Rule:

    def __init__(self, recon: Recon, cache_size: int, match_timeout: int, configuration) -> None:
        self.name = self.get_name()
        logger.info("Rule '%s' initializing...", self.name)

        self.recon = recon
        self.event_store = recon.event_store
        self.message_comparator = recon.message_comparator
        self.match_timeout = match_timeout

        self.rule_event: Event = \
            EventUtils.create_event(name=self.name,
                                    parent_id=recon.event_store.root_event.id,
                                    body=EventUtils.create_event_body(MessageComponent(message=self.get_description())))
        logger.info("Created report Event for Rule '%s': %s", self.name, self.rule_event)
        self.event_store.send_parent_event(self.rule_event)

        self.__cache = Cache(self.description_of_groups(), cache_size, self.event_store, self.rule_event)

        self.compared_groups: Dict[str, tuple] = {}  # {ReconMessage.group_id: (Cache.MessageGroup, ..)}
        for group_id in self.description_of_groups():
            self.compared_groups[group_id] = tuple(
                mg for mg in self.__cache.message_groups if mg.id != group_id)

        logger.info("Rule '%s' initialized", self.name)

        # Do not raise this line up because all the variables above must be available in the self.configure function
        self.configure(configuration)

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
    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        pass

    @abstractmethod
    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        pass

    @abstractmethod
    def check(self, messages: [ReconMessage], attributes: tuple, *args, **kwargs) -> Optional[Event]:
        pass

    def configure(self, configuration):
        pass

    def get_listener(self) -> AbstractHandler:
        return MessageHandler(self)

    def process(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        self.check_no_match_within_timeout(message.timestamp)

        self.group(message, attributes, *args, **kwargs)
        if message.group_id is None:
            logger.info("RULE '%s': Ignored  %s", self.name, message.get_all_info())
            return

        self.hash(message, attributes, *args, **kwargs)

        index_of_main_group = self.__cache.index_of_group(message.group_id)
        if index_of_main_group == -1:
            raise Exception(F"'group' method set incorrect groups.\n"
                            F" - message: {message.get_all_info()}\n"
                            F" - available groups: {self.description_of_groups()}\n"
                            F" - message.group_id: {message.group_id}")
        else:
            logger.info("RULE '%s': Received %s", self.name, message.get_all_info())

        group_indices = []
        group_sizes = []

        # Check if each group has messages with compared hash else put the message to cache
        for compared_group in self.compared_groups[message.group_id]:
            if message.hash not in compared_group:
                self.__cache.message_groups[index_of_main_group].put(message)
                return
            group_indices.append(0)
            group_sizes.append(len(compared_group.get(message.hash)))

        group_indices[-1] = -1
        while self.__increment_indices(group_sizes, group_indices):
            matched_messages = [message]
            for i in range(len(group_indices)):
                index_of_compared_group = i if i < index_of_main_group else i + 1
                matched_messages.append(
                    self.__cache.message_groups[index_of_compared_group].data[message.hash][group_indices[i]])
            self.__check_and_store_event(matched_messages, attributes, *args, **kwargs)

        for group in self.compared_groups[message.group_id]:
            if group.is_cleanable:
                group.remove(message.hash)

    def __check_and_store_event(self, matched_messages: List[ReconMessage], attributes: tuple, *args, **kwargs):
        for msg in matched_messages:
            msg.is_matched = True
        try:
            check_event: Event = self.check(matched_messages, attributes, *args, **kwargs)
        except Exception:
            logger.exception(f"RULE '{self.name}': An exception was caught while running 'check'")
            self.event_store.store_error(rule_event_id=self.rule_event.id,
                                         event_name="An exception was caught while running 'check'",
                                         error_message=traceback.format_exc(),
                                         messages=matched_messages)
            return

        if check_event is None:
            return

        max_timestamp_msg = max(matched_messages, key=_get_msg_timestamp)
        min_timestamp_msg = min(matched_messages, key=_get_msg_timestamp)

        if max_timestamp_msg.timestamp - min_timestamp_msg.timestamp > self.match_timeout:
            self.event_store.store_matched_out_of_timeout(rule_event_id=self.rule_event.id,
                                                          check_event=check_event)
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
