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

import copy
import logging
import traceback
from abc import abstractmethod
from typing import List, Optional, Dict

from google.protobuf import text_format
from th2_grpc_common.common_pb2 import Event

from th2_check2_recon.common import EventUtils, MessageComponent
from th2_check2_recon.handler import MessageHandler, AbstractHandler
from th2_check2_recon.recon import Recon
from th2_check2_recon.reconcommon import ReconMessage, get_message_timestamp, MessageGroupType
from th2_check2_recon.services import MessageComparator, MessageGroup


logger = logging.getLogger(__name__)


class Rule:

    def __init__(self, recon: Recon, cache_size: int, match_timeout: int, configuration) -> None:
        self.configure(configuration)
        self.name = self.get_name()
        logger.info("Rule '%s' initializing...", self.name)

        self.__recon = recon
        self.__event_store = recon.event_store
        self.message_comparator: Optional[MessageComparator] = recon.message_comparator
        self.match_timeout = match_timeout

        self.rule_event: Event = self.__create_rule_event()
        self.__message_groups: Dict[str, MessageGroup] = self.__create_message_groups(cache_size)

        logger.info("Rule '%s' initialized", self.name)

    def __create_message_groups(self, cache_size: int) -> Dict[str, MessageGroup]:
        message_groups: Dict[str, MessageGroup] = {}
        for group_id, group_type in self.description_of_groups().items():
            message_groups[group_id] = MessageGroup(id=group_id,
                                                    capacity=cache_size,
                                                    type=group_type,
                                                    event_store=self.__event_store,
                                                    parent_event=self.rule_event)

        multi_groups_count = sum(MessageGroupType.multi in group.type for group in message_groups.values())

        for group in message_groups.values():
            if multi_groups_count == 1 and MessageGroupType.single in group.type or multi_groups_count > 1:
                group.is_cleanable = False
        return message_groups

    def __create_rule_event(self) -> Event:
        rule_event = EventUtils.create_event(name=self.name,
                                             parent_id=self.__event_store.root_event.id,
                                             body=EventUtils.create_event_body(
                                                 MessageComponent(message=self.get_description())))
        logger.debug("Created report Event for Rule '%s': %s",
                     self.name, text_format.MessageToString(rule_event, as_one_line=True))
        self.__event_store.send_parent_event(rule_event)
        return rule_event

    def process(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        self.__check_messages_out_of_timeout(message.timestamp)
        self.__specify_group(message, attributes, *args, **kwargs)
        if message.group_id is None:
            logger.debug("RULE '%s': Ignored  %s", self.name, message.get_all_info())
            return
        self.__specify_hash(message, attributes, *args, **kwargs)
        self.__message_groups[message.group_id].put(message)

    def __specify_group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        try:
            self.group(message, attributes, *args, **kwargs)
        except Exception:
            logger.exception(f"RULE '{self.name}': An exception was caught while running 'group'")
            self.__event_store.store_error(rule_event_id=self.rule_event.id,
                                           event_name="An exception was caught while running 'group'",
                                           error_message=traceback.format_exc(),
                                           messages=message)

    def __specify_hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        try:
            self.hash(message, attributes, *args, **kwargs)
        except Exception:
            logger.exception(f"RULE '{self.name}': An exception was caught while running 'hash'")
            self.__event_store.store_error(rule_event_id=self.rule_event.id,
                                           event_name="An exception was caught while running 'hash'",
                                           error_message=traceback.format_exc(),
                                           messages=message)

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
            Returns a dictionary whose key is group_id and whose value is type or a set of type.
            Type can be MessageGroupType.single or MessageGroupType.multi, or MessageGroupType.shared.
        """
        pass

    def _description_of_groups_bridge(self) -> dict:
        result = dict()
        for key, value in self.description_of_groups().items():
            if not isinstance(value, set):
                result[key] = {value}
            else:
                result[key] = value
        return result

    @abstractmethod
    def group(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        pass

    @abstractmethod
    def hash(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        pass

    @abstractmethod
    def check(self, attributes: tuple, *args, **kwargs) -> Optional[Event]:
        pass

    def configure(self, configuration):
        pass

    def get_listener(self) -> AbstractHandler:
        return MessageHandler(self)

    def put_shared_message(self, shared_group_id: str, message: ReconMessage, attributes: tuple):
        new_message = copy.deepcopy(message)
        new_message.group_id = shared_group_id
        self.__recon.put_shared_message(shared_group_id, new_message, attributes)

    def __check_and_store_event(self, matched_messages: List[ReconMessage], attributes: tuple, *args, **kwargs):
        for msg in matched_messages:
            msg.is_matched = True
        try:
            check_event: Event = self.check(attributes, *args, **kwargs)
        except Exception:
            logger.exception(f"RULE '{self.name}': An exception was caught while running 'check'")
            self.__event_store.store_error(rule_event_id=self.rule_event.id,
                                           event_name="An exception was caught while running 'check'",
                                           error_message=traceback.format_exc(),
                                           messages=matched_messages)
            return

        if check_event is None:
            return

        max_timestamp_msg = max(matched_messages, key=get_message_timestamp)
        min_timestamp_msg = min(matched_messages, key=get_message_timestamp)

        if max_timestamp_msg.timestamp - min_timestamp_msg.timestamp > self.match_timeout:
            self.__event_store.store_matched_out_of_timeout(rule_event_id=self.rule_event.id,
                                                            check_event=check_event)
        else:
            self.__event_store.store_matched(rule_event_id=self.rule_event.id,
                                             check_event=check_event)

    def log_groups_size(self):
        return "[" + ', '.join(f"'{group.id}': {group.size} msg" for group in self.__message_groups.values()) + "]"

    def __check_messages_out_of_timeout(self, actual_time: int):
        lower_bound_timestamp = actual_time - self.match_timeout
        for group in self.__message_groups.values():
            group.check_messages_out_of_timeout(lower_bound_timestamp)

    def stop(self):
        for group in self.__message_groups.values():
            group.clear()
