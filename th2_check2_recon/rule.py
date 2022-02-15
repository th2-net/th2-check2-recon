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
import re
import traceback
from abc import abstractmethod
from typing import List, Optional, Dict

from google.protobuf import text_format
from prometheus_client import Histogram
from th2_common.schema.metrics import common_metrics
from th2_grpc_common.common_pb2 import Event

from th2_check2_recon.common import EventUtils, MessageComponent
from th2_check2_recon.handler import MessageHandler, AbstractHandler
from th2_check2_recon.recon import Recon
from th2_check2_recon.reconcommon import ReconMessage, _get_msg_timestamp
from th2_check2_recon.services import Cache, MessageComparator


logger = logging.getLogger(__name__)


class Rule:

    def __init__(self, recon: Recon, cache_size: int, match_timeout: int, configuration) -> None:
        self.configure(configuration)
        self.name = self.get_name()
        logger.info("Rule '%s' initializing...", self.name)

        self.recon = recon
        self.event_store = recon.event_store
        self.message_comparator: Optional[MessageComparator] = recon.message_comparator
        self.match_timeout = match_timeout

        self.rule_event: Event = \
            EventUtils.create_event(name=self.name,
                                    parent_id=recon.event_store.root_event.id,
                                    body=EventUtils.create_event_body(MessageComponent(message=self.get_description())),
                                    type=EventUtils.EventType.RULE)
        logger.debug("Created report Event for Rule '%s': %s", self.name,
                     text_format.MessageToString(self.rule_event, as_one_line=True))
        self.event_store.send_parent_event(self.rule_event)

        self.__cache = Cache(self, cache_size)

        self.compared_groups: Dict[str, tuple] = {}  # {ReconMessage.group_id: (Cache.MessageGroup, ..)}
        for group_id in self.description_of_groups_bridge():
            self.compared_groups[group_id] = tuple(
                mg for mg_id, mg in self.__cache.message_groups.items() if mg_id != group_id and mg_id in self.description_of_groups_bridge())

        self.RULE_PROCESSING_TIME = Histogram(f"th2_recon_{re.sub('[^a-zA-Z0-9_: ]', '', self.name).lower().replace(' ', '_')}_rule_processing_time",
                                              'Time of the message processing with a rule',
                                              buckets=common_metrics.DEFAULT_BUCKETS)

        logger.info("Rule '%s' initialized", self.name)

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

    def description_of_groups_bridge(self) -> dict:
        result = dict()
        for (key, value) in self.description_of_groups().items():
            if type(value) is not set:
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
    def check(self, messages: [ReconMessage], attributes: tuple, *args, **kwargs) -> Optional[Event]:
        pass

    def configure(self, configuration):
        pass

    def get_listener(self) -> AbstractHandler:
        return MessageHandler(self)

    def put_shared_message(self, shared_group_id: str, message: ReconMessage, attributes: tuple):
        new_message = copy.deepcopy(message)
        new_message.group_id = shared_group_id
        self.recon.put_shared_message(shared_group_id, new_message, attributes)

    def process(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        self.check_no_match_within_timeout(message.timestamp)

        self.__group_and_store_event(message, attributes, *args, **kwargs)
        if message.group_id is None:
            logger.debug("RULE '%s': Ignored  %s", self.name, message.get_all_info())
            return

        self.__hash_and_store_event(message, attributes, *args, **kwargs)
        try:
            main_group = self.__cache.message_groups[message.group_id]
        except KeyError:
            raise Exception(F"'group' method set incorrect groups.\n"
                            F" - message: {message.get_all_info()}\n"
                            F" - available groups: {self.description_of_groups_bridge()}\n"
                            F" - message.group_id: {message.group_id}")
        else:
            logger.debug("RULE '%s': Received %s", self.name, message.get_all_info())

        group_indices = []
        group_sizes = []

        # Check if each group has messages with compared hash else put the message to cache
        for compared_group in self.compared_groups[message.group_id]:
            if message.hash not in compared_group:
                main_group.put(message)
                return
            group_indices.append(0)
            group_sizes.append(len(compared_group.get(message.hash)))

        group_indices[-1] = -1
        while self.__increment_indices(group_sizes, group_indices):
            matched_messages = [message]
            for i, group in zip(range(len(group_indices)), self.compared_groups[message.group_id]):
                matched_messages.append(group.data[message.hash][group_indices[i]])
            self.__check_and_store_event(matched_messages, attributes, *args, **kwargs)

        for group in self.compared_groups[message.group_id]:
            if group.is_cleanable:
                group.remove(message.hash)

    def __group_and_store_event(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        try:
            self.group(message, attributes, *args, **kwargs)
        except Exception:
            logger.exception(f"RULE '{self.name}': An exception was caught while running 'group'")
            self.event_store.store_error(rule_event_id=self.rule_event.id,
                                         event_name="An exception was caught while running 'group'",
                                         error_message=traceback.format_exc(),
                                         messages=[message])

    def __hash_and_store_event(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        try:
            self.hash(message, attributes, *args, **kwargs)
        except Exception:
            logger.exception(f"RULE '{self.name}': An exception was caught while running 'hash'")
            self.event_store.store_error(rule_event_id=self.rule_event.id,
                                         event_name="An exception was caught while running 'hash'",
                                         error_message=traceback.format_exc(),
                                         messages=[message])

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
        for group in self.__cache.message_groups.values():
            res += f"'{group.id}': {group.size} msg, "
        res = "[" + res.strip(" ,") + "]"
        return res

    def cache_size(self):
        res = 0
        for group in self.__cache.message_groups.values():
            res += group.size
        return res

    def check_no_match_within_timeout(self, actual_time: int):
        for group in self.__cache.message_groups.values():
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
