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

import datetime
import functools
import logging
import traceback
from abc import abstractmethod
from typing import List, Optional, Dict

from prometheus_client import Histogram
from th2_common.schema.metrics import common_metrics
from th2_grpc_common.common_pb2 import Event, EventID

from th2_check2_recon.handler import MessageHandler, AbstractHandler
from th2_check2_recon.recon import Recon
from th2_check2_recon.reconcommon import ReconMessage, _get_msg_timestamp, MessageGroupDescription
from th2_check2_recon.services import Cache, MessageComparator, EventStore

logger = logging.getLogger(__name__)


class Rule:

    def __init__(self,
                 recon: Recon,
                 cache_size: int,
                 match_timeout: int,
                 match_all: bool,
                 autoremove_timeout: Optional[int],
                 configuration,
                 metric_number: int) -> None:
        self.recon = recon
        self.configure(configuration)
        self.name = self.get_name()
        logger.info("Rule '%s' is initializing...", self.name)

        self.event_store: EventStore = recon.event_store
        self.message_comparator: Optional[MessageComparator] = recon.message_comparator
        self.match_timeout = match_timeout
        self.autoremove_timeout = autoremove_timeout
        self.match_all = match_all

        self.reprocess_queue = []

        self.rule_event_id: EventID = self.event_store.put_rule_event(
            rule_name=self.name,
            rule_description=self.get_description()
        )

        self.__cache = Cache(self, cache_size)

        self.RULE_PROCESSING_TIME = Histogram(
            f'th2_recon_rule_{metric_number}_processing_time',
            'Time of the message processing with a rule',
            buckets=common_metrics.DEFAULT_BUCKETS)

        logger.info("Rule '%s' was initialized", self.name)

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_description(self) -> str:
        pass

    @abstractmethod
    def get_attributes(self) -> List[List[str]]:
        pass

    @abstractmethod
    def description_of_groups(self) -> Dict[str, MessageGroupDescription]:
        """
            Returns a dictionary whose key is group_name and value is MessageGroupDescription.
            MessageGroupDescription have boolean fields single, shared, shared and ignore_no_match.
        """
        pass

    @property
    def common_configuration(self):
        """Returns recon-level rule configuration if it was set, else - None."""
        return self.recon._config.configuration

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

    def put_shared_message(self, message: ReconMessage, attributes: tuple):
        # message = copy.deepcopy(message)
        self.recon.put_shared_message(message, attributes)

    def has_been_grouped(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        if message.group_name is None:
            self.__group_and_store_event(message, attributes, *args, **kwargs)
            if message.group_name is None:
                logger.debug("RULE '%s': Ignored %s due to unset group_id", self.name, message.all_info)
                return False
        return True

    def process(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        self.__hash_and_store_event(message, attributes, *args, **kwargs)
        if message.hash is None:
            logger.debug("RULE '%s': Ignored %s due to unset hash", self.name, message.all_info)
            return

        if message.group_name not in self.__cache.message_groups:
            raise Exception(f"'group' method set incorrect groups.\n"
                            f" - message: {message.all_info}\n"
                            f" - available groups: {self.description_of_groups()}\n"
                            f" - message.group_id: {message.group_name}")
        main_group = self.__cache.message_groups[message.group_name]
        logger.debug("RULE '%s': Received %s", self.name, message.all_info)

        for match in self.find_matched_messages(message, match_whole_list=self.match_all):
            if match is None:  # Will return None if some groups did not contain the message.
                main_group.put(message)
                return
            else:
                self.__check_and_store_event(match, attributes, *args, **kwargs)

        if self.match_all:
            main_group.put(message)

        for group_id, group in self.__cache.message_groups.items():
            if group_id != message.group_name and group.is_cleanable:
                group.remove(message.hash)

        for message in self.reprocess_queue:
            self.process(message, attributes)
        self.reprocess_queue.clear()

    def find_matched_messages(self, message: ReconMessage, match_whole_list: bool = False):

        if len(self.description_of_groups()) == 1:
            yield [message]
            return

        matched_messages = []
        for group_name, group in self.__cache.message_groups.items():
            if group_name == message.group_name:
                continue
            if message.hash not in group:
                yield None
            matched_messages.append(group.data[message.hash])

        if match_whole_list:
            yield [message] + functools.reduce(lambda inner_list1, inner_list2: inner_list1 + inner_list2,
                                               matched_messages)
            return

        for match in zip(*matched_messages):
            yield [message] + list(match)

    def queue_for_retransmitting(self, message: ReconMessage):
        self.reprocess_queue.append(message)

    def clear_cache(self):
        for group in self.__cache.message_groups.values():
            group.wipe()

    def __group_and_store_event(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        try:
            self.group(message, attributes, *args, **kwargs)
        except Exception as e:
            logger.exception(f"RULE '{self.name}' - An exception was caught while running 'group': {e}")
            self.event_store.store_error(rule_event_id=self.rule_event_id,
                                         event_name="An exception was caught while running 'group'",
                                         error_message=traceback.format_exc(),
                                         messages=[message])

    def __hash_and_store_event(self, message: ReconMessage, attributes: tuple, *args, **kwargs):
        try:
            self.hash(message, attributes, *args, **kwargs)
        except Exception as e:
            logger.exception(f"RULE '{self.name}' - An exception was caught while running 'hash': {e}")
            self.event_store.store_error(rule_event_id=self.rule_event_id,
                                         event_name="An exception was caught while running 'hash'",
                                         error_message=traceback.format_exc(),
                                         messages=[message])

    def __check_and_store_event(self, matched_messages: List[ReconMessage], attributes: tuple, *args, **kwargs):
        for msg in matched_messages:
            msg._is_matched = True
        try:
            check_event: Event = self.check(matched_messages, attributes, *args, **kwargs)
        except Exception as e:
            logger.exception(f"RULE '{self.name}' - An exception was caught while running 'check': {e}")
            self.event_store.store_error(rule_event_id=self.rule_event_id,
                                         event_name="An exception was caught while running 'check'",
                                         error_message=traceback.format_exc(),
                                         messages=matched_messages)
            return

        if check_event is None:
            return

        max_timestamp_msg = max(matched_messages, key=_get_msg_timestamp)
        min_timestamp_msg = min(matched_messages, key=_get_msg_timestamp)

        if max_timestamp_msg.timestamp_ns - min_timestamp_msg.timestamp_ns > self.match_timeout:
            self.event_store.store_matched_out_of_timeout(rule_event_id=self.rule_event_id,
                                                          check_event=check_event)
        else:
            self.event_store.store_matched(rule_event_id=self.rule_event_id,
                                           check_event=check_event)

    def log_groups_size(self):
        return "[" + ', '.join(
            f"'{group.name}': {group.size} msg" for group in self.__cache.message_groups.values()) + "]"

    def cache_size(self):
        return sum(group.size for group in self.__cache.message_groups.values())

    def check_no_match_within_timeout(self, actual_timestamp: datetime.datetime, actual_timestamp_ns: int):
        for group in self.__cache.message_groups.values():
            group.check_no_match_within_timeout(actual_timestamp=actual_timestamp,
                                                actual_timestamp_ns=actual_timestamp_ns,
                                                timeout=self.match_timeout)

    def clear_out_of_timeout(self, actual_timestamp: int):
        if self.autoremove_timeout is None:
            return
        elif isinstance(self.autoremove_timeout, datetime.datetime):
            autoremove_timestamp = self.autoremove_timeout.timestamp() * 1e9
            if autoremove_timestamp < actual_timestamp:
                self.autoremove_timeout += datetime.timedelta(days=1)
                self.clear_cache()
            return
        elif isinstance(self.autoremove_timeout, int):
            autoremove_timestamp = actual_timestamp - self.autoremove_timeout * 1e9
        for group in self.__cache.message_groups.values():
            group.clear_out_of_timeout(autoremove_timestamp)

    def stop(self):
        self.__cache.stop()
