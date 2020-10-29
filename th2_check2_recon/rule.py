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
from abc import abstractmethod
from concurrent.futures.thread import ThreadPoolExecutor

from grpc_common import common_pb2

from th2_check2_recon import store, comparator, services

logger = logging.getLogger()

IGNORED_HASH = 'ignored'


class Rule:
    def __init__(self, event_store: store.Store, routing_keys: list, cache_size: int, time_interval: int,
                 message_comparator: comparator.Comparator, enabled: bool, configuration) -> None:
        self.event_store = event_store
        self.routing_keys = routing_keys
        self.rule_event_id = store.new_event_id()
        self.event_store.create_event_groups(self.rule_event_id, self.get_name(), self.get_description())
        self.cache = services.Cache(cache_size, time_interval, routing_keys, event_store, self.rule_event_id)
        self.comparator = message_comparator
        self.enabled = enabled
        self.configure(configuration)

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_description(self) -> str:
        pass

    @abstractmethod
    def hash(self, message: common_pb2.Message) -> str:
        pass

    @abstractmethod
    def check(self, messages_by_routing_key: dict) -> common_pb2.Event:
        pass

    @abstractmethod
    def configure(self, configuration):
        pass

    def hashed_fields(self) -> list:
        pass

    def hashed_fields_values_to_string(self, message: common_pb2.Message, separator: str) -> str:
        """

        :return: String in format "{separator}{field_name}: '{field_simple_value}'"
        """
        result = ""
        for field_name in self.hashed_fields():
            result += separator
            result += f"{field_name}: '{message.fields[field_name].simple_value}'"
        return result

    def process(self, message: common_pb2.Message, routing_key: str, executor: ThreadPoolExecutor):
        hash_of_message = self.hash(message)
        if hash_of_message == IGNORED_HASH:
            return
        matched_messages = {routing_key: message}
        for key in self.routing_keys:
            if self.cache.contains(hash_of_message, key):
                if key != routing_key:
                    matched_messages[key] = self.cache.get(key, hash_of_message)
                else:
                    event_message = \
                        f'The message with the hash={hash_of_message} already contains in cache of {key}. ' \
                        f'The implementation of the hash function is incorrect, as it allows collisions.'
                    logger.debug(event_message)
                    self.event_store.store_error(self.rule_event_id, message, event_message)

        if len(matched_messages) == len(self.routing_keys):
            self.cache.put(routing_key, hash_of_message, message, self.hashed_fields_values_to_string(message, ". "))
            self.cache.remove_matched(hash_of_message, matched_messages)
            executor.submit(self.logging_event, routing_key, hash_of_message, message)
            executor.submit(self.check_and_store_event, matched_messages)
        else:
            self.cache.put(routing_key, hash_of_message, message, self.hashed_fields_values_to_string(message, ". "))
            executor.submit(self.logging_event, routing_key, hash_of_message, message)

    def check_and_store_event(self, messages_by_key: dict):
        check_event = self.check(messages_by_key)
        max_timestamp = -1
        min_timestamp = -1
        for message in messages_by_key.values():
            timestamp_seconds = message.metadata.timestamp.seconds
            max_timestamp = max(timestamp_seconds, max_timestamp)
            if timestamp_seconds < min_timestamp or min_timestamp == -1:
                min_timestamp = timestamp_seconds
        if max_timestamp - min_timestamp > self.cache.time_interval:
            self.event_store.store_matched_out_of_timeout(self.rule_event_id, check_event, self.cache.min_time,
                                                          self.cache.min_time + self.cache.time_interval)
        else:
            self.event_store.store_matched(self.rule_event_id, check_event)

    def logging_event(self, routing_key: str, hash_of_message: str, message: common_pb2.Message):
        event_message = f"Received '{message.metadata.message_type}' from '{routing_key}'. Hash: {hash_of_message}"
        event_message += self.hashed_fields_values_to_string(message, " ")
        logger.debug(f"RULE '{self.get_name()}': {event_message}")
