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

import comparator
import services
import store
from th2 import infra_pb2

logger = logging.getLogger()

IGNORED_HASH = "ignored"


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
    def hash(self, message: infra_pb2.Message) -> str:
        pass

    @abstractmethod
    def check(self, messages_by_routing_key: dict) -> infra_pb2.Event:
        pass

    @abstractmethod
    def configure(self, configuration):
        pass

    def process(self, message: infra_pb2.Message, routing_key: str, executor: ThreadPoolExecutor):
        hash_of_message = self.hash(message)
        if hash_of_message == IGNORED_HASH:
            return
        matched_messages = {routing_key: message}
        for key in self.routing_keys:
            if key == routing_key:
                continue
            if self.cache.contains(hash_of_message, key):
                if key != routing_key:
                    matched_messages[key] = self.cache.get(key, hash_of_message)
                else:
                    event_message = \
                        f"The message with the hash={hash_of_message} already contains in cache of {key}. " + \
                        "The implementation of the hash function is incorrect, as it allows collisions."
                    logger.debug(event_message)
                    self.event_store.store_error(self.rule_event_id, message, event_message)
            else:
                break
        if len(matched_messages) == len(self.routing_keys):
            self.cache.put(routing_key, hash_of_message, message)
            self.cache.remove_matched(hash_of_message, matched_messages)
            executor.submit(self.check_and_store_event, matched_messages)
        else:
            self.cache.put(routing_key, hash_of_message, message)

    def check_and_store_event(self, messages_by_key: dict):
        check_event = self.check(messages_by_key)
        out_of_timeout = False
        for message in messages_by_key.values():
            timestamp = self.cache.get_timestamp(message)
            if timestamp < self.cache.min_time or timestamp > self.cache.min_time + self.cache.time_interval:  # FIXME min_time not thread safe
                out_of_timeout = True
                break
        if out_of_timeout:
            self.event_store.store_matched_out_of_timeout(self.rule_event_id, check_event, self.cache.min_time,
                                                          self.cache.min_time + self.cache.time_interval)
        else:
            self.event_store.store_matched(self.rule_event_id, check_event)
