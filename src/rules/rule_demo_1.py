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

import rule
import store
from th2 import infra_pb2, message_comparator_pb2

logger = logging.getLogger()


def hashed_fields() -> list:
    return ['TrdMatchID']


class Rule(rule.Rule):

    def process_rule_configuration(self, configuration):
        pass

    def get_name(self) -> str:
        return "Rule_1_demo"

    def get_description(self) -> str:
        return "Rule_1 is used for demo"

    def hash(self, message: infra_pb2.Message) -> str:
        if message.metadata.message_type == 'Heartbeat':
            return rule.IGNORED_HASH
        str_fields = ""
        for field_name in hashed_fields():
            if message.fields[field_name].simple_value == '':
                return rule.IGNORED_HASH
            str_fields += message.fields[field_name].simple_value
        return str(hash(message.metadata.message_type + str_fields))

    def check(self, messages_by_routing_key: dict) -> infra_pb2.Event:
        settings = message_comparator_pb2.ComparisonSettings()
        settings.ignore_fields.extend(
            ['TargetCompID', 'SendingTime', 'BodyLength', 'CheckSum', 'MsgSeqNum', 'OnBehalfOfCompID'])
        messages = [msg for msg in messages_by_routing_key.values()]
        try:
            comparison_result = self.comparator.compare(messages[0], messages[1], settings).result()
            logger.debug(f"Rule: {self.get_name()}. Message type: {messages[0].metadata.message_type}. Check success")
            hash_field_values = dict()
            for field_name in hashed_fields():
                if not hash_field_values.__contains__(field_name):
                    hash_field_values[field_name] = []
                hash_field_values[field_name].append(messages[0].fields[field_name].simple_value)
                hash_field_values[field_name].append(messages[1].fields[field_name].simple_value)
            return store.create_verification_event(self.rule_event_id, comparison_result, hash_field_values)
        except Exception:
            logger.exception(
                f"Rule: {self.get_name()}. Error while send comparison request:\n"
                f"Expected:{messages[0]}.\n" +
                f"Actual:{messages[1]}. \n"
                f"Settings:{settings}")
