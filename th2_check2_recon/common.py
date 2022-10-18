# Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import enum
import logging
import uuid
from datetime import datetime
from json import JSONEncoder
from typing import Any, Dict

from google.protobuf.timestamp_pb2 import Timestamp
from th2_check2_recon.reconcommon import ReconMessage
from th2_grpc_common.common_pb2 import Event, ConnectionID, Direction
from th2_grpc_common.common_pb2 import EventStatus, EventID, MessageID, FilterOperation
from th2_grpc_util.util_pb2 import ComparisonEntryStatus, ComparisonEntry, ComparisonEntryType

logger = logging.getLogger(__name__)


class EventUtils:
    class __ComponentEncoder(JSONEncoder):
        def default(self, o):
            return o.__dict__

    @staticmethod
    def create_event_body(component) -> bytes:
        return EventUtils.component_encoder().encode(component).encode()

    @staticmethod
    def component_encoder() -> __ComponentEncoder:
        return EventUtils.__ComponentEncoder()

    @staticmethod
    def new_event_id():
        return EventID(id=str(uuid.uuid1()))

    class EventType(enum.Enum):
        ROOT = 'ReconRoot'
        RULE = 'ReconRule'
        STATUS = 'ReconStatus'
        EVENT = 'ReconEvent'
        UNKNOWN = ''

    @staticmethod
    def create_event(name: str,
                     id: EventID = None,
                     parent_id: EventID = None,
                     status: EventStatus = EventStatus.SUCCESS,
                     body: bytes = None,
                     attached_message_ids: [MessageID] = None,
                     type: EventType = EventType.UNKNOWN) -> Event:
        if id is None:
            id = EventUtils.new_event_id()
        if attached_message_ids is None:
            attached_message_ids = []
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        return Event(id=id,
                     parent_id=parent_id,
                     start_timestamp=Timestamp(seconds=seconds, nanos=nanos),
                     end_timestamp=Timestamp(seconds=seconds, nanos=nanos),
                     status=status,
                     name=name,
                     body=body,
                     type=type.value,
                     attached_message_ids=attached_message_ids)


class MessageUtils:

    @staticmethod
    def get_timestamp(message: Dict[str, Any]) -> datetime:
        timestamp = message['metadata']['timestamp']
        return timestamp if timestamp is not None else 0

    @staticmethod
    def get_timestamp_ns(message: Dict[str, Any]) -> int:
        timestamp = message['metadata']['timestamp']
        return int(timestamp.timestamp() * 10**9) if timestamp is not None else 0

    @staticmethod
    def str_message_id(message: Dict[str, Any]) -> str:
        res = ""
        params = message['metadata']['session_alias'], message['metadata']['direction'], message['metadata']['sequence']
        for param in params:
            res += str(param) + ':' if param else 'None: '
        return res


class ReconMessageUtils:
    """Some service methods for work with recon specifically and it's rules"""

    @staticmethod
    def get_value(message: ReconMessage, name: str, default: str = '') -> str:
        """return simple value of a given field if presented in the recon message."""
        return message.proto_message['fields'].get(name, default)

    @staticmethod
    def get_required_value(message: ReconMessage, name: str) -> str:
        """return simple value of a given field."""
        return message.proto_message['fields'][name]

    @staticmethod
    def get_inner_value(message: ReconMessage, *names: str, default: str = None) -> Any:
        value = message.proto_message['fields']
        for name in names:
            value = value.get(name)
            if value is None:
                return default
        return value

    @staticmethod
    def get_message_type(message: ReconMessage) -> str:
        return message.proto_message['metadata']['message_type']

    @staticmethod
    def get_session_alias(message: ReconMessage) -> str:
        return message.proto_message['metadata']['session_alias']

    @staticmethod
    def get_message_id(message: ReconMessage) -> str:
        return MessageID(connection_id=ConnectionID(session_alias=message.proto_message['metadata']['session_alias'],
                                                    session_group=message.proto_message['metadata']['session_group']),
                         direction=getattr(Direction, message.proto_message['metadata']['direction']),
                         sequence=message.proto_message['metadata']['sequence'],
                         subsequence=message.proto_message['metadata']['subsequence'])

    @staticmethod
    def equal_values_check(message1: ReconMessage, message2: ReconMessage, tag: str) -> bool:
        """check that tag values are equal for both messages"""
        return ReconMessageUtils.get_value(message1, tag) == ReconMessageUtils.get_value(message2, tag)

    @staticmethod
    def equal_if_not_empty_values_check(message1: ReconMessage, message2: ReconMessage, tag: str) -> bool:
        """check that tag values are equal (if they are not empty) for both messages"""
        if ReconMessageUtils.get_value(message1, tag) != '' and ReconMessageUtils.get_value(message2, tag) != '':
            return ReconMessageUtils.get_value(message1, tag) == ReconMessageUtils.get_value(message2, tag)
        return True

    @staticmethod
    def float_equal_values_check(message1: ReconMessage, message2: ReconMessage, tag: str) -> bool:
        """check that required float tag values are equal for both messages"""
        if ReconMessageUtils.get_value(message1, tag) != '' and ReconMessageUtils.get_value(message2, tag) != '':
            return float(ReconMessageUtils.get_value(message1, tag)) == \
                   float(ReconMessageUtils.get_value(message2, tag))
        return False

    @staticmethod
    def float_equal_if_not_empty_values_check(message1: ReconMessage, message2: ReconMessage, tag: str) -> bool:
        """check that float tag values (if they are not empty) are equal for both messages"""
        if ReconMessageUtils.get_value(message1, tag) != '' and ReconMessageUtils.get_value(message2, tag) != '':
            return float(ReconMessageUtils.get_value(message1, tag)) == \
                   float(ReconMessageUtils.get_value(message2, tag))
        return True

    @staticmethod
    def enum_value_check(message: ReconMessage, tag: str, enum_list: list) -> bool:
        return ReconMessageUtils.get_value(message, tag) in enum_list

    @staticmethod
    def not_zero_value_check(message: ReconMessage, tag: str) -> bool:
        """check that tag values isn't equal to zero for both messages"""
        val = ReconMessageUtils.get_value(message, tag)
        if val == '':
            return False
        return float(val) != 0

    @staticmethod
    def zero_value_check(message: ReconMessage, tag: str) -> bool:
        """check that tag value equals to zero"""
        val = ReconMessageUtils.get_value(message, tag)
        if val == '':
            return False
        return float(val) == 0


class ComparatorUtils:

    @staticmethod
    def __get_result_count(comparison_result, status) -> int:
        count = sum(ComparatorUtils.__get_result_count(sub_result, status)
                    for sub_result in comparison_result.fields.values())
        if status == comparison_result.status:
            count += 1

        return count

    @staticmethod
    def __get_status_type_by_val(failed, passed) -> ComparisonEntryStatus:
        if failed != 0:
            return ComparisonEntryStatus.FAILED
        else:
            if passed != 0:
                return ComparisonEntryStatus.PASSED
        return ComparisonEntryStatus.NA

    @staticmethod
    def get_status_type(comparison_result: ComparisonEntry) -> ComparisonEntryStatus:
        failed = ComparatorUtils.__get_result_count(comparison_result, ComparisonEntryStatus.FAILED)
        passed = ComparatorUtils.__get_result_count(comparison_result, ComparisonEntryStatus.PASSED)
        return ComparatorUtils.__get_status_type_by_val(failed, passed)


class MessageComponent:

    def __init__(self, message: str) -> None:
        self.type = 'message'
        self.data = message


class TableComponent:

    def __init__(self, column_names: list) -> None:
        self.type = 'table'
        self.rows = []
        self.__column_names = column_names

    def add_row(self, *values):
        self.rows.append({column_name: value for column_name, value in zip(self.__column_names, values)})


class VerificationComponent:

    def __init__(self, comparison_entry: ComparisonEntry) -> None:
        self.type = 'verification'
        self.fields = {field_name: VerificationComponent.VerificationEntry(entry)
                       for field_name, entry in comparison_entry.fields.items()}
        self.status = EventStatus.FAILED if ComparatorUtils.get_status_type(
            comparison_entry) == ComparisonEntryStatus.FAILED else EventStatus.SUCCESS

    class VerificationEntry:

        def __init__(self, comparison_entry: ComparisonEntry) -> None:
            self.type = ComparisonEntryType.Name(comparison_entry.type).lower()
            self.status = ComparisonEntryStatus.Name(comparison_entry.status)
            self.expected = comparison_entry.first
            self.actual = comparison_entry.second
            self.key = comparison_entry.is_key
            self.operation = FilterOperation.Name(comparison_entry.operation)
            self.fields = dict()

            if comparison_entry.type == ComparisonEntryType.COLLECTION:
                for field_name, entry in comparison_entry.fields.items():
                    self.fields[field_name] = VerificationComponent.VerificationEntry(entry)
