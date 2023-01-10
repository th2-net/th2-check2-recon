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

import datetime
import logging
from typing import Any, Dict, Optional, Union, List, Callable

from th2_common_utils.converters.message_converters import dict_to_message
from th2_grpc_common.common_pb2 import ConnectionID, Direction, MessageID, Message
from th2_grpc_util.util_pb2 import CompareMessageVsMessageResult, CompareMessageVsMessageRequest, \
    CompareMessageVsMessageTask, ComparisonSettings

from th2_check2_recon.utils import VerificationComponent

logger = logging.getLogger(__name__)


class ReconMessage:

    __slots__ = (
        'proto_message', 'group_name', 'hash', 'group_info', 'hash_info',
        '_is_matched', '_shared', '_timestamp', '_timestamp_ns', '_timestamp_s', '_batch_timestamp_s', '_info',
        '_was_checked_no_match_within_timeout'
    )

    def __init__(self, proto_message: Dict[str, Any]) -> None:
        self.proto_message = proto_message
        self.group_name: Optional[str] = None
        self.hash: Optional[int] = None

        self.hash_info: dict = {}

        self._is_matched: bool = False
        self._shared: bool = False
        self._timestamp: Optional[datetime.datetime] = None
        self._timestamp_ns: Optional[int] = None
        self._timestamp_s: Optional[int] = None
        self._info: Optional[str] = None

    @property
    def timestamp(self) -> datetime.datetime:
        if self._timestamp is None:
            self._timestamp = ReconMessageUtils.get_timestamp(self.proto_message)
        return self._timestamp

    @property
    def timestamp_ns(self) -> int:
        if self._timestamp_ns is None:
            self._timestamp_ns = ReconMessageUtils.get_timestamp_ns(self.proto_message)
        return self._timestamp_ns

    @property
    def timestamp_s(self) -> int:
        if self._timestamp_s is None:
            self._timestamp_s = round(self.timestamp_ns / 10**9)
        return self._timestamp_s

    @staticmethod
    def get_info(info_dict: dict) -> str:
        return '[' + ', '.join(f"'{key}': {value}" for key, value in info_dict.items()) + ']'

    @property
    def all_info(self) -> str:
        if self._info is None:
            result = f"'{self.proto_message['metadata']['message_type']} " \
                     f"'id='{ReconMessageUtils.str_message_id(self.proto_message)}'"
            self._info = result
        else:
            result = self._info
        if self.hash is not None:
            result += f" Hash='{self.hash}'"
        if self.group_name is not None:
            result += f" Group='{self.group_name}'"
        if len(self.hash_info) > 0:
            result += f' Hash{self.get_info(self.hash_info)}'
        return result


def _get_msg_timestamp(msg: ReconMessage) -> datetime.datetime:
    """Used instead of lambda in Rule.__check_and_store_event"""
    return msg.timestamp


class MessageGroupDescription:

    __slots__ = ('__single', '__multi', '__shared', '__ignore_no_match')

    def __init__(self,
                 single: bool = False,
                 multi: bool = False,
                 shared: bool = False,
                 ignore_no_match: bool = False) -> None:
        if single ^ multi:  # xor
            self.__single = single
            self.__multi = multi
        else:
            raise AttributeError('Group should be either single or multi')
        self.__shared = shared
        self.__ignore_no_match = ignore_no_match

    def __eq__(self, other: object) -> bool:
        properties = {'single', 'multi', 'shared', 'ignore_no_match'}
        if not isinstance(other, MessageGroupDescription):
            return NotImplemented
        return all(getattr(self, prop) == getattr(other, prop) for prop in properties)

    def __hash__(self) -> int:
        return hash((
            self.single, self.multi,
            self.shared, self.ignore_no_match
        ))

    @property
    def single(self) -> bool:
        return self.__single

    @property
    def multi(self) -> bool:
        return self.__multi

    @property
    def shared(self) -> bool:
        return self.__shared

    @property
    def ignore_no_match(self) -> bool:
        return self.__ignore_no_match


class ReconMessageUtils:
    """Some service methods for work with recon specifically and it's rules"""

    @staticmethod
    def get_timestamp(message: Dict[str, Any]) -> datetime.datetime:
        timestamp: datetime.datetime = message['metadata']['timestamp']
        return timestamp if timestamp is not None else datetime.datetime.min

    @staticmethod
    def get_timestamp_ns(message: Dict[str, Any]) -> int:
        timestamp = message['metadata']['timestamp']
        return int(timestamp.timestamp() * 10 ** 9) if timestamp is not None else 0

    @staticmethod
    def str_message_id(message: Dict[str, Any]) -> str:
        res = ''
        params = message['metadata']['session_alias'], message['metadata']['direction'], message['metadata']['sequence']
        for param in params:
            res += str(param) + ':' if param else 'None: '
        return res

    @staticmethod
    def get_value(message: ReconMessage, name: str, default: Any = '') -> Any:
        """return simple value of a given field if presented in the recon message."""
        return message.proto_message['fields'].get(name, default)

    @staticmethod
    def get_required_value(message: ReconMessage, name: str) -> Any:
        """return simple value of a given field."""
        return message.proto_message['fields'][name]

    @staticmethod
    def get_inner_value(message: ReconMessage, *names: Union[str, int], default: Any = '') -> Any:
        value = message.proto_message['fields']
        for name in names:
            if isinstance(name, int):
                if isinstance(value, list) and -len(value) <= name < len(value):
                    value = value[name]
                else:
                    value = None
            else:
                value = value.get(name)

            if value is None:
                return default
        return value

    @staticmethod
    def get_message_type(message: ReconMessage) -> str:
        return message.proto_message['metadata']['message_type']  # type: ignore

    @staticmethod
    def get_session_alias(message: ReconMessage) -> str:
        return message.proto_message['metadata']['session_alias']  # type: ignore

    @staticmethod
    def get_message_id(message: ReconMessage) -> MessageID:
        return MessageID(connection_id=ConnectionID(session_alias=message.proto_message['metadata']['session_alias'],
                                                    session_group=message.proto_message['metadata']['session_group']),
                         direction=getattr(Direction, message.proto_message['metadata']['direction']),
                         sequence=message.proto_message['metadata']['sequence'],
                         subsequence=message.proto_message['metadata']['subsequence'])

    @staticmethod
    def equal_values_check(message1: ReconMessage, message2: ReconMessage, tag: str) -> bool:
        """check that tag values are equal for both messages"""
        return ReconMessageUtils.get_value(message1, tag) == ReconMessageUtils.get_value(message2, tag)  # type: ignore

    @staticmethod
    def equal_if_not_empty_values_check(message1: ReconMessage, message2: ReconMessage, tag: str) -> bool:
        """check that tag values are equal (if they are not empty) for both messages"""
        value1 = ReconMessageUtils.get_value(message1, tag)
        value2 = ReconMessageUtils.get_value(message2, tag)
        if value1 != '' and value2 != '':
            return value1 == value2  # type: ignore
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


class MessageComparator:

    def __init__(self, comparator_service) -> None:  # type: ignore
        self.__comparator_service = comparator_service

    def compare(self, expected: Message, actual: Message,
                settings: ComparisonSettings) -> CompareMessageVsMessageResult:
        return self.comparing(expected, actual, settings)

    def comparing(self, expected: Message, actual: Message,
                  settings: ComparisonSettings) -> CompareMessageVsMessageResult:
        request = CompareMessageVsMessageRequest(comparison_tasks=[
            CompareMessageVsMessageTask(first=expected, second=actual, settings=settings)
        ])
        try:
            compare_response = self.__comparator_service.compareMessageVsMessage(request)
            for compare_result in compare_response.comparison_results:
                return compare_result  # type:ignore
        except Exception as e:
            logger.exception(f'Error while comparing: {e}.\nCompareMessageVsMessageRequest: {request}')
        return CompareMessageVsMessageResult()

    def compare_messages(self, messages: List[ReconMessage],
                         ignore_fields: Optional[List[str]] = None) -> Optional[VerificationComponent]:
        if len(messages) != 2:
            logger.exception('The number of messages to compare must be 2.')
            return None
        settings = ComparisonSettings()
        if ignore_fields is not None:
            settings.ignore_fields.extend(ignore_fields)

        compare_result = self.compare(dict_to_message(messages[0].proto_message['fields']),
                                      dict_to_message(messages[1].proto_message['fields']), settings)
        return VerificationComponent(compare_result.comparison_result)

    def stop(self) -> None:
        pass
