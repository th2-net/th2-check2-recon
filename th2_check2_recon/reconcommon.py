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

from typing import Any, Dict, Optional

from th2_check2_recon.common import MessageUtils


class ReconMessage:

    __slots__ = ('proto_message', 'group_name', 'hash', 'group_info', 'hash_info', '_is_matched',
                 '_shared', '_timestamp', '_timestamp_ns', '_info', '_was_checked_no_match_within_timeout')

    def __init__(self, proto_message: Dict[str, Any]) -> None:
        self.proto_message = proto_message
        self.group_name: Optional[str] = None
        self.hash = None

        self.group_info = {}
        self.hash_info = {}

        self._is_matched: bool = False
        self._shared: bool = False
        self._timestamp = None
        self._timestamp_ns = None
        self._info = None
        self._was_checked_no_match_within_timeout = False

    @property
    def timestamp(self):
        if self._timestamp is None:
            self._timestamp = MessageUtils.get_timestamp(self.proto_message)
        return self._timestamp

    @property
    def timestamp_ns(self):
        if self._timestamp_ns is None:
            self._timestamp_ns = MessageUtils.get_timestamp_ns(self.proto_message)
        return self._timestamp_ns

    @staticmethod
    def get_info(info_dict: dict) -> str:
        return '[' + ', '.join(f"'{key}': {value}" for key, value in info_dict.items()) + ']'

    @property
    def all_info(self) -> str:
        if self._info is None:
            result = f"'{self.proto_message['metadata']['message_type']}" \
                     f"' id='{MessageUtils.str_message_id(self.proto_message)}'"
            self._info = result
        else:
            result = self._info
        if self.hash is not None:
            result += f" Hash='{self.hash}'"
        if self.group_name is not None:
            result += f" Group='{self.group_name}'"
        if len(self.hash_info) > 0:
            result += f' Hash{self.get_info(self.hash_info)}'
        if len(self.group_info) > 0:
            result += f' GroupID{self.get_info(self.group_info)}'
        return result


def _get_msg_timestamp(msg: ReconMessage):
    """Used instead of lambda in Rule.__check_and_store_event"""
    return msg.timestamp


class MessageGroupDescription:

    __slots__ = ('__single', '__multi', '__shared', '__ignore_no_match')

    def __init__(self,
                 single: bool = False,
                 multi: bool = False,
                 shared: bool = False,
                 ignore_no_match: bool = False):
        if single ^ multi:  # xor
            self.__single = single
            self.__multi = multi
        else:
            raise AttributeError('Group should be either single or multi')
        self.__shared = shared
        self.__ignore_no_match = ignore_no_match

    def __eq__(self, other):
        properties = {'single', 'multi', 'shared', 'ignore_no_match'}
        return all(getattr(self, prop) == getattr(other, prop) for prop in properties)

    def __hash__(self):
        return hash((self.single, self.multi, self.shared, self.ignore_no_match))

    @property
    def single(self):
        return self.__single

    @property
    def multi(self):
        return self.__multi

    @property
    def shared(self):
        return self.__shared

    @property
    def ignore_no_match(self):
        return self.__ignore_no_match
