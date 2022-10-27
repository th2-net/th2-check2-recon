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
from typing import Any, List

from th2_grpc_common.common_pb2 import EventStatus, FilterOperation
from th2_grpc_util.util_pb2 import ComparisonEntry, ComparisonEntryStatus, ComparisonEntryType

logger = logging.getLogger(__name__)


class EventType(str, enum.Enum):
    ROOT = 'ReconRoot'
    RULE = 'ReconRule'
    STATUS = 'ReconStatus'
    EVENT = 'ReconEvent'
    UNKNOWN = ''


class ComparatorUtils:

    @staticmethod
    def __get_result_count(comparison_result: ComparisonEntry, status: ComparisonEntryStatus) -> int:
        count = sum(ComparatorUtils.__get_result_count(sub_result, status)
                    for sub_result in comparison_result.fields.values())
        if status == comparison_result.status:
            count += 1

        return count

    @staticmethod
    def __get_status_type_by_val(failed: int, passed: int) -> ComparisonEntryStatus:
        if failed != 0:
            return ComparisonEntryStatus.FAILED  # type: ignore
        else:
            if passed != 0:
                return ComparisonEntryStatus.PASSED  # type: ignore
        return ComparisonEntryStatus.NA  # type: ignore

    @staticmethod
    def get_status_type(comparison_result: ComparisonEntry) -> ComparisonEntryStatus:
        failed = ComparatorUtils.__get_result_count(
            comparison_result, ComparisonEntryStatus.FAILED)  # type: ignore
        passed = ComparatorUtils.__get_result_count(
            comparison_result, ComparisonEntryStatus.PASSED)  # type: ignore
        return ComparatorUtils.__get_status_type_by_val(failed, passed)


class MessageComponent:

    def __init__(self, message: str) -> None:
        self.type = 'message'
        self.data = message


class TableComponent:

    def __init__(self, column_names: list) -> None:
        self.type = 'table'
        self.rows: List[dict] = []
        self.__column_names = column_names

    def add_row(self, *values: Any) -> None:
        self.rows.append({
            column_name: value for column_name, value
            in zip(self.__column_names, values)
        })


class VerificationComponent:

    def __init__(self, comparison_entry: ComparisonEntry) -> None:
        self.type = 'verification'
        self.fields = {
            field_name: VerificationComponent.VerificationEntry(entry)
            for field_name, entry in comparison_entry.fields.items()
        }
        self.status = EventStatus.FAILED \
            if ComparatorUtils.get_status_type(comparison_entry) == ComparisonEntryStatus.FAILED \
            else EventStatus.SUCCESS

    class VerificationEntry:

        def __init__(self, comparison_entry: ComparisonEntry) -> None:
            self.type = ComparisonEntryType.Name(comparison_entry.type).lower()
            self.status = ComparisonEntryStatus.Name(comparison_entry.status)
            self.expected = comparison_entry.first
            self.actual = comparison_entry.second
            self.key = comparison_entry.is_key
            self.operation = FilterOperation.Name(comparison_entry.operation)
            self.fields = {}

            if comparison_entry.type == ComparisonEntryType.COLLECTION:
                for field_name, entry in comparison_entry.fields.items():
                    self.fields[field_name] = VerificationComponent.VerificationEntry(entry)
