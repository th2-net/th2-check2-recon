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
from concurrent.futures.thread import ThreadPoolExecutor

import grpc

from th2recon.th2 import message_comparator_pb2_grpc, infra_pb2, message_comparator_pb2

logger = logging.getLogger()


class Comparator:

    def __init__(self, comparator_uri: str) -> None:
        self.comparator_uri = comparator_uri
        self.executor = ThreadPoolExecutor(20)

    def compare(self, expected: infra_pb2.Message, actual: infra_pb2.Message,
                settings: message_comparator_pb2.ComparisonSettings) -> infra_pb2.Event:
        return self.executor.submit(self.comparing, expected, actual, settings)

    def comparing(self, expected: infra_pb2.Message, actual: infra_pb2.Message,
                  settings: message_comparator_pb2.ComparisonSettings) -> infra_pb2.Event:
        with grpc.insecure_channel(self.comparator_uri) as channel:
            grpc_stub = message_comparator_pb2_grpc.MessageComparatorServiceStub(channel)
            request = message_comparator_pb2.CompareMessageVsMessageRequest()
            request.comparison_tasks.append(
                message_comparator_pb2.CompareMessageVsMessageTask(first=expected, second=actual,
                                                                   settings=settings))
            compare_response = grpc_stub.compareMessageVsMessage(request)
            for compare_result in compare_response.comparison_results:  # TODO fix it
                return compare_result


def get_result_count(comparison_result, status) -> int:
    count = 0
    if status == comparison_result.status:
        count += 1

    for sub_result in comparison_result.fields.values():
        count += get_result_count(sub_result, status)

    return count


def get_status_type_by_val(failed, passed) -> message_comparator_pb2.ComparisonEntryStatus:
    if failed != 0:
        return message_comparator_pb2.ComparisonEntryStatus.FAILED
    else:
        if passed != 0:
            return message_comparator_pb2.ComparisonEntryStatus.PASSED
    return message_comparator_pb2.ComparisonEntryStatus.NA


def get_status_type(
        comparison_result: message_comparator_pb2.ComparisonEntry) -> message_comparator_pb2.ComparisonEntryStatus:
    failed = get_result_count(comparison_result, message_comparator_pb2.ComparisonEntryStatus.FAILED)
    passed = get_result_count(comparison_result, message_comparator_pb2.ComparisonEntryStatus.PASSED)
    return get_status_type_by_val(failed, passed)
