import logging
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Lock

import grpc

from th2 import infra_pb2
from th2 import message_comparator_pb2
from th2 import message_comparator_pb2_grpc

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
