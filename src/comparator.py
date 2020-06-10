import logging
import queue
from concurrent.futures.thread import ThreadPoolExecutor
from threading import Thread

import grpc

import store
from th2 import infra_pb2
from th2 import message_comparator_pb2
from th2 import message_comparator_pb2_grpc

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', level=logging.INFO)


class Comparator:

    def __init__(self, comparator_uri: str, event_store: store.Store) -> None:
        self.COMPARING_IS_ON = False
        self.tasks = queue.Queue()
        self.comparator_uri = comparator_uri
        self.parent_id = event_store.report_id
        self.thread_comparing = Thread(target=self.__comparing, args=())
        self.event_store = event_store

    @staticmethod
    def match(expected: infra_pb2.Message, actual: infra_pb2.Message):
        return expected.metadata.message_type == actual.metadata.message_type and \
               expected.fields['TrdMatchID'].simple_value == actual.fields['TrdMatchID'].simple_value

    def check(self, expected: infra_pb2.Message, actual: infra_pb2.Message):
        self.tasks.put([expected, actual])

    def __comparing(self):
        with ThreadPoolExecutor(20) as executor:
            logging.info("Comparator is started")
            while not self.tasks.empty() or self.COMPARING_IS_ON:
                try:
                    task = self.tasks.get(block=True, timeout=5)
                    executor.submit(self.compare, task[0], task[1])
                except queue.Empty:
                    pass

    def compare(self, expected: infra_pb2.Message, actual: infra_pb2.Message):
        with grpc.insecure_channel(self.comparator_uri) as channel:
            logging.info("Compare %r and %r" % (expected.metadata.message_type, expected.metadata.message_type))
            try:
                grpc_stub = message_comparator_pb2_grpc.MessageComparatorServiceStub(channel)
                request = message_comparator_pb2.CompareMessageVsMessageRequest()
                request.comparison_tasks.append(
                    message_comparator_pb2.CompareMessageVsMessageTask(first=expected, second=actual))
                compare_response = grpc_stub.compareMessageVsMessage(request)
                for compare_result in compare_response.comparison_results:
                    # logging.info(compare_result)
                    self.event_store.store_verification_event(self.parent_id, compare_result)
            except Exception as err:
                logging.info(err)

    def start(self):
        self.COMPARING_IS_ON = True
        self.thread_comparing.start()

    def stop(self):
        self.COMPARING_IS_ON = False
        self.thread_comparing.join()
        logging.info("Comparator is stopped")


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
