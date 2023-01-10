from typing import List, Optional, Iterable

from prometheus_client import Histogram

from th2_check2_recon.util.events.event_producers import MessageProcessorEventProducer, EventType
from th2_common.schema.metrics import common_metrics
from th2_common_utils import event_utils
from th2_grpc_common.common_pb2 import Event, EventID

from th2_check2_recon.recon_message import ReconMessage
from th2_check2_recon.util.cache.cache import Cache, MessageGroup
from th2_check2_recon.util.process_units import ProcessUnit, build_process_chain, CleanupProcessUnit
from th2_check2_recon.utils import MessageComponent


class MessageProcessor(MessageProcessorEventProducer):

    def __init__(self, rule, metric_number: int):
        super().__init__(rule.get_name(), rule.get_description())
        self._rule = rule
        self.__cache: Cache = Cache(capacity=rule.configuration.cache_size)
        self.__process_chain: Optional[ProcessUnit] = build_process_chain(self._rule, self.__cache)

        self.__reprocess_queue: List[ReconMessage] = []

        self.RULE_PROCESSING_TIME = Histogram(f'th2_recon_rule_{metric_number}_processing_time',
                                              'Time of the message processing with a rule',
                                              buckets=common_metrics.DEFAULT_BUCKETS)

    def add_message_group(self, message_group: MessageGroup) -> None:
        self.__cache.add_message_group(message_group)

    def get_message_groups(self) -> Iterable[MessageGroup]:
        return self.__cache.message_groups.values()

    def process(self, recon_messages: List[ReconMessage]) -> None:
        events = self.__process_chain.process(recon_messages)
        cpu = CleanupProcessUnit(self.__cache, self._rule.configuration.match_timeout)
        cpu.process(recon_messages)
        self._store_match_events(events)

    def reprocess(self) -> None:
        if len(self.__reprocess_queue) > 0:
            self.process(self.__reprocess_queue)
            self.__reprocess_queue.clear()

    # def put_shared_message(self, message: ReconMessage) -> None:
    #     self.__recon.put_shared_message(message)

    def put_in_queue_for_retransmitting(self, message: ReconMessage) -> None:
        self.__reprocess_queue.append(message)

    def cache_size(self) -> int:
        return self.__cache.size

    def log_groups_size(self) -> str:
        return '[' + ', '.join(f'"{group_name}": {group_size} msg' for group_name, group_size
                               in self.__cache.groups_size.items()) + ']'
