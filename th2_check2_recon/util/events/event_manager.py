from threading import Timer, Lock
from time import sleep
from typing import List, Optional, Dict, Callable

from th2_check2_recon.util.events.event_producers import EventProducer
from th2_common.schema.message.message_router import MessageRouter
from th2_grpc_common.common_pb2 import EventBatch, Event


class EventManager:
    class RepeatTimer(Timer):

        def run(self) -> None:
            while not self.finished.wait(self.interval):  # type: ignore
                self.function(*self.args, **self.kwargs)  # type: ignore

    def __init__(self) -> None:
        self._event_producers: List[EventProducer] = []
        self._event_router: Optional[MessageRouter] = None
        self._sending_interval: Optional[int] = None

        self.__event_batches_to_send: Dict[str, EventBatch] = {}
        self.__lock: Lock = Lock()
        self.__timer: Optional[EventManager.RepeatTimer] = None

    def add_event_producer(self, event_producer: 'EventProducer') -> None:
        self._event_producers.append(event_producer)

    def start(self, event_router: MessageRouter, event_batch_send_interval: int) -> None:
        self._event_router = event_router
        self._sending_interval = event_batch_send_interval
        self.__send_all()
        self.__timer = self.__create_timer(self._sending_interval, self.__timer_handle)

    def put_events_in_batch(self, *events: Event) -> None:
        for event in events:
            parent_id = event.parent_id
            if parent_id.id in self.__event_batches_to_send:
                event_batch = self.__event_batches_to_send[parent_id.id]
            else:
                event_batch = EventBatch()
                self.__event_batches_to_send[parent_id.id] = event_batch

            event_batch.events.append(event)

    def __send_all(self) -> None:
        for event_batch in self.__event_batches_to_send.values():
            self._event_router.send(event_batch)
        self.__event_batches_to_send.clear()

    @staticmethod
    def __create_timer(sending_interval: int, handler: Callable) -> 'EventManager.RepeatTimer':
        timer = EventManager.RepeatTimer(sending_interval, handler)
        timer.start()
        return timer

    def __timer_handle(self) -> None:
        with self.__lock:
            for event_producer in self._event_producers:
                self.put_events_in_batch(*event_producer.fetch_events())
            self.__send_all()
