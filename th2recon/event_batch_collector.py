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
from threading import Lock, Timer

import grpc

from th2recon.th2 import event_store_pb2, infra_pb2, event_store_pb2_grpc

logger = logging.getLogger()


class EventsBatchCollector:
    def __init__(self, event_store_uri: str, max_batch_size: int, timeout: int) -> None:
        self.batches = {}
        self.max_batch_size = max_batch_size
        self.event_store_uri = event_store_uri
        self.timeout = timeout
        self.lock = Lock()

    def put_event(self, event: infra_pb2.Event):
        with self.lock:
            if event.parent_id.id in self.batches:
                event_batch, batch_timer = self.batches.get(event.parent_id.id)
            else:
                event_batch = infra_pb2.EventBatch(parent_event_id=event.parent_id)
                batch_timer = self._create_timer(event_batch)
                self.batches[event.parent_id.id] = (event_batch, batch_timer)
            event_batch.events.append(event)
            if len(event_batch.events) == self.max_batch_size:
                self.batches.pop(event.parent_id.id)
                batch_timer.cancel()
            else:
                return

        self._send_batch(event_batch)

    def _timer_handle(self, batch: infra_pb2.EventBatch):
        with self.lock:
            if batch.parent_event_id.id in self.batches:
                self.batches.pop(batch.parent_event_id.id)
            else:
                return

        self._send_batch(batch)

    def _send_batch(self, batch: infra_pb2.EventBatch):
        with grpc.insecure_channel(self.event_store_uri) as channel:
            try:
                store_stub = event_store_pb2_grpc.EventStoreServiceStub(channel)
                batch_response = store_stub.StoreEventBatch(event_store_pb2.StoreEventBatchRequest(event_batch=batch))
                logger.debug("Batch id: %r" % batch_response)
            except Exception:
                logger.exception("Error while send batch")

    def _create_timer(self, batch: infra_pb2.EventBatch):
        timer = Timer(self.timeout, self._timer_handle, [batch])
        timer.start()
        return timer
