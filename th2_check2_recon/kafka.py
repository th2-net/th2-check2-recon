# Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
from confluent_kafka import Producer

import logging

logger = logging.getLogger(__name__)

class KafkaClient:
    @abstractmethod
    def send(self, message: str) -> None:
        pass

class KafkaProducer(KafkaClient):
    def __init__(self, config: KafkaConfiguration):
        self.topic = config.topic
        self.connection_string = config.bootstrap_servers
        self.fail_on_connection_failure = config.fail_on_connection_failure
        self.producer_config = config.producer_config
        self.connected = False
        self.producer = None

    def _on_delivery(self, err, msg):
        if err is not None:
            logger.error(f"Failed to deliver message: {str(msg)}: {str(err)}")
        else:
            logger.debug(f"Message produced: {str(msg)}")

    def _connect(self):
        try:
            self.producer = Producer(self.producer_config)
            self.connected = True
        except Exception as e:
            logger.error(f"Error while connecting to {self.connection_string}", e)
            self.connected = False
            self.producer = None
            if self.fail_on_connection_failure:
                raise

    def _is_connected(self):
        return self.connected and self.producer != None
    
    def send(self, message: str) -> None:
        if not self._is_connected():
            self.connect()

        if self._is_connected():  
            try:
                self.producer.poll(0)
                self.producer.produce(self.topic, value=message.encode('utf-8'), callback=self._on_delivery)
            except Exception as e:
                logger.error(f"Error while sending kafka message: {e}", e)

class BlobKafkaClient(KafkaClient):
    def send(self, message: str) -> None:
        pass