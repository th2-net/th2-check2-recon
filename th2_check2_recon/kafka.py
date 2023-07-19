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
from abc import ABCMeta, abstractmethod
import logging

logger = logging.getLogger(__name__)

class KafkaConfiguration:
    def __init__(self, config: dict):
        self.topic = config.pop("topic", "default")
        self.key = config.pop("key", 'default_key')
        self.use_keys = config.pop("use_keys", False)
        self.fail_on_connection_failure = config.pop("fail_on_connection_failure", False)
        self.bootstrap_servers = config.get("bootstrap.servers", "localhost:9092")
        self.producer_config = config

class KafkaClient:
    @abstractmethod
    def send(self, message: str) -> None:
        pass

    @abstractmethod
    def send_with_topic(self, topic, key, message: str) -> None:
        pass

    @abstractmethod
    def flush(self) -> None:
        pass

class KafkaProducer(KafkaClient):
    def __init__(self, config: KafkaConfiguration):
        self.topic = config.topic
        self.key = config.key
        self.use_keys = config.use_keys
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
    
    def send_with_topic(self, topic, key, message: str) -> None:
        if not self._is_connected():
            self._connect()

        if key == None:
            key = self.key
        
        if topic == None:
            topic = self.topic

        if self._is_connected():  
            try:
                self.producer.poll(0)
                if self.use_keys:
                    self.producer.produce(topic, key=key, value=message.encode('utf-8'), callback=self._on_delivery)
                else:
                    self.producer.produce(topic, value=message.encode('utf-8'), callback=self._on_delivery)
            except Exception as e:
                logger.error(f"Error while sending kafka message: {e}", e)
    
    def send(self, message: str) -> None:
        self.send_with_topic(self.topic, self.key, message)
    
    def flush(self) -> None:
        if self.producer:
            self.producer.poll(1)

class BlobKafkaClient(KafkaClient):
    def send(self, message: str) -> None:
        pass

    def send_with_topic(self, topic, key, message: str) -> None:
        pass

    def flush(self) -> None:
        pass