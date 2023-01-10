# Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
from typing import Dict, Optional

from th2_check2_recon.util.events.event_manager import EventManager
from th2_check2_recon.util.events.event_producers import ReconEventProducer, EventType
from th2_common.schema.message.message_router import MessageRouter
from th2_common_utils import event_utils
from th2_grpc_common.common_pb2 import EventID, Event
from th2_grpc_crawler_data_processor import crawler_data_processor_pb2_grpc

from th2_check2_recon.recon_message import MessageGroupDescription, ReconMessage
from th2_check2_recon.util.cache.cache import MessageGroup
from th2_check2_recon.util.configuration import ReconConfiguration
from th2_check2_recon.util.handler import GRPCHandler, MessageHandler
from th2_check2_recon.util.message_processor import MessageProcessor

logger = logging.getLogger(__name__)


class Recon(ReconEventProducer):

    def __init__(self, name: str) -> None:
        super().__init__(name)
        logger.info('Recon initializing...')
        self.message_processors: list = []
        self.name = name

        # self.configuration = ReconConfiguration(**custom_config)
        self.message_router: Optional[MessageRouter] = None
        self.event_router: Optional[MessageRouter] = None
        # self.event_manager = EventManager(event_router, self.configuration.event_batch_send_interval)
        # self.message_comparator: Optional[MessageComparator] = message_comparator
        # self.grpc_server: Optional[Server] = grpc_server
        self.shared_message_groups: Dict[str, MessageGroup] = {}

        self.event_batch = None
        self.event_producers = None

    def add_message_processor(self, message_processor: MessageProcessor) -> None:
        self.message_processors.append(message_processor)

    def start(self, message_router: MessageRouter, recon_configuration: ReconConfiguration, grpc_server) -> None:
        try:
            self.message_router = message_router
            self.message_router.subscribe_all(MessageHandler(self.message_processors), *recon_configuration.attributes)

            if grpc_server is not None:
                grpc_handler = GRPCHandler(self.message_processors,
                                           recon_configuration.crawler_connection_configuration,
                                           self.event_id)
                crawler_data_processor_pb2_grpc.add_DataProcessorServicer_to_server(grpc_handler, grpc_server)
                grpc_server.start()

        except Exception:
            logger.exception('Recon work interrupted')
            raise

    def stop(self) -> None:
        logger.info('Recon is trying to stop')
        try:
            self.message_router.unsubscribe_all()
            for rule in self.rules:
                rule.stop()
            if self.message_comparator is not None:
                self.message_comparator.stop()
            # self.event_manager.stop()
        except Exception as e:
            logger.exception(f'Error while stopping Recon: {e}')
        finally:
            logger.info('Recon was stopped!')

    def put_shared_message(self, recon_message: ReconMessage) -> None:
        for rule in self.rules:
            if recon_message.group_name is None:
                raise AttributeError('Group name should be set in ReconMessage before putting it in shared group')
            message_group = recon_message.group_name
            rule_groups: Dict[str, MessageGroupDescription] = rule.description_of_groups()
            if message_group in rule_groups:
                if rule_groups[message_group].shared:
                    recon_message._shared = True
                else:
                    raise AttributeError(
                        f'Trying to put shared message in group that '
                        f'is not shared: {message_group}')
