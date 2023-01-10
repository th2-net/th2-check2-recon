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

import logging
import time
from typing import Any, List

from google.protobuf.empty_pb2 import Empty
from google.protobuf.text_format import MessageToString
from th2_common.schema.message.message_listener import MessageListener
from th2_common_utils import message_to_dict
from th2_grpc_common.common_pb2 import EventID, MessageBatch
from th2_grpc_crawler_data_processor.crawler_data_processor_pb2 import DataProcessorInfo, EventResponse, \
    MessageResponse, Status
from th2_grpc_crawler_data_processor.crawler_data_processor_pb2_grpc import DataProcessorServicer

from th2_check2_recon.recon_message import ReconMessage
from th2_check2_recon.util.configuration import CrawlerConnectionConfiguration

logger = logging.getLogger(__name__)


class MessageHandler(MessageListener):

    def __init__(self, message_processors: list) -> None:
        self._message_processors = message_processors

    def handler(self, attributes: tuple, batch: MessageBatch) -> None:
        try:
            dict_messages: List[dict] = [
                message_to_dict(proto_message)
                for proto_message in batch.messages
            ]

            for message_processor in self._message_processors:
                recon_messages = [ReconMessage(proto_message=dict_message) for dict_message in dict_messages]

                process_timer = message_processor.RULE_PROCESSING_TIME
                start_time = time.time()

                message_processor.process(recon_messages)
                message_processor.reprocess()

                process_timer.observe((time.time() - start_time) / len(recon_messages))

                if logger.level == logging.DEBUG:
                    logger.debug("Rule %s processed %s messages (MQ). Cache size: %s" %
                                 (message_processor._rule.get_name(), len(recon_messages), message_processor.log_groups_size()))

        except Exception as e:
            logger.exception(f'An error occurred while processing the received message batch: {e}')


class GRPCHandler(DataProcessorServicer):

    def __init__(self,
                 message_processors: list,
                 crawler_connection_configuration:
                 CrawlerConnectionConfiguration,
                 root_event_id: EventID) -> None:
        self._message_processors: list = message_processors
        self._crawler_connection_configuration = crawler_connection_configuration
        self._recon_root_event_id: EventID = root_event_id

    def CrawlerConnect(self, request: Any, context: Any) -> DataProcessorInfo:
        # logger.debug('CrawlerId %s connected' % request.id.name)
        return DataProcessorInfo(name=self._crawler_connection_configuration.name,
                                 version=self._crawler_connection_configuration.version)

    def IntervalStart(self, request: Any, context: Any) -> Empty:
        # logger.debug('Interval set from %s to %s' % (request.start_time, request.end_time))
        return Empty()

    def SendEvent(self, request: Any, context: Any) -> EventResponse:
        # logger.debug('CrawlerID %s sent events %s' %
        #              (request.id.name, MessageToString(request.event_data, as_one_line=True)))
        return EventResponse(id=self._recon_root_event_id, status=Status(handshake_required=False))

    def SendMessage(self, request: Any, context: Any) -> MessageResponse:
        try:
            # logger.debug('CrawlerID %s sent %s messages' % (request.id.name, len(request.message_data)))
            dict_messages = [message_to_dict(data.message) for data in request.message_data]

            for message_processor in self._message_processors:
                recon_messages = [ReconMessage(proto_message=dict_message) for dict_message in dict_messages]
                process_timer = message_processor.RULE_PROCESSING_TIME
                start_time = time.time()

                message_processor.process(recon_messages)
                message_processor.reprocess()

                process_timer.observe(time.time() - start_time)

                # logger.debug("Rule %s processed %s messages (gRPC). Cache size: %s" %
                #              (rule.get_name(), len(recon_messages), rule.log_groups_size()))

            return MessageResponse(ids=[data.message.metadata.id for data in request.message_data],
                                   status=Status(handshake_required=False))

        except Exception as e:
            logger.exception(f'SendMessage request failed: {e}')
            return MessageResponse(ids=[], status=Status(handshake_required=True))
