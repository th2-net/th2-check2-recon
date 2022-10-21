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

from abc import ABC, abstractmethod
import logging
import time
from typing import Any

from google.protobuf.empty_pb2 import Empty
from google.protobuf.text_format import MessageToString
from th2_check2_recon.configuration import CrawlerConnectionConfiguration
from th2_check2_recon.reconcommon import ReconMessage, ReconMessageUtils
from th2_common.schema.message.message_listener import MessageListener
from th2_common_utils import message_to_dict
from th2_grpc_common.common_pb2 import EventID, MessageBatch
from th2_grpc_crawler_data_processor.crawler_data_processor_pb2 import DataProcessorInfo, EventResponse, \
    MessageResponse, Status
from th2_grpc_crawler_data_processor.crawler_data_processor_pb2_grpc import DataProcessorServicer

logger = logging.getLogger(__name__)


class AbstractHandler(MessageListener, ABC):
    def __init__(self, rule: Any) -> None:
        self._rule = rule

    @abstractmethod
    def handler(self, attributes: tuple, batch: Any) -> Any:
        pass


class MessageHandler(AbstractHandler):
    def handler(self, attributes: tuple, batch: MessageBatch) -> None:
        try:
            for proto_message in batch.messages:
                data = message_to_dict(proto_message)
                message = ReconMessage(proto_message=data)

                process_timer = self._rule.RULE_PROCESSING_TIME
                start_time = time.time()

                self._rule.clear_out_of_timeout(message.timestamp_ns)
                self._rule.check_no_match_within_timeout(actual_timestamp=message.timestamp,
                                                         actual_timestamp_ns=message.timestamp_ns)
                if self._rule.has_been_grouped(message, attributes):
                    self._rule.process(message, attributes)

                process_timer.observe(time.time() - start_time)

                logger.debug("Processed '%s' id='%s'" %
                             (data['metadata']['message_type'], ReconMessageUtils.str_message_id(data)))

            logger.debug("Cache size '%s': %s." % (self._rule.get_name(), self._rule.log_groups_size()))

        except Exception as e:
            logger.exception(f'Rule: {self._rule.get_name()}. '
                             f'An error occurred while processing the received message: {e}\n'
                             f'Message: {MessageToString(batch, as_one_line=True)}')


class GRPCHandler(DataProcessorServicer):

    def __init__(self,
                 rules: list,
                 crawler_connection_configuration:
                 CrawlerConnectionConfiguration,
                 root_event_id: EventID) -> None:
        self._rules: list = rules
        self._crawler_connection_configuration = crawler_connection_configuration
        self._recon_root_event_id: EventID = root_event_id

    def CrawlerConnect(self, request: Any, context: Any) -> DataProcessorInfo:
        logger.debug('CrawlerId %s connected' % request.id.name)
        return DataProcessorInfo(name=self._crawler_connection_configuration.name,
                                 version=self._crawler_connection_configuration.version)

    def IntervalStart(self, request: Any, context: Any) -> Empty:
        logger.debug('Interval set from %s to %s' % (request.start_time, request.end_time))
        return Empty()

    def SendEvent(self, request: Any, context: Any) -> EventResponse:
        logger.debug('CrawlerID %s sent events %s' %
                     (request.id.name, MessageToString(request.event_data, as_one_line=True)))
        return EventResponse(id=self._recon_root_event_id, status=Status(handshake_required=False))

    def SendMessage(self, request: Any, context: Any) -> MessageResponse:
        try:
            logger.debug('CrawlerID %s sent %s messages' % (request.id.name, len(request.message_data)))
            messages = [data.message for data in request.message_data]
            for proto_message in messages:
                data = message_to_dict(proto_message)
                for rule in self._rules:
                    message = ReconMessage(proto_message=data)
                    process_timer = rule.RULE_PROCESSING_TIME
                    start_time = time.time()
                    try:
                        rule.clear_out_of_timeout(message.timestamp_ns)
                        rule.check_no_match_within_timeout(actual_timestamp=message.timestamp,
                                                           actual_timestamp_ns=message.timestamp_ns)
                        if rule.has_been_grouped(message, ()):
                            rule.process(message, ())

                    except Exception as e:
                        logger.exception(
                            f'Rule: {rule.get_name()}. '
                            f'An error occurred while processing the message: {e}\n'
                            f'Message: {MessageToString(proto_message, as_one_line=True)}')
                    finally:
                        process_timer.observe(time.time() - start_time)
                logger.debug("Processed '%s' id='%s'"
                             % (data['metadata']['message_type'], ReconMessageUtils.str_message_id(data)))
            return MessageResponse(ids=[msg.metadata.id for msg in messages], status=Status(handshake_required=False))
        except Exception as e:
            logger.exception(f'SendMessage request failed: {e}')
            return MessageResponse(ids=[], status=Status(handshake_required=True))
