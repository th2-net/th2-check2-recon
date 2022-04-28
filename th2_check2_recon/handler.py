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
import json
import logging
import time
from abc import ABC, abstractmethod

import google.protobuf.empty_pb2
from google.protobuf.text_format import MessageToString
from th2_common.schema.message.message_listener import MessageListener
from th2_grpc_common.common_pb2 import MessageBatch, MessageGroupBatch
from th2_grpc_crawler_data_processor.crawler_data_processor_pb2 import DataProcessorInfo, EventResponse, Status, \
    MessageResponse
from th2_grpc_crawler_data_processor.crawler_data_processor_pb2_grpc import DataProcessorServicer

from th2_check2_recon.common import MessageUtils
from th2_check2_recon.recon import Recon
from th2_check2_recon.reconcommon import ReconMessage

logger = logging.getLogger(__name__)


class AbstractHandler(MessageListener, ABC):
    def __init__(self, rule) -> None:
        self._rule = rule

    @abstractmethod
    def handler(self, attributes: tuple, batch):
        pass


class MessageHandler(AbstractHandler):
    def handler(self, attributes: tuple, batch: MessageBatch):
        try:
            for proto_message in batch.messages:
                message = ReconMessage(proto_message=proto_message)

                process_timer = self._rule.RULE_PROCESSING_TIME
                start_time = time.time()

                self._rule.process(message, attributes)

                process_timer.observe(time.time() - start_time)

                logger.debug("Processed '%s' id='%s'",
                             proto_message.metadata.message_type,
                             MessageUtils.str_message_id(proto_message))

            logger.debug("Cache size '%s': %s.", self._rule.get_name(), self._rule.log_groups_size())
        except Exception:
            logger.exception(f'Rule: {self._rule.get_name()}. '
                             f'An error occurred while processing the received message. '
                             f'Message: {MessageToString(batch, as_one_line=True)}')


class GRPCHandler(DataProcessorServicer):

    def __init__(self, recon: Recon) -> None:
        self._recon = recon

    def CrawlerConnect(self, request, context):
        logger.debug('CrawlerId {0} connected'.format(request.id.name))
        version = json.loads(open('../package_info.json').read())['package_version']
        return DataProcessorInfo(name='Recon', version=version)

    def IntervalStart(self, request, context):
        logger.debug('Interval set from {0} to {1}'.format(request.start_time, request.end_time))
        return google.protobuf.empty_pb2.Empty()

    def SendEvent(self, request, context):
        logger.debug('CrawlerID {0} sent events {1}'.format(request.id.name, MessageToString(request.event_data, as_one_line=True)))
        return EventResponse(id=self._recon.event_store.root_event.id, status=Status(handshake_required=False))

    def SendMessage(self, request, context):
        try:
            logger.debug('CrawlerID {0} sent messages {1}'.format(request.id.name, MessageToString(request.message_data, as_one_line=True)))
            batch = MessageGroupBatch()
            batch.ParseFromString(request.message_data.body_raw)
            messages = [any_message.message for group in batch.groups
                        for any_message in group.messages if any_message.HasField('message')]
            for proto_message in messages:
                message = ReconMessage(proto_message=proto_message)
                for rule in self._recon.rules:
                    process_timer = rule.RULE_PROCESSING_TIME
                    start_time = time.time()
                    try:
                        rule.process((), message)
                    except Exception:
                        logger.exception(f'Rule: {rule.get_name()}. '
                                         f'An error occurred while processing the message. '
                                         f'Message: {MessageToString(proto_message, as_one_line=True)}')
                    finally:
                        process_timer.observe(time.time() - start_time)
                logger.debug(f"Processed '{proto_message.metadata.message_type}' "
                             f"id='{MessageUtils.str_message_id(proto_message)}'")
            return MessageResponse(ids=[msg.metadata.id for msg in messages], status=Status(handshake_required=False))
        except Exception as e:
            logger.exception('SendMessage request failed')
            return MessageResponse(ids=[], status=Status(handshake_required=True))
