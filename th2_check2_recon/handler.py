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
from abc import ABC, abstractmethod

from google.protobuf.text_format import MessageToString
from th2_common.schema.message.message_listener import MessageListener
from th2_grpc_check2_recon.check2_recon_pb2_grpc import Check2ReconServicer
from th2_grpc_common.common_pb2 import MessageBatch, RequestStatus

from th2_check2_recon.common import MessageUtils
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
                self._rule.process(message, attributes)
                logger.debug("  Processed '%s' id='%s'",
                             proto_message.metadata.message_type,
                             MessageUtils.str_message_id(proto_message))

            logger.debug("  Cache size '%s': %s.", self._rule.get_name(), self._rule.log_groups_size())
        except Exception:
            logger.exception(f'Rule: {self._rule.get_name()}. '
                             f'An error occurred while processing the received message. '
                             f'Message: {MessageToString(batch, as_one_line=True)}')


class GRPCHandler(Check2ReconServicer):

    def __init__(self, rules: list) -> None:
        self._rules = rules

    def submitGroupBatch(self, request, context):
        try:
            logger.debug(f'submitGroupBatch request: {MessageToString(request, as_one_line=True)}')

            messages = [message.message for group in request.groups
                        for message in group.messages if message.HasField('message')]

            for proto_message in messages:
                message = ReconMessage(proto_message=proto_message)

                for rule in self._rules:
                    try:
                        rule.process((), message)
                    except Exception:
                        logger.exception(f'Rule: {rule.get_name()}. '
                                         f'An error occurred while processing the message. '
                                         f'Message: {MessageToString(proto_message, as_one_line=True)}')

                logger.debug(f"Processed '{proto_message.metadata.message_type}' "
                             f"id='{MessageUtils.str_message_id(proto_message)}'")

            return RequestStatus(status=RequestStatus.SUCCESS, message='Successfully processed batch')
        except Exception as e:
            logger.exception('submitGroupBatch request failed')
            return RequestStatus(status=RequestStatus.ERROR, message=str(e))
