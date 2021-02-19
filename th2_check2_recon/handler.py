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
import time
from abc import ABC, abstractmethod

from th2_common.schema.message.message_listener import MessageListener
from th2_grpc_common.common_pb2 import MessageBatch

from th2_check2_recon.common import MessageUtils
from th2_check2_recon.reconcommon import ReconMessage

logger = logging.getLogger()


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
                start_time = time.time_ns()
                message = ReconMessage(proto_message=proto_message)
                self._rule.process(message, attributes)
                logger.info("  Processed '%s' id='%s' in %s ms",
                            proto_message.metadata.message_type,
                            MessageUtils.str_message_id(proto_message),
                            (time.time_ns() - start_time) / 1_000_000)

            logger.info("  Cache size '%s': %s.", self._rule.get_name(), self._rule.log_groups_size())
        except Exception:
            logger.exception(f'An error occurred while processing the received message. Body: {batch}')
