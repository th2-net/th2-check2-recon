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
from abc import abstractmethod
from typing import Dict, List, Optional

from prometheus_client import Histogram
from th2_common.schema.metrics import common_metrics
from th2_grpc_common.common_pb2 import Event

from th2_check2_recon.recon_message import MessageGroupDescription, ReconMessage
from th2_check2_recon.util.configuration import RuleConfiguration

logger = logging.getLogger(__name__)


class Rule:

    def __init__(self, configuration: RuleConfiguration) -> None:
        self.configuration = configuration
        self.unique_id: Optional[str] = None

        self.configure(configuration.custom_configuration)
        self.__match_timeout = configuration.match_timeout
        self.__reprocess_queue: List[ReconMessage] = []

        logger.info(f"Rule '{self.get_name()}' was initialized")

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_description(self) -> str:
        pass

    @abstractmethod
    def description_of_groups(self) -> Dict[str, MessageGroupDescription]:
        """
            Returns a dictionary whose key is group_name and
            value is MessageGroupDescription.
            MessageGroupDescription have boolean fields single, multi,
            shared and ignore_no_match.
        """
        pass

    @abstractmethod
    def group(self, message: ReconMessage) -> None:
        pass

    @abstractmethod
    def hash(self, message: ReconMessage) -> None:
        pass

    @abstractmethod
    def check(self, messages: List[ReconMessage]) -> Optional[Event]:
        pass

    def configure(self, configuration: str) -> None:
        pass

    def put_shared_message(self, message: ReconMessage) -> None:
        self.__recon.put_shared_message(message)

    def put_in_queue_for_retransmitting(self, message: ReconMessage) -> None:
        self.__reprocess_queue.append(message)

    def cache_size(self) -> int:
        return self.__cache.size

    def log_groups_size(self) -> str:
        return '[' + ', '.join(f'"{group_name}": {group_size} msg' for group_name, group_size
                               in self.__cache.groups_size.items()) + ']'

    # TODO: implement rule stop()
