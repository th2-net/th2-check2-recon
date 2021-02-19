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

import asyncio
import importlib
import logging
from typing import Optional

from th2_common.schema.event.event_batch_router import EventBatchRouter
from th2_common.schema.message.message_router import MessageRouter

from th2_check2_recon.configuration import ReconConfiguration
from th2_check2_recon.reconcommon import MessageGroupType, ReconMessage
from th2_check2_recon.services import EventStore, AbstractService

logger = logging.getLogger()


class Recon:
    def __init__(self, event_router: EventBatchRouter, message_router: MessageRouter,
                 custom_config: dict, message_comparator: Optional[AbstractService] = None) -> None:
        logger.info('Recon initializing...')
        self.rules = []
        self.__loop = asyncio.get_event_loop()
        self.__config = ReconConfiguration(**custom_config)
        self.__message_router = message_router

        self.message_comparator = message_comparator
        self.event_store = EventStore(event_router=event_router,
                                      report_name=self.__config.recon_name,
                                      event_batch_max_size=self.__config.event_batch_max_size,
                                      event_batch_send_interval=self.__config.event_batch_send_interval)

    def start(self):
        try:
            logger.info('Recon running...')
            self.rules = self.__load_rules()
            for rule in self.rules:
                for attrs in rule.get_attributes():
                    self.__message_router.subscribe_all(rule.get_listener(), *attrs)
            logger.info('Recon started!')
            self.__loop.run_forever()
        except Exception:
            logger.exception(f'Recon running error')

    def stop(self):
        logger.info('Recon try to stop')
        try:
            self.__message_router.unsubscribe_all()
            for rule in self.rules:
                rule.stop()
            if self.message_comparator is not None:
                self.message_comparator.stop()
            self.event_store.stop()
        except Exception:
            logger.exception('Error while stop Recon')
        finally:
            self.__loop.close()
            logger.info('Recon stopped!')

    def __load_rules(self):
        logger.info('Try load rules')
        rules_package = importlib.import_module(self.__config.rules_package_path)
        loaded_rules = []
        for rule_config in self.__config.rules:
            if rule_config.enabled:
                module = importlib.import_module(rules_package.__name__ + '.' + rule_config.name)
                match_timeout = rule_config.match_timeout * 1_000_000_000 + rule_config.match_timeout_offset_ns
                loaded_rules.append(module.Rule(recon=self,
                                                cache_size=self.__config.cache_size,
                                                match_timeout=match_timeout,
                                                configuration=rule_config.configuration))
        logger.info('Rules loaded')
        return loaded_rules

    def put_shared_message(self, shared_group_id: str, message: ReconMessage, attributes: tuple):
        for rule in self.rules:
            groups = rule.description_of_groups()
            if shared_group_id in groups.keys() and groups[shared_group_id] == MessageGroupType.shared:
                rule.process(message, attributes)
