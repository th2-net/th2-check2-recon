# Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

from th2_common.schema.event.event_batch_router import EventBatchRouter
from th2_common.schema.grpc.router.grpc_router import GrpcRouter
from th2_common.schema.message.message_router import MessageRouter
from th2_grpc_util.util_service import MessageComparatorServiceService

from th2_check2_recon.configuration import ReconConfiguration
from th2_check2_recon.rule import Rule
from th2_check2_recon.services import EventStore
from th2_check2_recon.services import MessageComparator

logger = logging.getLogger()


class Recon:
    def __init__(self, event_router: EventBatchRouter, grpc_router: GrpcRouter, message_router: MessageRouter,
                 custom_config: dict) -> None:
        logger.info('Recon initializing...')
        self.__message_router = message_router
        self.__message_comparator = MessageComparator(grpc_router.get_service(MessageComparatorServiceService))
        self.__config = ReconConfiguration(**custom_config)

        self.__event_store = EventStore(event_router=event_router,
                                        report_name=self.__config.recon_name,
                                        event_batch_max_size=self.__config.event_batch_max_size,
                                        event_batch_send_interval=self.__config.event_batch_send_interval)

        self.__loop = asyncio.get_event_loop()
        self.__cnt = 0
        self.__border = 0
        self.__step_cnt_border = 100

        self.rules = []

    def start(self):
        try:
            logger.info('Recon running...')
            self.rules = self.__load_rules(self.__event_store, self.__message_comparator)
            for rule in self.rules:
                for attrs in rule.get_attributes():
                    self.__message_router.subscribe_all(rule.get_listener(), *attrs)
            logger.info('Recon started!')
            self.__loop.run_forever()
        except Exception:
            logger.exception(f'Recon running error')

    def stop(self):
        logger.info(f'Recon try to stop')
        try:
            self.__message_router.unsubscribe_all()
            for rule in self.rules:
                rule.stop()
            self.__message_comparator.stop()
            self.__event_store.stop()
        except Exception:
            logger.exception('Error while stop Recon')
        finally:
            self.__loop.close()
            logger.info(f'Recon stopped!')

    def __load_rules(self, event_store: EventStore, message_comparator: MessageComparator) -> [Rule]:
        logger.info(f'Try load rules')
        rules_package = importlib.import_module(self.__config.rules_package_path)
        loaded_rules = []
        for rule_config in self.__config.rules:
            if rule_config.enabled:
                module = importlib.import_module(rules_package.__name__ + '.' + rule_config.name)
                match_timeout = rule_config.match_timeout * 1_000_000_000 + rule_config.match_timeout_offset_ns
                loaded_rules.append(module.Rule(event_store=event_store,
                                                message_comparator=message_comparator,
                                                cache_size=self.__config.cache_size,
                                                match_timeout=match_timeout,
                                                configuration=rule_config.configuration))
        logger.info(f'Rules loaded')
        return loaded_rules
