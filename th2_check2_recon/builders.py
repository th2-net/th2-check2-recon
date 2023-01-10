import importlib
import logging
from abc import abstractmethod, ABC
from time import sleep
from typing import Any, Optional, List, Dict

from th2_grpc_common.common_pb2 import Event, EventID

from th2_check2_recon.recon import Recon
from th2_check2_recon.recon_message import MessageGroupDescription
from th2_check2_recon.rule import Rule
from th2_check2_recon.util.cache.cache import MessageGroup
from th2_check2_recon.util.cache.message_groups import SingleMG, SingleSharedMG
from th2_check2_recon.util.configuration import ReconConfiguration
from th2_check2_recon.util.events.event_manager import EventManager
from th2_check2_recon.util.events.event_producers import EventPack, EventProducer
from th2_check2_recon.util.message_processor import MessageProcessor

logger = logging.getLogger(__name__)


class ReconBuilder(ABC):

    @property
    @abstractmethod
    def result(self) -> Any:
        pass

    @abstractmethod
    def add_recon(self) -> None:
        pass

    @abstractmethod
    def add_message_processors(self) -> None:
        pass


class ReconAppBuilder(ReconBuilder):
    _recon: Optional[Recon] = None

    def __init__(self, recon_name: str, rules: List[Rule]) -> None:
        self._recon_name = recon_name
        self._rules = rules

    @property
    def result(self) -> Recon:
        if self._recon is not None:
            return self._recon
        else:
            raise AttributeError('Referencing Recon object before its creation.')

    def add_recon(self) -> None:
        self._recon = Recon(self._recon_name)

    def add_message_processors(self) -> None:
        for metric_number, rule in enumerate(self._rules, start=1):
            message_processor = MessageProcessor(rule, metric_number)

            for group_name, group_description in rule.description_of_groups().items():
                if group_description.shared:
                    message_group = self._get_shared_message_group(group_name, group_description)
                else:
                    message_group = SingleMG(group_name, group_description)
                message_processor.add_message_group(message_group)
            self._recon.add_message_processor(message_processor)

    def _get_shared_message_group(self, group_name: str, group_description: MessageGroupDescription) -> MessageGroup:
        if group_name not in self._recon.shared_message_groups:
            message_group = SingleSharedMG(group_name, group_description)
            self._recon.shared_message_groups[group_name] = message_group
            return message_group
        else:
            return self._recon.shared_message_groups[group_name]


class EventManagerBuilder(ReconBuilder):

    def __init__(self, recon: Recon) -> None:
        self._event_manager: EventManager = EventManager()
        self._recon = recon

    @property
    def result(self) -> EventManager:
        if self._event_manager is not None:
            return self._event_manager
        else:
            raise AttributeError('Referencing EventManager object before its creation.')

    def add_recon(self) -> None:
        events = self._recon.initialize()
        self._event_manager.put_events_in_batch(*events)

        for message_group in self._recon.shared_message_groups.values():
            self._add_event_producer(message_group, self._recon.shared_messages_event_id)

    def add_message_processors(self) -> None:
        for message_processor in self._recon.message_processors:
            self._add_event_producer(message_processor, self._recon.event_id)

            parent_event_id = message_processor.get_pack_event_id(EventPack.REMOVED_MESSAGES)
            for message_group in message_processor.get_message_groups():
                if not message_group.shared:
                    self._add_event_producer(message_group, parent_event_id)

    def _add_event_producer(self, event_producer: EventProducer, parent_event_id: Optional[EventID] = None) -> None:
        events = event_producer.initialize(parent_event_id)
        self._event_manager.put_events_in_batch(*events)
        self._event_manager.add_event_producer(event_producer)


class ReconManager:
    recon = None
    event_manager = None
    _recon_configuration = None

    def build_recon(self, recon_configuration: ReconConfiguration) -> None:
        self._recon_configuration = recon_configuration

        recon_app_builder = ReconAppBuilder(recon_name=recon_configuration.recon_name,
                                            rules=self._load_rules(recon_configuration))
        self.recon = self._build(recon_app_builder)

        event_manager_builder = EventManagerBuilder(self.recon)
        self.event_manager = self._build(event_manager_builder)

    def start_recon(self, event_router, message_router, grpc_server):
        logger.info('Recon is starting...')
        self.event_manager.start(event_router, self._recon_configuration.event_batch_send_interval)
        self.recon.start(message_router, self._recon_configuration, grpc_server)
        logger.info('Recon have started!')

    def stop_recon(self):
        pass

    @staticmethod
    def _build(builder: ReconBuilder) -> Any:
        builder.add_recon()
        builder.add_message_processors()
        return builder.result

    @staticmethod
    def _load_rules(recon_configuration: ReconConfiguration) -> list:
        logger.info('Start loading rules')
        rules_package = importlib.import_module(recon_configuration.rules_package_path)
        loaded_rules = []
        for number, rule_config in enumerate(recon_configuration.rules, start=1):
            if rule_config.enabled:
                module = importlib.import_module(rules_package.__name__ + '.' + rule_config.name)
                loaded_rules.append(module.Rule(configuration=rule_config))
        logger.info('Rules were loaded')
        return loaded_rules

    @staticmethod
    def _check_shared_message_groups(rules: List[Rule]) -> None:
        shared_group_descriptions_by_group_name: Dict[str, MessageGroupDescription] = {}

        for rule in rules:
            for group_name, group_description in rule.description_of_groups().items():
                if group_description.shared:
                    if group_name not in shared_group_descriptions_by_group_name:
                        shared_group_descriptions_by_group_name[group_name] = group_description
                    elif shared_group_descriptions_by_group_name[group_name] != group_description:
                        raise AttributeError(f'Shared group "{group_name}" should have the same '
                                             f'attributes in all rules')
