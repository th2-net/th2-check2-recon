import enum
from abc import ABC, abstractmethod
from threading import Lock, Timer
from typing import Dict, List, Optional, Callable, Iterable

from th2_common.schema.message.message_router import MessageRouter
from th2_common_utils import event_utils
from th2_grpc_common.common_pb2 import EventBatch, EventID, Event, EventStatus

from th2_check2_recon.recon_message import ReconMessage
from th2_check2_recon.utils import MessageComponent


class EventType(str, enum.Enum):
    ROOT = 'ReconRoot'
    RULE = 'ReconRule'
    STATUS = 'ReconStatus'
    EVENT = 'ReconEvent'
    UNKNOWN = ''


class EventPack(str, enum.Enum):
    FAILED_MATCHES = 'Failed matches'
    PASSED_MATCHES = 'Passed matches'
    REMOVED_MESSAGES = 'Removed messages'


class EventProducer(ABC):
    _event_id: Optional[EventID] = None

    def __init__(self, name: str, description: str = ''):
        self.name = name
        self.description = description

    @abstractmethod
    def initialize(self, parent_event_id: Optional[EventID] = None):
        pass

    def fetch_events(self) -> List[Event]:
        pass

    @property
    def event_id(self) -> EventID:
        if self._event_id is not None:
            return self._event_id
        else:
            raise AttributeError("Trying to access Rule's event ID before its creation.")

    @abstractmethod
    def _create_root_event(self, parent_event_id: Optional[EventID]) -> Event:
        pass


class MessageGroupEventProducer(EventProducer):

    def __init__(self, name: str, description: str = ''):
        super().__init__(name, description)
        self._removed_out_of_timeout_messages: List[ReconMessage] = []
        self._removed_old_messages: List[ReconMessage] = []
        self._removed_duplicate_messages: List[ReconMessage] = []

    def initialize(self, parent_event_id: Optional[EventID] = None) -> List[Event]:
        event = self._create_root_event(parent_event_id)
        self._event_id = event.id
        return [event]

    def fetch_events(self) -> List[Event]:
        events = []
        if len(self._removed_out_of_timeout_messages) > 0:
            events.append(self.__create_removed_out_of_timeout_event())
        if len(self._removed_old_messages) > 0:
            events.append(self.__create_removed_old_event())
        if len(self._removed_duplicate_messages) > 0:
            events.append(self.__create_removed_duplicate_messages())
        return events

    def _create_root_event(self, parent_event_id: Optional[EventID]) -> Event:
        return event_utils.create_event(name=self.name,
                                        parent_id=parent_event_id,
                                        event_type=EventType.RULE)

    def __create_removed_out_of_timeout_event(self) -> Event:
        removed_out_of_timeout_messages_info = [message.all_info for message in self._removed_out_of_timeout_messages]
        self._removed_out_of_timeout_messages.clear()
        return event_utils.create_event(name=f"Timeout is over: remove {len(removed_out_of_timeout_messages_info)} "
                                             f"message(s)",
                                        parent_id=self._event_id,
                                        body=event_utils.create_event_body(
                                            MessageComponent(message='\n'.join(removed_out_of_timeout_messages_info))),
                                        event_type=EventType.EVENT,
                                        status=EventStatus.FAILED)

    def __create_removed_old_event(self) -> Event:
        removed_old_messages_info = [message.all_info for message in self._removed_old_messages]
        self._removed_old_messages.clear()
        return event_utils.create_event(name=f"No free space in cache: remove {len(removed_old_messages_info)} "
                                             f"message(s)",
                                        parent_id=self._event_id,
                                        body=event_utils.create_event_body(
                                            MessageComponent(message='\n'.join(removed_old_messages_info))),
                                        event_type=EventType.EVENT,
                                        status=EventStatus.FAILED)

    def __create_removed_duplicate_messages(self) -> Event:
        removed_duplicate_messages_info = [message.all_info for message in self._removed_duplicate_messages]
        self._removed_duplicate_messages.clear()
        return event_utils.create_event(name=f"Duplicates: remove {len(removed_duplicate_messages_info)} "
                                             f"message(s)",
                                        parent_id=self._event_id,
                                        body=event_utils.create_event_body(
                                            MessageComponent(message='\n'.join(removed_duplicate_messages_info))),
                                        event_type=EventType.EVENT,
                                        status=EventStatus.FAILED)


class MessageProcessorEventProducer(EventProducer):

    def __init__(self, name: str, description: str = ''):
        super().__init__(name, description)
        self._match_events: List[Event] = []
        self._event_packs: Optional[Dict[str, EventID]] = None

    def initialize(self, parent_event_id: Optional[EventID] = None):
        event = self._create_root_event(parent_event_id)
        self._event_id = event.id

        packs_events = self._create_packs_events(self._event_id)
        self._event_packs = {event.name: event.id for event in packs_events}

        return [event, *packs_events]

    def fetch_events(self) -> List[Event]:
        events = self._match_events
        self._match_events = []
        return events

    def get_pack_event_id(self, pack: EventPack) -> EventID:
        if self._event_packs is not None:
            return self._event_packs[pack]
        else:
            raise AttributeError("Trying to access Rule's pack event ID before its creation.")

    def _create_root_event(self, parent_event_id: Optional[EventID]) -> Event:
        return event_utils.create_event(name=self.name,
                                        parent_id=parent_event_id,
                                        body=event_utils.create_event_body(
                                            MessageComponent(message=self.description)),
                                        event_type=EventType.RULE)

    @staticmethod
    def _create_packs_events(event_id: EventID) -> List[Event]:
        return [
            event_utils.create_event(name=_enum.value,
                                     parent_id=event_id,
                                     event_type=EventType.STATUS)
            for _enum in EventPack
        ]

    def _store_match_events(self, match_events: List[Event]) -> None:
        for event in match_events:
            event_pack = EventPack.PASSED_MATCHES if event.status == EventStatus.SUCCESS else EventPack.FAILED_MATCHES
            event.parent_id.CopyFrom(self._event_packs[event_pack])
            self._match_events.append(event)


class ReconEventProducer(EventProducer):

    def __init__(self, name: str, description: str = ''):
        super().__init__(name, description)
        _shared_messages_event_id: Optional[EventID] = None

    def initialize(self, parent_event_id: Optional[EventID] = None):
        event = self._create_root_event(parent_event_id)
        self._event_id = event.id
        shared_messages_event = self._create_shared_messages_event()

        return [event, shared_messages_event]

    @property
    def shared_messages_event_id(self) -> EventID:
        if self._shared_messages_event_id is not None:
            return self._shared_messages_event_id
        else:
            raise AttributeError("Trying to access Recon's shared messages event ID before its creation.")

    def _create_root_event(self, parent_event_id: Optional[EventID]) -> Event:
        root_event = event_utils.create_event(name=f'Recon: {self.name}', event_type=EventType.ROOT)
        self._event_id = root_event.id
        return root_event

    def _create_shared_messages_event(self) -> Event:
        shared_messages_event = event_utils.create_event(parent_id=self._event_id,
                                                         name='Removed shared messages',
                                                         event_type=EventType.RULE)
        self._shared_messages_event_id = shared_messages_event.id
        return shared_messages_event
