import logging
from abc import ABC, abstractmethod
from typing import List, Optional

from th2_common_utils import event_utils
from th2_grpc_common.common_pb2 import EventID, Event

from th2_check2_recon.recon_message import ReconMessage
from th2_check2_recon.util.cache.storages import MessageByHashStorage, HashByTimestampStorage
from th2_check2_recon.util.events.event_producers import MessageGroupEventProducer, EventType

logger = logging.getLogger(__name__)


class MessageGroup(MessageGroupEventProducer):

    def __init__(self, group_name: str, group_description=None) -> None:
        super().__init__(group_name)
        self.name = group_name
        self.shared = group_description.shared
        self._message_by_hash_storage = MessageByHashStorage()
        self._hash_by_timestamp_storage = HashByTimestampStorage()

        self._removed_out_of_timeout_messages: List[ReconMessage] = []
        self._removed_old_messages: List[ReconMessage] = []

    def __contains__(self, item: int) -> bool:
        return item in self._message_by_hash_storage

    def _add(self, message: ReconMessage) -> None:
        self._message_by_hash_storage.add(message)
        self._hash_by_timestamp_storage.add(message)

    def add(self, message: ReconMessage):
        raise RuntimeError()

    def _remove(self, message: ReconMessage) -> None:
        self._message_by_hash_storage.remove(message.hash)
        self._hash_by_timestamp_storage.remove(message)

    def remove_out_of_timeout(self, deletion_bound_timestamp: int) -> None:
        removed_hashes = self._hash_by_timestamp_storage.remove_out_of_timeout(deletion_bound_timestamp)
        self._removed_out_of_timeout_messages.extend(
            self._message_by_hash_storage.remove_out_of_timeout(_hash)
            for _hash in removed_hashes
        )

        if logger.level == logging.DEBUG:
            logger.debug(f'{len(removed_hashes)} was removed from group {self.name} because of no match within timeout. '
                         f'Current size: {self.size}')

    def remove_oldest(self) -> None:
        if self.is_not_empty():
            removed_hashes = self._hash_by_timestamp_storage.remove_oldest()
            for _hash in removed_hashes:
                self._removed_old_messages.append(self._message_by_hash_storage.remove(_hash))

    def get(self, _hash: int) -> Optional[ReconMessage]:
        return self._message_by_hash_storage.get(_hash)

    def fetch(self, _hash: int) -> Optional[ReconMessage]:
        fetched = self._message_by_hash_storage.get(_hash)
        if fetched is not None:
            self._remove(fetched)
        return fetched

    @property
    def size(self) -> int:
        return self._message_by_hash_storage.size

    def get_oldest_timestamp(self) -> int:
        return self._hash_by_timestamp_storage.get_oldest_timestamp()

    def is_not_empty(self) -> bool:
        return self.size > 0


class SingleMG(MessageGroup):

    def add(self, message: ReconMessage) -> None:
        if message.hash not in self._message_by_hash_storage:
            self._add(message)
        else:
            fetched_message = self.fetch(message.hash)
            self._removed_duplicate_messages.append(fetched_message)
            self._add(message)


class SingleSharedMG(MessageGroup):

    def __init__(self, group_name: str, group_description=None) -> None:
        super().__init__(group_name, group_description)
        self.processed_messages_ids = set()

    def add(self, message: ReconMessage) -> None:
        if id(message.proto_message) in self.processed_messages_ids:
            return

        if (existing_message := self.get(message.hash)) is None:
            self._add(message)
            self.processed_messages_ids.add(id(message.proto_message))

        elif existing_message.proto_message is not message.proto_message:
            self._remove(existing_message)
            self._removed_duplicate_messages.append(existing_message)
            self._add(message)
