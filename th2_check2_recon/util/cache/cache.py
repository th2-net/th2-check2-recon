import logging
from typing import Dict, List, Optional, Iterable

from th2_check2_recon.recon_message import ReconMessage
from th2_check2_recon.util.cache.message_groups import MessageGroup

logger = logging.getLogger(__name__)


class Cache:

    def __init__(self, capacity: int):
        self._capacity = capacity
        self._size: int = 0

        self.message_groups: Dict[str, MessageGroup] = {}

    def add_message_group(self, message_group: MessageGroup) -> None:
        self.message_groups[message_group.name] = message_group

    def add(self, message: ReconMessage) -> None:
        message_group: MessageGroup = self.message_groups[message.group_name]
        if self.size < self._capacity:
            message_group.add(message)
            self._size += 1
        else:
            for mg in self.message_groups.values():
                mg.remove_oldest()
            message_group.add(message)

    def get_matching(self, message: ReconMessage) -> Optional[List[ReconMessage]]:
        matching_messages = []
        for message_group in filter(lambda group: group.name != message.group_name, self.message_groups.values()):
            matching_message = message_group.fetch(message.hash)
            if matching_message is None:
                return None
            else:
                matching_messages.append(matching_message)
                self._size -= 1

        return matching_messages

    def remove_out_of_timeout(self, deletion_bound_timestamp: int) -> None:
        for group in self._get_groups_with_outdated_messages(deletion_bound_timestamp):
            group.remove_out_of_timeout(deletion_bound_timestamp)

    @property
    def size(self) -> int:
        return self._size

    @property
    def groups_size(self) -> Dict[str, int]:
        return {f'{group.name}[{id(group)}]': group.size for group in self.message_groups.values()}

    def _get_groups_with_outdated_messages(self, lower_bound_timestamp: int) -> Iterable[MessageGroup]:
        return filter(lambda group: group.is_not_empty() and group.get_oldest_timestamp() <= lower_bound_timestamp,
                      self.message_groups.values())
