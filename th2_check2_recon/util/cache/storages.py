from typing import Dict, Optional, Set, List

import sortedcollections

from th2_check2_recon.recon_message import ReconMessage


class MessageByHashStorage:

    def __init__(self) -> None:
        self._message_by_hash: Dict[int, ReconMessage] = {}

    def __contains__(self, item: int) -> bool:
        return item in self._message_by_hash

    def add(self, message: ReconMessage) -> None:
        self._message_by_hash[message.hash] = message

    def remove_out_of_timeout(self, _hash: int) -> ReconMessage:
        removed_message = self._message_by_hash[_hash]
        self.remove(_hash)
        return removed_message

    def get(self, _hash: int) -> Optional[ReconMessage]:
        return self._message_by_hash.get(_hash)

    @property
    def size(self) -> int:
        return len(self._message_by_hash.values())

    def remove(self, _hash: int) -> ReconMessage:
        removed_message = self._message_by_hash[_hash]
        del self._message_by_hash[_hash]
        return removed_message


class HashByTimestampStorage:

    def __init__(self) -> None:
        self._hash_by_timestamp: sortedcollections.SortedDict[int, Set[int]] = sortedcollections.SortedDict()

    def add(self, message: ReconMessage) -> None:
        self._hash_by_timestamp.setdefault(message.timestamp_s, set()).add(message.hash)

    def remove(self, message: ReconMessage) -> None:
        self._hash_by_timestamp[message.timestamp_s].remove(message.hash)

    def remove_out_of_timeout(self, deletion_bound_timestamp: int) -> List[int]:
        out_of_timeout_hashes = []

        for timestamp, hashes in self._hash_by_timestamp.items():
            if timestamp <= deletion_bound_timestamp:
                out_of_timeout_hashes.extend(hashes)
                del self._hash_by_timestamp[timestamp]
            else:
                break

        return out_of_timeout_hashes

    def remove_oldest(self) -> set:
        return self._hash_by_timestamp.popitem(0)[1]

    def get_oldest_timestamp(self) -> int:
        return self._hash_by_timestamp.peekitem(0)[0]
