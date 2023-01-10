from abc import ABC, abstractmethod
from typing import List, Optional, Callable, Any, Iterable

from th2_check2_recon.rule import Rule
from th2_grpc_common.common_pb2 import Event

from th2_check2_recon.util.cache.cache import Cache
from th2_check2_recon.recon_message import ReconMessage


class ProcessUnit(ABC):

    @abstractmethod
    def set_next(self, processor: 'ProcessUnit') -> 'ProcessUnit':
        pass

    @abstractmethod
    def process(self, messages: List[Any]) -> List[Event]:
        pass


class AbstractProcessUnit(ProcessUnit):

    _next_processor: Optional[ProcessUnit] = None

    def set_next(self, processor: 'ProcessUnit') -> 'ProcessUnit':
        self._next_processor = processor
        return processor

    def process(self, messages: List[Any], *args: Any, **kwargs: Any) -> List[Event]:
        if self._next_processor is not None:
            return self._next_processor.process(messages)

        return []


class GroupProcessUnit(AbstractProcessUnit):

    def __init__(self, group_method: Callable):
        self.group_method = group_method

    def process(self, messages: List[ReconMessage], *args: Any, **kwargs: Any) -> List[Event]:
        grouped_messages: List[ReconMessage] = []

        for message in messages:
            if message.group_name is None:
                self.group_method(message)
                if message.group_name is None:
                    continue
            grouped_messages.append(message)

        if len(grouped_messages) == 0:
            return []
        else:
            return super().process(grouped_messages)


class HashProcessUnit(AbstractProcessUnit):

    def __init__(self, hash_method: Callable):
        self.hash_method = hash_method

    def process(self, messages: List[ReconMessage], *args: Any, **kwargs: Any) -> List[Event]:
        hashed_messages: List[ReconMessage] = []

        for message in messages:
            if message.hash is None:
                self.hash_method(message)
                if message.hash is None:
                    continue
            hashed_messages.append(message)

        if len(hashed_messages) == 0:
            return []
        else:
            return super().process(hashed_messages)


class MatchProcessUnit(AbstractProcessUnit):

    def __init__(self, cache: Cache):
        self._cache = cache

    def process(self, messages: List[ReconMessage], *args: Any, **kwargs: Any) -> List[Event]:
        matched_messages: List[List[ReconMessage]] = []

        for message in messages:
            matching = self._get_matching(message)
            if matching is None:
                self._cache.add(message)
            else:
                matched_messages.append([message, *matching])

        if len(matched_messages) == 0:
            return []
        else:
            return super().process(matched_messages)

    def _get_matching(self, message: ReconMessage) -> Optional[Iterable[ReconMessage]]:
        return self._cache.get_matching(message)


class CheckProcessUnit(AbstractProcessUnit):

    def __init__(self, check_method: Callable):
        self.check_method = check_method

    def process(self, messages: List[List[ReconMessage]], *args: Any, **kwargs: Any) -> List[Event]:
        events: List[Event] = []
        for matched_messages in messages:
            for message in matched_messages:
                message._is_matched = True
            events.append(self.check_method(matched_messages))

        return events


def build_process_chain(rule: Rule, cache: Cache) -> ProcessUnit:
    group_processor = GroupProcessUnit(rule.group)
    hash_processor = HashProcessUnit(rule.hash)
    match_processor = MatchProcessUnit(cache)
    check_processor = CheckProcessUnit(rule.check)

    group_processor.set_next(hash_processor).set_next(match_processor).set_next(check_processor)

    return group_processor


class CleanupProcessUnit(AbstractProcessUnit):

    def __init__(self, cache: Cache, match_timeout: int):
        self._cache = cache
        self._match_timeout = match_timeout

    def process(self, messages: List[ReconMessage], *args: Any, **kwargs: Any) -> None:
        deletion_bound_timestamp = self._get_deletion_bound_timestamp(messages)
        self._cache.remove_out_of_timeout(deletion_bound_timestamp)

    def _get_deletion_bound_timestamp(self, messages: List[ReconMessage]) -> int:
        actual_timestamp = min(messages, key=lambda m: m.timestamp_s).timestamp_s
        return actual_timestamp - self._match_timeout
