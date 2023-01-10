from abc import abstractmethod
from typing import List, Dict

from th2_check2_recon.recon_message import ReconMessage, MessageGroupDescription
from th2_check2_recon.utils import VerificationComponent


class MessageGroup:

    @abstractmethod
    def group(self, message: ReconMessage) -> None:
        pass

    @abstractmethod
    def hash(self, message: ReconMessage) -> None:
        pass


class NewOrderSingleGroup(MessageGroup):

    def group(self, message: ReconMessage) -> None:
        message.group_name = 'NewOrderSingle'

    def hash(self, message: ReconMessage) -> None:
        message.hash = message.proto_message['field']


class ExecutionReportGroup(MessageGroup):

    def group(self, message: ReconMessage) -> None:
        message.group_name = 'ExecutionReport'

    def hash(self, message: ReconMessage) -> None:
        message.hash = message.proto_message['field']


class Rule:

    @property
    def groups(self) -> Dict[str, MessageGroupDescription]:
        return {
            'New': MessageGroupDescription(template=ExecutionReportGroup, single=True, shared=True),
            'ER': MessageGroupDescription(single=True)
        }

    def compare(self, messages_by_group: Dict[str, ReconMessage]) -> VerificationComponent:
        pass
