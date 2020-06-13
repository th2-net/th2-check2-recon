import rule
from th2 import infra_pb2


class Rule(rule.Rule):

    def get_name(self) -> str:
        return "Rule_1_demo"

    def get_description(self) -> str:
        return "Rule_1 is used for demo"

    def hash(self, message: infra_pb2.Message) -> str:
        pass

    def check(self, first: infra_pb2.Message, second: infra_pb2.Message) -> infra_pb2.Event:
        pass
