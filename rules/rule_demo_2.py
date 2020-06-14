import logging

import rule
import store
from th2 import infra_pb2, message_comparator_pb2

logger = logging.getLogger()


class Rule(rule.Rule):
    def get_name(self) -> str:
        return "Rule_2_demo"

    def get_description(self) -> str:
        return "Rule_2 is used for demo"

    def hash(self, message: infra_pb2.Message) -> str:
        return str(hash(message.metadata.message_type + message.fields['TrdMatchID'].simple_value))

    def check(self, messages_by_routing_key: dict) -> infra_pb2.Event:
        settings = message_comparator_pb2.ComparisonSettings()
        messages = [msg for msg in messages_by_routing_key.values()]
        try:
            comparison_result = self.comparator.compare(messages[0], messages[1], settings).result()  # TODO fix it
            logger.debug(f"Rule: {self.get_name()}. Check success")
            return store.create_verification_event(self.rule_event_id, comparison_result)
        except Exception:
            logger.exception(
                f"Rule: {self.get_name()}. Error while send comparison request:\n"
                f"Expected:{messages[0].metadata.id.sequence}.\n" +
                f"Actual:{messages[1].metadata.id.sequence}. \n"
                f"Settings:{settings}")
