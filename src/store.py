import logging
import uuid
from datetime import datetime
from json import JSONEncoder

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import comparator
from th2 import event_store_pb2
from th2 import event_store_pb2_grpc
from th2 import infra_pb2
from th2 import message_comparator_pb2

logger = logging.getLogger()

MATCHED_FAILED = "Matched failed"
MATCHED_PASSED = "Matched passed"
MATCHED_OUT_OF_TIMEOUT = "Matched out of timeout"
NO_MATCH_WITHIN_TIMEOUT = "No match within timeout"
NO_MATCH = "No match"
ERRORS = "Errors"


def new_event_id():
    return infra_pb2.EventID(id=str(uuid.uuid1()))


class Store:

    def __init__(self, event_store_uri, report_name: str) -> None:
        self.event_store_uri = event_store_uri
        self.report_id = new_event_id()
        self.send_event_group(self.report_id, None, 'Recon_' + report_name)
        self.event_group_by_rule_id = dict()
        self.event_group_names = [MATCHED_FAILED, MATCHED_PASSED, MATCHED_OUT_OF_TIMEOUT, NO_MATCH_WITHIN_TIMEOUT,
                                  NO_MATCH, ERRORS]

    def send_event(self, event: infra_pb2.Event):
        with grpc.insecure_channel(self.event_store_uri) as channel:
            try:
                store_stub = event_store_pb2_grpc.EventStoreServiceStub(channel)
                event_response = store_stub.StoreEvent(event_store_pb2.StoreEventRequest(event=event))
                logger.debug("Event id: %r" % event_response)
            except Exception:
                logger.exception("Error while send event")

    def send_event_group(self, event_id: infra_pb2.EventID, parent_id: infra_pb2.EventID, name: str):
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        status = infra_pb2.EventStatus.FAILED if name == MATCHED_FAILED or name == ERRORS \
            else infra_pb2.EventStatus.SUCCESS
        event = infra_pb2.Event(id=event_id,
                                name=name,
                                status=status,
                                start_timestamp=Timestamp(seconds=seconds, nanos=nanos))
        if parent_id is not None:
            event.parent_id.CopyFrom(parent_id)
        self.send_event(event)

    def store_no_match_within_timeout(self, rule_event_id: infra_pb2.EventID, message: infra_pb2.Message,
                                      event_message: str):
        event = infra_pb2.Event()
        event.id.CopyFrom(new_event_id())
        event.parent_id.CopyFrom(self.event_group_by_rule_id[rule_event_id.id][NO_MATCH_WITHIN_TIMEOUT])
        event.name = "Removed from cache. (Change it)"  # TODO change it
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        event.start_timestamp.CopyFrom(Timestamp(seconds=seconds, nanos=nanos))
        event.attached_message_ids.append(message.metadata.id)
        message_bytes = ComponentEncoder().encode(MessageComponent(event_message)).encode()
        event.body = message_bytes + event.body  # Encode to bytes
        self.send_event(event)

    def store_no_match(self, rule_event_id: infra_pb2.EventID, message: infra_pb2.Message, event_message: str):
        event = infra_pb2.Event()
        event.id.CopyFrom(new_event_id())
        event.parent_id.CopyFrom(self.event_group_by_rule_id[rule_event_id.id][NO_MATCH])
        event.name = "Removed from cache. (Change it)"  # TODO change it
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        event.start_timestamp.CopyFrom(Timestamp(seconds=seconds, nanos=nanos))
        event.attached_message_ids.append(message.metadata.id)
        message_bytes = ComponentEncoder().encode(MessageComponent(event_message)).encode()
        event.body = message_bytes + event.body  # Encode to bytes
        self.send_event(event)

    def store_error(self, rule_event_id: infra_pb2.EventID, message: infra_pb2.Message, event_message: str):
        event = infra_pb2.Event()
        event.id.CopyFrom(new_event_id())
        event.parent_id.CopyFrom(self.event_group_by_rule_id[rule_event_id.id][ERRORS])
        event.name = "Error. (Change it)"  # TODO change it
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        event.start_timestamp.CopyFrom(Timestamp(seconds=seconds, nanos=nanos))
        event.attached_message_ids.append(message.metadata.id)
        message_bytes = ComponentEncoder().encode(MessageComponent(event_message)).encode()
        event.body = message_bytes + event.body  # Encode to bytes
        self.send_event(event)

    def store_matched_out_of_timeout(self, rule_event_id: infra_pb2.EventID, check_event: infra_pb2.Event, min_time,
                                     max_time):
        check_event.id.CopyFrom(new_event_id())
        check_event.parent_id.CopyFrom(self.event_group_by_rule_id[rule_event_id.id][MATCHED_OUT_OF_TIMEOUT])
        self.send_event(check_event)

    def store_matched(self, rule_event_id: infra_pb2.EventID, check_event: infra_pb2.Event):
        check_event.id.CopyFrom(new_event_id())
        if check_event.status == infra_pb2.EventStatus.SUCCESS:
            check_event.parent_id.CopyFrom(self.event_group_by_rule_id[rule_event_id.id][MATCHED_PASSED])
        else:
            check_event.parent_id.CopyFrom(self.event_group_by_rule_id[rule_event_id.id][MATCHED_FAILED])
        self.send_event(check_event)

    def create_event_groups(self, rule_event_id: infra_pb2.EventID, name: str, description: str):
        event = infra_pb2.Event()
        event.id.CopyFrom(rule_event_id)
        event.parent_id.CopyFrom(self.report_id)
        event.name = name
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        event.start_timestamp.CopyFrom(Timestamp(seconds=seconds, nanos=nanos))
        message_bytes = ComponentEncoder().encode(MessageComponent(description)).encode()
        event.body = message_bytes + event.body  # Encode to bytes
        self.send_event(event)
        for group_name in self.event_group_names:
            event_id = new_event_id()
            if not self.event_group_by_rule_id.__contains__(rule_event_id.id):
                self.event_group_by_rule_id[rule_event_id.id] = dict()
            self.event_group_by_rule_id[rule_event_id.id][group_name] = event_id
            self.send_event_group(event_id, rule_event_id, group_name)


def create_verification_event(parent_id: infra_pb2.EventID,
                              compare_result: message_comparator_pb2.CompareMessageVsMessageResult,
                              field_value_by_name: dict) -> infra_pb2.Event:
    verification_component = VerificationBuilder()
    if len(compare_result.comparison_result.fields.keys()) > 0:
        for field_name in compare_result.comparison_result.fields.keys():
            verification_component.verification(field_name, compare_result.comparison_result.fields[field_name])

    start_time = datetime.now()
    seconds = int(start_time.timestamp())
    nanos = int(start_time.microsecond * 1000)
    status = infra_pb2.EventStatus.FAILED if comparator.get_status_type(
        compare_result.comparison_result) == message_comparator_pb2.ComparisonEntryStatus.FAILED \
        else infra_pb2.EventStatus.SUCCESS
    event_name = "Check"
    for field_name in field_value_by_name.keys():
        event_name += f" '{field_name}':"
        for idx in range(len(field_value_by_name[field_name])):
            field_value = field_value_by_name[field_name][idx]
            if idx != 0:
                event_name += ","
            event_name += f"'{field_value}'"
    event = infra_pb2.Event(id=new_event_id(),
                            parent_id=parent_id,
                            name=event_name,
                            start_timestamp=Timestamp(seconds=seconds, nanos=nanos),
                            status=status,
                            body=ComponentEncoder().encode(verification_component.build()).encode())
    event.attached_message_ids.append(compare_result.first_message_id)
    event.attached_message_ids.append(compare_result.second_message_id)
    return event


class MessageComponent:

    def __init__(self, message: str) -> None:
        self.type = "message"
        self.data = message


class Verification:

    def __init__(self) -> None:
        self.type = None
        self.status = None
        self.fields = None


class VerificationBuilder:

    def __init__(self) -> None:
        self.status = None
        self.fields = dict()

    def verification(self, field_name: str, comparison_result):
        self.fields[field_name] = VerificationEntryUtils.create_verification_entry(comparison_result)

    def build(self) -> Verification:
        verification = Verification()
        verification.type = "verification"
        verification.status = self.status
        verification.fields = self.fields
        return verification


class VerificationEntry:

    def __init__(self) -> None:
        self.type = None
        self.status = None
        self.expected = None
        self.actual = None
        self.operation = None
        self.key = None
        self.fields = dict()


def to_verification_status(status: message_comparator_pb2.ComparisonEntryStatus) -> str:
    if status == message_comparator_pb2.ComparisonEntryStatus.NA:
        return 'NA'
    else:
        if status == message_comparator_pb2.ComparisonEntryStatus.FAILED:
            return 'FAILED'
    return 'PASSED'


def to_verification_filter_operation(operation) -> str:
    if operation == infra_pb2.FilterOperation.EQUAL:
        return 'EQUAL'
    if operation == infra_pb2.FilterOperation.NOT_EQUAL:
        return 'NOT_EQUAL'
    if operation == infra_pb2.FilterOperation.EMPTY:
        return 'EMPTY'
    if operation == infra_pb2.FilterOperation.NOT_EMPTY:
        return 'NOT_EMPTY'


class VerificationEntryUtils(object):
    @classmethod
    def create_verification_entry(cls, comparison_result) -> VerificationEntry:
        verification_entry = VerificationEntry()
        verification_entry.expected = comparison_result.first
        verification_entry.actual = comparison_result.second
        verification_entry.status = to_verification_status(comparison_result.status)
        verification_entry.key = comparison_result.is_key
        verification_entry.operation = to_verification_filter_operation(comparison_result.operation)
        if comparison_result.type == message_comparator_pb2.ComparisonEntryType.COLLECTION:
            verification_entry.type = "collection"
            for field_name in comparison_result.fields.keys():
                verification_entry.fields[field_name] = cls.create_verification_entry(
                    comparison_result.fields[field_name])
        else:
            verification_entry.type = "field"

        return verification_entry


class ComponentEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__
