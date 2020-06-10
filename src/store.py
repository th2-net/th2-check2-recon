import logging
import uuid
from datetime import datetime
from json import JSONEncoder

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from th2 import message_comparator_pb2
from th2 import event_store_pb2
from th2 import infra_pb2
from th2 import event_store_pb2_grpc


class Store:

    def __init__(self, event_store_uri) -> None:
        self.event_store_uri = event_store_uri

    @staticmethod
    def new_event_id():
        return infra_pb2.EventID(id=str(uuid.uuid1()))

    def send_event(self, event: infra_pb2.Event):
        with grpc.insecure_channel(self.event_store_uri) as channel:
            store_stub = event_store_pb2_grpc.EventStoreServiceStub(channel)
            event_response = store_stub.StoreEvent(event_store_pb2.StoreEventRequest(event=event))
            logging.info("Event id: %r" % event_response)

    def store_verification_event(self, parent_id: infra_pb2.EventID,
                                 compare_result: message_comparator_pb2.CompareMessageVsMessageResult):
        verification_component = VerificationBuilder()
        if len(compare_result.comparison_result.fields.keys()) > 0:
            for field_name in compare_result.comparison_result.fields.keys():
                verification_component.verification(field_name, compare_result.comparison_result.fields[field_name])

        event = infra_pb2.Event()
        event.id.CopyFrom(self.new_event_id())
        event.parent_id.CopyFrom(parent_id)
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        event.start_timestamp.CopyFrom(Timestamp(seconds=seconds, nanos=nanos))
        event.status = infra_pb2.EventStatus.SUCCESS  # TODO fix
        event.attached_message_ids.append(compare_result.first_message_id)
        event.attached_message_ids.append(compare_result.second_message_id)
        body = str(VerificationEncoder().encode(verification_component.build()))
        event.body = body.encode()
        self.send_event(event)

    def send_report(self, report_id, name: str):
        event = infra_pb2.Event()
        event.id.CopyFrom(report_id)
        event.name = name
        start_time = datetime.now()
        seconds = int(start_time.timestamp())
        nanos = int(start_time.microsecond * 1000)
        event.start_timestamp.CopyFrom(Timestamp(seconds=seconds, nanos=nanos))
        self.send_event(event)


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
        self.first = None
        self.second = None
        self.operation = None
        self.key = None
        self.fields = dict()


class VerificationEntryUtils(object):
    @classmethod
    def create_verification_entry(cls, comparison_result) -> VerificationEntry:
        verification_entry = VerificationEntry()
        verification_entry.first = comparison_result.first
        verification_entry.second = comparison_result.second
        verification_entry.status = comparison_result.status
        verification_entry.key = comparison_result.is_key
        verification_entry.operation = comparison_result.operation
        if comparison_result.type == message_comparator_pb2.ComparisonEntryType.COLLECTION:
            verification_entry.type = "collection"
        else:
            verification_entry.type = "field"

        return verification_entry


class VerificationEncoder(JSONEncoder):
    def default(self, o):
        return o.__dict__
