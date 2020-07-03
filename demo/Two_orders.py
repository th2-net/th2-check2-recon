# Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function

import logging
import time
import uuid
from datetime import datetime

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from th2recon.th2 import act_fix_pb2
from th2recon.th2 import act_fix_pb2_grpc
from th2recon.th2 import event_store_pb2
from th2recon.th2 import event_store_pb2_grpc
from th2recon.th2 import infra_pb2
from th2recon.th2 import verifier_pb2
from th2recon.th2 import verifier_pb2_grpc

ACT = 'th2-kuber-node01:30002'  # Components
VERIFIER = 'th2-kuber-node01:30001'
EVENT_STORAGE = 'th2-dev:30003'


def ClOrdID():
    return str(round(time.time() * 100))


def wrapParties(partiesList):
    return infra_pb2.Value(
        message_value=infra_pb2.Message(
            fields={
                'NoPartyIDs': buildComplexListValue(partiesList)
            }))


def wrapPartiesfilter(partiesList):
    return infra_pb2.ValueFilter(
        message_filter=infra_pb2.MessageFilter(
            fields={
                'NoPartyIDs': buildComplexListValueFilter(partiesList)
            }))


def buildSimpleValue(value):
    return infra_pb2.Value(simple_value=value)


def buildSimpleFilter(value):
    return infra_pb2.ValueFilter(simple_filter=value)


def buildComplexValue(value):
    return infra_pb2.Value(message_value=value)


def buildComplexFilter(value):
    return infra_pb2.ValueFilter(message_filter=value)


def buildComplexListValue(value):
    return infra_pb2.Value(list_value=value)


def buildComplexListValueFilter(value):
    return infra_pb2.ValueFilter(list_filter=value)


def buildKeyFilter(value):
    return infra_pb2.ValueFilter(simple_filter=value, key=True)


def buildEmptyFilter():
    return infra_pb2.ValueFilter(operation=verifier_pb2.FilterOperation.EMPTY)


def buildNotEmptyFilter():
    return infra_pb2.ValueFilter(operation=verifier_pb2.FilterOperation.NOT_EMPTY)


def buildNotEqualsFilter(value):
    return infra_pb2.ValueFilter(value=value, operation=verifier_pb2.FilterOperation.NOT_EQUAL)


def createReport(report_name):
    with grpc.insecure_channel(EVENT_STORAGE) as channel:
        store_stub = event_store_pb2_grpc.EventStoreServiceStub(channel)
        reportStartTime = datetime.now()
        seconds = int(reportStartTime.timestamp())
        nanos = int(reportStartTime.microsecond * 1000)
        event_id = infra_pb2.EventID(id=str(uuid.uuid1()))
        report_response = store_stub.StoreEvent(event_store_pb2.StoreEventRequest(event=(infra_pb2.Event(
            id=event_id,

            name=report_name,
            start_timestamp=Timestamp(seconds=seconds, nanos=nanos)))))
        print(report_response)
    return event_id


def sendMessageFIX(connectivity, event_id, messageType, message):
    with grpc.insecure_channel(ACT) as channel:
        act_stub = act_fix_pb2_grpc.ActStub(channel)
        # print(message)
        if messageType.upper() == "NEWORDERSINGLE":
            act_response = act_stub.placeOrderFIX(
                act_fix_pb2.PlaceMessageRequest(message=message, connection_id=connectivity, parent_event_id=event_id))

        elif messageType.upper() == "ORDERMASSCANCELREQUEST":
            act_response = act_stub.placeOrderMassCancelRequestFIX(
                act_fix_pb2.PlaceMessageRequest(message=message, connectivityId=connectivity, parentEventId=event_id))

        elif messageType.upper() == "QUOTECANCEL":
            act_response = act_stub.placeQuoteCancelFIX(
                act_fix_pb2.PlaceMessageRequest(message=message, connectivityId=connectivity, parentEventId=event_id))

        elif messageType.upper() == "QUOTEREQUEST":
            act_response = act_stub.placeQuiteRequestFIX(
                act_fix_pb2.PlaceMessageRequest(message=message, connectivityId=connectivity, parentEventId=event_id))
    # print(act_response)
    return act_response


def verifyMessageResponse(filterMessage, act_response, connectivity, event_id):
    with grpc.insecure_channel(VERIFIER) as channel:
        verify_stub = verifier_pb2_grpc.VerifierStub(channel)
        checkRuleRequest = verifier_pb2.CheckRuleRequest(connectivity_id=connectivity,
                                                         filter=filterMessage, checkpoint=act_response.checkpoint_id,
                                                         timeout=3000, parent_event_id=event_id)
        # print(checkRuleRequest)
        verify_stub.submitCheckRule(checkRuleRequest)


# Trading Party description for send start

PartyIDs_client_id_1 = buildComplexValue(infra_pb2.Message(
    metadata=infra_pb2.MessageMetadata(message_type='TradingParty_NoPartyIDs'),
    fields={
        'PartyID': buildSimpleValue("0"),
        'PartyIDSource': buildSimpleValue("P"),
        'PartyRole': buildSimpleValue("3")
    }))
PartyIDs_investment_decision_maker_1 = buildComplexValue(infra_pb2.Message(
    metadata=infra_pb2.MessageMetadata(message_type='TradingParty_NoPartyIDs'),
    fields={
        'PartyID': buildSimpleValue("0"),
        'PartyIDSource': buildSimpleValue("P"),
        'PartyRole': buildSimpleValue("122")
    }))
PartyIDs_executing_trader_1 = buildComplexValue(infra_pb2.Message(
    metadata=infra_pb2.MessageMetadata(message_type='TradingParty_NoPartyIDs'),
    fields={
        'PartyID': buildSimpleValue("3"),
        'PartyIDSource': buildSimpleValue("P"),
        'PartyRole': buildSimpleValue("12")
    }))
TraderGroup_Requester1 = buildComplexValue(infra_pb2.Message(
    metadata=infra_pb2.MessageMetadata(message_type='TradingParty_NoPartyIDs'),
    fields={
        'PartyID': buildSimpleValue("MODEL1FIX02"),
        'PartyIDSource': buildSimpleValue("D"),
        'PartyRole': buildSimpleValue("76")
    }))

TradingParty = infra_pb2.ListValue(
    values=[TraderGroup_Requester1, PartyIDs_client_id_1,
            PartyIDs_investment_decision_maker_1, PartyIDs_executing_trader_1])

# Trading Party description for send start end

# Trading Party description for verify start

PartyIDs_client_id_1_filter = buildComplexFilter(infra_pb2.MessageFilter(
    fields={
        'PartyID': buildSimpleFilter("0"),
        'PartyIDSource': buildSimpleFilter("P"),
        'PartyRole': buildSimpleFilter("3")
    }))
PartyIDs_investment_decision_maker_1_filter = buildComplexFilter(infra_pb2.MessageFilter(
    fields={
        'PartyID': buildSimpleFilter("0"),
        'PartyIDSource': buildSimpleFilter("P"),
        'PartyRole': buildSimpleFilter("122")
    }))
PartyIDs_executing_trader_1_filter = buildComplexFilter(infra_pb2.MessageFilter(
    fields={
        'PartyID': buildSimpleFilter("3"),
        'PartyIDSource': buildSimpleFilter("P"),
        'PartyRole': buildSimpleFilter("12")
    }))
TraderGroup_Requester1_filter = buildComplexFilter(infra_pb2.MessageFilter(
    fields={
        'PartyID': buildSimpleFilter('MODEL1FIX02'),
        'PartyIDSource': buildSimpleFilter("D"),
        'PartyRole': buildSimpleFilter("76")
    }))

TradingParty_filter = infra_pb2.ListValueFilter(
    values=[TraderGroup_Requester1_filter, PartyIDs_client_id_1_filter,
            PartyIDs_investment_decision_maker_1_filter, PartyIDs_executing_trader_1_filter])


# Trading Party description for verify end


def scenario():
    repId = createReport('tstRep' + str(datetime.now()))

    connectFIX1_1 = infra_pb2.ConnectionID(session_alias="MODEL1FIX02")  # Set connectivity

    NewOrder1 = infra_pb2.Message(metadata=infra_pb2.MessageMetadata(message_type='NewOrderSingle'),
                                  # Order description for NewOrder1
                                  fields={
                                      'ClOrdID': buildSimpleValue("NewOrder1"),
                                      'SecurityID': buildSimpleValue("5221002"),
                                      'SecurityIDSource': buildSimpleValue("8"),
                                      'OrdType': buildSimpleValue("2"),
                                      'Side': buildSimpleValue("1"),
                                      'OrderQty': buildSimpleValue("10"),
                                      'Price': buildSimpleValue("100"),
                                      'DisplayQty': buildSimpleValue("10"),
                                      'AccountType': buildSimpleValue("1"),
                                      'OrderCapacity': buildSimpleValue("A"),
                                      'TradingParty': wrapParties(TradingParty),
                                      'TransactTime': buildSimpleValue(datetime.now().isoformat())
                                  })

    ExecutionReport1 = infra_pb2.MessageFilter(messageType='ExecutionReport',
                                               # Execution report description: status NEW
                                               fields={
                                                   'ClOrdID': buildKeyFilter(NewOrder1.fields['ClOrdID'].simple_value),

                                                   'Side': buildSimpleFilter(NewOrder1.fields['Side'].simple_value),
                                                   'LeavesQty': buildSimpleFilter(
                                                       NewOrder1.fields['OrderQty'].simple_value),
                                                   'SecurityID': buildSimpleFilter(
                                                       NewOrder1.fields['SecurityID'].simple_value),
                                                   'SecurityIDSource': buildSimpleFilter(
                                                       NewOrder1.fields['SecurityIDSource'].simple_value),
                                                   'OrdType': buildSimpleFilter(
                                                       NewOrder1.fields['OrdType'].simple_value),
                                                   'OrderQty': buildSimpleFilter(
                                                       NewOrder1.fields['OrderQty'].simple_value),
                                                   'ExecType': buildSimpleFilter("0"),
                                                   'OrdStatus': buildSimpleFilter("0"),
                                                   'CumQty': buildSimpleFilter("0"),
                                                   'TradingParty': wrapPartiesfilter(TradingParty_filter)
                                               })
    try:
        verifyMessageResponse(ExecutionReport1,
                              sendMessageFIX(connectFIX1_1,
                                             repId, "NewOrderSingle", NewOrder1), connectFIX1_1,
                              repId)  # Construction with send NewOrder1 and verify Execution report on it
    except Exception as ex:
        print(ex)

    NewOrder2 = infra_pb2.Message(metadata=infra_pb2.MessageMetadata(message_type='NewOrderSingle'),
                                  # Order description for NewOrder2
                                  fields={
                                      'ClOrdID': buildSimpleValue("NewOrder2"),
                                      'SecurityID': buildSimpleValue("5221002"),
                                      'SecurityIDSource': buildSimpleValue("8"),
                                      'OrdType': buildSimpleValue("2"),
                                      'Side': buildSimpleValue("2"),
                                      'OrderQty': buildSimpleValue("10"),
                                      'Price': buildSimpleValue("100"),
                                      'DisplayQty': buildSimpleValue("10"),
                                      'AccountType': buildSimpleValue("1"),
                                      'OrderCapacity': buildSimpleValue("A"),
                                      'TradingParty': wrapParties(TradingParty),
                                      'TransactTime': buildSimpleValue(datetime.now().isoformat())
                                  })

    ExecutionReport2 = infra_pb2.MessageFilter(messageType='ExecutionReport',
                                               # Execution report description: status TRADE for NewOrder2
                                               fields={
                                                   'ClOrdID': buildKeyFilter(NewOrder2.fields['ClOrdID'].simple_value),

                                                   'Side': buildSimpleFilter(NewOrder2.fields['Side'].simple_value),
                                                   'LeavesQty': buildSimpleFilter("0"),
                                                   'SecurityID': buildSimpleFilter(
                                                       NewOrder2.fields['SecurityID'].simple_value),
                                                   'SecurityIDSource': buildSimpleFilter(
                                                       NewOrder2.fields['SecurityIDSource'].simple_value),
                                                   'OrdType': buildSimpleFilter(
                                                       NewOrder2.fields['OrdType'].simple_value),
                                                   'OrderQty': buildSimpleFilter(
                                                       NewOrder2.fields['OrderQty'].simple_value),
                                                   'ExecType': buildSimpleFilter("F"),
                                                   'OrdStatus': buildSimpleFilter("2"),
                                                   'CumQty': buildSimpleFilter("10"),
                                                   'TradingParty': wrapPartiesfilter(TradingParty_filter)
                                               })
    try:
        NewOrder2Response = sendMessageFIX(connectFIX1_1, repId, "NewOrderSingle",
                                           NewOrder2)  # Construction with send NewOrder2
        verifyMessageResponse(ExecutionReport2, NewOrder2Response, connectFIX1_1,
                              repId)  # Construction with verify ExecutionReport on NewOrder2
    except Exception as ex:
        print(ex)

    ExecutionReport3 = infra_pb2.MessageFilter(messageType='ExecutionReport',
                                               # Execution report description: status TRADE for NewOrder1
                                               fields={
                                                   'ClOrdID': buildKeyFilter(NewOrder1.fields['ClOrdID'].simple_value),

                                                   'Side': buildSimpleFilter(NewOrder1.fields['Side'].simple_value),
                                                   'LeavesQty': buildSimpleFilter("0"),
                                                   'SecurityID': buildSimpleFilter(
                                                       NewOrder1.fields['SecurityID'].simple_value),
                                                   'SecurityIDSource': buildSimpleFilter(
                                                       NewOrder1.fields['SecurityIDSource'].simple_value),
                                                   'OrdType': buildSimpleFilter(
                                                       NewOrder1.fields['OrdType'].simple_value),
                                                   'OrderQty': buildSimpleFilter(
                                                       NewOrder1.fields['OrderQty'].simple_value),
                                                   'ExecType': buildKeyFilter("F"),
                                                   'OrdStatus': buildSimpleFilter("2"),
                                                   'CumQty': buildSimpleFilter("10"),
                                                   'TradingParty': wrapPartiesfilter(TradingParty_filter)
                                               })
    try:
        verifyMessageResponse(ExecutionReport3, NewOrder2Response, connectFIX1_1,
                              repId)  # Construction with verify ExecutionReport on NewOrder1 after TRADE with NewOrder2
    except Exception as ex:
        print(ex)


if __name__ == '__main__':
    logging.basicConfig()
    scenario()
