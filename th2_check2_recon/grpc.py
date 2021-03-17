# Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import logging

import grpc
from th2_grpc_check2_recon import check2_recon_pb2_grpc
from th2_grpc_check2_recon.check2_recon_pb2_grpc import Check2ReconServicer
from th2_grpc_common.common_pb2 import RequestStatus

from th2_check2_recon.common import MessageUtils
from th2_check2_recon.reconcommon import ReconMessage
from th2_check2_recon.rule import Rule

logger = logging.getLogger()


class Check2ReconHandler(Check2ReconServicer):

    def __init__(self, rules: [Rule]) -> None:
        self.__rules = rules

    def submitGroupBatch(self, request, context):
        try:
            for message_group in request.groups:
                for any_message in message_group.messages:
                    for rule in self.__rules:
                        message = ReconMessage(proto_message=any_message)
                        rule.process(message, ())
                        logger.info("  Processed msg id='%s'", MessageUtils.str_message_id(any_message))
                        logger.info("  Cache size '%s': %s.", rule.get_name(), rule.log_groups_size())
            return RequestStatus(status=RequestStatus.Status.SUCCESS)
        except Exception:
            logger.exception(f'Submit group batch failed', request)
            return RequestStatus(status=RequestStatus.Status.ERROR,
                                 message="Submit group batch failed, see logs of Recon")


class GRPCServer:

    def __init__(self, server: grpc.Server, rules: [Rule]) -> None:
        self.__server: grpc.Server = server
        self.__handler: Check2ReconHandler = Check2ReconHandler(rules)

    def start(self):
        check2_recon_pb2_grpc.add_Check2ReconServicer_to_server(self.__handler, self.__server)
        self.__server.start()
        logger.info('GRPC Server started')

    def stop(self):
        self.__server.stop(None)
        logger.info('GRPC Server stopped')
