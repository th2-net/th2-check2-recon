# Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import datetime
from typing import Dict, Optional


class RuleConfiguration:

    def __init__(self,
                 name: str,
                 enabled: str,
                 match_timeout: str,
                 match_timeout_offset_ns: str,
                 match_all: Optional[str] = None,
                 autoremove_timeout: Optional[str] = None,
                 configuration: Optional[str] = None) -> None:

        self.name = str(name)
        self.enabled = enabled.lower() == 'true'
        self.match_timeout = int(match_timeout)
        self.match_timeout_offset_ns = int(match_timeout_offset_ns)
        self.match_all = bool(match_all is not None and match_all.lower() == 'true')
        datetime_object = False
        if autoremove_timeout is not None:
            try:
                timeout_int = int(autoremove_timeout)
            except ValueError:
                timeout = datetime.datetime.strptime(autoremove_timeout, '%H:%M')
                timeout_datetime = timeout.combine(datetime.datetime.now().date(), timeout.time())
                datetime_object = True
            self.autoremove_timeout = timeout_int if not datetime_object else timeout_datetime
        else:
            self.autoremove_timeout = None
        self.configuration = configuration


class CrawlerConnectionConfiguration:

    def __init__(self, name: str = 'Recon Data Processor', version: str = '1.0.0') -> None:

        self.name = name
        self.version = version


class ReconConfiguration:
    def __init__(self,
                 recon_name: str,
                 cache_size: int,
                 event_batch_max_size: int,
                 event_batch_send_interval: int,
                 rules_package_path: str,
                 rules: list,
                 crawler_connection_configuration: Optional[Dict[str, str]] = None,
                 configuration: Optional[str] = None) -> None:

        self.recon_name = recon_name
        self.cache_size = int(cache_size)
        self.event_batch_max_size = int(event_batch_max_size)
        self.event_batch_send_interval = int(event_batch_send_interval)
        self.rules_package_path = rules_package_path
        self.rules = [RuleConfiguration(**rule) for rule in rules]

        if crawler_connection_configuration is None:
            self.crawler_connection_configuration = CrawlerConnectionConfiguration()
        else:
            self.crawler_connection_configuration = CrawlerConnectionConfiguration(**crawler_connection_configuration)

        self.configuration = configuration
