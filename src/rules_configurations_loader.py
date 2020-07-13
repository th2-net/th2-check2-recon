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

import importlib

import yaml


class RuleConfiguration:

    def __init__(self, name: str, module: str, enabled: bool, configuration) -> None:
        self.name = name
        self.module = module
        self.enabled = enabled
        self.configuration = configuration


def load_rules(rules_configurations_path, rules_package_path) -> [RuleConfiguration]:
    rules_configuration_file = open(rules_configurations_path, 'r')
    rules_configuration = yaml.load(rules_configuration_file, Loader=yaml.FullLoader)
    if isinstance(rules_package_path, str):
        rules_package_path = importlib.import_module(rules_package_path)
    imported_rules = []
    for rule in rules_configuration["rules"]:
        imported_rules.append(
            RuleConfiguration(
                rule["name"],
                importlib.import_module(rules_package_path.__name__ + '.' + rule["name"]),
                rule["enabled"],
                rule["configuration"]))
    return imported_rules
