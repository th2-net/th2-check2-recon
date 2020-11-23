#   Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import json

from setuptools import setup, find_packages


def get_dependency(dependency_name, dependency_version,
                   dependency_repository='https://nexus.exactpro.com/repository/th2-pypi/packages/'):
    return f"{dependency_name} @ {dependency_repository}{dependency_name}/{dependency_version}/" \
           f"{dependency_name.replace('-', '_')}-{dependency_version}.tar.gz"


with open('package_info.json', 'r') as file:
    package_info = json.load(file)

package_name = package_info['package_name'].replace('-', '_')
package_version = package_info['package_version']

with open('README.md', 'r') as file:
    long_description = file.read()

setup(
    name=package_name,
    version=package_version,
    description=package_name,
    long_description=long_description,
    author='TH2-devs',
    author_email='th2-devs@exactprosystems.com',
    url='https://gitlab.exactpro.com/vivarium/th2/th2-core-open-source/th2-check2-recon',
    license='Apache License 2.0',
    python_requires='>=3.7',
    install_requires=[
        'sortedcollections==1.2.1',
        get_dependency(dependency_name='th2-grpc-util', dependency_version='2.3.1'),
        get_dependency(dependency_name='th2-common', dependency_version='2.2.2')
    ],
    packages=[''] + find_packages(include=['th2_check2_recon', 'th2_check2_recon.*']),
    package_data={'': ['package_info.json']}
)
