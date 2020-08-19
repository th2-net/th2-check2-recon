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

import os

from setuptools import setup, find_packages

setup(
    name='th2-recon',
    version=f"1.1.59",
    packages=find_packages(include=['th2recon', 'th2recon.*']),
    install_requires=[
        'pika==1.1.0',
        'grpcio==1.27.2',
        'google-api-core==1.22.0',
        'PyYAML==5.3.1'
    ],
    url='https://gitlab.exactpro.com/vivarium/th2/th2-core-open-source/th2-recon/',
    license='Apache License 2.0',
    author='TH2-devs',
    python_requires='>=3.7',
    author_email='th2-devs@exactprosystems.com',
    description='TH2-recon core project',
    long_description=open('README.md').read(),
)
