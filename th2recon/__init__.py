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

import os


def write_version(ver: str):
    with open("th2recon/version.txt", "w") as file:
        file.write(ver)


def load_version() -> str:
    if not os.path.isfile("th2recon/version.txt"):
        return "local_build"
    with open("th2recon/version.txt", "r") as file:
        return file.read()


if os.environ.get('CI_COMMIT_TAG'):
    __version__ = os.environ['CI_COMMIT_TAG']
    write_version(__version__)
elif os.environ.get('CI_JOB_ID'):
    __version__ = os.environ['CI_JOB_ID']
    write_version(__version__)
else:
    __version__ = load_version()
