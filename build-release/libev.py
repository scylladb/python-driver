# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Libev build paths shared by packaging and the native callback tests."""

import json
import os
from pathlib import Path
import sys


def libev_paths(driver_root):
    includes = (os.environ.get('CASS_DRIVER_LIBEV_INCLUDES') or
                '/usr/include/libev,/usr/local/include,/opt/local/include,'
                '/usr/include').split(',')
    libraries = (os.environ.get('CASS_DRIVER_LIBEV_LIBS') or
                 '/usr/local/lib,/opt/local/lib,/usr/lib64').split(',')
    if sys.platform.startswith('darwin'):
        includes.extend(['/opt/homebrew/include',
                         str(Path.home() / 'homebrew/include')])
        libraries.append('/opt/homebrew/lib')
        for cellar in (Path('/opt/homebrew/Cellar/libev'),
                       Path.home() / 'homebrew/Cellar/libev'):
            versions = sorted(cellar.glob('[0-9]*/include'), reverse=True)
            if versions:
                includes.append(str(versions[0]))
                break
    conan_env = Path(driver_root) / 'build-release/conan/conandeps.env'
    if conan_env.exists():
        paths = json.loads(conan_env.read_text())
        includes.extend(paths.get('include_dirs', []))
        libraries.extend(paths.get('library_dirs', []))
    return ([path.strip() for path in includes if path.strip()],
            [path.strip() for path in libraries if path.strip()])
