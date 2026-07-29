# Copyright 2026 ScyllaDB, Inc.
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

import ssl
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

from cassandra.datastax import cloud


def test_pyopenssl_context_from_cert_configures_context():
    context = Mock()
    ssl_module = SimpleNamespace()

    with patch.dict(sys.modules, {'OpenSSL': SimpleNamespace(SSL=ssl_module)}), \
            patch.object(
                cloud,
                '_build_pyopenssl_context_from_options',
                return_value=context) as build_context:
        result = cloud._pyopenssl_context_from_cert(
            'ca.crt', 'client.crt', 'client.key')

    assert result is context
    build_context.assert_called_once_with(
        ssl_module,
        {
            'ca_certs': 'ca.crt',
            'certfile': 'client.crt',
            'keyfile': 'client.key',
            'cert_reqs': ssl.CERT_REQUIRED,
        }
    )
