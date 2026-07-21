# Copyright DataStax, Inc.
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

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from cassandra.datastax import cloud
from cassandra.connection import _default_pyopenssl_ssl_method


def test_default_pyopenssl_ssl_method_prefers_tls_client_method():
    tls_client_method = object()

    ssl_module = SimpleNamespace(
        TLS_CLIENT_METHOD=tls_client_method,
        TLS_METHOD=object(),
        TLSv1_2_METHOD=object())

    assert _default_pyopenssl_ssl_method(ssl_module) is tls_client_method


def test_default_pyopenssl_ssl_method_falls_back_to_tls_method():
    tls_method = object()

    ssl_module = SimpleNamespace(
        TLS_METHOD=tls_method,
        TLSv1_2_METHOD=object())

    assert _default_pyopenssl_ssl_method(ssl_module) is tls_method


def test_default_pyopenssl_ssl_method_falls_back_to_tlsv1_2_method():
    tlsv1_2_method = object()

    ssl_module = SimpleNamespace(TLSv1_2_METHOD=tlsv1_2_method)

    assert _default_pyopenssl_ssl_method(ssl_module) is tlsv1_2_method


def test_default_pyopenssl_ssl_method_requires_secure_method():
    with pytest.raises(ImportError, match="secure TLS client method"):
        _default_pyopenssl_ssl_method(SimpleNamespace())


def test_pyopenssl_context_from_cert_uses_shared_options_builder():
    ssl_module = SimpleNamespace(VERIFY_PEER=object())
    openssl_module = SimpleNamespace(SSL=ssl_module)

    with patch.dict('sys.modules', {'OpenSSL': openssl_module}):
        with patch.object(cloud, '_build_pyopenssl_context_from_options') as build_context:
            context = cloud._pyopenssl_context_from_cert('ca.crt', 'cert', 'key')

    assert context is build_context.return_value
    build_context.assert_called_once_with(
        {
            'ca_certs': 'ca.crt',
            'certfile': 'cert',
            'keyfile': 'key',
            'cert_reqs': ssl_module.VERIFY_PEER,
        },
        ssl_module
    )


def test_parse_cloud_config_builds_pyopenssl_context_with_custom_ssl_context(tmp_path):
    path = tmp_path / 'config.json'
    path.write_text(json.dumps({'host': 'metadata.host', 'port': 443}))
    ssl_context = object()
    pyopenssl_context = object()

    with patch.object(cloud, '_pyopenssl_context_from_cert', return_value=pyopenssl_context) as build_context:
        config = cloud.parse_cloud_config(str(path), {'ssl_context': ssl_context}, create_pyopenssl_context=True)

    assert config.ssl_context is ssl_context
    assert config.pyopenssl_context is pyopenssl_context
    build_context.assert_called_once_with(
        str(tmp_path / 'ca.crt'),
        str(tmp_path / 'cert'),
        str(tmp_path / 'key'))


def test_get_cloud_config_uses_pyopenssl_context_when_requested():
    ssl_context = object()
    pyopenssl_context = object()
    config = cloud.CloudConfig()
    config.ssl_context = ssl_context
    config.pyopenssl_context = pyopenssl_context

    with patch.object(cloud, 'read_cloud_config_from_zip', return_value=config):
        with patch.object(cloud, 'read_metadata_info', return_value=config):
            result = cloud.get_cloud_config(
                {'secure_connect_bundle': 'bundle.zip'},
                create_pyopenssl_context=True)

    assert result.ssl_context is pyopenssl_context
