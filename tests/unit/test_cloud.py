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

from types import SimpleNamespace

import pytest

from cassandra.datastax import cloud


def test_default_pyopenssl_ssl_method_prefers_tls_client_method():
    tls_client_method = object()

    ssl_module = SimpleNamespace(
        TLS_CLIENT_METHOD=tls_client_method,
        TLS_METHOD=object(),
        TLSv1_2_METHOD=object())

    assert cloud._default_pyopenssl_ssl_method(ssl_module) is tls_client_method


def test_default_pyopenssl_ssl_method_falls_back_to_tls_method():
    tls_method = object()

    ssl_module = SimpleNamespace(
        TLS_METHOD=tls_method,
        TLSv1_2_METHOD=object())

    assert cloud._default_pyopenssl_ssl_method(ssl_module) is tls_method


def test_default_pyopenssl_ssl_method_falls_back_to_tlsv1_2_method():
    tlsv1_2_method = object()

    ssl_module = SimpleNamespace(TLSv1_2_METHOD=tlsv1_2_method)

    assert cloud._default_pyopenssl_ssl_method(ssl_module) is tlsv1_2_method


def test_default_pyopenssl_ssl_method_requires_secure_method():
    with pytest.raises(ImportError, match="secure TLS client method"):
        cloud._default_pyopenssl_ssl_method(SimpleNamespace())
