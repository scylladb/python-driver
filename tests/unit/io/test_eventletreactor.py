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

import os
import ssl
import unittest
from types import SimpleNamespace

from unittest.mock import Mock, patch

from tests.unit.io.utils import TimerTestMixin
from tests import notpypy, EVENT_LOOP_MANAGER


try:
    from eventlet import monkey_patch
    from cassandra.io import eventletreactor
    from cassandra.io.eventletreactor import EventletConnection
except (ImportError, AttributeError):
    eventletreactor = None
    EventletConnection = None  # noqa

from cassandra.connection import Connection, DefaultEndPoint

CA_CERTS = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'integration', 'long', 'ssl', 'rootCa.crt'))


class _FakeX509Name(object):
    def __init__(self, common_name):
        self._common_name = common_name

    def get_components(self):
        return [(b'CN', self._common_name.encode('utf-8'))]


class _FakeX509Extension(object):
    def __init__(self, short_name, value):
        self._short_name = short_name
        self._value = value

    def get_short_name(self):
        return self._short_name

    def __str__(self):
        return self._value


class _FakeX509Certificate(object):
    def __init__(self, common_name, san_dns_names=None):
        self._subject = _FakeX509Name(common_name)
        self._extensions = []
        if san_dns_names:
            self._extensions.append(_FakeX509Extension(
                b'subjectAltName',
                ', '.join('DNS:%s' % name for name in san_dns_names)))

    def get_subject(self):
        return self._subject

    def get_extension_count(self):
        return len(self._extensions)

    def get_extension(self, i):
        return self._extensions[i]


def _make_certificate(common_name, san_dns_names=None):
    return _FakeX509Certificate(common_name, san_dns_names)


@unittest.skipIf(EventletConnection is None, "eventlet is not available")
@unittest.skipIf(not getattr(eventletreactor, '_PYOPENSSL', False), "pyOpenSSL is not available")
class EventletSSLContextTest(unittest.TestCase):

    def test_empty_ssl_options_build_pyopenssl_context(self):
        conn = EventletConnection.__new__(EventletConnection)

        Connection.__init__(conn, DefaultEndPoint('1.2.3.4'), ssl_options={})

        assert conn._ssl_enabled
        assert conn.ssl_context is not None

    def test_empty_ssl_options_default_to_negotiating_tls(self):
        with patch.object(eventletreactor.SSL, 'Context') as context_mock:
            context = eventletreactor._build_pyopenssl_context_from_options({})

        context_mock.assert_called_once_with(eventletreactor._default_ssl_method())
        assert context is context_mock.return_value

    def test_default_ssl_method_falls_back_to_tls_method(self):
        tls_method = object()

        with patch.object(eventletreactor, 'SSL', SimpleNamespace(TLS_METHOD=tls_method)):
            assert eventletreactor._default_ssl_method() is tls_method

    def test_default_ssl_method_falls_back_to_tlsv1_2_method(self):
        tlsv1_2_method = object()

        with patch.object(eventletreactor, 'SSL', SimpleNamespace(TLSv1_2_METHOD=tlsv1_2_method)):
            assert eventletreactor._default_ssl_method() is tlsv1_2_method

    def test_ssl_version_option_is_preserved(self):
        with patch.object(eventletreactor.SSL, 'Context') as context_mock:
            eventletreactor._build_pyopenssl_context_from_options(
                {'ssl_version': eventletreactor.SSL.TLSv1_2_METHOD})

        context_mock.assert_called_once_with(eventletreactor.SSL.TLSv1_2_METHOD)

    def test_stdlib_ssl_version_option_is_translated(self):
        with patch.object(eventletreactor.SSL, 'Context') as context_mock:
            eventletreactor._build_pyopenssl_context_from_options(
                {'ssl_version': ssl.PROTOCOL_TLS})

        context_mock.assert_called_once_with(eventletreactor.SSL.TLS_METHOD)

    def test_ca_certs_default_to_required_validation(self):
        conn = EventletConnection.__new__(EventletConnection)

        Connection.__init__(conn, DefaultEndPoint('1.2.3.4'), ssl_options={'ca_certs': CA_CERTS})

        assert conn.ssl_context.get_verify_mode() == eventletreactor.SSL.VERIFY_PEER

    def test_stdlib_cert_reqs_option_is_translated(self):
        context = eventletreactor._build_pyopenssl_context_from_options({'cert_reqs': ssl.CERT_REQUIRED})

        assert context.get_verify_mode() == eventletreactor.SSL.VERIFY_PEER

    def test_ciphers_option_is_applied(self):
        with patch.object(eventletreactor.SSL, 'Context') as context_mock:
            eventletreactor._build_pyopenssl_context_from_options({'ciphers': 'ECDHE+AESGCM'})

        context_mock.return_value.set_cipher_list.assert_called_once_with(b'ECDHE+AESGCM')

    def test_check_hostname_option_enables_hostname_validation(self):
        conn = EventletConnection.__new__(EventletConnection)

        Connection.__init__(conn, DefaultEndPoint('1.2.3.4'), ssl_options={'check_hostname': True})

        assert conn._check_hostname

    def test_wrap_socket_from_context_returns_wrapped_socket(self):
        conn = EventletConnection.__new__(EventletConnection)
        conn.ssl_context = Mock()
        conn.ssl_context.get_verify_mode.return_value = eventletreactor.SSL.VERIFY_PEER
        conn.ssl_options = {}
        original_socket = object()
        conn._socket = original_socket

        with patch.object(eventletreactor.SSL, 'Connection') as mock_connection:
            wrapped_socket = mock_connection.return_value

            assert conn._wrap_socket_from_context() is wrapped_socket

        mock_connection.assert_called_once_with(conn.ssl_context, original_socket)
        wrapped_socket.set_connect_state.assert_called_once_with()
        assert conn._socket is wrapped_socket

    def test_validate_hostname_uses_server_hostname_and_san(self):
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = DefaultEndPoint('10.0.0.1')
        conn.ssl_options = {'server_hostname': 'node.example.com'}
        conn._socket = Mock()
        conn._socket.get_peer_certificate.return_value = _make_certificate(
            'wrong.example.com', san_dns_names=['node.example.com'])

        conn._validate_hostname()

    def test_validate_hostname_prefers_san_over_common_name(self):
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = DefaultEndPoint('node.example.com')
        conn.ssl_options = {}
        conn._socket = Mock()
        conn._socket.get_peer_certificate.return_value = _make_certificate(
            'node.example.com', san_dns_names=['other.example.com'])

        with self.assertRaises(ssl.CertificateError):
            conn._validate_hostname()


skip_condition = EventletConnection is None or EVENT_LOOP_MANAGER != "eventlet"
# There are some issues with some versions of pypy and eventlet
@notpypy
@unittest.skipIf(skip_condition, "Skipping the eventlet tests because it's not installed")
class EventletTimerTest(TimerTestMixin, unittest.TestCase):

    connection_class = EventletConnection

    @classmethod
    def setUpClass(cls):
        # This is run even though the class is skipped, so we need
        # to make sure no monkey patching is happening
        if skip_condition:
            return

        # This is being added temporarily due to a bug in eventlet:
        # https://github.com/eventlet/eventlet/issues/401
        import eventlet
        eventlet.sleep()
        monkey_patch()
        # cls.connection_class = EventletConnection

        EventletConnection.initialize_reactor()
        assert EventletConnection._timers is not None

    def setUp(self):
        socket_patcher = patch('eventlet.green.socket.socket')
        self.addCleanup(socket_patcher.stop)
        socket_patcher.start()

        super(EventletTimerTest, self).setUp()

        recv_patcher = patch.object(self.connection._socket,
                                    'recv',
                                    return_value=b'')
        self.addCleanup(recv_patcher.stop)
        recv_patcher.start()

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        return self.connection._timers

    # There is no unpatching because there is not a clear way
    # of doing it reliably
