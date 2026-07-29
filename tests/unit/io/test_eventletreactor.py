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

import unittest

from unittest.mock import Mock, patch

from tests.unit.io.utils import (TimerTestMixin, UNIT_CA_CERT,
                                 make_pyopenssl_x509_certificate)
from tests import notpypy, EVENT_LOOP_MANAGER


try:
    from eventlet import monkey_patch
    from cassandra.io import eventletreactor
    from cassandra.io.eventletreactor import EventletConnection
except (ImportError, AttributeError):
    eventletreactor = None
    EventletConnection = None  # noqa

from cassandra.connection import (Connection, ConnectionException,
                                  DefaultEndPoint, SniEndPoint)


@unittest.skipIf(EventletConnection is None, "eventlet is not available")
@unittest.skipIf(not getattr(eventletreactor, '_PYOPENSSL', False), "pyOpenSSL is not available")
class EventletSSLContextTest(unittest.TestCase):

    def test_empty_ssl_options_build_pyopenssl_context(self):
        conn = EventletConnection.__new__(EventletConnection)

        Connection.__init__(conn, DefaultEndPoint('1.2.3.4'), ssl_options={})

        assert conn._ssl_enabled
        assert conn.ssl_context is not None

    def test_check_hostname_option_enables_hostname_validation(self):
        conn = EventletConnection.__new__(EventletConnection)

        Connection.__init__(conn, DefaultEndPoint('1.2.3.4'), ssl_options={'check_hostname': True})

        assert conn._check_hostname

    def test_explicit_empty_options_with_endpoint_sni_remain_unverified(self):
        conn = EventletConnection.__new__(EventletConnection)

        Connection.__init__(
            conn,
            SniEndPoint('1.2.3.4', 'node.example.com'),
            ssl_options={})

        assert conn.ssl_options['server_hostname'] == 'node.example.com'
        assert conn.ssl_context.get_verify_mode() == eventletreactor.SSL.VERIFY_NONE

    def test_endpoint_context_options_rejected_for_supplied_context(self):
        endpoint = SniEndPoint('1.2.3.4', 'node.example.com')
        endpoint._ssl_options.update({
            'ca_certs': UNIT_CA_CERT,
            'check_hostname': True,
        })
        context = eventletreactor.SSL.Context(
            eventletreactor.SSL.TLS_METHOD)
        conn = EventletConnection.__new__(EventletConnection)

        with self.assertRaisesRegex(ValueError, "independent SSL context"):
            Connection.__init__(conn, endpoint, ssl_context=context)

        assert context.get_verify_mode() == eventletreactor.SSL.VERIFY_NONE

    def test_explicit_empty_options_with_endpoint_ca_require_verification(self):
        endpoint = SniEndPoint('1.2.3.4', 'node.example.com')
        endpoint._ssl_options = {'ca_certs': UNIT_CA_CERT}
        conn = EventletConnection.__new__(EventletConnection)

        Connection.__init__(conn, endpoint, ssl_options={})

        assert conn.ssl_options == {'ca_certs': UNIT_CA_CERT}
        assert conn.ssl_context.get_verify_mode() == eventletreactor.SSL.VERIFY_PEER

    def test_wrap_socket_from_context_returns_wrapped_socket(self):
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = DefaultEndPoint('1.2.3.4')
        conn.ssl_context = Mock()
        conn.ssl_context.get_verify_mode.return_value = eventletreactor.SSL.VERIFY_PEER
        conn.ssl_options = {}
        conn._check_hostname = False
        original_socket = object()
        conn._socket = original_socket

        with patch.object(eventletreactor.SSL, 'Connection') as mock_connection:
            wrapped_socket = mock_connection.return_value

            assert conn._wrap_socket_from_context() is wrapped_socket

        mock_connection.assert_called_once_with(conn.ssl_context, original_socket)
        wrapped_socket.set_connect_state.assert_called_once_with()
        wrapped_socket.set_tlsext_host_name.assert_not_called()
        assert conn._socket is wrapped_socket

    def test_wrap_socket_uses_endpoint_address_for_sni_when_checking_hostname(self):
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = DefaultEndPoint('node.example.com')
        conn.ssl_context = Mock()
        conn.ssl_context.get_verify_mode.return_value = eventletreactor.SSL.VERIFY_PEER
        conn.ssl_options = {}
        conn._check_hostname = True
        original_socket = object()
        conn._socket = original_socket

        with patch.object(eventletreactor.SSL, 'Connection') as mock_connection:
            wrapped_socket = mock_connection.return_value

            assert conn._wrap_socket_from_context() is wrapped_socket

        mock_connection.assert_called_once_with(conn.ssl_context, original_socket)
        wrapped_socket.set_tlsext_host_name.assert_called_once_with(b'node.example.com')
        assert conn._socket is wrapped_socket

    def test_wrap_socket_idna_encodes_unicode_sni(self):
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = DefaultEndPoint('täst.example')
        conn.ssl_context = Mock()
        conn.ssl_context.get_verify_mode.return_value = eventletreactor.SSL.VERIFY_PEER
        conn.ssl_options = {}
        conn._check_hostname = True
        conn._socket = object()

        with patch.object(eventletreactor.SSL, 'Connection') as mock_connection:
            wrapped_socket = mock_connection.return_value
            assert conn._wrap_socket_from_context() is wrapped_socket

        wrapped_socket.set_tlsext_host_name.assert_called_once_with(
            b'xn--tst-qla.example')

    def test_wrap_socket_omits_implicit_sni_for_ip_addresses(self):
        for address in ('1.2.3.4', '2001:db8::1'):
            with self.subTest(address=address):
                conn = EventletConnection.__new__(EventletConnection)
                conn.endpoint = DefaultEndPoint(address)
                conn.ssl_context = Mock()
                conn.ssl_context.get_verify_mode.return_value = eventletreactor.SSL.VERIFY_PEER
                conn.ssl_options = {}
                conn._check_hostname = True
                conn._socket = object()

                with patch.object(eventletreactor.SSL, 'Connection') as mock_connection:
                    wrapped_socket = mock_connection.return_value
                    assert conn._wrap_socket_from_context() is wrapped_socket

                wrapped_socket.set_tlsext_host_name.assert_not_called()
                certificate = wrapped_socket.get_peer_certificate.return_value
                with patch.object(
                        eventletreactor, '_validate_pyopenssl_hostname') as validate_hostname:
                    conn._validate_hostname()

                validate_hostname.assert_called_once_with(certificate, address)

    def test_wrap_socket_preserves_explicit_ip_sni(self):
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = DefaultEndPoint('node.example.com')
        conn.ssl_context = Mock()
        conn.ssl_context.get_verify_mode.return_value = eventletreactor.SSL.VERIFY_PEER
        conn.ssl_options = {'server_hostname': '1.2.3.4'}
        conn._check_hostname = True
        conn._socket = object()

        with patch.object(eventletreactor.SSL, 'Connection') as mock_connection:
            wrapped_socket = mock_connection.return_value
            assert conn._wrap_socket_from_context() is wrapped_socket

        wrapped_socket.set_tlsext_host_name.assert_called_once_with(b'1.2.3.4')

    def test_sni_endpoint_routes_by_host_id_but_verifies_proxy_address(self):
        host_id = '01234567-89ab-cdef-0123-456789abcdef'
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = SniEndPoint('proxy.example.com', host_id)
        conn.ssl_context = Mock()
        conn.ssl_context.get_verify_mode.return_value = (
            eventletreactor.SSL.VERIFY_PEER)
        conn.ssl_options = conn.endpoint.ssl_options
        conn._check_hostname = True
        conn._socket = object()

        with patch.object(eventletreactor.SSL, 'Connection') as connection_mock:
            wrapped_socket = connection_mock.return_value
            assert conn._wrap_socket_from_context() is wrapped_socket

        wrapped_socket.set_tlsext_host_name.assert_called_once_with(
            host_id.encode('ascii'))
        certificate = wrapped_socket.get_peer_certificate.return_value
        with patch.object(
                eventletreactor, '_validate_pyopenssl_hostname') as validate_hostname:
            conn._validate_hostname()

        validate_hostname.assert_called_once_with(
            certificate, conn.endpoint.address)

    def test_validate_hostname_uses_server_hostname_and_san(self):
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = DefaultEndPoint('10.0.0.1')
        conn.ssl_options = {'server_hostname': 'node.example.com'}
        conn._socket = Mock()
        conn._socket.get_peer_certificate.return_value = make_pyopenssl_x509_certificate(
            'wrong.example.com', san_dns_names=['node.example.com'])

        conn._validate_hostname()

    def test_validate_hostname_prefers_san_over_common_name(self):
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = DefaultEndPoint('node.example.com')
        conn.ssl_options = {}
        conn._socket = Mock()
        conn._socket.get_peer_certificate.return_value = make_pyopenssl_x509_certificate(
            'node.example.com', san_dns_names=['other.example.com'])

        with self.assertRaises(ConnectionException):
            conn._validate_hostname()

    @staticmethod
    def _connection_for_address_attempts(sockets):
        conn = EventletConnection.__new__(EventletConnection)
        conn.endpoint = DefaultEndPoint('node.example.com')
        conn._get_socket_addresses = Mock(return_value=[
            (eventletreactor.socket.AF_INET, eventletreactor.socket.SOCK_STREAM,
             6, '', ('192.0.2.1', 9042)),
            (eventletreactor.socket.AF_INET, eventletreactor.socket.SOCK_STREAM,
             6, '', ('192.0.2.2', 9042)),
        ])
        conn._socket_impl = Mock()
        conn._socket_impl.socket.side_effect = sockets
        conn.ssl_context = None
        conn._ssl_options_explicit = True
        conn.features = Mock(shard_id=None)
        conn.connect_timeout = 5
        conn._check_hostname = False
        conn.sockopts = None
        return conn

    def test_handshake_error_closes_socket_and_tries_next_address(self):
        first_socket = Mock()
        second_socket = Mock()
        handshake_error = eventletreactor.SSL.Error('handshake failed')
        first_socket.do_handshake.side_effect = handshake_error
        conn = self._connection_for_address_attempts(
            [first_socket, second_socket])

        conn._connect_socket()

        first_socket.connect.assert_called_once_with(('192.0.2.1', 9042))
        first_socket.close.assert_called_once_with()
        second_socket.connect.assert_called_once_with(('192.0.2.2', 9042))
        second_socket.do_handshake.assert_called_once_with()
        second_socket.close.assert_not_called()
        assert conn._socket is second_socket

    def test_hostname_error_closes_socket_and_tries_next_address(self):
        first_socket = Mock()
        second_socket = Mock()
        conn = self._connection_for_address_attempts(
            [first_socket, second_socket])
        conn._check_hostname = True
        hostname_error = ConnectionException(
            'Hostname verification failed', conn.endpoint)
        conn._validate_hostname = Mock(
            side_effect=[hostname_error, None])

        conn._connect_socket()

        first_socket.connect.assert_called_once_with(('192.0.2.1', 9042))
        first_socket.close.assert_called_once_with()
        second_socket.connect.assert_called_once_with(('192.0.2.2', 9042))
        second_socket.close.assert_not_called()
        assert conn._socket is second_socket
        assert conn._validate_hostname.call_count == 2

    def test_hostname_error_is_not_masked_by_later_socket_error(self):
        first_socket = Mock()
        second_socket = Mock()
        second_socket.connect.side_effect = eventletreactor.socket.error(
            111, 'Connection refused')
        conn = self._connection_for_address_attempts(
            [first_socket, second_socket])
        conn._check_hostname = True
        hostname_error = ConnectionException(
            'Hostname verification failed', conn.endpoint)
        conn._validate_hostname = Mock(side_effect=hostname_error)

        with self.assertRaises(ConnectionException) as raised:
            conn._connect_socket()

        assert raised.exception is hostname_error
        first_socket.close.assert_called_once_with()
        second_socket.close.assert_called_once_with()

    @staticmethod
    def _run_io_handler(handler_name, error):
        conn = Mock(in_buffer_size=4096)
        conn._write_queue.get.return_value = b'message'
        operation = 'recv' if handler_name == 'handle_read' else 'sendall'
        getattr(conn._socket, operation).side_effect = error

        getattr(EventletConnection, handler_name)(conn)
        return conn

    def test_zero_return_during_io_closes_connection_cleanly(self):
        for handler_name in ('handle_read', 'handle_write'):
            with self.subTest(handler=handler_name):
                conn = self._run_io_handler(
                    handler_name, eventletreactor.SSL.ZeroReturnError())

                conn.close.assert_called_once_with()
                conn.defunct.assert_not_called()

    def test_tls_error_during_io_defuncts_connection(self):
        for handler_name in ('handle_read', 'handle_write'):
            with self.subTest(handler=handler_name):
                error = eventletreactor.SSL.Error('boom')
                conn = self._run_io_handler(handler_name, error)

                conn.defunct.assert_called_once_with(error)
                conn.close.assert_not_called()

    def test_socket_error_during_io_still_defuncts_connection(self):
        for handler_name in ('handle_read', 'handle_write'):
            with self.subTest(handler=handler_name):
                error = eventletreactor.socket.error('boom')
                conn = self._run_io_handler(handler_name, error)

                conn.defunct.assert_called_once_with(error)
                conn.close.assert_not_called()

    def test_greenlet_exit_during_io_remains_graceful(self):
        for handler_name in ('handle_read', 'handle_write'):
            with self.subTest(handler=handler_name):
                conn = self._run_io_handler(
                    handler_name, eventletreactor.GreenletExit())

                conn.defunct.assert_not_called()
                conn.close.assert_not_called()

    def test_falsey_tls_recv_closes_before_writing_to_buffer(self):
        conn = Mock(in_buffer_size=4096)
        conn._socket.recv.return_value = ''

        EventletConnection.handle_read(conn)

        conn.close.assert_called_once_with()
        conn._iobuf.write.assert_not_called()
        conn.defunct.assert_not_called()


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
