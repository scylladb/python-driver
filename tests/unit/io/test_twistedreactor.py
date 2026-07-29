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

from cassandra.connection import (Connection, ConnectionException,
                                  DefaultEndPoint, SniEndPoint)

try:
    from twisted.test import proto_helpers
    from cassandra.io import twistedreactor
    from cassandra.io.twistedreactor import TwistedConnection
except ImportError:
    twistedreactor = TwistedConnection = None  # NOQA


from cassandra.connection import _Frame

from tests.unit.io.utils import TimerTestMixin, UNIT_CA_CERT


@unittest.skipIf(TwistedConnection is None, "Twisted libraries are not available")
@unittest.skipIf(not getattr(twistedreactor, '_HAS_SSL', False), "pyOpenSSL is not available")
class TwistedSSLContextTest(unittest.TestCase):

    def test_ssl_creator_requires_prepared_context(self):
        with self.assertRaisesRegex(ValueError, 'prepared pyOpenSSL context'):
            twistedreactor._SSLCreator(
                DefaultEndPoint('node.example.com'), None, {}, False)

    def test_check_hostname_option_enables_hostname_validation(self):
        conn = TwistedConnection.__new__(TwistedConnection)

        Connection.__init__(conn, DefaultEndPoint('1.2.3.4'), ssl_options={'check_hostname': True})

        assert conn._check_hostname

    def test_explicit_empty_options_with_endpoint_sni_remain_unverified(self):
        conn = TwistedConnection.__new__(TwistedConnection)

        Connection.__init__(
            conn,
            SniEndPoint('1.2.3.4', 'node.example.com'),
            ssl_options={})

        assert conn.ssl_options['server_hostname'] == 'node.example.com'
        assert conn.ssl_context.get_verify_mode() == twistedreactor.SSL.VERIFY_NONE

    def test_endpoint_context_options_rejected_for_supplied_context(self):
        endpoint = SniEndPoint('1.2.3.4', 'node.example.com')
        endpoint._ssl_options.update({
            'ca_certs': UNIT_CA_CERT,
            'check_hostname': True,
        })
        context = twistedreactor.SSL.Context(
            twistedreactor.SSL.TLS_METHOD)
        conn = TwistedConnection.__new__(TwistedConnection)

        with self.assertRaisesRegex(ValueError, "independent SSL context"):
            Connection.__init__(conn, endpoint, ssl_context=context)

        assert context.get_verify_mode() == twistedreactor.SSL.VERIFY_NONE

    def test_supplied_context_check_hostname_requires_peer_verification(self):
        context = twistedreactor.SSL.Context(twistedreactor.SSL.TLS_METHOD)

        assert context.get_verify_mode() == twistedreactor.SSL.VERIFY_NONE

        twistedreactor._SSLCreator(
            DefaultEndPoint('node.example.com'),
            context,
            {'check_hostname': True},
            True)

        assert context.get_verify_mode() == twistedreactor.SSL.VERIFY_PEER

    def test_ssl_creator_sets_hostname_callback_on_connection(self):
        context = Mock()
        context.get_verify_mode.return_value = twistedreactor.SSL.VERIFY_PEER

        with patch.object(twistedreactor.SSL, 'Connection') as connection_mock:
            creator = twistedreactor._SSLCreator(
                DefaultEndPoint('node.example.com'),
                context,
                {'server_hostname': 'node.example.com'},
                True)
            connection = connection_mock.return_value
            result = creator.clientConnectionForTLS(Mock())

        context.set_info_callback.assert_not_called()
        assert result is connection
        connection.set_info_callback.assert_called_once_with(twistedreactor._SSLCreator.info_callback)
        connection.set_tlsext_host_name.assert_called_once_with(b'node.example.com')

    def test_sni_endpoint_routes_by_host_id_but_verifies_proxy_address(self):
        host_id = '01234567-89ab-cdef-0123-456789abcdef'
        endpoint = SniEndPoint('proxy.example.com', host_id)
        context = Mock()
        context.get_verify_mode.return_value = twistedreactor.SSL.VERIFY_PEER

        with patch.object(twistedreactor.SSL, 'Connection') as connection_mock:
            tls_protocol = Mock()
            creator = twistedreactor._SSLCreator(
                endpoint, context, endpoint.ssl_options, True)
            connection = connection_mock.return_value
            creator.clientConnectionForTLS(tls_protocol)

        connection.set_tlsext_host_name.assert_called_once_with(
            host_id.encode('ascii'))
        tls_app_data = getattr(
            tls_protocol, twistedreactor._TLS_APP_DATA_ATTR)
        assert tls_app_data.expected_name == endpoint.address

        connection.get_app_data.return_value = tls_protocol
        certificate = connection.get_peer_certificate.return_value
        with patch.object(
                twistedreactor, '_validate_pyopenssl_hostname') as validate_hostname:
            twistedreactor._SSLCreator.info_callback(
                connection, twistedreactor.SSL.SSL_CB_HANDSHAKE_DONE, 1)

        validate_hostname.assert_called_once_with(
            certificate, endpoint.address)

    def test_ssl_creator_uses_endpoint_address_for_sni_when_checking_hostname(self):
        context = Mock()
        context.get_verify_mode.return_value = twistedreactor.SSL.VERIFY_PEER

        with patch.object(twistedreactor.SSL, 'Connection') as connection_mock:
            creator = twistedreactor._SSLCreator(
                DefaultEndPoint('node.example.com'),
                context,
                {},
                True)
            connection = connection_mock.return_value
            result = creator.clientConnectionForTLS(Mock())

        assert result is connection
        connection.set_tlsext_host_name.assert_called_once_with(b'node.example.com')

    def test_ssl_creator_idna_encodes_unicode_sni(self):
        context = Mock()
        context.get_verify_mode.return_value = twistedreactor.SSL.VERIFY_PEER

        with patch.object(twistedreactor.SSL, 'Connection') as connection_mock:
            creator = twistedreactor._SSLCreator(
                DefaultEndPoint('täst.example'),
                context,
                {},
                True)
            connection = connection_mock.return_value
            result = creator.clientConnectionForTLS(Mock())

        assert result is connection
        connection.set_tlsext_host_name.assert_called_once_with(
            b'xn--tst-qla.example')

    def test_ssl_creator_omits_implicit_sni_for_ip_addresses(self):
        context = Mock()
        context.get_verify_mode.return_value = twistedreactor.SSL.VERIFY_PEER

        for address in ('1.2.3.4', '2001:db8::1'):
            with self.subTest(address=address):
                with patch.object(twistedreactor.SSL, 'Connection') as connection_mock:
                    creator = twistedreactor._SSLCreator(
                        DefaultEndPoint(address),
                        context,
                        {},
                        True)
                    tls_protocol = Mock()
                    connection = connection_mock.return_value
                    result = creator.clientConnectionForTLS(tls_protocol)

                assert result is connection
                connection.set_tlsext_host_name.assert_not_called()
                tls_app_data = getattr(
                    tls_protocol, twistedreactor._TLS_APP_DATA_ATTR)
                assert tls_app_data.expected_name == address

    def test_ssl_creator_preserves_explicit_ip_sni(self):
        context = Mock()
        context.get_verify_mode.return_value = twistedreactor.SSL.VERIFY_PEER

        with patch.object(twistedreactor.SSL, 'Connection') as connection_mock:
            creator = twistedreactor._SSLCreator(
                DefaultEndPoint('node.example.com'),
                context,
                {'server_hostname': '1.2.3.4'},
                True)
            connection = connection_mock.return_value
            tls_protocol = Mock()
            result = creator.clientConnectionForTLS(tls_protocol)

        assert result is connection
        connection.set_tlsext_host_name.assert_called_once_with(b'1.2.3.4')
        tls_app_data = getattr(tls_protocol, twistedreactor._TLS_APP_DATA_ATTR)
        assert tls_app_data.expected_name == '1.2.3.4'

    def test_ssl_creator_uses_context_callback_when_connection_callback_is_unavailable(self):
        class ConnectionWithoutInfoCallback(object):
            def __init__(self, context, socket):
                self.context = context
                self.socket = socket
                self.app_data = None
                self.peer_cert = Mock()

            def set_app_data(self, app_data):
                self.app_data = app_data

            def get_app_data(self):
                return self.app_data

            def get_peer_certificate(self):
                return self.peer_cert

            def set_tlsext_host_name(self, server_hostname):
                self.server_hostname = server_hostname

        context = Mock()
        context.get_verify_mode.return_value = twistedreactor.SSL.VERIFY_PEER

        with self.assertLogs(
                'cassandra.io.twistedreactor', level='WARNING') as logs:
            with patch.object(
                    twistedreactor.SSL, 'Connection',
                    ConnectionWithoutInfoCallback):
                creator = twistedreactor._SSLCreator(
                    DefaultEndPoint('node.example.com'),
                    context,
                    {'server_hostname': 'node.example.com'},
                    True)
                tls_protocol = Mock()
                result = creator.clientConnectionForTLS(tls_protocol)

        assert "replacing any callback" in logs.output[0]
        context.set_info_callback.assert_called_once_with(twistedreactor._SSLCreator.info_callback)
        assert result.context is context
        assert result.socket is None
        assert result.app_data is tls_protocol
        tls_app_data = getattr(tls_protocol, twistedreactor._TLS_APP_DATA_ATTR)
        assert tls_app_data.endpoint == creator.endpoint
        assert tls_app_data.expected_name == 'node.example.com'
        assert result.server_hostname == b'node.example.com'

    def test_context_callback_uses_connection_app_data(self):
        class ConnectionWithoutInfoCallback(object):
            def __init__(self, context, socket):
                self.context = context
                self.socket = socket
                self.app_data = None
                self.peer_cert = Mock()

            def set_app_data(self, app_data):
                self.app_data = app_data

            def get_app_data(self):
                return self.app_data

            def get_peer_certificate(self):
                return self.peer_cert

            def set_tlsext_host_name(self, server_hostname):
                self.server_hostname = server_hostname

        context = Mock()
        context.get_verify_mode.return_value = twistedreactor.SSL.VERIFY_PEER

        with patch.object(twistedreactor.SSL, 'Connection', ConnectionWithoutInfoCallback):
            first_creator = twistedreactor._SSLCreator(
                DefaultEndPoint('node1.example.com'),
                context,
                {},
                True)
            first_protocol = Mock()
            first_connection = first_creator.clientConnectionForTLS(first_protocol)
            twistedreactor._SSLCreator(
                DefaultEndPoint('node2.example.com'),
                context,
                {},
                True).clientConnectionForTLS(Mock())

        assert first_connection.get_app_data() is first_protocol
        callback = context.set_info_callback.call_args[0][0]
        with patch.object(twistedreactor, '_validate_pyopenssl_hostname') as validate_hostname:
            callback(first_connection, twistedreactor.SSL.SSL_CB_HANDSHAKE_DONE, 1)

        validate_hostname.assert_called_once_with(
            first_connection.peer_cert, 'node1.example.com')

    def test_info_callback_fails_closed_on_unexpected_exception(self):
        connection = Mock()
        tls_protocol = Mock()
        setattr(tls_protocol, twistedreactor._TLS_APP_DATA_ATTR,
                twistedreactor._TLSAppData(
                    DefaultEndPoint('node.example.com'), 'node.example.com', True))
        connection.get_app_data.return_value = tls_protocol
        connection.get_peer_certificate.return_value = object()

        with patch.object(twistedreactor, '_validate_pyopenssl_hostname',
                          side_effect=RuntimeError('boom')):
            twistedreactor._SSLCreator.info_callback(
                connection, twistedreactor.SSL.SSL_CB_HANDSHAKE_DONE, 1)

        failure = tls_protocol.failVerification.call_args[0][0]
        assert isinstance(failure.value, ConnectionException)
        assert "Hostname verification failed" in str(failure.value)


class TestTwistedTimer(TimerTestMixin, unittest.TestCase):
    """
    Simple test class that is used to validate that the TimerManager, and timer
    classes function appropriately with the twisted infrastructure
    """

    connection_class = TwistedConnection

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        return self.connection._loop._timers

    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        super(TestTwistedTimer, self).setUp()


class TestTwistedProtocol(unittest.TestCase):

    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        twistedreactor.TwistedConnection.initialize_reactor()
        self.tr = proto_helpers.StringTransportWithDisconnection()
        self.tr.connector = Mock()
        self.mock_connection = Mock()
        self.obj_ut = twistedreactor.TwistedConnectionProtocol(self.mock_connection)
        self.tr.protocol = self.obj_ut

    def tearDown(self):
        loop = twistedreactor.TwistedConnection._loop
        if loop and not loop._reactor_stopped():
            loop._cleanup()

    def test_makeConnection(self):
        """
        Verify that the protocol class notifies the connection
        object that a successful connection was made.
        """
        self.obj_ut.makeConnection(self.tr)
        assert self.mock_connection.client_connection_made.called

    def test_receiving_data(self):
        """
        Verify that the dataReceived() callback writes the data to
        the connection object's buffer and calls handle_read().
        """
        self.obj_ut.makeConnection(self.tr)
        self.obj_ut.dataReceived('foobar')
        assert self.mock_connection.handle_read.called
        self.mock_connection._iobuf.write.assert_called_with("foobar")


class TestTwistedConnection(unittest.TestCase):
    def setUp(self):
        if twistedreactor is None:
            raise unittest.SkipTest("Twisted libraries not available")
        if twistedreactor.TwistedConnection._loop:
            twistedreactor.TwistedConnection._loop._cleanup()
        twistedreactor.TwistedConnection.initialize_reactor()
        self.reactor_cft_patcher = patch(
            'twisted.internet.reactor.callFromThread')
        self.reactor_run_patcher = patch('twisted.internet.reactor.run')
        # Patch reactor.running to False so maybe_start() always enters
        # the branch that spawns the reactor thread. Without this, leaked
        # reactor state from prior tests can cause reactor.running to be
        # True, making maybe_start() a no-op and the reactor.run mock
        # never called — leading to a flaky test_connection_initialization.
        self.reactor_running_patcher = patch(
            'twisted.internet.reactor.running', new=False)
        self.mock_reactor_cft = self.reactor_cft_patcher.start()
        self.mock_reactor_run = self.reactor_run_patcher.start()
        self.reactor_running_patcher.start()
        self.obj_ut = twistedreactor.TwistedConnection(DefaultEndPoint('1.2.3.4'),
                                                       cql_version='3.0.1')

    def tearDown(self):
        self.reactor_cft_patcher.stop()
        self.reactor_run_patcher.stop()
        self.reactor_running_patcher.stop()

    def test_connection_initialization(self):
        """
        Verify that __init__() works correctly.
        """
        self.mock_reactor_cft.assert_called_with(self.obj_ut.add_connection)
        self.mock_reactor_run.assert_called_with(installSignalHandlers=False)

    def test_client_connection_made(self):
        """
        Verifiy that _send_options_message() is called in
        client_connection_made()
        """
        self.obj_ut._send_options_message = Mock()
        self.obj_ut.client_connection_made(Mock())
        self.obj_ut._send_options_message.assert_called_with()

    @patch('twisted.internet.reactor.connectTCP')
    def test_close(self, mock_connectTCP):
        """
        Verify that close() disconnects the connector and errors callbacks.
        """
        transport = Mock()
        self.obj_ut.error_all_requests = Mock()
        self.obj_ut.add_connection()
        self.obj_ut.client_connection_made(transport)
        self.obj_ut.is_closed = False
        self.obj_ut.close()

        assert self.obj_ut.connected_event.is_set()
        assert self.obj_ut.error_all_requests.called

    def test_handle_read__incomplete(self):
        """
        Verify that handle_read() processes incomplete messages properly.
        """
        self.obj_ut.process_msg = Mock()
        assert self.obj_ut._iobuf.getvalue() == b''  # buf starts empty
        # incomplete header
        self.obj_ut._iobuf.write(b'\x84\x00\x00\x00\x00')
        self.obj_ut.handle_read()
        assert self.obj_ut._io_buffer.cql_frame_buffer.getvalue() == b'\x84\x00\x00\x00\x00'

        # full header, but incomplete body
        self.obj_ut._iobuf.write(b'\x00\x00\x00\x15')
        self.obj_ut.handle_read()
        assert self.obj_ut._io_buffer.cql_frame_buffer.getvalue() == b'\x84\x00\x00\x00\x00\x00\x00\x00\x15'
        assert self.obj_ut._current_frame.end_pos == 30

        # verify we never attempted to process the incomplete message
        assert not self.obj_ut.process_msg.called

    def test_handle_read__fullmessage(self):
        """
        Verify that handle_read() processes complete messages properly.
        """
        self.obj_ut.process_msg = Mock()
        assert self.obj_ut._iobuf.getvalue() == b''  # buf starts empty

        # write a complete message, plus 'NEXT' (to simulate next message)
        # assumes protocol v3+ as default Connection.protocol_version
        body = b'this is the drum roll'
        extra = b'NEXT'
        self.obj_ut._iobuf.write(
            b'\x84\x01\x00\x02\x03\x00\x00\x00\x15' + body + extra)
        self.obj_ut.handle_read()
        assert self.obj_ut._io_buffer.cql_frame_buffer.getvalue() == extra
        self.obj_ut.process_msg.assert_called_with(
            _Frame(version=4, flags=1, stream=2, opcode=3, body_offset=9, end_pos=9 + len(body)), body)

    @patch('twisted.internet.reactor.connectTCP')
    def test_push(self, mock_connectTCP):
        """
        Verifiy that push() calls transport.write(data).
        """
        self.obj_ut.add_connection()
        transport_mock = Mock()
        self.obj_ut.transport = transport_mock
        self.obj_ut.push('123 pickup')
        self.mock_reactor_cft.assert_called_with(
            transport_mock.write, '123 pickup')

    @unittest.skipIf(not getattr(twistedreactor, '_HAS_SSL', False), "pyOpenSSL is not available")
    @patch('cassandra.io.twistedreactor.connectProtocol')
    @patch('cassandra.io.twistedreactor.TCP4ClientEndpoint')
    @patch('cassandra.io.twistedreactor.SSL4ClientEndpoint')
    def test_empty_ssl_options_use_ssl_endpoint(self, mock_ssl_endpoint, mock_tcp_endpoint, mock_connect_protocol):
        conn = twistedreactor.TwistedConnection(
            DefaultEndPoint('1.2.3.4'),
            cql_version='3.0.1',
            ssl_options={})

        conn.add_connection()

        mock_ssl_endpoint.assert_called_once()
        mock_tcp_endpoint.assert_not_called()
        mock_connect_protocol.assert_called_once()
