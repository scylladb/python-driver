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
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock, patch

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    from OpenSSL import crypto
except ImportError:
    crypto = None

from cassandra.connection import Connection, DefaultEndPoint

try:
    from twisted.test import proto_helpers
    from cassandra.io import twistedreactor
    from cassandra.io.twistedreactor import TwistedConnection
except ImportError:
    twistedreactor = TwistedConnection = None  # NOQA


from cassandra.connection import _Frame

from tests.unit.io.utils import TimerTestMixin

CA_CERTS = os.path.abspath(os.path.join(
    os.path.dirname(__file__), '..', '..', 'integration', 'long', 'ssl', 'rootCa.crt'))


def _make_certificate(common_name, san_dns_names=None):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.COMMON_NAME, common_name),
    ])
    now = datetime.now(timezone.utc)
    builder = (x509.CertificateBuilder()
               .subject_name(subject)
               .issuer_name(issuer)
               .public_key(key.public_key())
               .serial_number(x509.random_serial_number())
               .not_valid_before(now - timedelta(days=1))
               .not_valid_after(now + timedelta(days=1)))
    if san_dns_names:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.DNSName(name) for name in san_dns_names]),
            critical=False)

    return crypto.X509.from_cryptography(builder.sign(key, hashes.SHA256()))


@unittest.skipIf(TwistedConnection is None, "Twisted libraries are not available")
@unittest.skipIf(not getattr(twistedreactor, '_HAS_SSL', False), "pyOpenSSL is not available")
class TwistedSSLContextTest(unittest.TestCase):

    def test_empty_ssl_options_default_to_negotiating_tls(self):
        with patch.object(twistedreactor.SSL, 'Context') as context_mock:
            context = twistedreactor._build_pyopenssl_context_from_options({})

        context_mock.assert_called_once_with(twistedreactor.SSL.TLS_CLIENT_METHOD)
        assert context is context_mock.return_value

    def test_default_ssl_method_falls_back_to_tls_method(self):
        tls_method = object()

        with patch.object(twistedreactor, 'SSL', SimpleNamespace(TLS_METHOD=tls_method)):
            assert twistedreactor._default_ssl_method() is tls_method

    def test_default_ssl_method_falls_back_to_tlsv1_2_method(self):
        tlsv1_2_method = object()

        with patch.object(twistedreactor, 'SSL', SimpleNamespace(TLSv1_2_METHOD=tlsv1_2_method)):
            assert twistedreactor._default_ssl_method() is tlsv1_2_method

    def test_ssl_version_option_is_preserved(self):
        with patch.object(twistedreactor.SSL, 'Context') as context_mock:
            twistedreactor._build_pyopenssl_context_from_options(
                {'ssl_version': twistedreactor.SSL.TLSv1_2_METHOD})

        context_mock.assert_called_once_with(twistedreactor.SSL.TLSv1_2_METHOD)

    def test_ca_certs_default_to_required_validation(self):
        context = twistedreactor._build_pyopenssl_context_from_options({'ca_certs': CA_CERTS})

        assert context.get_verify_mode() == twistedreactor.SSL.VERIFY_PEER

    def test_check_hostname_option_enables_hostname_validation(self):
        conn = TwistedConnection.__new__(TwistedConnection)

        Connection.__init__(conn, DefaultEndPoint('1.2.3.4'), ssl_options={'check_hostname': True})

        assert conn._check_hostname

    def test_hostname_verification_uses_server_hostname(self):
        context = Mock()
        creator = twistedreactor._SSLCreator(DefaultEndPoint('proxy.host'), context,
                                             {'server_hostname': 'sni.host'}, True, None)
        connection = Mock()
        connection.get_peer_certificate.return_value = _make_certificate('proxy.host', ['sni.host'])

        creator.info_callback(connection, twistedreactor.SSL.SSL_CB_HANDSHAKE_DONE, None)

        connection.get_app_data.assert_not_called()

    def test_hostname_verification_prefers_san_over_common_name(self):
        context = Mock()
        creator = twistedreactor._SSLCreator(DefaultEndPoint('sni.host'), context, {}, True, None)
        connection = Mock()
        transport = Mock()
        connection.get_app_data.return_value = transport
        connection.get_peer_certificate.return_value = _make_certificate('sni.host', ['other.host'])

        creator.info_callback(connection, twistedreactor.SSL.SSL_CB_HANDSHAKE_DONE, None)

        transport.failVerification.assert_called_once()

    def test_hostname_verification_matches_wildcard_san(self):
        context = Mock()
        creator = twistedreactor._SSLCreator(DefaultEndPoint('node.example.com'), context, {}, True, None)
        connection = Mock()
        connection.get_peer_certificate.return_value = _make_certificate('other.host', ['*.example.com'])

        creator.info_callback(connection, twistedreactor.SSL.SSL_CB_HANDSHAKE_DONE, None)

        connection.get_app_data.assert_not_called()


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
