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
import itertools
import socket
import ssl
import unittest
from io import BytesIO
import time
from threading import Lock
from unittest.mock import Mock, ANY, call, patch

from cassandra import OperationTimedOut
from cassandra.cluster import Cluster
from cassandra.connection import (Connection, HEADER_DIRECTION_TO_CLIENT, ProtocolError,
                                  locally_supported_compressions, ConnectionHeartbeat, HeartbeatFuture, _Frame, Timer, TimerManager,
                                  ConnectionException, ConnectionShutdown, DefaultEndPoint, SniEndPoint, ShardAwarePortGenerator)
from cassandra.marshal import uint8_pack, uint32_pack, int32_pack
from cassandra.protocol import (write_stringmultimap, write_int, write_string,
                                SupportedMessage, ProtocolHandler, ResultMessage,
                                RESULT_KIND_SET_KEYSPACE)
from tests.util import wait_until, assertRegex
import pytest


class ConnectionTest(unittest.TestCase):

    def make_connection(self, **kwargs):
        c = Connection(DefaultEndPoint('1.2.3.4'), **kwargs)
        c._socket = Mock()
        c._socket.send.side_effect = lambda x: len(x)
        return c

    def make_header_prefix(self, message_class, version=Connection.protocol_version, stream_id=0):
        return bytes().join(map(uint8_pack, [
            0xff & (HEADER_DIRECTION_TO_CLIENT | version),
            0,  # flags (compression)
            0,  # MSB for v3+ stream
            stream_id,
            message_class.opcode  # opcode
        ]))

    def make_options_body(self):
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.1'],
            'COMPRESSION': []
        })
        return options_buf.getvalue()

    def make_error_body(self, code, msg):
        buf = BytesIO()
        write_int(buf, code)
        write_string(buf, msg)
        return buf.getvalue()

    def make_msg(self, header, body=""):
        return header + uint32_pack(len(body)) + body

    def test_connection_endpoint(self):
        endpoint = DefaultEndPoint('1.2.3.4')
        c = Connection(endpoint)
        assert c.endpoint == endpoint
        assert c.endpoint.address == endpoint.address

        c = Connection(host=endpoint)  # kwarg
        assert c.endpoint == endpoint
        assert c.endpoint.address == endpoint.address

        c = Connection('10.0.0.1')
        endpoint = DefaultEndPoint('10.0.0.1')
        assert c.endpoint == endpoint
        assert c.endpoint.address == endpoint.address

    def test_omitted_ssl_options_do_not_enable_ssl(self):
        c = Connection(DefaultEndPoint('1.2.3.4'))

        assert c.ssl_context is None
        assert not c._ssl_enabled

    def test_tls_error_is_not_masked_by_later_socket_error(self):
        first_socket = Mock()
        second_socket = Mock()
        tls_error = ssl.SSLError(1, 'certificate verify failed')
        first_socket.connect.side_effect = tls_error
        second_socket.connect.side_effect = socket.error(
            111, 'Connection refused')
        c = Connection.__new__(Connection)
        c.endpoint = DefaultEndPoint('node.example.com')
        c._get_socket_addresses = Mock(return_value=[
            (socket.AF_INET, socket.SOCK_STREAM, 6, '',
             ('192.0.2.1', 9042)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, '',
             ('192.0.2.2', 9042)),
        ])
        c._socket_impl = Mock()
        c._socket_impl.socket.side_effect = [first_socket, second_socket]
        c.ssl_context = None
        c.features = Mock(shard_id=None)
        c.connect_timeout = 5
        c._check_hostname = False
        c.sockopts = None

        with self.assertRaises(ssl.SSLError) as raised:
            c._connect_socket()

        assert raised.exception is tls_error
        first_socket.close.assert_called_once_with()
        second_socket.close.assert_called_once_with()

    def test_empty_ssl_options_enable_ssl(self):
        c = Connection(DefaultEndPoint('1.2.3.4'), ssl_options={})

        assert isinstance(c.ssl_context, ssl.SSLContext)
        assert c.ssl_context.verify_mode == ssl.CERT_NONE
        assert c.ssl_options == {}
        assert c._ssl_enabled

    def test_symbolic_stdlib_ssl_protocol_builds_stdlib_context(self):
        c = Connection(
            DefaultEndPoint('1.2.3.4'),
            ssl_options={'ssl_version': 'PROTOCOL_TLS'})

        assert c.ssl_context.protocol == ssl.PROTOCOL_TLS

    def test_check_hostname_secures_supplied_stdlib_context(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        c = Connection(
            DefaultEndPoint('node.example.com'),
            ssl_context=context,
            ssl_options={'check_hostname': True})

        assert c._check_hostname
        assert context.check_hostname
        assert context.verify_mode == ssl.CERT_REQUIRED

    def test_empty_ssl_options_with_endpoint_sni_preserve_cert_none(self):
        c = Connection(
            SniEndPoint('1.2.3.4', 'node.example.com'),
            ssl_options={})

        assert c.ssl_options == {'server_hostname': 'node.example.com'}
        assert c.ssl_context.verify_mode == ssl.CERT_NONE
        assert not c._ssl_options_verify_by_default

    def test_empty_ssl_options_with_endpoint_ca_default_to_cert_required(self):
        endpoint = SniEndPoint('1.2.3.4', 'node.example.com')
        endpoint._ssl_options = {'ca_certs': 'endpoint-ca.pem'}
        context = Mock()

        with patch('cassandra.tls.ssl.SSLContext', return_value=context):
            c = Connection(endpoint, ssl_options={})

        assert c.ssl_options == {'ca_certs': 'endpoint-ca.pem'}
        assert c._ssl_options_verify_by_default
        assert context.verify_mode == ssl.CERT_REQUIRED
        context.load_verify_locations.assert_called_once_with(
            'endpoint-ca.pem')

    def test_omitted_ssl_options_with_endpoint_sni_default_to_cert_required(self):
        c = Connection(SniEndPoint('1.2.3.4', 'node.example.com'))

        assert c.ssl_options == {'server_hostname': 'node.example.com'}
        assert c.ssl_context.verify_mode == ssl.CERT_REQUIRED
        assert c._ssl_options_verify_by_default

    def test_explicit_verification_options_override_origin_default(self):
        endpoint = SniEndPoint('1.2.3.4', 'endpoint.example.com')

        c = Connection(
            endpoint,
            ssl_options={
                'server_hostname': 'caller.example.com',
                'cert_reqs': ssl.CERT_NONE,
                'check_hostname': False,
            })

        assert c.ssl_options['server_hostname'] == 'endpoint.example.com'
        assert c.ssl_context.verify_mode == ssl.CERT_NONE
        assert not c.ssl_context.check_hostname

    def test_endpoint_check_hostname_overrides_disabled_origin_default(self):
        endpoint = SniEndPoint('1.2.3.4', 'endpoint.example.com')
        endpoint._ssl_options['check_hostname'] = True

        c = Connection(endpoint, ssl_options={})

        assert c.ssl_context.verify_mode == ssl.CERT_REQUIRED
        assert c.ssl_context.check_hostname

    def test_endpoint_context_options_rejected_for_supplied_context(self):
        endpoint = SniEndPoint('1.2.3.4', 'endpoint.example.com')
        endpoint._ssl_options.update({
            'ca_certs': 'endpoint-ca.pem',
            'cert_reqs': ssl.CERT_REQUIRED,
            'check_hostname': True,
        })
        context = Mock(spec=ssl.SSLContext)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        with pytest.raises(ValueError, match="independent SSL context"):
            Connection(endpoint, ssl_context=context)

        assert context.verify_mode == ssl.CERT_NONE
        assert not context.check_hostname
        context.load_verify_locations.assert_not_called()

    def test_endpoint_sni_does_not_mutate_supplied_context(self):
        endpoint = SniEndPoint('1.2.3.4', 'endpoint.example.com')
        context = Mock(spec=ssl.SSLContext)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        c = Connection(endpoint, ssl_context=context)

        assert c.ssl_context is context
        assert c.ssl_options == {
            'server_hostname': 'endpoint.example.com'}
        assert context.verify_mode == ssl.CERT_NONE
        assert not context.check_hostname
        context.load_verify_locations.assert_not_called()
        context.load_cert_chain.assert_not_called()
        context.set_ciphers.assert_not_called()

    def test_endpoint_option_does_not_promote_explicit_cert_none(self):
        endpoint = SniEndPoint('1.2.3.4', 'endpoint.example.com')
        endpoint._ssl_options = {'ciphers': 'DEFAULT'}
        context = Mock(spec=ssl.SSLContext)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        with pytest.raises(ValueError, match="independent SSL context"):
            Connection(
                endpoint,
                ssl_context=context,
                ssl_options={'cert_reqs': ssl.CERT_NONE})

        assert context.verify_mode == ssl.CERT_NONE
        assert not context.check_hostname
        context.set_ciphers.assert_not_called()

    def test_non_empty_ssl_options_default_to_cert_required(self):
        for ssl_options in ({'server_hostname': 'node.example.com'}, {'ciphers': 'DEFAULT'}):
            with self.subTest(ssl_options=ssl_options):
                c = Connection(DefaultEndPoint('1.2.3.4'), ssl_options=ssl_options)

                assert isinstance(c.ssl_context, ssl.SSLContext)
                assert c.ssl_context.verify_mode == ssl.CERT_REQUIRED

    def test_ssl_options_cert_reqs_applied_to_context(self):
        c = Connection(DefaultEndPoint('1.2.3.4'), ssl_options={'cert_reqs': ssl.CERT_REQUIRED})

        assert isinstance(c.ssl_context, ssl.SSLContext)
        assert c.ssl_context.verify_mode == ssl.CERT_REQUIRED

    def test_ssl_options_check_hostname_requires_validation(self):
        c = Connection(DefaultEndPoint('1.2.3.4'), ssl_options={'check_hostname': True})

        assert isinstance(c.ssl_context, ssl.SSLContext)
        assert c.ssl_context.verify_mode == ssl.CERT_REQUIRED
        assert c.ssl_context.check_hostname
        assert c._check_hostname

    def test_ssl_options_check_hostname_promotes_cert_none_to_cert_required(self):
        c = Connection(
            DefaultEndPoint('1.2.3.4'),
            ssl_options={'cert_reqs': ssl.CERT_NONE, 'check_hostname': True})

        assert isinstance(c.ssl_context, ssl.SSLContext)
        assert c.ssl_context.verify_mode == ssl.CERT_REQUIRED
        assert c.ssl_context.check_hostname
        assert c._check_hostname

    def test_bad_protocol_version(self, *args):
        c = self.make_connection()
        c._requests = Mock()
        c.defunct = Mock()

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage, version=0x7f)
        options = self.make_options_body()
        message = self.make_msg(header, options)
        c._iobuf._io_buffer = BytesIO()
        c._iobuf.write(message)
        c.process_io_buffer()

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        assert isinstance(args[0], ProtocolError)

    def test_negative_body_length(self, *args):
        c = self.make_connection()
        c._requests = Mock()
        c.defunct = Mock()

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)
        message = header + int32_pack(-13)
        c._iobuf._io_buffer = BytesIO()
        c._iobuf.write(message)
        c.process_io_buffer()

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        assert isinstance(args[0], ProtocolError)

    def test_unsupported_cql_version(self, *args):
        c = self.make_connection()
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message, [])}
        c.defunct = Mock()
        c.cql_version = "3.0.3"

        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['7.8.9'],
            'COMPRESSION': []
        })
        options = options_buf.getvalue()

        c.process_msg(_Frame(version=4, flags=0, stream=0, opcode=SupportedMessage.opcode, body_offset=9, end_pos=9 + len(options)), options)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        assert isinstance(args[0], ProtocolError)

    def test_prefer_lz4_compression(self, *args):
        c = self.make_connection()
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message, [])}
        c.defunct = Mock()
        c.cql_version = "3.0.3"

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # read in a SupportedMessage response
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy', 'lz4']
        })
        options = options_buf.getvalue()

        c.process_msg(_Frame(version=4, flags=0, stream=0, opcode=SupportedMessage.opcode, body_offset=9, end_pos=9 + len(options)), options)

        assert c.decompressor == locally_supported_compressions['lz4'][1]

    def test_requested_compression_not_available(self, *args):
        c = self.make_connection()
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message, [])}
        c.defunct = Mock()
        # request lz4 compression
        c.compression = "lz4"

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # the server only supports snappy
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy']
        })
        options = options_buf.getvalue()

        c.process_msg(_Frame(version=4, flags=0, stream=0, opcode=SupportedMessage.opcode, body_offset=9, end_pos=9 + len(options)), options)

        # make sure it errored correctly
        c.defunct.assert_called_once_with(ANY)
        args, kwargs = c.defunct.call_args
        assert isinstance(args[0], ProtocolError)

    def test_use_requested_compression(self, *args):
        c = self.make_connection(protocol_version=4)
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message, [])}
        c.defunct = Mock()
        # request snappy compression
        c.compression = "snappy"

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # the server only supports snappy
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy', 'lz4']
        })
        options = options_buf.getvalue()

        c.process_msg(_Frame(version=4, flags=0, stream=0, opcode=SupportedMessage.opcode, body_offset=9, end_pos=9 + len(options)), options)

        assert c.decompressor == locally_supported_compressions['snappy'][1]

    def test_disable_compression(self, *args):
        c = self.make_connection()
        c._requests = {0: (c._handle_options_response, ProtocolHandler.decode_message)}
        c.defunct = Mock()
        # disable compression
        c.compression = False

        locally_supported_compressions.pop('lz4', None)
        locally_supported_compressions.pop('snappy', None)
        locally_supported_compressions['lz4'] = ('lz4compress', 'lz4decompress')
        locally_supported_compressions['snappy'] = ('snappycompress', 'snappydecompress')

        # read in a SupportedMessage response
        header = self.make_header_prefix(SupportedMessage)

        # the server only supports snappy
        options_buf = BytesIO()
        write_stringmultimap(options_buf, {
            'CQL_VERSION': ['3.0.3'],
            'COMPRESSION': ['snappy', 'lz4']
        })
        options = options_buf.getvalue()

        message = self.make_msg(header, options)
        c.process_msg(message, len(message) - 8)

        assert c.decompressor == None

    def test_not_implemented(self):
        """
        Ensure the following methods throw NIE's. If not, come back and test them.
        """
        c = self.make_connection()
        with pytest.raises(NotImplementedError):
            c.close()

    def test_set_keyspace_blocking(self):
        c = self.make_connection()

        assert c.keyspace == None
        c.set_keyspace_blocking(None)
        assert c.keyspace == None

        c.keyspace = 'ks'
        c.set_keyspace_blocking('ks')
        assert c.keyspace == 'ks'

    def test_set_keyspace_blocking_escapes_quotes(self):
        """
        Test that set_keyspace_blocking properly escapes double quotes in
        keyspace names to prevent CQL injection. This is the Python equivalent
        of the vulnerability fixed in the Go driver:
        https://github.com/scylladb/gocql/pull/783
        """
        c = self.make_connection()
        c.wait_for_response = Mock(return_value=ResultMessage(kind=RESULT_KIND_SET_KEYSPACE))

        c.set_keyspace_blocking('my"ks')
        query_msg = c.wait_for_response.call_args[0][0]
        assert query_msg.query == 'USE "my""ks"', (
            "Double quotes in keyspace name must be escaped as double-double quotes")

    def test_set_keyspace_async_escapes_quotes(self):
        """
        Test that set_keyspace_async properly escapes double quotes in
        keyspace names to prevent CQL injection.
        """
        c = self.make_connection()
        c.lock = Lock()
        c.in_flight = 0
        c.max_request_id = 100
        c.get_request_id = Mock(return_value=1)
        c.send_msg = Mock()

        callback = Mock()
        c.set_keyspace_async('my"ks', callback)

        query_msg = c.send_msg.call_args[0][0]
        assert query_msg.query == 'USE "my""ks"', (
            "Double quotes in keyspace name must be escaped as double-double quotes")

    def test_send_msg_passes_negotiated_features_to_encoder(self):
        """
        send_msg must hand the connection's negotiated ProtocolFeatures to the
        encoder, so message serialization can emit fields belonging to protocol
        extensions exactly on the connections that negotiated them.
        """
        c = self.make_connection()
        c.push = Mock()
        captured = {}

        def encoder(msg, stream_id, protocol_version, compressor, allow_beta_protocol_version,
                    protocol_features=None):
            captured['protocol_features'] = protocol_features
            return b'encoded-frame'

        c.send_msg(Mock(), 1, cb=Mock(), encoder=encoder, decoder=Mock())

        assert captured['protocol_features'] is c.features
        c.push.assert_called_once_with(b'encoded-frame')

    def test_set_connection_class(self):
        cluster = Cluster(connection_class='test')
        assert 'test' == cluster.connection_class

    def test_connection_shutdown_includes_last_error(self):
        """
        Test that ConnectionShutdown exceptions include the last_error when available.
        This helps debug issues like "Bad file descriptor" by showing the original cause.
        See https://github.com/scylladb/python-driver/issues/614
        """
        c = self.make_connection()
        c.lock = Lock()
        c._requests = {}

        # Simulate the connection becoming defunct with a specific error
        original_error = OSError(9, "Bad file descriptor")
        c.is_defunct = True
        c.last_error = original_error

        # send_msg should raise ConnectionShutdown that includes the last_error
        with pytest.raises(ConnectionShutdown) as exc_info:
            c.send_msg(Mock(), 1, Mock())

        # Verify the error message includes the original error
        error_message = str(exc_info.value)
        assert "is defunct" in error_message
        assert "Bad file descriptor" in error_message

    def test_connection_shutdown_closed_includes_last_error(self):
        """
        Test that ConnectionShutdown exceptions for closed connections include last_error.
        """
        c = self.make_connection()
        c.lock = Lock()
        c._requests = {}

        # Simulate the connection being closed with a specific error
        original_error = OSError(9, "Bad file descriptor")
        c.is_closed = True
        c.last_error = original_error

        # send_msg should raise ConnectionShutdown that includes the last_error
        with pytest.raises(ConnectionShutdown) as exc_info:
            c.send_msg(Mock(), 1, Mock())

        # Verify the error message includes the original error
        error_message = str(exc_info.value)
        assert "is closed" in error_message
        assert "Bad file descriptor" in error_message

    def test_wait_for_responses_shutdown_includes_last_error(self):
        """
        Test that wait_for_responses raises ConnectionShutdown with last_error.
        """
        c = self.make_connection()
        c.lock = Lock()
        c._requests = {}

        # Simulate the connection being defunct with a specific error
        original_error = OSError(9, "Bad file descriptor")
        c.is_defunct = True
        c.last_error = original_error

        # wait_for_responses should raise ConnectionShutdown that includes the last_error
        with pytest.raises(ConnectionShutdown) as exc_info:
            c.wait_for_responses(Mock())

        # Verify the error message includes the original error
        error_message = str(exc_info.value)
        assert "already closed" in error_message
        assert "Bad file descriptor" in error_message


@patch('cassandra.connection.ConnectionHeartbeat._raise_if_stopped')
class ConnectionHeartbeatTest(unittest.TestCase):

    @staticmethod
    def make_get_holders(len):
        holders = []
        for _ in range(len):
            holder = Mock()
            holder.get_connections = Mock(return_value=[])
            holders.append(holder)
        get_holders = Mock(return_value=holders)
        return get_holders

    def run_heartbeat(self, get_holders_fun, count=2, interval=0.05, timeout=0.05):
        ch = ConnectionHeartbeat(interval, get_holders_fun, timeout=timeout)
        # wait until the thread is started
        wait_until(lambda: get_holders_fun.call_count > 0, 0.01, 100)
        time.sleep(interval * (count-1))
        ch.stop()
        assert get_holders_fun.call_count

    def test_empty_connections(self, *args):
        count = 3
        get_holders = self.make_get_holders(1)

        self.run_heartbeat(get_holders, count)

        assert get_holders.call_count >= count-1
        assert get_holders.call_count <= count
        holder = get_holders.return_value[0]
        holder.get_connections.assert_has_calls([call()] * get_holders.call_count)

    def test_idle_non_idle(self, *args):
        request_id = 999

        # connection.send_msg(OptionsMessage(), connection.get_request_id(), self._options_callback)
        def send_msg(msg, req_id, msg_callback):
            msg_callback(SupportedMessage([], {}))

        idle_connection = Mock(spec=Connection, host='localhost',
                               max_request_id=127,
                               lock=Lock(),
                               in_flight=0, is_idle=True,
                               is_defunct=False, is_closed=False,
                               get_request_id=lambda: request_id,
                               send_msg=Mock(side_effect=send_msg))
        non_idle_connection = Mock(spec=Connection, in_flight=0, is_idle=False, is_defunct=False, is_closed=False)

        get_holders = self.make_get_holders(1)
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(idle_connection)
        holder.get_connections.return_value.append(non_idle_connection)

        self.run_heartbeat(get_holders)

        holder.get_connections.assert_has_calls([call()] * get_holders.call_count)
        assert idle_connection.in_flight == 0
        assert non_idle_connection.in_flight == 0

        idle_connection.send_msg.assert_has_calls([call(ANY, request_id, ANY)] * get_holders.call_count)
        assert non_idle_connection.send_msg.call_count == 0

    def test_closed_defunct(self, *args):
        get_holders = self.make_get_holders(1)
        closed_connection = Mock(spec=Connection, in_flight=0, is_idle=False, is_defunct=False, is_closed=True)
        defunct_connection = Mock(spec=Connection, in_flight=0, is_idle=False, is_defunct=True, is_closed=False)
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(closed_connection)
        holder.get_connections.return_value.append(defunct_connection)

        self.run_heartbeat(get_holders)

        holder.get_connections.assert_has_calls([call()] * get_holders.call_count)
        assert closed_connection.in_flight == 0
        assert defunct_connection.in_flight == 0
        assert closed_connection.send_msg.call_count == 0
        assert defunct_connection.send_msg.call_count == 0

    def test_no_req_ids(self, *args):
        in_flight = 3

        get_holders = self.make_get_holders(1)
        max_connection = Mock(spec=Connection, host='localhost',
                              lock=Lock(),
                              max_request_id=in_flight - 1, in_flight=in_flight,
                              is_idle=True, is_defunct=False, is_closed=False)
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(max_connection)

        self.run_heartbeat(get_holders)

        holder.get_connections.assert_has_calls([call()] * get_holders.call_count)
        assert max_connection.in_flight == in_flight
        assert max_connection.send_msg.call_count == 0
        assert max_connection.send_msg.call_count == 0
        max_connection.defunct.assert_has_calls([call(ANY)] * get_holders.call_count)
        holder.return_connection.assert_has_calls(
            [call(max_connection)] * get_holders.call_count)

    def test_heartbeat_future_releases_request_id_when_send_fails(self, *args):
        connection = Connection(DefaultEndPoint('1.2.3.4'))
        connection.push = Mock(side_effect=ConnectionException("write failed"))
        owner = Mock()
        initial_in_flight = connection.in_flight
        initial_request_ids = len(connection.request_ids)

        # HostConnection.return_connection releases the heartbeat's in-flight slot.
        def return_connection(conn):
            with conn.lock:
                conn.in_flight -= 1

        owner.return_connection.side_effect = return_connection

        future = HeartbeatFuture(connection, owner)

        with pytest.raises(ConnectionException):
            future.wait(0)

        owner.return_connection(connection)

        assert connection.in_flight == initial_in_flight
        assert len(connection.request_ids) == initial_request_ids
        assert not connection._requests

    def test_unexpected_response(self, *args):
        request_id = 999

        get_holders = self.make_get_holders(1)

        def send_msg(msg, req_id, msg_callback):
            msg_callback(object())

        connection = Mock(spec=Connection, host='localhost',
                          max_request_id=127,
                          lock=Lock(),
                          in_flight=0, is_idle=True,
                          is_defunct=False, is_closed=False,
                          get_request_id=lambda: request_id,
                          send_msg=Mock(side_effect=send_msg))
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(connection)

        self.run_heartbeat(get_holders)

        assert connection.in_flight == get_holders.call_count
        connection.send_msg.assert_has_calls([call(ANY, request_id, ANY)] * get_holders.call_count)
        connection.defunct.assert_has_calls([call(ANY)] * get_holders.call_count)
        exc = connection.defunct.call_args_list[0][0][0]
        assert isinstance(exc, ConnectionException)
        assertRegex(exc.args[0], r'^Received unexpected response to OptionsMessage.*')
        holder.return_connection.assert_has_calls(
            [call(connection)] * get_holders.call_count)

    def test_timeout(self, *args):
        request_id = 999

        get_holders = self.make_get_holders(1)

        def send_msg(msg, req_id, msg_callback):
            pass

        # we used endpoint=X here because it's a mock and we need connection.endpoint to be set
        connection = Mock(spec=Connection, endpoint=DefaultEndPoint('localhost'),
                          max_request_id=127,
                          lock=Lock(),
                          in_flight=0, is_idle=True,
                          is_defunct=False, is_closed=False,
                          get_request_id=lambda: request_id,
                          send_msg=Mock(side_effect=send_msg))
        holder = get_holders.return_value[0]
        holder.get_connections.return_value.append(connection)

        self.run_heartbeat(get_holders)

        assert connection.in_flight == get_holders.call_count
        connection.send_msg.assert_has_calls([call(ANY, request_id, ANY)] * get_holders.call_count)
        connection.defunct.assert_has_calls([call(ANY)] * get_holders.call_count)
        exc = connection.defunct.call_args_list[0][0][0]
        assert isinstance(exc, OperationTimedOut)
        assert exc.errors == 'Connection heartbeat timeout after 0.05 seconds'
        assert exc.last_host == DefaultEndPoint('localhost')
        assert exc.timeout == 0.05
        assert isinstance(exc.in_flight, int)
        holder.return_connection.assert_has_calls(
            [call(connection)] * get_holders.call_count)


class TimerTest(unittest.TestCase):

    def test_timer_collision(self):
        # simple test demonstrating #466
        # same timeout, comparison will defer to the Timer object itself
        t1 = Timer(0, lambda: None)
        t2 = Timer(0, lambda: None)
        t2.end = t1.end

        tm = TimerManager()
        tm.add_timer(t1)
        tm.add_timer(t2)
        # Prior to #466: "TypeError: unorderable types: Timer() < Timer()"
        tm.service_timeouts()


class DefaultEndPointTest(unittest.TestCase):

    def test_default_endpoint_properties(self):
        endpoint = DefaultEndPoint('10.0.0.1')
        assert endpoint.address == '10.0.0.1'
        assert endpoint.port == 9042
        assert str(endpoint) == '10.0.0.1:9042'

        endpoint = DefaultEndPoint('10.0.0.1', 8888)
        assert endpoint.address == '10.0.0.1'
        assert endpoint.port == 8888
        assert str(endpoint) == '10.0.0.1:8888'

    def test_endpoint_equality(self):
        assert DefaultEndPoint('10.0.0.1') == DefaultEndPoint('10.0.0.1')

        assert DefaultEndPoint('10.0.0.1') == DefaultEndPoint('10.0.0.1', 9042)

        assert DefaultEndPoint('10.0.0.1') != DefaultEndPoint('10.0.0.2')

        assert DefaultEndPoint('10.0.0.1') != DefaultEndPoint('10.0.0.1', 0000)

    def test_endpoint_resolve(self):
        assert DefaultEndPoint('10.0.0.1').resolve() == ('10.0.0.1', 9042)

        assert DefaultEndPoint('10.0.0.1', 3232).resolve() == ('10.0.0.1', 3232)


class TestShardawarePortGenerator(unittest.TestCase):
    @patch('random.randrange')
    def test_generate_ports_basic(self, mock_randrange):
        mock_randrange.return_value = 10005
        gen = ShardAwarePortGenerator(10000, 10020)
        ports = list(itertools.islice(gen.generate(shard_id=1, total_shards=3), 5))

        # Starting from aligned 10005 + shard_id (1), step by 3
        assert ports == [10006, 10009, 10012, 10015, 10018]

    @patch('random.randrange')
    def test_wraps_around_to_start(self, mock_randrange):
        mock_randrange.return_value = 10008
        gen = ShardAwarePortGenerator(10000, 10020)
        ports = list(itertools.islice(gen.generate(shard_id=2, total_shards=4), 5))

        # Expected wrap-around from start_port after end_port is exceeded
        assert ports == [10010, 10014, 10018, 10002, 10006]

    @patch('random.randrange')
    def test_all_ports_have_correct_modulo(self, mock_randrange):
        mock_randrange.return_value = 10012
        total_shards = 5
        shard_id = 3
        gen = ShardAwarePortGenerator(10000, 10020)

        for port in gen.generate(shard_id=shard_id, total_shards=total_shards):
            assert port % total_shards == shard_id

    @patch('random.randrange')
    def test_generate_is_repeatable_with_same_mock(self, mock_randrange):
        mock_randrange.return_value = 10010
        gen = ShardAwarePortGenerator(10000, 10020)

        first_run = list(itertools.islice(gen.generate(0, 2), 5))
        second_run = list(itertools.islice(gen.generate(0, 2), 5))

        assert first_run == second_run
