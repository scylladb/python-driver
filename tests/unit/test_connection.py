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
import unittest
from io import BytesIO
import time
from threading import Event, Lock, RLock, Thread, get_ident
from unittest.mock import Mock, ANY, call, patch

from cassandra import InvalidRequest, OperationTimedOut, Unauthorized
from cassandra.cluster import Cluster
from cassandra.connection import (Connection, ConnectionBusy, HEADER_DIRECTION_TO_CLIENT, ProtocolError,
                                  locally_supported_compressions, ConnectionHeartbeat, HeartbeatFuture, _Frame, Timer, TimerManager,
                                  ConnectionException, ConnectionShutdown, DefaultEndPoint, ShardAwarePortGenerator,
                                  _ConnectionClosedDuringStartup,
                                  _set_keyspace_blocking,
                                  _startup_close_error)
from cassandra.marshal import uint8_pack, uint32_pack, int32_pack
from cassandra.protocol import (write_stringmultimap, write_int, write_string,
                                SupportedMessage, ProtocolHandler, ResultMessage,
                                ReadyMessage, AuthSuccessMessage,
                                InvalidRequestException,
                                UnauthorizedErrorMessage,
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

    def test_set_keyspace_blocking_passes_timeout(self):
        c = self.make_connection()
        c.wait_for_response = Mock(
            return_value=ResultMessage(kind=RESULT_KIND_SET_KEYSPACE))

        c.set_keyspace_blocking('ks', timeout=1.25)

        query_msg = c.wait_for_response.call_args[0][0]
        assert query_msg.query == 'USE "ks"'
        c.wait_for_response.assert_called_once_with(
            query_msg, timeout=1.25)

    def test_keyspace_timeout_adapter_preserves_custom_signatures(self):
        calls = []

        class HistoricalConnection(object):
            def set_keyspace_blocking(self, keyspace):
                calls.append(("historical", keyspace))

        class PositionalTimeoutConnection(object):
            def set_keyspace_blocking(self, keyspace, timeout, /):
                calls.append(("positional", keyspace, timeout))

        class VarargsTimeoutConnection(object):
            def set_keyspace_blocking(self, keyspace, *args):
                calls.append(("varargs", keyspace, args))

        _set_keyspace_blocking(HistoricalConnection(), "ks1", 1.5)
        _set_keyspace_blocking(PositionalTimeoutConnection(), "ks2", 2.5)
        _set_keyspace_blocking(VarargsTimeoutConnection(), "ks3", 3.5)

        assert calls == [
            ("historical", "ks1"),
            ("positional", "ks2", 2.5),
            ("varargs", "ks3", (3.5,)),
        ]

    def test_validation_response_does_not_defunct_transport(self):
        cases = (
            (
                InvalidRequestException(
                    code=0x2200, message="invalid", info=None),
                InvalidRequest),
            (
                UnauthorizedErrorMessage(
                    code=0x2100, message="unauthorized", info=None),
                Unauthorized),
        )
        for response, error_type in cases:
            with self.subTest(error_type=error_type):
                c = self.make_connection()
                success = ResultMessage(kind=RESULT_KIND_SET_KEYSPACE)
                responses = [response, success]

                def send_response(message, request_id, callback):
                    callback(responses.pop(0))

                c.send_msg = send_response

                with pytest.raises(error_type):
                    c.wait_for_response(Mock())

                assert not c.is_defunct
                assert not c.is_closed
                assert c.last_error is None
                assert c.wait_for_response(Mock()) is success

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

    def test_set_keyspace_async_pre_push_failure_restores_request_id(self):
        c = self.make_connection()
        original_request_ids = tuple(c.request_ids)
        request_id = original_request_ids[0]
        c._socket_writable = False

        with pytest.raises(ConnectionBusy):
            c.set_keyspace_async("ks", Mock())

        assert c._requests == {}
        assert tuple(c.request_ids).count(request_id) == 1
        assert set(c.request_ids) == set(original_request_ids)
        # The pool caller owns balancing the documented unconditional
        # increment even when dispatch raises synchronously.
        assert c.in_flight == 1

    def test_set_keyspace_async_ambiguous_send_failure_defuncts(self):
        c = self.make_connection()
        original_request_ids = tuple(c.request_ids)
        request_id = original_request_ids[0]
        queued_frames = []
        send_error = RuntimeError("wakeup failed after enqueue")

        def close():
            with c.lock:
                if c.is_closed:
                    return
                c.is_closed = True

        def fail_after_enqueue(frame):
            queued_frames.append(frame)
            raise send_error

        c.close = close
        c.push = fail_after_enqueue
        callback = Mock()

        with pytest.raises(RuntimeError, match="wakeup failed after enqueue"):
            c.set_keyspace_async("ks", callback)

        assert len(queued_frames) == 1
        assert c.is_defunct
        assert c.is_closed
        assert c.last_error is send_error
        assert c._requests == {}
        assert request_id not in c.request_ids
        assert set(c.request_ids) == set(original_request_ids[1:])
        callback.assert_called_once()

    def test_final_continuous_paging_release_notifies_owner_outside_lock(self):
        owner = Mock()
        c = self.make_connection()
        c._owning_pool = owner
        c.lock = Lock()
        c._continuous_paging_sessions = {
            301: Mock(),
            302: Mock(),
        }

        callback_had_lock = []

        def on_connection_released(connection):
            acquired = connection.lock.acquire(False)
            callback_had_lock.append(not acquired)
            if acquired:
                connection.lock.release()

        owner.on_connection_released.side_effect = on_connection_released

        c.remove_continuous_paging_session(301)
        owner.on_connection_released.assert_not_called()

        c.remove_continuous_paging_session(302)

        owner.on_connection_released.assert_called_once_with(c)
        assert callback_had_lock == [False]
        assert 301 in c.request_ids
        assert 302 in c.request_ids

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

    def test_factory_returns_maintenance_mode_startup_close(self):
        """
        Maintenance mode accepts regular CQL sockets and closes them during
        startup. The low-level factory keeps that close observable while
        still tracking pool-owned startup connections for shutdown cleanup.
        """

        class MaintenanceModeCqlServer(object):
            def __init__(self):
                self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self._sock.bind(('127.0.0.1', 0))
                self._sock.listen(1)
                self._sock.settimeout(2)
                self.port = self._sock.getsockname()[1]
                self.first_frame = b''
                self.ready = Event()
                self.received_frame = Event()
                self.error = None
                self.thread = Thread(target=self._run)
                self.thread.daemon = True
                self.thread.start()

            def _run(self):
                self.ready.set()
                try:
                    client, _ = self._sock.accept()
                    with client:
                        client.settimeout(2)
                        while len(self.first_frame) < 9:
                            chunk = client.recv(9 - len(self.first_frame))
                            if not chunk:
                                break
                            self.first_frame += chunk
                except Exception as exc:
                    self.error = exc
                finally:
                    self.received_frame.set()

            def close(self):
                self._sock.close()
                self.thread.join(2)

        class MaintenanceModeConnection(Connection):
            def __init__(self, *args, **kwargs):
                super(MaintenanceModeConnection, self).__init__(*args, **kwargs)
                self._reader = None
                self._connect_socket()
                self._send_options_message()
                self._reader = Thread(target=self._read_until_server_closes)
                self._reader.daemon = True
                self._reader.start()

            def push(self, data):
                self._socket.sendall(data)

            def close(self):
                with self.lock:
                    if self.is_closed:
                        return
                    self.is_closed = True

                if self._socket:
                    self._socket.close()

                if not self.is_defunct:
                    shutdown_error = ConnectionShutdown("Connection to %s was closed" % self.endpoint)
                    self.error_all_requests(shutdown_error)
                    if not self.connected_event.is_set():
                        self.last_error = shutdown_error
                    self.connected_event.set()

            def _read_until_server_closes(self):
                try:
                    while True:
                        data = self._socket.recv(self.in_buffer_size)
                        if not data:
                            self.close()
                            return
                        self._iobuf.write(data)
                        self.process_io_buffer()
                except socket.error as exc:
                    if not self.is_closed:
                        self.defunct(exc)

        class PendingConnections(list):
            def __init__(self):
                super(PendingConnections, self).__init__()
                self.appended = []

            def append(self, conn):
                self.appended.append(conn)
                super(PendingConnections, self).append(conn)

        server = MaintenanceModeCqlServer()
        try:
            assert server.ready.wait(2)
            conn = MaintenanceModeConnection.factory(
                DefaultEndPoint('127.0.0.1', server.port), timeout=2)

            assert conn.is_closed
            assert server.received_frame.wait(2)
            assert server.error is None
            assert len(server.first_frame) >= 5
            assert server.first_frame[4] == 0x05  # OPTIONS
        finally:
            server.close()

        server = MaintenanceModeCqlServer()
        try:
            assert server.ready.wait(2)
            host_conn = Mock()
            host_conn.is_shutdown = False
            host_conn._pending_connections = PendingConnections()

            conn = MaintenanceModeConnection.factory(
                DefaultEndPoint('127.0.0.1', server.port),
                timeout=2,
                host_conn=host_conn)

            assert conn.is_closed
            assert server.received_frame.wait(2)
            assert server.error is None
            assert len(server.first_frame) >= 5
            assert server.first_frame[4] == 0x05  # OPTIONS
            assert host_conn._pending_connections == []
            assert len(host_conn._pending_connections.appended) == 1
            assert host_conn._pending_connections.appended[0] is conn
        finally:
            server.close()

    def test_factory_raises_clean_close_after_ready(self):
        class ReadyThenCleanCloseConnection(Connection):
            def __init__(self, *args, **kwargs):
                super(ReadyThenCleanCloseConnection, self).__init__(
                    *args, **kwargs)
                self._compressor = None
                self._handle_startup_response(ReadyMessage())
                self.close()

            def close(self):
                with self.lock:
                    if self.is_closed:
                        return
                    self.is_closed = True
                self.connected_event.set()

        with pytest.raises(ConnectionShutdown) as exc_info:
            ReadyThenCleanCloseConnection.factory(
                DefaultEndPoint('127.0.0.1', 9042), timeout=2)

        assert "closed by server" in str(exc_info.value)

    def test_factory_marks_legacy_event_only_startup_complete(self):
        class LegacyReadyConnection(Connection):
            def __init__(self, *args, **kwargs):
                super(LegacyReadyConnection, self).__init__(*args, **kwargs)
                # Historical third-party reactors used only this event to
                # publish successful startup.
                self.connected_event.set()

            def close(self):
                with self.lock:
                    self.is_closed = True
                self.connected_event.set()

        connection = LegacyReadyConnection.factory(
            DefaultEndPoint('127.0.0.1', 9042), timeout=2)

        assert connection._startup_completed is True
        connection.close()
        assert isinstance(
            _startup_close_error(connection, connection.endpoint),
            ConnectionShutdown)
        assert not isinstance(
            _startup_close_error(connection, connection.endpoint),
            _ConnectionClosedDuringStartup)

    def test_factory_preserves_positional_constructor_arguments(self):
        expected_option = object()

        class PositionalOptionConnection(Connection):
            def __init__(self, endpoint, positional_option, *args, **kwargs):
                self.positional_option = positional_option
                super(PositionalOptionConnection, self).__init__(
                    endpoint, *args, **kwargs)
                self.connected_event.set()

            def close(self):
                with self.lock:
                    self.is_closed = True
                self.connected_event.set()

        connection = PositionalOptionConnection.factory(
            DefaultEndPoint('127.0.0.1', 9042),
            2,
            expected_option)

        assert connection.positional_option is expected_option
        connection.close()

    def test_factory_records_host_connection_as_owner(self):
        class ImmediateConnection(Connection):
            def __init__(self, *args, **kwargs):
                super(ImmediateConnection, self).__init__(*args, **kwargs)
                self.connected_event.set()

            def close(self):
                with self.lock:
                    self.is_closed = True
                self.connected_event.set()

        class Owner(object):
            is_shutdown = False

            def __init__(self):
                self._pending_connections = []

        owner = Owner()
        connection = ImmediateConnection.factory(
            DefaultEndPoint('127.0.0.1', 9042),
            timeout=2,
            host_conn=owner)

        assert connection._owning_pool is owner
        assert owner._pending_connections == [connection]
        owner._pending_connections.remove(connection)
        connection.close()

    def test_factory_preserves_legacy_ready_when_close_wins_snapshot(self):
        class LegacyReadyThenClosedConnection(Connection):
            instance = None

            def __init__(self, *args, **kwargs):
                super(LegacyReadyThenClosedConnection, self).__init__(
                    *args, **kwargs)
                LegacyReadyThenClosedConnection.instance = self
                # Historical reactors published READY only through this event.
                self.connected_event.set()
                # Deterministically place the close between READY publication
                # and the factory state snapshot.
                self.close()

            def close(self):
                with self.lock:
                    if self.is_closed:
                        return
                    self.is_closed = True
                    if not self._startup_completed:
                        self.last_error = ConnectionShutdown(
                            "legacy connection closed after READY",
                            self.endpoint)
                self.connected_event.set()

        with pytest.raises(ConnectionShutdown) as exc_info:
            LegacyReadyThenClosedConnection.factory(
                DefaultEndPoint('127.0.0.1', 9042), timeout=2)

        connection = LegacyReadyThenClosedConnection.instance
        assert connection._startup_completed is True
        assert exc_info.value is connection.last_error
        assert "closed after READY" in str(exc_info.value)
        assert not isinstance(
            _startup_close_error(connection, connection.endpoint),
            _ConnectionClosedDuringStartup)

    def test_legacy_startup_event_publication_is_atomic_with_close(self):
        c = self.make_connection()
        acquire_attempted = Event()

        class ObservedLock(object):
            def __init__(self):
                self.inner = RLock()

            def __enter__(self):
                acquire_attempted.set()
                self.inner.acquire()
                return self

            def __exit__(self, *args):
                self.inner.release()

        observed_lock = ObservedLock()
        c.lock = observed_lock
        observed_lock.inner.acquire()
        setter = Thread(target=c.connected_event.set)
        setter.start()
        try:
            assert acquire_attempted.wait(1)
            # Close wins while the event publisher is waiting for the same
            # state lock; it must prevent publication of a legacy READY.
            c.is_closed = True
        finally:
            observed_lock.inner.release()
        setter.join(1)

        assert not setter.is_alive()
        assert c.connected_event.is_set()
        assert not c._startup_event_set_while_open

    def test_factory_raises_clean_close_after_auth_success(self):
        class AuthSuccessThenCleanCloseConnection(Connection):
            def __init__(self, *args, **kwargs):
                super(AuthSuccessThenCleanCloseConnection, self).__init__(
                    *args, **kwargs)
                self.authenticator = Mock()
                self._compressor = None
                self._handle_auth_response(AuthSuccessMessage(b'token'))
                self.close()

            def close(self):
                with self.lock:
                    if self.is_closed:
                        return
                    self.is_closed = True
                self.connected_event.set()

        with pytest.raises(ConnectionShutdown) as exc_info:
            AuthSuccessThenCleanCloseConnection.factory(
                DefaultEndPoint('127.0.0.1', 9042), timeout=2)

        assert "closed by server" in str(exc_info.value)

    def test_factory_observes_defunct_error_atomically_after_ready(self):
        expected_error = RuntimeError("post-READY transport failure")
        setter_entered = Event()
        factory_waiting_for_lock = Event()
        release_setter = Event()
        factory_thread_id = get_ident()

        class ObservedRLock(object):
            def __init__(self):
                self._lock = RLock()

            def __enter__(self):
                if (get_ident() == factory_thread_id
                        and setter_entered.is_set()):
                    factory_waiting_for_lock.set()
                self._lock.acquire()
                return self

            def __exit__(self, *args):
                self._lock.release()

        class AtomicDefunctConnection(Connection):
            instance = None

            @property
            def last_error(self):
                return self._last_error

            @last_error.setter
            def last_error(self, exc):
                if self._block_error_publication:
                    setter_entered.set()
                    release_setter.wait(2)
                self._last_error = exc

            def __init__(self, *args, **kwargs):
                self._last_error = None
                self._block_error_publication = False
                super(AtomicDefunctConnection, self).__init__(*args, **kwargs)
                self.lock = ObservedRLock()
                self._compressor = None
                self._handle_startup_response(ReadyMessage())
                self._block_error_publication = True
                self.defunct_thread = Thread(
                    target=self.defunct, args=(expected_error,))
                self.defunct_thread.daemon = True
                self.defunct_thread.start()
                assert setter_entered.wait(2)

                def release_when_factory_takes_snapshot():
                    factory_waiting_for_lock.wait(2)
                    release_setter.set()

                self.release_thread = Thread(
                    target=release_when_factory_takes_snapshot)
                self.release_thread.daemon = True
                self.release_thread.start()
                AtomicDefunctConnection.instance = self

            def close(self):
                with self.lock:
                    if self.is_closed:
                        return
                    self.is_closed = True

        try:
            with pytest.raises(RuntimeError) as exc_info:
                AtomicDefunctConnection.factory(
                    DefaultEndPoint('127.0.0.1', 9042), timeout=2)
            assert exc_info.value is expected_error
            assert factory_waiting_for_lock.is_set()
        finally:
            release_setter.set()
            conn = AtomicDefunctConnection.instance
            if conn is not None:
                conn.defunct_thread.join(2)
                conn.release_thread.join(2)

    def test_factory_returns_reactor_clean_startup_close(self):
        class ReactorCleanClose(Exception):
            pass

        class ReactorCleanCloseConnection(Connection):
            def __init__(self, *args, **kwargs):
                super(ReactorCleanCloseConnection, self).__init__(*args, **kwargs)
                self.last_error = ReactorCleanClose("closed cleanly")
                self.is_closed = True
                self.is_defunct = True
                self.connected_event.set()

            def close(self):
                self.is_closed = True

            def _is_clean_close_error(self, exc):
                return isinstance(exc, ReactorCleanClose)

        conn = ReactorCleanCloseConnection.factory(
            DefaultEndPoint('127.0.0.1', 9042), timeout=2)

        assert conn.is_closed
        assert conn.is_defunct
        assert isinstance(conn.last_error, ReactorCleanClose)

    def test_factory_raises_reactor_unclean_startup_close(self):
        class ReactorUncleanClose(Exception):
            pass

        class ReactorUncleanCloseConnection(Connection):
            def __init__(self, *args, **kwargs):
                super(ReactorUncleanCloseConnection, self).__init__(*args, **kwargs)
                self.last_error = ReactorUncleanClose("closed uncleanly")
                self.is_closed = True
                self.is_defunct = True
                self.connected_event.set()

            def close(self):
                self.is_closed = True

        with pytest.raises(ReactorUncleanClose):
            ReactorUncleanCloseConnection.factory(
                DefaultEndPoint('127.0.0.1', 9042), timeout=2)

    def test_factory_raises_defunct_connection_shutdown_startup_error(self):
        class DefunctConnectionShutdownConnection(Connection):
            def __init__(self, *args, **kwargs):
                super(DefunctConnectionShutdownConnection, self).__init__(*args, **kwargs)
                self.last_error = ConnectionShutdown("defunct startup failure")
                self.is_closed = True
                self.is_defunct = True
                self.connected_event.set()

            def close(self):
                self.is_closed = True

        with pytest.raises(ConnectionShutdown):
            DefunctConnectionShutdownConnection.factory(
                DefaultEndPoint('127.0.0.1', 9042), timeout=2)

    def test_factory_closes_socket_when_wait_is_cancelled(self):
        class FactoryCancelled(BaseException):
            pass

        class InterruptingEvent(object):
            def wait(self, timeout):
                raise FactoryCancelled()

        class InterruptedConnection(Connection):
            instance = None

            def __init__(self, *args, **kwargs):
                super(InterruptedConnection, self).__init__(*args, **kwargs)
                self.connected_event = InterruptingEvent()
                self.close_count = 0
                InterruptedConnection.instance = self

            def close(self):
                self.close_count += 1
                self.is_closed = True

        with pytest.raises(FactoryCancelled):
            InterruptedConnection.factory(
                DefaultEndPoint('127.0.0.1', 9042), timeout=2)

        assert InterruptedConnection.instance.is_closed
        assert InterruptedConnection.instance.close_count == 1

    def test_owner_does_not_reclassify_post_startup_close(self):
        connection = self.make_connection()
        connection._startup_completed = True
        connection.is_closed = True

        error = _startup_close_error(connection)

        assert isinstance(error, ConnectionShutdown)
        assert not isinstance(error, _ConnectionClosedDuringStartup)
        assert "after startup" in str(error)


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
        callback_had_lock = []

        def on_connection_released(connection):
            acquired = connection.lock.acquire(False)
            callback_had_lock.append(not acquired)
            if acquired:
                connection.lock.release()
            raise RuntimeError("retirement bookkeeping failed")

        holder.on_connection_released.side_effect = on_connection_released

        with patch('cassandra.connection.log.exception') as log_exception:
            self.run_heartbeat(get_holders)

        holder.get_connections.assert_has_calls([call()] * get_holders.call_count)
        assert idle_connection.in_flight == 0
        assert non_idle_connection.in_flight == 0

        idle_connection.send_msg.assert_has_calls([call(ANY, request_id, ANY)] * get_holders.call_count)
        assert non_idle_connection.send_msg.call_count == 0
        holder.on_connection_released.assert_has_calls(
            [call(idle_connection)] * get_holders.call_count)
        assert callback_had_lock == [False] * get_holders.call_count
        idle_connection.defunct.assert_not_called()
        holder.return_connection.assert_not_called()
        assert log_exception.call_count == get_holders.call_count

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
            future.wait(timeout=0, original_timeout=0)

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
        assert exc.errors == 'Connection heartbeat timeout (total wait=0.05 seconds, this wait call=0.05 seconds)'
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
