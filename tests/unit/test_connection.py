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
import unittest
import uuid
from io import BytesIO
import time
from threading import Lock
from unittest.mock import Mock, ANY, call, patch

from cassandra import OperationTimedOut
from cassandra.application_info import ApplicationInfoBase
from cassandra.cluster import Cluster
from cassandra.connection import (Connection, HEADER_DIRECTION_TO_CLIENT, ProtocolError,
                                  locally_supported_compressions, ConnectionHeartbeat, HeartbeatFuture, _Frame, Timer, TimerManager,
                                  ConnectionException, ConnectionShutdown, DefaultEndPoint, ShardAwarePortGenerator,
                                  DRIVER_NAME, DRIVER_VERSION)
from cassandra.driver_config import DRIVER_CONFIG_OPTION, SESSION_ID_OPTION
from cassandra.marshal import uint8_pack, uint32_pack, int32_pack
from cassandra.protocol import (write_stringmultimap, write_int, write_string,
                                read_stringmap, SupportedMessage, ProtocolHandler,
                                ResultMessage, RESULT_KIND_SET_KEYSPACE)

from tests.unit.utils import StubReporter, ThrowingReporter
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

    def test_startup_message_can_be_sent_without_extra_options(self):
        """
        _send_startup_message defaults extra_options to None but splats it, so
        omitting it raised a TypeError that defunct_on_error turned into a silent
        defunct: no STARTUP frame was ever sent.

        The only caller inside the driver always passes a dict, so the default
        went unexercised; the caller that does omit it is the mock the simulacron
        heartbeat test installs over the options exchange, which has therefore
        been defuncting every connection it touched.
        """
        c = self.make_connection()
        c.send_msg = Mock()
        c.defunct = Mock()
        c.cql_version = '3.0.3'

        c._send_startup_message(no_compact=True)

        c.defunct.assert_not_called()
        c.send_msg.assert_called_once()
        options = c.send_msg.call_args[0][0].options
        assert options['DRIVER_NAME'] == DRIVER_NAME
        assert options['NO_COMPACT'] == 'true'

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

    def test_continuous_paging_cancel_releases_request_id_when_send_fails(self):
        c = self.make_connection()
        c.push = Mock(side_effect=ConnectionException("write failed"))
        state = Mock(max_queue_size=100, num_pages_requested=0, num_pages_received=0)
        session = c.new_continuous_paging_session(1, Mock(), Mock(), state)
        initial_request_ids = len(c.request_ids)

        with pytest.raises(ConnectionException):
            session.cancel()

        assert len(c.request_ids) == initial_request_ids
        assert not c._requests

class DerivedConnectionLimitsTest(unittest.TestCase):
    """
    max_request_id and orphaned_threshold are derived from max_in_flight, which
    is tuned at runtime -- assigned on the class, or patched in a test -- so a
    value derived once would not follow it.
    """

    def test_the_derivations(self):
        assert Connection.max_request_id_for(32768) == 32767
        assert Connection.max_request_id_for(256) == 255
        # Capped at the stream id range the protocol can address.
        assert Connection.max_request_id_for(2 ** 20) == (2 ** 15) - 1
        assert Connection.orphaned_threshold_for(32768) == 24576
        assert Connection.orphaned_threshold_for(256) == 192
        # Capped the same way, and for the same reason.
        assert Connection.orphaned_threshold_for(2 ** 20) == 24576

    def test_the_threshold_stays_within_the_ids_a_connection_holds(self):
        """
        A connection hands out request ids zero through max_request_id, so it
        can hold no more orphans than one more than that. A threshold above the
        pool is one `len(orphaned_request_ids) >= orphaned_threshold` never
        reaches, which leaves orphan-based connection replacement dead -- what a
        max_in_flight raised past the stream id range used to do.
        """
        for max_in_flight in (2, 256, 32768, 2 ** 16, 2 ** 20):
            pool = Connection.max_request_id_for(max_in_flight) + 1

            assert Connection.orphaned_threshold_for(max_in_flight) <= pool, max_in_flight

    def test_a_connection_derives_both_from_the_limit_in_force(self):
        """
        The regression this guards: __init__ derives them, so a limit tuned
        before the connection is built is the one it carries. Deriving them once
        on the class instead left every connection at the original bound.
        """
        with patch('cassandra.connection.Connection.max_in_flight', 50):
            connection = Connection(DefaultEndPoint('1.2.3.4'))

        assert connection.max_request_id == 49
        assert connection.orphaned_threshold == 37

    def test_the_limits_follow_a_runtime_assignment(self):
        """
        Connection.max_in_flight = N on the class, which the integration tests
        covering the in-flight bound do. A limit derived once would leave the
        pool's `in_flight < max_request_id` gate at the old value and never
        trip.
        """
        original = Connection.max_in_flight
        try:
            Connection.max_in_flight = 50

            assert Connection.max_request_id_for(Connection.max_in_flight) == 49
            assert Connection.orphaned_threshold_for(Connection.max_in_flight) == 37
        finally:
            Connection.max_in_flight = original

    def test_the_limits_follow_a_patched_limit(self):
        """
        The other shape the integration tests use.
        """
        with patch('cassandra.connection.Connection.max_in_flight', 2):
            assert Connection.max_request_id_for(Connection.max_in_flight) == 1
            assert Connection.orphaned_threshold_for(Connection.max_in_flight) == 1

    def test_a_subclass_that_lowers_the_limit_derives_its_own(self):
        class Small(Connection):
            max_in_flight = 256

        assert Small.max_request_id_for(Small.max_in_flight) == 255
        assert Small.orphaned_threshold_for(Small.max_in_flight) == 192
        # The point of deriving: a connection can hold no more orphans than it
        # has request ids, so the base class's 24576 could never be reached.
        assert Small.orphaned_threshold_for(Small.max_in_flight) < Small.max_in_flight


class StartupOptionsTest(unittest.TestCase):
    """
    Covers the options the driver puts in the STARTUP frame, by driving a
    connection through the SUPPORTED response that triggers it and reading the
    frame it hands to send_msg.
    """

    SESSION_ID = uuid.UUID('91b0b1a2-0000-4000-8000-000000000001')

    def startup_options(self, **kwargs):
        c = Connection(DefaultEndPoint('1.2.3.4'), **kwargs)
        c._socket = Mock()
        c.send_msg = Mock()
        c.defunct = Mock()

        c._handle_options_response(
            SupportedMessage(cql_versions=['3.0.3'], options={'COMPRESSION': []}))

        c.defunct.assert_not_called()
        c.send_msg.assert_called_once()
        return c.send_msg.call_args[0][0].options

    def test_session_id_is_reported(self):
        options = self.startup_options(session_id=self.SESSION_ID)

        assert options[SESSION_ID_OPTION] == str(self.SESSION_ID)

    def test_session_id_is_absent_when_not_configured(self):
        """
        Connections built outside of a Cluster (lower-level integrations, tests)
        have no cluster to be correlated with, so they report no SESSION_ID
        rather than an empty one.
        """
        options = self.startup_options()

        assert SESSION_ID_OPTION not in options

    def test_application_info_cannot_override_the_session_id(self):
        """
        SESSION_ID is driver-owned: it is what correlates a cluster's connections
        in the clients table, so an application-supplied value must not win.
        """
        class SpoofingApplicationInfo(ApplicationInfoBase):
            def add_startup_options(self, options):
                options[SESSION_ID_OPTION] = 'not-the-session-id'
                options['APPLICATION_NAME'] = 'app'

        options = self.startup_options(session_id=self.SESSION_ID,
                                       application_info=SpoofingApplicationInfo())

        assert options[SESSION_ID_OPTION] == str(self.SESSION_ID)
        # Keys the driver does not own must still come through.
        assert options['APPLICATION_NAME'] == 'app'

    def test_application_info_cannot_override_the_driver_config(self):
        """
        DRIVER_CONFIG is driver-owned too: an operator reading it out of the
        clients table must be reading the driver's description of itself, not an
        application's. That has to hold on the connections and in the
        configurations that report none of their own, which is where merely
        writing it last would leave the application's value standing.
        """
        class SpoofingApplicationInfo(ApplicationInfoBase):
            def add_startup_options(self, options):
                options[DRIVER_CONFIG_OPTION] = '{"version":999,"spoofed":true}'
                options['APPLICATION_NAME'] = 'app'

        # Four independent guarantees, each in its own subtest: run sequentially
        # the first regression would hide the other three, and which of them
        # break is what says where the hole is.
        ABSENT = object()
        cases = [
            ("a pool connection reports no configuration at all",
             {'driver_config_reporter': StubReporter()},
             ABSENT),
            ("nor does a control connection with reporting disabled",
             {'is_control_connection': True},
             ABSENT),
            ("a dropped report is not a hole for one either",
             {'is_control_connection': True, 'driver_config_reporter': ThrowingReporter()},
             ABSENT),
            ("the driver's own report wins where there is one",
             {'is_control_connection': True, 'driver_config_reporter': StubReporter()},
             StubReporter.REPORT),
        ]

        for description, kwargs, expected in cases:
            with self.subTest(description):
                options = self.startup_options(session_id=self.SESSION_ID,
                                               application_info=SpoofingApplicationInfo(),
                                               **kwargs)
                if expected is ABSENT:
                    assert DRIVER_CONFIG_OPTION not in options
                else:
                    assert options[DRIVER_CONFIG_OPTION] == expected
                # Keys the driver does not own must still come through.
                assert options['APPLICATION_NAME'] == 'app'

    def test_application_info_cannot_override_the_driver_identity(self):
        """
        DRIVER_NAME and DRIVER_VERSION are driver-owned for the same reason as
        the two options above: they land in the same clients-table row, read by
        the same operator, and a spoofed value would misreport the driver for the
        life of the connection.

        They are the pair that made the ordering matter: _send_startup_message
        merges the application's options *after* its own literals, so before they
        were cleared here an application-supplied value won.
        """
        class SpoofingApplicationInfo(ApplicationInfoBase):
            def add_startup_options(self, options):
                options['DRIVER_NAME'] = 'not-the-driver'
                options['DRIVER_VERSION'] = '0.0.0'
                options['APPLICATION_NAME'] = 'app'

        options = self.startup_options(application_info=SpoofingApplicationInfo())

        assert options['DRIVER_NAME'] == DRIVER_NAME
        assert options['DRIVER_VERSION'] == DRIVER_VERSION
        # Keys the driver does not own must still come through.
        assert options['APPLICATION_NAME'] == 'app'

    def test_ignored_option_is_warned_about_once_per_cluster(self):
        """
        The offending key is on every connection of the cluster, since they share
        one application info object, so warning on each would mean
        hosts x shards + 1 lines per connect() and as many again on every pool
        replacement. The control connection, established once and before the
        pools, carries the warning; the rest stay at debug.
        """
        class SpoofingApplicationInfo(ApplicationInfoBase):
            def add_startup_options(self, options):
                options['DRIVER_NAME'] = 'not-the-driver'

        with self.assertLogs('cassandra.connection', level='DEBUG') as captured:
            self.startup_options(is_control_connection=True,
                                 application_info=SpoofingApplicationInfo())
        assert [r.getMessage() for r in captured.records if r.levelname == 'WARNING'] == [
            "Ignoring the application-supplied DRIVER_NAME startup option on 1.2.3.4:9042: "
            "the option is reserved for the driver"]

        with self.assertLogs('cassandra.connection', level='DEBUG') as captured:
            self.startup_options(application_info=SpoofingApplicationInfo())
        assert [r.levelname for r in captured.records if 'DRIVER_NAME' in r.getMessage()] == ['DEBUG']

    def test_application_info_cannot_override_the_cql_version(self):
        """
        CQL_VERSION is driver-owned as well, but needs no clearing: StartupMessage
        writes it after the options map. Pinned here so that the reason the key is
        absent from the cleared set stays true.
        """
        class SpoofingApplicationInfo(ApplicationInfoBase):
            def add_startup_options(self, options):
                options['CQL_VERSION'] = '9.9.9'

        c = Connection(DefaultEndPoint('1.2.3.4'),
                       application_info=SpoofingApplicationInfo())
        c._socket = Mock()
        c.send_msg = Mock()
        c.defunct = Mock()
        c._handle_options_response(
            SupportedMessage(cql_versions=['3.0.3'], options={'COMPRESSION': []}))
        c.defunct.assert_not_called()

        buf = BytesIO()
        c.send_msg.call_args[0][0].send_body(buf, c.protocol_version)
        buf.seek(0)
        assert read_stringmap(buf)['CQL_VERSION'] == '3.0.3'

    def test_driver_config_is_reported_on_the_control_connection(self):
        options = self.startup_options(is_control_connection=True,
                                       driver_config_reporter=StubReporter())

        assert options[DRIVER_CONFIG_OPTION] == StubReporter.REPORT

    def test_driver_config_is_not_reported_on_a_regular_connection(self):
        """
        The configuration is the same for every connection of a cluster, so only
        the control connection reports it; the pool connections still report the
        session id that ties them to it.
        """
        options = self.startup_options(session_id=self.SESSION_ID,
                                       driver_config_reporter=StubReporter())

        assert SESSION_ID_OPTION in options
        assert DRIVER_CONFIG_OPTION not in options

    def test_driver_config_is_not_reported_without_a_reporter(self):
        """
        A reporter left as None is how a Cluster with
        driver_config_reporting_enabled=False reaches its connections. The
        session id is documented not to be affected by that setting.
        """
        options = self.startup_options(is_control_connection=True,
                                       session_id=self.SESSION_ID)

        assert options[SESSION_ID_OPTION] == str(self.SESSION_ID)
        assert DRIVER_CONFIG_OPTION not in options

    def test_a_failing_reporter_does_not_break_the_handshake(self):
        """
        Reporting is a diagnostic aid: a reporter that cannot produce a report
        must leave the STARTUP frame otherwise intact rather than fail the
        connection.
        """
        options = self.startup_options(is_control_connection=True,
                                       session_id=self.SESSION_ID,
                                       driver_config_reporter=ThrowingReporter())

        assert DRIVER_CONFIG_OPTION not in options
        assert options[SESSION_ID_OPTION] == str(self.SESSION_ID)


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
