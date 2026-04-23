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

import logging
import socket
from concurrent.futures import Future
from threading import RLock
from unittest.mock import patch, Mock
import uuid

from cassandra import ConsistencyLevel, DriverException, Timeout, Unavailable, RequestExecutionException, ReadTimeout, WriteTimeout, CoordinationFailure, ReadFailure, WriteFailure, FunctionFailure, AlreadyExists,\
    InvalidRequest, Unauthorized, AuthenticationFailed, OperationTimedOut, UnsupportedOperation, RequestValidationException, ConfigurationException, ProtocolVersion
from cassandra.cluster import _Scheduler, Session, Cluster, default_lbp_factory, \
    ExecutionProfile, _ConfigMode, EXEC_PROFILE_DEFAULT, _ControlHostSession, ResponseFuture, NoHostAvailable
from cassandra.connection import DefaultEndPoint, EndPoint
from cassandra.pool import Host, HostConnection
from cassandra.policies import HostDistance, RetryPolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy, SimpleConvictionPolicy, WhiteListRoundRobinPolicy
from cassandra.protocol import QueryMessage
from cassandra.query import SimpleStatement, named_tuple_factory, tuple_factory
from tests.unit.utils import mock_session_pools
from tests import connection_class
import pytest


log = logging.getLogger(__name__)


class _HostAwareProxyEndPoint(EndPoint):
    def __init__(self, address, affinity_key, port=9042):
        self._address = address
        self._affinity_key = affinity_key
        self._port = port

    @property
    def address(self):
        return self._address

    @property
    def port(self):
        return self._port

    def resolve(self):
        return self._address, self._port

    def __eq__(self, other):
        return isinstance(other, _HostAwareProxyEndPoint) and \
            self.address == other.address and self.port == other.port and \
            self._affinity_key == other._affinity_key

    def __hash__(self):
        return hash((self.address, self.port, self._affinity_key))

    def __lt__(self, other):
        if not isinstance(other, _HostAwareProxyEndPoint):
            return NotImplemented
        return (self.address, self.port, str(self._affinity_key)) < \
            (other.address, other.port, str(other._affinity_key))


class ExceptionTypeTest(unittest.TestCase):

    def test_exception_types(self):
        """
        PYTHON-443
        Sanity check to ensure we don't unintentionally change class hierarchy of exception types
        """
        assert issubclass(Unavailable, DriverException)
        assert issubclass(Unavailable, RequestExecutionException)

        assert issubclass(ReadTimeout, DriverException)
        assert issubclass(ReadTimeout, RequestExecutionException)
        assert issubclass(ReadTimeout, Timeout)

        assert issubclass(WriteTimeout, DriverException)
        assert issubclass(WriteTimeout, RequestExecutionException)
        assert issubclass(WriteTimeout, Timeout)

        assert issubclass(CoordinationFailure, DriverException)
        assert issubclass(CoordinationFailure, RequestExecutionException)

        assert issubclass(ReadFailure, DriverException)
        assert issubclass(ReadFailure, RequestExecutionException)
        assert issubclass(ReadFailure, CoordinationFailure)

        assert issubclass(WriteFailure, DriverException)
        assert issubclass(WriteFailure, RequestExecutionException)
        assert issubclass(WriteFailure, CoordinationFailure)

        assert issubclass(FunctionFailure, DriverException)
        assert issubclass(FunctionFailure, RequestExecutionException)

        assert issubclass(RequestValidationException, DriverException)

        assert issubclass(ConfigurationException, DriverException)
        assert issubclass(ConfigurationException, RequestValidationException)

        assert issubclass(AlreadyExists, DriverException)
        assert issubclass(AlreadyExists, RequestValidationException)
        assert issubclass(AlreadyExists, ConfigurationException)

        assert issubclass(InvalidRequest, DriverException)
        assert issubclass(InvalidRequest, RequestValidationException)

        assert issubclass(Unauthorized, DriverException)
        assert issubclass(Unauthorized, RequestValidationException)

        assert issubclass(AuthenticationFailed, DriverException)

        assert issubclass(OperationTimedOut, DriverException)

        assert issubclass(UnsupportedOperation, DriverException)


class OperationTimedOutTest(unittest.TestCase):

    def test_message_without_timeout(self):
        """Default message format when no timeout info is provided."""
        exc = OperationTimedOut(errors={'host1': 'some error'}, last_host='host1')
        msg = str(exc)
        assert "errors={'host1': 'some error'}" in msg
        assert "last_host=host1" in msg
        assert "timeout=" not in msg
        assert "in_flight=" not in msg

    def test_message_with_timeout_and_in_flight(self):
        """Message includes timeout and in_flight when both are provided."""
        exc = OperationTimedOut(errors={'host1': 'err'}, last_host='host1',
                                timeout=10.0, in_flight=42)
        msg = str(exc)
        assert "(timeout=10.0s, in_flight=42)" in msg

    def test_message_with_timeout_no_in_flight(self):
        """Message includes timeout but not in_flight when only timeout is set."""
        exc = OperationTimedOut(timeout=5.0)
        msg = str(exc)
        assert "(timeout=5.0s)" in msg
        assert "in_flight=" not in msg

    def test_message_no_args(self):
        """No-argument form should not crash and should have clean message."""
        exc = OperationTimedOut()
        msg = str(exc)
        assert "errors=None, last_host=None" in msg
        assert "timeout=" not in msg

    def test_attributes_accessible(self):
        """New and existing attributes should be readable."""
        exc = OperationTimedOut(errors={'h': 'e'}, last_host='h',
                                timeout=10.0, in_flight=42)
        assert exc.errors == {'h': 'e'}
        assert exc.last_host == 'h'
        assert exc.timeout == 10.0
        assert exc.in_flight == 42

    def test_attributes_default_none(self):
        """New attributes should default to None when not provided."""
        exc = OperationTimedOut()
        assert exc.timeout is None
        assert exc.in_flight is None
        assert exc.errors is None
        assert exc.last_host is None

    def test_backward_compat_positional(self):
        """Existing two-positional-arg form should still work."""
        exc = OperationTimedOut({'h': 'err'}, 'host1')
        assert exc.errors == {'h': 'err'}
        assert exc.last_host == 'host1'
        assert exc.timeout is None
        assert exc.in_flight is None


class ClusterTest(unittest.TestCase):

    def test_tuple_for_contact_points(self):
        cluster = Cluster(contact_points=[('localhost', 9045), ('127.0.0.2', 9046), '127.0.0.3'], port=9999)
        # Refactored for clarity
        addr_info = socket.getaddrinfo("localhost", 80)
        sockaddr_tuples = [info[4] for info in addr_info]  # info[4] is sockaddr
        localhost_addr = set([sockaddr[0] for sockaddr in sockaddr_tuples])
        for cp in cluster.endpoints_resolved:
            if cp.address in localhost_addr:
                assert cp.port == 9045
            elif cp.address == '127.0.0.2':
                assert cp.port == 9046
            else:
                assert cp.address == '127.0.0.3'
                assert cp.port == 9999

    def test_invalid_contact_point_types(self):
        with pytest.raises(ValueError):
            Cluster(contact_points=[None], protocol_version=4, connect_timeout=1)
        with pytest.raises(TypeError):
            Cluster(contact_points="not a sequence", protocol_version=4, connect_timeout=1)

    def test_port_str(self):
        """Check port passed as string is converted and checked properly"""
        cluster = Cluster(contact_points=['127.0.0.1'], port='1111')
        for cp in cluster.endpoints_resolved:
            if cp.address in ('::1', '127.0.0.1'):
                assert cp.port == 1111

        with pytest.raises(ValueError):
            cluster = Cluster(contact_points=['127.0.0.1'], port='string')


    def test_port_range(self):
        for invalid_port in [0, 65536, -1]:
            with pytest.raises(ValueError):
                cluster = Cluster(contact_points=['127.0.0.1'], port=invalid_port)

    def test_compression_autodisabled_without_libraries(self):
        with patch.dict('cassandra.cluster.locally_supported_compressions', {}, clear=True):
            with patch('cassandra.cluster.log') as patched_logger:
                cluster = Cluster(compression=True)

        patched_logger.error.assert_called_once()
        assert cluster.compression is False

    def test_compression_validates_requested_algorithm(self):
        with patch.dict('cassandra.cluster.locally_supported_compressions', {}, clear=True):
            with pytest.raises(ValueError):
                Cluster(compression='lz4')

        with patch.dict('cassandra.cluster.locally_supported_compressions', {'lz4': ('c', 'd')}, clear=True):
            with patch('cassandra.cluster.log') as patched_logger:
                cluster = Cluster(compression='lz4')

        patched_logger.error.assert_not_called()
        assert cluster.compression == 'lz4'

    def test_compression_type_validation(self):
        with pytest.raises(TypeError):
            Cluster(compression=123)

    def test_connection_factory_passes_compression_kwarg(self):
        endpoint = Mock(address='127.0.0.1')
        scenarios = [
            ({}, True, False),
            ({'snappy': ('c', 'd')}, True, True),
            ({'lz4': ('c', 'd')}, 'lz4', 'lz4'),
            ({'lz4': ('c', 'd'), 'snappy': ('c', 'd')}, False, False),
            ({'lz4': ('c', 'd'), 'snappy': ('c', 'd')}, None, False),
        ]

        for supported, configured, expected in scenarios:
            with patch.dict('cassandra.cluster.locally_supported_compressions', supported, clear=True):
                with patch.object(Cluster.connection_class, 'factory', autospec=True, return_value='connection') as factory:
                    cluster = Cluster(compression=configured)
                    conn = cluster.connection_factory(endpoint)

                assert conn == 'connection'
                assert factory.call_count == 1
                assert factory.call_args.kwargs['compression'] == expected
                assert cluster.compression == expected

    def test_get_control_connection_host_falls_back_to_host_id(self):
        cluster = Cluster(contact_points=['127.0.0.1'])
        host = Host(DefaultEndPoint('192.168.1.10'), SimpleConvictionPolicy, host_id=uuid.uuid4())

        metadata = Mock()
        metadata.get_host.return_value = None
        metadata.get_host_by_host_id.return_value = host
        cluster.metadata = metadata

        connection = Mock(endpoint=DefaultEndPoint('127.254.254.101', 9042))
        cluster.control_connection = Mock(_connection=connection, _current_host_id=host.host_id)

        assert cluster.get_control_connection_host() is host
        metadata.get_host.assert_called_once_with(connection.endpoint)
        metadata.get_host_by_host_id.assert_called_once_with(host.host_id)

    def test_is_shard_aware_ignores_non_shard_aware_pools(self):
        cluster = Cluster(contact_points=['127.0.0.1'])

        shard_pool = Mock()
        shard_pool.host = Mock(
            endpoint=DefaultEndPoint("127.0.0.1"),
            sharding_info=Mock(shards_count=8))
        shard_pool._connections = {0: Mock(), 1: Mock()}

        control_pool = Mock()
        control_pool.host = Mock(
            endpoint=DefaultEndPoint("127.254.254.101"),
            sharding_info=None)
        control_pool._connections = {0: Mock()}

        cluster.get_all_pools = Mock(return_value=[control_pool, shard_pool])

        assert cluster.is_shard_aware() is True

    def test_shard_aware_stats_ignores_non_shard_aware_pools(self):
        cluster = Cluster(contact_points=['127.0.0.1'])

        shard_pool = Mock()
        shard_pool.host = Mock(
            endpoint=DefaultEndPoint("127.0.0.1"),
            sharding_info=Mock(shards_count=8))
        shard_pool._connections = {0: Mock(), 1: Mock()}

        control_pool = Mock()
        control_pool.host = Mock(
            endpoint=DefaultEndPoint("127.254.254.101"),
            sharding_info=None)
        control_pool._connections = {0: Mock()}

        cluster.get_all_pools = Mock(return_value=[shard_pool, control_pool])

        assert cluster.shard_aware_stats() == {
            "127.0.0.1:9042": {"shards_count": 8, "connected": 2}
        }


class SchedulerTest(unittest.TestCase):
    # TODO: this suite could be expanded; for now just adding a test covering a ticket

    @patch('time.time', return_value=3)  # always queue at same time
    @patch('cassandra.cluster._Scheduler.run')  # don't actually run the thread
    def test_event_delay_timing(self, *_):
        """
        Schedule something with a time collision to make sure the heap comparison works

        PYTHON-473
        """
        sched = _Scheduler(None)
        sched.schedule(0, lambda: None)
        sched.schedule(0, lambda: None)  # pre-473: "TypeError: unorderable types: function() < function()"


class SessionTest(unittest.TestCase):
    def setUp(self):
        if connection_class is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        connection_class.initialize_reactor()

    @staticmethod
    def _completed_future(result):
        future = Future()
        future.set_result(result)
        return future

    @staticmethod
    def _proxy_endpoint(address, affinity_key, port=9042):
        return _HostAwareProxyEndPoint(address, affinity_key, port)

    def _make_control_host_session(self, cluster, source_host, endpoint="127.254.254.101"):
        endpoint = endpoint if isinstance(endpoint, EndPoint) else self._proxy_endpoint(endpoint, source_host.host_id)
        cluster.control_connection = Mock(
            _connection=Mock(endpoint=endpoint))
        cluster.get_control_connection_host = Mock(return_value=source_host)
        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            return cluster._new_control_host_session(None)

    # TODO: this suite could be expanded; for now just adding a test covering a PR
    @mock_session_pools
    def test_default_serial_consistency_level_ep(self, *_):
        """
        Make sure default_serial_consistency_level passes through to a query message using execution profiles.
        Also make sure Statement.serial_consistency_level overrides the default.

        PR #510
        """
        c = Cluster(protocol_version=4)
        s = Session(c, [Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])
        c.connection_class.initialize_reactor()

        # default is None
        default_profile = c.profile_manager.default
        assert default_profile.serial_consistency_level is None

        for cl in (None, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
            s.get_execution_profile(EXEC_PROFILE_DEFAULT).serial_consistency_level = cl

            # default is passed through
            f = s.execute_async(query='')
            assert f.message.serial_consistency_level == cl

            # any non-None statement setting takes precedence
            for cl_override in (ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
                f = s.execute_async(SimpleStatement(query_string='', serial_consistency_level=cl_override))
                assert default_profile.serial_consistency_level == cl
                assert f.message.serial_consistency_level == cl_override

    @mock_session_pools
    def test_default_serial_consistency_level_legacy(self, *_):
        """
        Make sure default_serial_consistency_level passes through to a query message using legacy settings.
        Also make sure Statement.serial_consistency_level overrides the default.

        PR #510
        """
        c = Cluster(protocol_version=4)
        s = Session(c, [Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])
        c.connection_class.initialize_reactor()
        # default is None
        assert s.default_serial_consistency_level is None

        # Should fail
        with pytest.raises(ValueError):
            s.default_serial_consistency_level = ConsistencyLevel.ANY
        with pytest.raises(ValueError):
            s.default_serial_consistency_level = 1001

        for cl in (None, ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
            s.default_serial_consistency_level = cl

            # any non-None statement setting takes precedence
            for cl_override in (ConsistencyLevel.LOCAL_SERIAL, ConsistencyLevel.SERIAL):
                f = s.execute_async(SimpleStatement(query_string='', serial_consistency_level=cl_override))
                assert s.default_serial_consistency_level == cl
                assert f.message.serial_consistency_level == cl_override



    @mock_session_pools
    def test_set_keyspace_escapes_quotes(self, *_):
        """
        Test that Session.set_keyspace properly escapes double quotes in
        keyspace names to prevent CQL injection.
        Requested in review of PR #758.
        """
        c = Cluster(protocol_version=4)
        s = Session(c, [Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])
        c.connection_class.initialize_reactor()

        s.execute = Mock()

        s.set_keyspace('my"ks')
        query = s.execute.call_args[0][0]
        assert query == 'USE "my""ks"', (
            "Double quotes in keyspace name must be escaped as double-double quotes, "
            "got: %r" % query)

        # Also verify a simple keyspace name doesn't get unnecessarily quoted
        s.execute.reset_mock()
        s.set_keyspace('simple_ks')
        query = s.execute.call_args[0][0]
        assert query == 'USE simple_ks', (
            "Simple keyspace names should not be quoted, got: %r" % query)

    def test_control_host_session_prepare_targets_pinned_host(self):
        cluster = Cluster(
            load_balancing_policy=RoundRobinPolicy(),
            protocol_version=4,
            prepare_on_all_hosts=False)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()
        pinned_host = Host(DefaultEndPoint("127.254.254.101"), SimpleConvictionPolicy, host_id=host_id)
        pinned_host.set_up()

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            session = _ControlHostSession(cluster, source_host, pinned_host)

        prepare_response = Mock(
            query_id=b"prepared-id",
            bind_metadata=[],
            pk_indexes=[],
            column_metadata=[],
            result_metadata_id=None,
            is_lwt=False)
        response_rows = Mock()
        response_rows.one.return_value = prepare_response

        response_future = Mock()
        response_future.result.return_value = response_rows
        response_future.custom_payload = {"prepared-by": b"control-host"}

        prepared_statement = Mock()
        with patch.object(cluster, "add_prepared") as add_prepared, \
                patch("cassandra.cluster.ResponseFuture", return_value=response_future) as response_future_cls, \
                patch("cassandra.cluster.PreparedStatement.from_message",
                      return_value=prepared_statement):
            prepared = session.prepare("SELECT release_version FROM system.local")

        assert prepared is prepared_statement
        response_future_cls.assert_called_once()
        assert response_future_cls.call_args.kwargs["host"] is pinned_host
        add_prepared.assert_called_once_with(prepare_response.query_id, prepared_statement)
        assert prepared_statement.custom_payload == response_future.custom_payload

    def test_control_host_session_prepare_skips_raw_host_fanout_on_all_hosts(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(
            DefaultEndPoint("127.0.0.1"),
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        source_host.set_up()
        pinned_host = Host(
            DefaultEndPoint("127.254.254.101"),
            SimpleConvictionPolicy,
            host_id=source_host.host_id)
        pinned_host.set_up()

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            session = _ControlHostSession(cluster, source_host, pinned_host)

        prepare_response = Mock(
            query_id=b"prepared-id",
            bind_metadata=[],
            pk_indexes=[],
            column_metadata=[],
            result_metadata_id=None,
            is_lwt=False)
        response_rows = Mock()
        response_rows.one.return_value = prepare_response

        response_future = Mock()
        response_future.result.return_value = response_rows
        response_future.custom_payload = {"prepared-by": b"control-host"}
        response_future._current_host = pinned_host

        prepared_statement = Mock(
            query_string="SELECT release_version FROM system.local")

        with patch.object(cluster, "add_prepared") as add_prepared, \
                patch.object(cluster, "_prepare_query_on_all_hosts") as prepare_all, \
                patch("cassandra.cluster.ResponseFuture",
                      return_value=response_future) as response_future_cls, \
                patch("cassandra.cluster.PreparedStatement.from_message",
                      return_value=prepared_statement):
            prepared = session.prepare("SELECT release_version FROM system.local")

        assert prepared is prepared_statement
        response_future_cls.assert_called_once()
        assert response_future_cls.call_args.kwargs["host"] is pinned_host
        add_prepared.assert_called_once_with(prepare_response.query_id, prepared_statement)
        prepare_all.assert_not_called()

    def test_connect_to_control_host_rejects_shared_default_endpoint(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        source_host.set_up()

        cluster.control_connection = Mock(
            _connection=Mock(endpoint=DefaultEndPoint("127.254.254.101")))
        cluster.get_control_connection_host = Mock(return_value=source_host)

        verification_connection = Mock()
        verification_connection.wait_for_response.return_value = Mock(
            column_names=["host_id"],
            parsed_rows=[(uuid.uuid4(),)])

        with patch.object(cluster, "connection_factory",
                          return_value=verification_connection), \
                pytest.raises(DriverException, match="host-specific control endpoint"):
            cluster._new_control_host_session(None)

        assert verification_connection.close.call_count == 1

    def test_connect_to_control_host_accepts_host_specific_default_endpoint(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(
            DefaultEndPoint("127.0.0.1"),
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        source_host.set_up()

        connection_endpoint = DefaultEndPoint("proxy.control.example")
        cluster.control_connection = Mock(
            _connection=Mock(endpoint=connection_endpoint))
        cluster.get_control_connection_host = Mock(return_value=source_host)

        verification_connection = Mock()
        verification_connection.wait_for_response.return_value = Mock(
            column_names=["host_id"],
            parsed_rows=[(source_host.host_id,)])

        with patch.object(cluster, "connection_factory",
                          return_value=verification_connection) as connection_factory, \
                patch.object(Session, "_add_or_renew_pool_for_distance",
                             return_value=self._completed_future(True)):
            session = cluster._new_control_host_session(None)

        assert session._pinned_host.endpoint == connection_endpoint
        assert connection_factory.call_count == 3
        assert verification_connection.close.call_count == 3

    def test_control_host_session_uses_host_aware_metadata_endpoint_when_control_connection_is_shared(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_endpoint = self._proxy_endpoint("proxy.control.example", host_id)
        source_host = Host(source_endpoint, SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()

        cluster.control_connection = Mock(
            _connection=Mock(endpoint=DefaultEndPoint("127.254.254.101")))
        cluster.get_control_connection_host = Mock(return_value=source_host)

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            session = cluster._new_control_host_session(None)

        assert session._pinned_host.endpoint == source_endpoint

    def test_control_host_session_analytics_master_lookup_keeps_pinned_host(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()
        pinned_host = Host(self._proxy_endpoint("proxy.control.example", host_id),
                           SimpleConvictionPolicy, host_id=host_id)
        pinned_host.set_up()

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            session = _ControlHostSession(cluster, source_host, pinned_host)

        master_future = Mock()
        master_future.result.return_value = [({'location': '127.0.0.99:8182'},)]

        query_future = Mock()
        query_future._host = pinned_host
        query_future.query = SimpleStatement("g.V()")
        query_future._load_balancer = Mock()
        query_future.send_request = Mock()
        query_future.query_plan = iter(())

        with patch.object(session, "submit",
                          side_effect=lambda fn, *args, **kwargs: self._completed_future(fn(*args, **kwargs))):
            session._on_analytics_master_result(None, master_future, query_future)

        assert list(query_future.query_plan) == [pinned_host]
        query_future._load_balancer.make_query_plan.assert_not_called()
        query_future.send_request.assert_called_once_with()

    def test_control_host_session_preserves_source_host_distance(self):
        cluster = Cluster(
            load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
            protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()
        pinned_host = Host(DefaultEndPoint("127.254.254.101"), SimpleConvictionPolicy, host_id=host_id)
        pinned_host.set_up()

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)) as add_pool:
            _ControlHostSession(cluster, source_host, pinned_host)

        add_pool.assert_called_once_with(pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_prefers_pinned_host_distance_for_proxy_whitelist(self):
        cluster = Cluster(
            load_balancing_policy=WhiteListRoundRobinPolicy(["127.254.254.101"]),
            protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()
        pinned_host = Host(DefaultEndPoint("127.254.254.101"), SimpleConvictionPolicy, host_id=host_id)
        pinned_host.set_up()

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)) as add_pool:
            _ControlHostSession(cluster, source_host, pinned_host)

        add_pool.assert_called_once_with(pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_reopens_pinned_pool_during_source_host_lifecycle(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()
        pinned_host = Host(DefaultEndPoint("127.254.254.101"), SimpleConvictionPolicy, host_id=host_id)
        pinned_host.set_up()

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            session = _ControlHostSession(cluster, source_host, pinned_host)

        with patch.object(session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(False)) as add_pool:
            future = session.add_or_renew_pool(source_host, is_host_addition=False)

        assert future.result() is True
        add_pool.assert_called_once_with(session._pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_waits_for_all_on_up_futures_before_marking_source_host_up(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_down()

        control_session = self._make_control_host_session(cluster, source_host)

        cluster._prepare_all_queries = Mock()
        cluster._start_reconnector = Mock()
        cluster.control_connection.on_up = Mock()
        cluster.control_connection.on_down = Mock()

        regular_session = Mock(spec=Session)
        regular_session.remove_pool.return_value = None
        regular_session.add_or_renew_pool.return_value = self._completed_future(False)
        regular_session.update_created_pools.return_value = set()

        cluster.sessions = [control_session, regular_session]

        with patch.object(control_session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)) as add_pool:
            cluster.on_up(source_host)

        assert source_host.is_up is False
        cluster.control_connection.on_up.assert_called_once_with(source_host)
        cluster.control_connection.on_down.assert_called_once_with(source_host)
        regular_session.add_or_renew_pool.assert_called_once_with(source_host, is_host_addition=False)
        regular_session.update_created_pools.assert_not_called()
        cluster._start_reconnector.assert_called_once_with(source_host, is_host_addition=False)
        add_pool.assert_called_once_with(control_session._pinned_host, HostDistance.LOCAL, False)

    @mock_session_pools
    def test_session_preserves_down_event_discounting_after_endpoint_update(self, *_):
        class _DeterministicHashEndPoint(EndPoint):
            def __init__(self, address, hash_value, port=9042):
                self._address = address
                self._hash_value = hash_value
                self._port = port

            @property
            def address(self):
                return self._address

            @property
            def port(self):
                return self._port

            def resolve(self):
                return self._address, self._port

            def __eq__(self, other):
                return isinstance(other, _DeterministicHashEndPoint) and \
                    self.address == other.address and self.port == other.port

            def __hash__(self):
                return self._hash_value

            def __lt__(self, other):
                return (self.address, self.port) < (other.address, other.port)

        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        host.set_up()

        session = Session(cluster, [host])
        cluster.sessions.add(session)

        pool = Mock()
        pool.get_state.return_value = {"open_count": 1}

        host.endpoint = _DeterministicHashEndPoint("127.0.0.1", 1)
        session._pools = {host: pool}
        host.endpoint = _DeterministicHashEndPoint("127.0.0.2", 2)

        cluster.on_down_potentially_blocking = Mock()

        cluster.on_down(host, is_host_addition=False)

        assert host.is_up is True
        cluster.on_down_potentially_blocking.assert_not_called()
        assert session.get_pool_state_for_host(host) == {"open_count": 1}

    def test_control_host_session_does_not_mask_source_host_down_events(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()

        control_session = self._make_control_host_session(cluster, source_host)
        pool = Mock()
        pool.get_state.return_value = {"open_count": 1}
        control_session._pools[control_session._pinned_host] = pool

        regular_session = Mock(spec=Session)
        regular_session.get_pool_state_for_host.return_value = None
        cluster.sessions = {control_session, regular_session}

        cluster.on_down_potentially_blocking = Mock()

        cluster.on_down(source_host, is_host_addition=False)

        assert source_host.is_up is False
        cluster.on_down_potentially_blocking.assert_called_once_with(source_host, False)
        assert control_session.get_pool_state_for_host(source_host) is None

    def test_control_host_session_does_not_mask_source_host_down_events_with_shared_host_aware_endpoint(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_endpoint = self._proxy_endpoint("proxy.control.example", host_id)
        source_host = Host(source_endpoint, SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()

        cluster.control_connection = Mock(
            _connection=Mock(endpoint=source_endpoint))
        cluster.get_control_connection_host = Mock(return_value=source_host)
        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            control_session = cluster._new_control_host_session(None)

        pool = Mock()
        pool.get_state.return_value = {"open_count": 1}
        control_session._pools[control_session._pinned_host] = pool

        regular_session = Mock(spec=Session)
        regular_session.get_pool_state_for_host.return_value = None
        cluster.sessions = {control_session, regular_session}

        cluster.on_down_potentially_blocking = Mock()

        cluster.on_down(source_host, is_host_addition=False)

        assert source_host.is_up is False
        cluster.on_down_potentially_blocking.assert_called_once_with(source_host, False)
        assert control_session.get_pool_state_for_host(source_host) is None

    def test_control_host_session_proxy_failures_schedule_backoff_reconnects(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()

        session = self._make_control_host_session(cluster, source_host)
        pool = Mock(is_shutdown=False)
        session._pools[session._pinned_host] = pool

        cluster.on_down = Mock()
        cluster.scheduler = Mock()
        cluster.reconnection_policy = Mock(new_schedule=Mock(return_value=iter([1.25])))

        def submit_sync(fn, *args, **kwargs):
            return self._completed_future(fn(*args, **kwargs))

        with patch.object(session, "submit", side_effect=submit_sync):
            session._handle_pool_down(session._pinned_host, is_host_addition=False)

        pool.shutdown.assert_called_once_with()
        cluster.on_down.assert_not_called()
        cluster.scheduler.schedule.assert_called_once()
        assert cluster.scheduler.schedule.call_args.args[0] == 1.25
        assert session._pinned_host.is_currently_reconnecting() is True

    def test_control_host_session_pool_failures_do_not_block_source_host_up(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_down()

        control_session = self._make_control_host_session(cluster, source_host)

        cluster.control_connection = Mock(on_up=Mock(), on_down=Mock())
        cluster._prepare_all_queries = Mock()

        regular_session = Mock(spec=Session)
        regular_session.remove_pool.return_value = None
        regular_session.add_or_renew_pool.return_value = self._completed_future(True)
        regular_session.update_created_pools.return_value = set()

        cluster.sessions = {control_session, regular_session}

        with patch.object(control_session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(False)) as add_pool:
            cluster.on_up(source_host)

        assert source_host.is_up is True
        cluster.control_connection.on_up.assert_called_once_with(source_host)
        cluster.control_connection.on_down.assert_not_called()
        regular_session.add_or_renew_pool.assert_called_once_with(source_host, is_host_addition=False)
        regular_session.update_created_pools.assert_called_once_with()
        assert add_pool.call_count == 2
        assert add_pool.call_args_list[0].args == (control_session._pinned_host, HostDistance.LOCAL, False)
        assert add_pool.call_args_list[1].args == (control_session._pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_pool_failures_do_not_block_source_host_up_with_shared_host_aware_endpoint(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_endpoint = self._proxy_endpoint("proxy.control.example", host_id)
        source_host = Host(source_endpoint, SimpleConvictionPolicy, host_id=host_id)
        source_host.set_down()

        cluster.control_connection = Mock(
            _connection=Mock(endpoint=source_endpoint),
            on_up=Mock(),
            on_down=Mock())
        cluster.get_control_connection_host = Mock(return_value=source_host)
        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            control_session = cluster._new_control_host_session(None)

        cluster._prepare_all_queries = Mock()

        regular_session = Mock(spec=Session)
        regular_session.remove_pool.return_value = None
        regular_session.add_or_renew_pool.return_value = self._completed_future(True)
        regular_session.update_created_pools.return_value = set()

        cluster.sessions = {control_session, regular_session}

        with patch.object(control_session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(False)) as add_pool:
            cluster.on_up(source_host)

        assert source_host.is_up is True
        cluster.control_connection.on_up.assert_called_once_with(source_host)
        cluster.control_connection.on_down.assert_not_called()
        regular_session.add_or_renew_pool.assert_called_once_with(source_host, is_host_addition=False)
        regular_session.update_created_pools.assert_called_once_with()
        assert add_pool.call_count == 2
        assert add_pool.call_args_list[0].args == (control_session._pinned_host, HostDistance.LOCAL, False)
        assert add_pool.call_args_list[1].args == (control_session._pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_disables_shard_aware_fanout(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()
        proxy_endpoint = self._proxy_endpoint("127.254.254.101", host_id)

        cluster.control_connection = Mock(
            _connection=Mock(endpoint=proxy_endpoint))
        cluster.get_control_connection_host = Mock(return_value=source_host)

        first_connection = Mock(is_defunct=False, is_closed=False)
        first_connection.features = Mock(
            shard_id=0,
            sharding_info=Mock(shard_aware_port=19042, shard_aware_port_ssl=None),
            tablets_routing_v1=False)
        cluster.connection_factory = Mock(return_value=first_connection)

        with patch.object(HostConnection, "_open_connections_for_all_shards") as open_shards:
            session = cluster._new_control_host_session(None)

        open_shards.assert_not_called()
        assert session.is_shard_aware_disabled() is True
        assert session._pinned_host.sharding_info is None

    def test_control_host_session_update_created_pools_resyncs_source_host_state(self):
        cluster = Cluster(
            load_balancing_policy=WhiteListRoundRobinPolicy(["127.0.0.1"]),
            protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()
        pinned_host = Host(DefaultEndPoint("127.254.254.101"), SimpleConvictionPolicy, host_id=host_id)
        pinned_host.set_down()

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            session = _ControlHostSession(cluster, source_host, pinned_host)

        session._pinned_host.set_down()
        source_host.set_up()

        with patch.object(session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)) as add_pool:
            session.update_created_pools()

        assert session._pinned_host.is_up is True
        add_pool.assert_called_once_with(session._pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_accepts_replacement_control_host_during_on_up(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        source_host.set_up()
        replacement_host = Host(DefaultEndPoint("127.0.0.2"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        replacement_host.set_up()

        session = self._make_control_host_session(cluster, source_host, endpoint="127.254.254.101")
        replacement_endpoint = self._proxy_endpoint("127.254.254.102", replacement_host.host_id)
        cluster.control_connection._connection.endpoint = replacement_endpoint
        cluster.get_control_connection_host.return_value = replacement_host

        with patch.object(session, "_add_or_renew_pool_for_distance") as add_pool:
            future = session.add_or_renew_pool(replacement_host, is_host_addition=False)

        assert future.result() is True
        assert session._source_host is replacement_host
        assert session._pinned_host.endpoint == replacement_endpoint
        add_pool.assert_not_called()

    def test_control_host_session_on_down_rebinds_source_host_after_failover(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        source_host.set_up()
        replacement_host = Host(DefaultEndPoint("127.0.0.2"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        replacement_host.set_up()

        old_endpoint = self._proxy_endpoint("127.254.254.101", source_host.host_id)
        new_endpoint = self._proxy_endpoint("127.254.254.102", replacement_host.host_id)
        cluster.control_connection = Mock(_connection=Mock(endpoint=old_endpoint))
        cluster.get_control_connection_host = Mock(
            side_effect=lambda: source_host
            if cluster.control_connection._connection.endpoint == old_endpoint
            else replacement_host)

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            session = cluster._new_control_host_session(None)

        pool = Mock(is_shutdown=False, host_distance=HostDistance.LOCAL)
        session._pools[session._pinned_host] = pool
        cluster.control_connection._connection.endpoint = new_endpoint

        with patch.object(session, "submit",
                          side_effect=lambda fn, *args, **kwargs: self._completed_future(fn(*args, **kwargs))), \
                patch.object(session, "_add_or_renew_pool_for_distance",
                             return_value=self._completed_future(True)) as add_pool:
            session.on_down(source_host)

        assert session._source_host is replacement_host
        assert session._pinned_host.endpoint == new_endpoint
        assert session._pinned_host.host_id == replacement_host.host_id
        assert session._pools.get(session._pinned_host) is None
        pool.shutdown.assert_called_once_with()
        add_pool.assert_called_once_with(session._pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_update_created_pools_rebinds_pinned_endpoint(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_up()

        session = self._make_control_host_session(cluster, source_host, endpoint="127.254.254.101")
        pool = Mock(is_shutdown=False, host_distance=HostDistance.LOCAL)
        pool.get_state.return_value = {"open_count": 1}
        session._pools[session._pinned_host] = pool

        new_endpoint = self._proxy_endpoint("127.254.254.102", host_id)
        cluster.control_connection._connection.endpoint = new_endpoint

        with patch.object(session, "submit",
                          side_effect=lambda fn, *args, **kwargs: self._completed_future(fn(*args, **kwargs))), \
                patch.object(session, "_add_or_renew_pool_for_distance",
                             return_value=self._completed_future(True)) as add_pool:
            session.update_created_pools()

        assert session._pinned_host.endpoint == new_endpoint
        assert session._pools.get(session._pinned_host) is None
        assert session.get_pool_state_for_host(session._pinned_host) is None
        pool.shutdown.assert_called_once_with()
        add_pool.assert_called_once_with(session._pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_update_created_pools_rebinds_source_host_after_failover(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        source_host.set_up()
        replacement_host = Host(DefaultEndPoint("127.0.0.2"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        replacement_host.set_up()

        session = self._make_control_host_session(cluster, source_host, endpoint="127.254.254.101")
        pool = Mock(is_shutdown=False, host_distance=HostDistance.LOCAL)
        session._pools[session._pinned_host] = pool

        replacement_endpoint = self._proxy_endpoint("127.254.254.102", replacement_host.host_id)
        cluster.control_connection._connection.endpoint = replacement_endpoint
        cluster.get_control_connection_host.return_value = replacement_host

        with patch.object(session, "submit",
                          side_effect=lambda fn, *args, **kwargs: self._completed_future(fn(*args, **kwargs))), \
                patch.object(session, "_add_or_renew_pool_for_distance",
                             return_value=self._completed_future(True)) as add_pool:
            session.update_created_pools()

        assert session._source_host is replacement_host
        assert session._pinned_host.endpoint == replacement_endpoint
        assert session._pinned_host.host_id == replacement_host.host_id
        assert session._pools.get(session._pinned_host) is None
        pool.shutdown.assert_called_once_with()
        add_pool.assert_called_once_with(session._pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_rebind_rejects_shared_default_endpoint_after_failover(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        source_host.set_up()
        replacement_host = Host(DefaultEndPoint("127.0.0.2"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        replacement_host.set_up()

        session = self._make_control_host_session(cluster, source_host, endpoint="127.254.254.101")
        original_pinned_host = session._pinned_host
        pool = Mock(is_shutdown=False, host_distance=HostDistance.LOCAL)
        session._pools[original_pinned_host] = pool

        cluster.control_connection._connection.endpoint = DefaultEndPoint("127.254.254.200")
        cluster.get_control_connection_host.return_value = replacement_host

        with patch.object(session, "submit",
                          side_effect=lambda fn, *args, **kwargs: self._completed_future(fn(*args, **kwargs))), \
                patch.object(session, "_add_or_renew_pool_for_distance") as add_pool:
            assert session.update_created_pools() == set()

        assert session._pinned_host is original_pinned_host
        assert session._pools.get(original_pinned_host) is None
        pool.shutdown.assert_called_once_with()
        add_pool.assert_not_called()

        with pytest.raises(DriverException, match="host-specific control endpoint"):
            session.prepare("SELECT release_version FROM system.local")

    def test_control_host_session_rebind_replaces_pinned_host_without_retargeting_inflight_requests(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        source_host.set_up()
        replacement_host = Host(DefaultEndPoint("127.0.0.2"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        replacement_host.set_up()

        session = self._make_control_host_session(cluster, source_host, endpoint="127.254.254.101")
        original_pinned_host = session._pinned_host

        original_pool = Mock(is_shutdown=False, host_distance=HostDistance.LOCAL)
        original_connection = Mock()
        original_connection._requests = {}
        original_connection.lock = RLock()
        original_connection.orphaned_request_ids = set()
        original_connection.orphaned_threshold = 10
        original_connection.orphaned_threshold_reached = False
        original_pool.borrow_connection.return_value = (original_connection, 1)
        session._pools[original_pinned_host] = original_pool

        query = SimpleStatement("SELECT release_version FROM system.local")
        timeout_message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
        timeout_future = ResponseFuture(
            session, timeout_message, query, None, host=original_pinned_host)
        timeout_future.send_request()
        original_connection._requests[timeout_future._req_id] = object()

        replacement_endpoint = self._proxy_endpoint("127.254.254.102", replacement_host.host_id)
        cluster.control_connection._connection.endpoint = replacement_endpoint
        cluster.get_control_connection_host.return_value = replacement_host

        with patch.object(session, "submit",
                          side_effect=lambda fn, *args, **kwargs: self._completed_future(fn(*args, **kwargs))), \
                patch.object(session, "_add_or_renew_pool_for_distance",
                             return_value=self._completed_future(True)) as add_pool:
            session.update_created_pools()

        assert session._pinned_host is not original_pinned_host
        assert session._pinned_host.endpoint == replacement_endpoint
        assert session._pinned_host.host_id == replacement_host.host_id
        assert session._pools.get(original_pinned_host) is None
        original_pool.shutdown.assert_called_once_with()
        add_pool.assert_called_once_with(session._pinned_host, HostDistance.LOCAL, False)

        replacement_pool = Mock(is_shutdown=False)
        session._pools[session._pinned_host] = replacement_pool

        timeout_future._on_timeout()
        replacement_pool.return_connection.assert_not_called()
        with pytest.raises(OperationTimedOut):
            timeout_future.result()

        retry_message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
        retry_future = ResponseFuture(
            session, retry_message, query, None, host=original_pinned_host)
        retry_future._retry_task(True, original_pinned_host)

        replacement_pool.borrow_connection.assert_not_called()
        assert isinstance(retry_future._final_exception, NoHostAvailable)

    def test_control_host_session_rebind_ignores_stale_pinned_host_failures(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        source_host.set_up()

        session = self._make_control_host_session(cluster, source_host, endpoint="127.254.254.101")
        original_pinned_host = session._pinned_host

        replacement_endpoint = self._proxy_endpoint("127.254.254.102", source_host.host_id)
        cluster.control_connection._connection.endpoint = replacement_endpoint

        with patch.object(session, "submit",
                          side_effect=lambda fn, *args, **kwargs: self._completed_future(fn(*args, **kwargs))), \
                patch.object(session, "_add_or_renew_pool_for_distance",
                             return_value=self._completed_future(True)):
            session.update_created_pools()

        assert session._pinned_host is not original_pinned_host

        with patch.object(Session, "_signal_connection_failure", return_value=True) as base_signal_failure:
            assert session._signal_connection_failure(original_pinned_host, Exception("stale failure")) is False

        with patch.object(Session, "_handle_pool_down") as base_handle_pool_down, \
                patch.object(session, "_start_control_host_reconnector") as start_reconnector:
            session._handle_pool_down(original_pinned_host, is_host_addition=False)

        base_signal_failure.assert_not_called()
        base_handle_pool_down.assert_not_called()
        start_reconnector.assert_not_called()

    def test_control_connection_failover_resyncs_control_host_sessions(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        source_host.set_up()
        replacement_host = Host(DefaultEndPoint("127.0.0.2"), SimpleConvictionPolicy, host_id=uuid.uuid4())
        replacement_host.set_up()

        old_endpoint = self._proxy_endpoint("127.254.254.101", source_host.host_id)
        new_endpoint = self._proxy_endpoint("127.254.254.102", replacement_host.host_id)
        old_connection = Mock(endpoint=old_endpoint)
        new_connection = Mock(endpoint=new_endpoint)
        cluster.control_connection._connection = old_connection
        cluster.get_control_connection_host = Mock(
            side_effect=lambda: source_host
            if cluster.control_connection._connection.endpoint == old_endpoint
            else replacement_host)

        with patch.object(Session, "_add_or_renew_pool_for_distance",
                          return_value=self._completed_future(True)):
            session = cluster._new_control_host_session(None)

        pool = Mock(is_shutdown=False, host_distance=HostDistance.LOCAL)
        session._pools[session._pinned_host] = pool

        with patch.object(session, "submit",
                          side_effect=lambda fn, *args, **kwargs: self._completed_future(fn(*args, **kwargs))), \
                patch.object(session, "_add_or_renew_pool_for_distance",
                             return_value=self._completed_future(True)) as add_pool:
            cluster.control_connection._set_new_connection(new_connection)

        old_connection.close.assert_called_once_with()
        assert session._source_host is replacement_host
        assert session._pinned_host.endpoint == new_endpoint
        assert session._pinned_host.host_id == replacement_host.host_id
        assert session._pools.get(session._pinned_host) is None
        pool.shutdown.assert_called_once_with()
        add_pool.assert_called_once_with(session._pinned_host, HostDistance.LOCAL, False)

    def test_control_host_session_source_host_up_failure_keeps_reopened_pinned_pool(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), protocol_version=4)
        self.addCleanup(cluster.shutdown)

        host_id = uuid.uuid4()
        source_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy, host_id=host_id)
        source_host.set_down()

        control_session = self._make_control_host_session(cluster, source_host)

        cluster.control_connection = Mock(
            on_up=Mock(),
            on_down=Mock(),
            _connection=cluster.control_connection._connection)
        cluster._prepare_all_queries = Mock()

        regular_session = Mock(spec=Session)
        regular_session.remove_pool.return_value = None
        regular_session.add_or_renew_pool.return_value = self._completed_future(False)
        regular_session.update_created_pools.return_value = set()

        cluster.sessions = {control_session, regular_session}

        reopened_pool = Mock(is_shutdown=False, host_distance=HostDistance.LOCAL)

        def reopen_pool(host, distance, is_host_addition):
            control_session._pools[host] = reopened_pool
            return self._completed_future(True)

        with patch.object(control_session, "_add_or_renew_pool_for_distance",
                          side_effect=reopen_pool) as add_pool:
            cluster.on_up(source_host)

        assert source_host.is_up is False
        assert control_session._pools.get(control_session._pinned_host) is reopened_pool
        cluster.control_connection.on_up.assert_called_once_with(source_host)
        cluster.control_connection.on_down.assert_called_once_with(source_host)
        regular_session.add_or_renew_pool.assert_called_once_with(source_host, is_host_addition=False)
        add_pool.assert_called_once_with(control_session._pinned_host, HostDistance.LOCAL, False)

class ProtocolVersionTests(unittest.TestCase):

    def test_protocol_downgrade_test(self):
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V5)
        assert ProtocolVersion.V4 == lower
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V4)
        assert ProtocolVersion.V3 == lower
        lower = ProtocolVersion.get_lower_supported(ProtocolVersion.V3)
        assert 0 == lower

        assert not ProtocolVersion.uses_error_code_map(ProtocolVersion.V4)
        assert not ProtocolVersion.uses_int_query_flags(ProtocolVersion.V4)


class ExecutionProfileTest(unittest.TestCase):
    def setUp(self):
        if connection_class is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        connection_class.initialize_reactor()

    def _verify_response_future_profile(self, rf, prof):
        assert rf._load_balancer == prof.load_balancing_policy
        assert rf._retry_policy == prof.retry_policy
        assert rf.message.consistency_level == prof.consistency_level
        assert rf.message.serial_consistency_level == prof.serial_consistency_level
        assert rf.timeout == prof.request_timeout
        assert rf.row_factory == prof.row_factory

    @mock_session_pools
    def test_default_exec_parameters(self):
        cluster = Cluster()
        assert cluster._config_mode == _ConfigMode.UNCOMMITTED
        assert cluster.load_balancing_policy.__class__ == default_lbp_factory().__class__
        assert cluster.profile_manager.default.load_balancing_policy.__class__ == default_lbp_factory().__class__
        assert cluster.default_retry_policy.__class__ == RetryPolicy
        assert cluster.profile_manager.default.retry_policy.__class__ == RetryPolicy
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])
        assert session.default_timeout == 10.0
        assert cluster.profile_manager.default.request_timeout == 10.0
        assert session.default_consistency_level == ConsistencyLevel.LOCAL_ONE
        assert cluster.profile_manager.default.consistency_level == ConsistencyLevel.LOCAL_ONE
        assert session.default_serial_consistency_level is None
        assert cluster.profile_manager.default.serial_consistency_level is None
        assert session.row_factory == named_tuple_factory
        assert cluster.profile_manager.default.row_factory == named_tuple_factory

    @mock_session_pools
    def test_default_legacy(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), default_retry_policy=DowngradingConsistencyRetryPolicy())
        assert cluster._config_mode == _ConfigMode.LEGACY
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])
        session.default_timeout = 3.7
        session.default_consistency_level = ConsistencyLevel.ALL
        session.default_serial_consistency_level = ConsistencyLevel.SERIAL
        rf = session.execute_async("query")
        expected_profile = ExecutionProfile(cluster.load_balancing_policy, cluster.default_retry_policy,
                                            session.default_consistency_level, session.default_serial_consistency_level,
                                            session.default_timeout, session.row_factory)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_default_profile(self):
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])

        assert cluster._config_mode == _ConfigMode.PROFILES

        default_profile = cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT]
        rf = session.execute_async("query")
        self._verify_response_future_profile(rf, default_profile)

        rf = session.execute_async("query", execution_profile='non-default')
        self._verify_response_future_profile(rf, non_default_profile)

        for name, ep in cluster.profile_manager.profiles.items():
            assert ep == session.get_execution_profile(name)

        # invalid ep
        with pytest.raises(ValueError):
            session.get_execution_profile('non-existent')

    def test_serial_consistency_level_validation(self):
        # should pass
        ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=ConsistencyLevel.SERIAL)
        ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL)

        # should not pass
        with pytest.raises(ValueError):
            ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=ConsistencyLevel.ANY)
        with pytest.raises(ValueError):
            ep = ExecutionProfile(RoundRobinPolicy(), serial_consistency_level=42)

    @mock_session_pools
    def test_statement_params_override_legacy(self):
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy(), default_retry_policy=DowngradingConsistencyRetryPolicy())
        assert cluster._config_mode == _ConfigMode.LEGACY
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])

        ss = SimpleStatement("query", retry_policy=DowngradingConsistencyRetryPolicy(),
                             consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
        my_timeout = 1.1234

        assert ss.retry_policy.__class__ != cluster.default_retry_policy
        assert ss.consistency_level != session.default_consistency_level
        assert ss._serial_consistency_level != session.default_serial_consistency_level
        assert my_timeout != session.default_timeout

        rf = session.execute_async(ss, timeout=my_timeout)
        expected_profile = ExecutionProfile(load_balancing_policy=cluster.load_balancing_policy, retry_policy=ss.retry_policy,
                                            request_timeout=my_timeout, consistency_level=ss.consistency_level,
                                            serial_consistency_level=ss._serial_consistency_level)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_statement_params_override_profile(self):
        non_default_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        cluster = Cluster(execution_profiles={'non-default': non_default_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])

        assert cluster._config_mode == _ConfigMode.PROFILES

        rf = session.execute_async("query", execution_profile='non-default')

        ss = SimpleStatement("query", retry_policy=DowngradingConsistencyRetryPolicy(),
                             consistency_level=ConsistencyLevel.ALL, serial_consistency_level=ConsistencyLevel.SERIAL)
        my_timeout = 1.1234

        assert ss.retry_policy.__class__ != rf._load_balancer.__class__
        assert ss.consistency_level != rf.message.consistency_level
        assert ss._serial_consistency_level != rf.message.serial_consistency_level
        assert my_timeout != rf.timeout

        rf = session.execute_async(ss, timeout=my_timeout, execution_profile='non-default')
        expected_profile = ExecutionProfile(non_default_profile.load_balancing_policy, ss.retry_policy,
                                            ss.consistency_level, ss._serial_consistency_level, my_timeout, non_default_profile.row_factory)
        self._verify_response_future_profile(rf, expected_profile)

    @mock_session_pools
    def test_no_profile_with_legacy(self):
        # don't construct with both
        with pytest.raises(ValueError):
            Cluster(load_balancing_policy=RoundRobinPolicy(), execution_profiles={'a': ExecutionProfile()})
        with pytest.raises(ValueError):
            Cluster(default_retry_policy=DowngradingConsistencyRetryPolicy(), execution_profiles={'a': ExecutionProfile()})
        with pytest.raises(ValueError):
            Cluster(load_balancing_policy=RoundRobinPolicy(),
                          default_retry_policy=DowngradingConsistencyRetryPolicy(), execution_profiles={'a': ExecutionProfile()})

        # can't add after
        cluster = Cluster(load_balancing_policy=RoundRobinPolicy())
        with pytest.raises(ValueError):
            cluster.add_execution_profile('name', ExecutionProfile())

        # session settings lock out profiles
        cluster = Cluster()
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])
        for attr, value in (('default_timeout', 1),
                            ('default_consistency_level', ConsistencyLevel.ANY),
                            ('default_serial_consistency_level', ConsistencyLevel.SERIAL),
                            ('row_factory', tuple_factory)):
            cluster._config_mode = _ConfigMode.UNCOMMITTED
            setattr(session, attr, value)
            with pytest.raises(ValueError):
                cluster.add_execution_profile('name' + attr, ExecutionProfile())

        # don't accept profile
        with pytest.raises(ValueError):
            session.execute_async("query", execution_profile='some name here')

    @mock_session_pools
    def test_no_legacy_with_profile(self):
        cluster_init = Cluster(execution_profiles={'name': ExecutionProfile()})
        cluster_add = Cluster()
        cluster_add.add_execution_profile('name', ExecutionProfile())
        # for clusters with profiles added either way...
        for cluster in (cluster_init, cluster_init):
            # don't allow legacy parameters set
            for attr, value in (('default_retry_policy', RetryPolicy()),
                                ('load_balancing_policy', default_lbp_factory())):
                with pytest.raises(ValueError):
                    setattr(cluster, attr, value)
            session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])
            for attr, value in (('default_timeout', 1),
                                ('default_consistency_level', ConsistencyLevel.ANY),
                                ('default_serial_consistency_level', ConsistencyLevel.SERIAL),
                                ('row_factory', tuple_factory)):
                with pytest.raises(ValueError):
                    setattr(session, attr, value)

    @mock_session_pools
    def test_profile_name_value(self):

        internalized_profile = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        cluster = Cluster(execution_profiles={'by-name': internalized_profile})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])
        assert cluster._config_mode == _ConfigMode.PROFILES

        rf = session.execute_async("query", execution_profile='by-name')
        self._verify_response_future_profile(rf, internalized_profile)

        by_value = ExecutionProfile(RoundRobinPolicy(), *[object() for _ in range(2)])
        rf = session.execute_async("query", execution_profile=by_value)
        self._verify_response_future_profile(rf, by_value)

    @mock_session_pools
    def test_exec_profile_clone(self):

        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(), 'one': ExecutionProfile()})
        session = Session(cluster, hosts=[Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())])

        profile_attrs = {'request_timeout': 1,
                         'consistency_level': ConsistencyLevel.ANY,
                         'serial_consistency_level': ConsistencyLevel.SERIAL,
                         'row_factory': tuple_factory,
                         'retry_policy': RetryPolicy(),
                         'load_balancing_policy': default_lbp_factory()}
        reference_attributes = ('retry_policy', 'load_balancing_policy')

        # default and one named
        for profile in (EXEC_PROFILE_DEFAULT, 'one'):
            active = session.get_execution_profile(profile)
            clone = session.execution_profile_clone_update(profile)
            assert clone is not active

            all_updated = session.execution_profile_clone_update(clone, **profile_attrs)
            assert all_updated is not clone
            for attr, value in profile_attrs.items():
                assert getattr(clone, attr) == getattr(active, attr)
                if attr in reference_attributes:
                    assert getattr(clone, attr) is getattr(active, attr)
                assert getattr(all_updated, attr) != getattr(active, attr)

        # cannot clone nonexistent profile
        with pytest.raises(ValueError):
            session.execution_profile_clone_update('DOES NOT EXIST', **profile_attrs)

    def test_no_profiles_same_name(self):
        # can override default in init
        cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(), 'one': ExecutionProfile()})

        # cannot update default
        with pytest.raises(ValueError):
            cluster.add_execution_profile(EXEC_PROFILE_DEFAULT, ExecutionProfile())

        # cannot update named init
        with pytest.raises(ValueError):
            cluster.add_execution_profile('one', ExecutionProfile())

        # can add new name
        cluster.add_execution_profile('two', ExecutionProfile())

        # cannot add a profile added dynamically
        with pytest.raises(ValueError):
            cluster.add_execution_profile('two', ExecutionProfile())

    def test_warning_on_no_lbp_with_contact_points_legacy_mode(self):
        """
        Test that users are warned when they instantiate a Cluster object in
        legacy mode with contact points but no load-balancing policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result logs

        @test_category configuration
        """
        self._check_warning_on_no_lbp_with_contact_points(
            cluster_kwargs={'contact_points': ['127.0.0.1']}
        )

    def test_warning_on_no_lbp_with_contact_points_profile_mode(self):
        """
        Test that users are warned when they instantiate a Cluster object in
        execution profile mode with contact points but no load-balancing
        policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result logs

        @test_category configuration
        """
        self._check_warning_on_no_lbp_with_contact_points(cluster_kwargs={
            'contact_points': ['127.0.0.1'],
            'execution_profiles': {EXEC_PROFILE_DEFAULT: ExecutionProfile()}
        })

    @mock_session_pools
    def _check_warning_on_no_lbp_with_contact_points(self, cluster_kwargs):
        with patch('cassandra.cluster.log') as patched_logger:
            Cluster(**cluster_kwargs)
        patched_logger.warning.assert_called_once()
        warning_message = patched_logger.warning.call_args[0][0]
        assert 'please specify a load-balancing policy' in warning_message
        assert "contact_points = ['127.0.0.1']" in warning_message

    def test_no_warning_on_contact_points_with_lbp_legacy_mode(self):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy in legacy mode.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        self._check_no_warning_on_contact_points_with_lbp({
            'contact_points': ['127.0.0.1'],
            'load_balancing_policy': object()
        })

    def test_no_warning_on_contact_points_with_lbp_profiles_mode(self):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy in execution profile
        mode.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        ep_with_lbp = ExecutionProfile(load_balancing_policy=object())
        self._check_no_warning_on_contact_points_with_lbp(cluster_kwargs={
            'contact_points': ['127.0.0.1'],
            'execution_profiles': {
                EXEC_PROFILE_DEFAULT: ep_with_lbp
            }
        })

    @mock_session_pools
    def _check_no_warning_on_contact_points_with_lbp(self, cluster_kwargs):
        """
        Test that users aren't warned when they instantiate a Cluster object
        with contact points and a load-balancing policy.

        @since 3.12.0
        @jira_ticket PYTHON-812
        @expected_result no logs

        @test_category configuration
        """
        with patch('cassandra.cluster.log') as patched_logger:
            Cluster(**cluster_kwargs)
        patched_logger.warning.assert_not_called()

    @mock_session_pools
    def test_warning_adding_no_lbp_ep_to_cluster_with_contact_points(self):
        ep_with_lbp = ExecutionProfile(load_balancing_policy=object())
        cluster = Cluster(
            contact_points=['127.0.0.1'],
            execution_profiles={EXEC_PROFILE_DEFAULT: ep_with_lbp})
        with patch('cassandra.cluster.log') as patched_logger:
            cluster.add_execution_profile(
                name='no_lbp',
                profile=ExecutionProfile()
            )

        patched_logger.warning.assert_called_once()
        warning_message = patched_logger.warning.call_args[0][0]
        assert 'no_lbp' in warning_message
        assert 'trying to add' in warning_message
        assert 'please specify a load-balancing policy' in warning_message

    @mock_session_pools
    def test_no_warning_adding_lbp_ep_to_cluster_with_contact_points(self):
        ep_with_lbp = ExecutionProfile(load_balancing_policy=object())
        cluster = Cluster(
            contact_points=['127.0.0.1'],
            execution_profiles={EXEC_PROFILE_DEFAULT: ep_with_lbp})
        with patch('cassandra.cluster.log') as patched_logger:
            cluster.add_execution_profile(
                name='with_lbp',
                profile=ExecutionProfile(load_balancing_policy=Mock(name='lbp'))
            )

        patched_logger.warning.assert_not_called()
