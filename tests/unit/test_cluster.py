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

from concurrent.futures import Future
import logging
import socket
from threading import Event, Lock, RLock, Thread
from types import SimpleNamespace

from unittest.mock import patch, Mock
import uuid

from cassandra import ConsistencyLevel, DriverException, Timeout, Unavailable, RequestExecutionException, ReadTimeout, WriteTimeout, CoordinationFailure, ReadFailure, WriteFailure, FunctionFailure, AlreadyExists,\
    InvalidRequest, Unauthorized, AuthenticationFailed, OperationTimedOut, UnsupportedOperation, RequestValidationException, ConfigurationException, ProtocolVersion
from cassandra.cluster import _Scheduler, Session, Cluster, ResultSet, SchemaAgreementScope, ControlConnectionQueryFallback, default_lbp_factory, \
    ExecutionProfile, _ConfigMode, EXEC_PROFILE_DEFAULT, \
    _HostTransitionResult, _SESSION_LOCAL_POOL_FAILURE, \
    _STALE_POOL_ATTEMPT
from cassandra.connection import (ConnectionBusy, ConnectionException,
                                  DefaultEndPoint,
                                  _ConnectionClosedDuringStartup)
from cassandra.metadata import Metadata
from cassandra.pool import Host, _HostReconnectionHandler
from cassandra.policies import HostDistance, RetryPolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy, SimpleConvictionPolicy
from cassandra.query import SimpleStatement, named_tuple_factory, tuple_factory
from tests.unit.utils import mock_session_pools
from tests import connection_class
import pytest


log = logging.getLogger(__name__)

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

    @staticmethod
    def _new_transition_cluster():
        cluster = object.__new__(Cluster)
        cluster.is_shutdown = False
        cluster.allow_control_connection_query_fallback = (
            ControlConnectionQueryFallback.Disabled)
        cluster._lock = RLock()
        cluster._discount_down_events = False
        cluster.sessions = []
        cluster._listener_lock = RLock()
        cluster._listeners = set()
        cluster.profile_manager = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        cluster.control_connection = Mock()
        cluster._prepare_all_queries = Mock()
        cluster._start_reconnector = Mock()
        cluster.on_down_potentially_blocking = (
            lambda host, is_host_addition, recovery_epoch:
            cluster._run_host_transition(
                host,
                lambda: cluster._on_down_potentially_blocking_serialized(
                    host, is_host_addition, recovery_epoch)))
        return cluster


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

    def test_control_connection_query_fallback_modes(self):
        assert Cluster().allow_control_connection_query_fallback is ControlConnectionQueryFallback.Disabled
        with pytest.raises(TypeError):
            Cluster(allow_control_connection_query_fallback=False)
        with pytest.raises(TypeError):
            Cluster(allow_control_connection_query_fallback=True)
        assert (
            Cluster(allow_control_connection_query_fallback=ControlConnectionQueryFallback.Fallback)
            .allow_control_connection_query_fallback
            is ControlConnectionQueryFallback.Fallback
        )
        assert Cluster(
            allow_control_connection_query_fallback=ControlConnectionQueryFallback.SkipPoolCreation
        ).allow_control_connection_query_fallback is ControlConnectionQueryFallback.SkipPoolCreation

    def test_control_connection_query_fallback_no_node_pool_mode_skips_pool_creation(self):
        cluster = Cluster(
            allow_control_connection_query_fallback=ControlConnectionQueryFallback.SkipPoolCreation,
            monitor_reporting_enabled=False,
        )
        host = Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())

        with patch.object(Session, "add_or_renew_pool") as mocked_add_or_renew_pool:
            session = Session(cluster, [host])

        mocked_add_or_renew_pool.assert_not_called()
        assert session._initial_connect_futures == set()
        assert session._pools == {}
        assert session.update_created_pools() == set()

    def test_control_connection_query_fallback_fallback_tolerates_empty_initial_pools(self):
        cluster = Cluster(
            allow_control_connection_query_fallback=ControlConnectionQueryFallback.Fallback,
            monitor_reporting_enabled=False,
        )
        host = Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())
        future = Future()
        future.set_result(False)

        with patch.object(Session, "add_or_renew_pool", return_value=future) as mocked_add_or_renew_pool:
            session = Session(cluster, [host])

        mocked_add_or_renew_pool.assert_called_once_with(host, is_host_addition=False)
        assert session._initial_connect_futures == {future}
        assert session._pools == {}

    def test_session_returns_after_first_pool_without_waiting_for_pending_pool(
            self):
        cluster = Cluster(monitor_reporting_enabled=False)
        hosts = [
            Host(
                "127.0.0.1",
                SimpleConvictionPolicy,
                host_id=uuid.uuid4()),
            Host(
                "127.0.0.2",
                SimpleConvictionPolicy,
                host_id=uuid.uuid4()),
        ]
        created = Future()
        created.set_result(True)
        pending = Future()
        futures = {
            id(hosts[0]): created,
            id(hosts[1]): pending,
        }

        def add_pool(session, host, is_host_addition):
            return futures[id(host)]

        with patch.object(
                Session,
                "add_or_renew_pool",
                new=add_pool):
            session = Session(cluster, hosts)

        assert session._initial_connect_futures == {created, pending}
        pending.set_result(False)
        session.shutdown()

    def test_session_constructor_failure_closes_published_pool(self):
        cluster = Cluster(
            column_encryption_policy=object(),
            monitor_reporting_enabled=False)
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        pool = Mock()

        def publish_pool(session, pool_host, is_host_addition):
            session._pools[pool_host] = pool
            future = Future()
            future.set_result(True)
            return future

        with patch.object(
                Session,
                "add_or_renew_pool",
                new=publish_pool):
            with pytest.raises(
                    Exception,
                    match="column_encryption_policy is temporary disabled"):
                Session(cluster, [host])

        pool.shutdown.assert_called_once_with()

    def test_new_session_reconciles_down_host_before_registration(self):
        cluster = object.__new__(Cluster)
        cluster._lock = RLock()
        cluster.is_shutdown = False
        cluster.sessions = set()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        cluster.metadata = Mock()
        cluster.metadata.all_hosts.return_value = [host]

        pool = Mock()
        session = Mock()
        session.hosts = [host]
        session._lock = RLock()
        session._initial_connect_host_futures = []
        session._pop_pools_locked.return_value = [pool]
        cluster._session_register_user_types = Mock(
            side_effect=lambda _: host.set_down())

        with patch('cassandra.cluster.Session', return_value=session):
            created = cluster._new_session(None)

        assert created is session
        assert session in cluster.sessions
        session._advance_pool_generation_locked.assert_called_once_with(
            host)
        pool.shutdown.assert_called_once_with()
        session.update_created_pools.assert_called_once_with(
            skip_host_ids=set())

    def test_new_session_reconciles_each_initial_host_once(self):
        cluster = object.__new__(Cluster)
        cluster._lock = RLock()
        cluster.is_shutdown = False
        cluster.sessions = set()
        hosts = [
            Host(
                "127.0.0.1",
                SimpleConvictionPolicy,
                host_id=uuid.uuid4()),
            Host(
                "127.0.0.2",
                SimpleConvictionPolicy,
                host_id=uuid.uuid4()),
        ]
        cluster.metadata = Mock()
        cluster.metadata.all_hosts.return_value = hosts
        cluster._session_register_user_types = Mock()

        initial_futures = [Future(), Future()]
        session = Mock()
        session.hosts = hosts
        session._lock = RLock()
        session._initial_connect_host_futures = list(zip(
            hosts, initial_futures))

        with patch('cassandra.cluster.Session', return_value=session):
            assert cluster._new_session(None) is session

        session.update_created_pools.assert_called_once_with(
            skip_host_ids={id(host) for host in hosts})

        initial_futures[0].set_result(False)
        assert session.update_created_pools.call_args_list[-1].kwargs == {
            'only_host_ids': (id(hosts[0]),)}

        initial_futures[1].set_result(False)
        assert session.update_created_pools.call_args_list[-1].kwargs == {
            'only_host_ids': (id(hosts[1]),)}
        assert session.update_created_pools.call_count == 3

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
            connection = Mock(is_closed=False)
            with patch.dict('cassandra.cluster.locally_supported_compressions', supported, clear=True):
                with patch.object(Cluster.connection_class, 'factory', autospec=True, return_value=connection) as factory:
                    cluster = Cluster(compression=configured)
                    conn = cluster.connection_factory(endpoint)

                assert conn is connection
                assert factory.call_count == 1
                assert factory.call_args.kwargs['compression'] == expected
                assert cluster.compression == expected

    def test_reconnection_factory_returns_open_connection(self):
        endpoint = Mock(address='127.0.0.1')
        host = Mock(endpoint=endpoint)
        connection = Mock(is_closed=False)

        with patch.object(Cluster.connection_class, 'factory', autospec=True, return_value=connection) as factory:
            cluster = Cluster()
            conn_factory = cluster._make_connection_factory(host)
            conn = conn_factory()

        assert conn is connection
        assert factory.call_count == 1

    def test_connection_factory_preserves_positional_arguments(self):
        endpoint = Mock(address='127.0.0.1')
        positional_option = object()
        connection = Mock(is_closed=False)

        with patch.object(
                Cluster.connection_class,
                'factory',
                autospec=True,
                return_value=connection) as factory:
            cluster = Cluster()
            conn = cluster.connection_factory(endpoint, positional_option)

        assert conn is connection
        assert factory.call_args.args[2] is positional_option

    def test_reconnection_factory_preserves_positional_arguments(self):
        endpoint = Mock(address='127.0.0.1')
        host = Mock(endpoint=endpoint)
        positional_option = object()
        connection = Mock(is_closed=False)

        with patch.object(
                Cluster.connection_class,
                'factory',
                autospec=True,
                return_value=connection) as factory:
            cluster = Cluster()
            conn_factory = cluster._make_connection_factory(
                host, positional_option)
            conn = conn_factory()

        assert conn is connection
        assert factory.call_args.args[2] is positional_option

    def test_connection_factory_returns_startup_close(self):
        endpoint = Mock(address='127.0.0.1')
        connection = Mock(is_closed=True)
        connection.endpoint = endpoint

        with patch.object(Cluster.connection_class, 'factory', autospec=True, return_value=connection) as factory:
            cluster = Cluster()
            conn = cluster.connection_factory(endpoint)

        assert conn is connection
        factory.assert_called_once()

    def test_reconnection_factory_returns_startup_close_result(self):
        endpoint = Mock(address='127.0.0.1')
        host = Mock(endpoint=endpoint)
        connection = Mock(is_closed=True)
        connection.endpoint = endpoint

        with patch.object(Cluster.connection_class, 'factory', autospec=True, return_value=connection):
            cluster = Cluster()
            conn_factory = cluster._make_connection_factory(host)
            conn = conn_factory()

        assert conn is connection

    def test_host_reconnection_handler_parks_startup_close(self):
        endpoint = Mock(address='127.0.0.1')
        host = Mock(endpoint=endpoint)
        connection = Mock(is_closed=True, endpoint=endpoint)
        connection_factory = Mock(return_value=connection)
        scheduler = Mock()
        on_add = Mock()
        on_up = Mock()
        callback = Mock()
        handler = _HostReconnectionHandler(
            host, connection_factory, False, on_add, on_up,
            scheduler, iter([1]), callback)

        handler.run()

        connection_factory.assert_called_once_with()
        connection.close.assert_called_once_with()
        on_add.assert_not_called()
        on_up.assert_not_called()
        callback.assert_not_called()
        scheduler.schedule.assert_not_called()

    def test_prepare_all_queries_rejects_startup_close(self):
        endpoint = Mock(address='127.0.0.1')
        host = Mock(endpoint=endpoint)
        connection = Mock(is_closed=True, endpoint=endpoint)
        cluster = Cluster()
        cluster._prepared_statements = {b'query-id': Mock()}

        with patch.object(cluster, 'connection_factory', return_value=connection), \
                patch.object(cluster, '_send_chunks') as send_chunks:
            cluster._prepare_all_queries(host)

        connection.set_keyspace_blocking.assert_not_called()
        send_chunks.assert_not_called()
        connection.close.assert_called_once_with()

    def test_force_on_down_bypasses_open_pool_discount(self):
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        session = Mock()
        session.get_pool_state.return_value = {
            host: {'open_count': 1},
        }
        cluster = object.__new__(Cluster)
        cluster.is_shutdown = False
        cluster.allow_control_connection_query_fallback = (
            ControlConnectionQueryFallback.Disabled)
        cluster._lock = RLock()
        cluster._discount_down_events = True
        cluster.profile_manager = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        cluster.sessions = [session]
        cluster.on_down_potentially_blocking = Mock()

        cluster.on_down(host, is_host_addition=False)

        assert host.is_up
        cluster.on_down_potentially_blocking.assert_not_called()

        cluster.on_down(
            host,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        assert not host.is_up
        cluster.on_down_potentially_blocking.assert_called_once_with(
            host, False, host._recovery_epoch)

    def test_endpoint_relocation_cleans_old_hash_before_readding_host(self):
        cluster = self._new_transition_cluster()
        host = Host(
            DefaultEndPoint("127.0.0.1"),
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        policy = RoundRobinPolicy()
        policy.populate(cluster, [host])
        profile_manager = Mock()
        profile_manager.distance.return_value = HostDistance.LOCAL
        profile_manager.on_down.side_effect = policy.on_down
        profile_manager.on_up.side_effect = policy.on_up
        cluster.profile_manager = profile_manager
        cluster.metadata = Mock()
        new_endpoint = DefaultEndPoint("127.0.0.2")

        assert cluster._force_down_for_endpoint_change(
            host,
            new_endpoint)

        assert host.endpoint == new_endpoint
        assert host.is_up is True
        assert tuple(policy.make_query_plan()) == (host,)
        assert len(policy._live_hosts) == 1
        cluster.metadata.update_host.assert_called_once_with(
            host,
            DefaultEndPoint("127.0.0.1"))
        cluster._start_reconnector.assert_not_called()

    def test_endpoint_relocation_mutates_before_reentrant_up(self):
        cluster = self._new_transition_cluster()
        host = Host(
            DefaultEndPoint("127.0.0.1"),
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        policy = RoundRobinPolicy()
        policy.populate(cluster, [host])
        reentered = []

        def on_down(down_host):
            policy.on_down(down_host)
            if not reentered:
                reentered.append(True)
                cluster.on_up(down_host)

        cluster.profile_manager.on_down.side_effect = on_down
        cluster.profile_manager.on_up.side_effect = policy.on_up
        cluster.metadata = Mock()
        new_endpoint = DefaultEndPoint("127.0.0.2")

        assert cluster._force_down_for_endpoint_change(
            host,
            new_endpoint)

        assert host.endpoint == new_endpoint
        assert host.is_up is True
        assert policy._live_hosts == frozenset((host,))

        policy.on_down(host)
        assert not policy._live_hosts

    def test_endpoint_relocation_updates_in_skip_pool_creation_mode(self):
        cluster = self._new_transition_cluster()
        cluster.allow_control_connection_query_fallback = (
            ControlConnectionQueryFallback.SkipPoolCreation)
        cluster.metadata = Mock()
        old_endpoint = DefaultEndPoint("127.0.0.1")
        new_endpoint = DefaultEndPoint("127.0.0.2")
        host = Host(
            old_endpoint,
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        policy = RoundRobinPolicy()
        policy.populate(cluster, [host])
        cluster.profile_manager.on_down.side_effect = policy.on_down
        cluster.profile_manager.on_up.side_effect = policy.on_up

        assert cluster._force_down_for_endpoint_change(
            host,
            new_endpoint)

        assert host.endpoint == new_endpoint
        assert host.is_up is True
        assert policy._live_hosts == frozenset((host,))
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.profile_manager.on_up.assert_called_once_with(host)
        cluster.metadata.update_host.assert_called_once_with(
            host,
            old_endpoint)

    def test_endpoint_relocation_racing_up_keeps_one_policy_entry(self):
        cluster = self._new_transition_cluster()
        old_endpoint = DefaultEndPoint("127.0.0.1")
        new_endpoint = DefaultEndPoint("127.0.0.2")
        host = Host(
            old_endpoint,
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        policy = RoundRobinPolicy()
        policy.populate(cluster, [host])
        cluster.profile_manager.on_down.side_effect = policy.on_down
        cluster.profile_manager.on_up.side_effect = policy.on_up
        cluster.metadata = Mock()
        lane_entered = Event()
        release_lane = Event()
        relocation_result = []

        def hold_lane():
            lane_entered.set()
            assert release_lane.wait(2)

        lane_thread = Thread(
            target=lambda: cluster._run_host_transition(
                host, hold_lane))
        lane_thread.start()
        assert lane_entered.wait(2)

        relocation_thread = Thread(
            target=lambda: relocation_result.append(
                cluster._force_down_for_endpoint_change(
                    host, new_endpoint)))
        relocation_thread.start()
        cluster.on_up(host)
        release_lane.set()
        lane_thread.join(2)
        relocation_thread.join(2)

        assert not lane_thread.is_alive()
        assert not relocation_thread.is_alive()
        assert relocation_result == [True]
        assert host.endpoint == new_endpoint
        assert host.is_up is True
        assert policy._live_hosts == frozenset((host,))
        assert len(policy._live_hosts) == 1

    def test_endpoint_relocation_does_not_resurrect_removed_host(self):
        cluster = self._new_transition_cluster()
        old_endpoint = DefaultEndPoint("127.0.0.1")
        new_endpoint = DefaultEndPoint("127.0.0.2")
        host = Host(
            old_endpoint,
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        cluster.metadata = Mock()
        lane_entered = Event()
        release_lane = Event()
        relocation_result = []

        def hold_lane():
            lane_entered.set()
            assert release_lane.wait(2)

        lane_thread = Thread(
            target=lambda: cluster._run_host_transition(
                host, hold_lane))
        lane_thread.start()
        assert lane_entered.wait(2)

        relocation_thread = Thread(
            target=lambda: relocation_result.append(
                cluster._force_down_for_endpoint_change(
                    host, new_endpoint)))
        relocation_thread.start()
        cluster.on_remove(host)
        release_lane.set()
        lane_thread.join(2)
        relocation_thread.join(2)

        assert not lane_thread.is_alive()
        assert not relocation_thread.is_alive()
        assert relocation_result == [False]
        assert host.endpoint == old_endpoint
        assert host._is_removed
        cluster.metadata.update_host.assert_not_called()

    def test_endpoint_relocation_honors_metadata_removal_gap(self):
        cluster = self._new_transition_cluster()
        old_endpoint = DefaultEndPoint("127.0.0.1")
        new_endpoint = DefaultEndPoint("127.0.0.2")
        host = Host(
            old_endpoint,
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        cluster.metadata = Metadata()
        cluster.metadata.add_or_return_host(host)
        lane_entered = Event()
        release_lane = Event()
        relocation_result = []

        def hold_lane():
            lane_entered.set()
            assert release_lane.wait(2)

        lane_thread = Thread(
            target=lambda: cluster._run_host_transition(
                host, hold_lane))
        lane_thread.start()
        assert lane_entered.wait(2)
        relocation_thread = Thread(
            target=lambda: relocation_result.append(
                cluster._force_down_for_endpoint_change(
                    host, new_endpoint)))
        relocation_thread.start()

        # Model Metadata.remove_host() winning just before Cluster.on_remove()
        # can mark the Host object itself.
        assert cluster.metadata.remove_host(host)
        assert not host._is_removed
        release_lane.set()
        lane_thread.join(2)
        relocation_thread.join(2)

        assert not lane_thread.is_alive()
        assert not relocation_thread.is_alive()
        assert relocation_result == [False]
        assert host.endpoint == old_endpoint
        assert cluster.metadata.get_host_by_host_id(host.host_id) is None

    def test_reentrant_endpoint_relocation_keeps_newest_endpoint(self):
        cluster = self._new_transition_cluster()
        first_endpoint = DefaultEndPoint("127.0.0.1")
        second_endpoint = DefaultEndPoint("127.0.0.2")
        newest_endpoint = DefaultEndPoint("127.0.0.3")
        host = Host(
            first_endpoint,
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        cluster.metadata = Mock()
        reentered = []

        def relocate_again(_):
            if not reentered:
                reentered.append(True)
                assert cluster._force_down_for_endpoint_change(
                    host, newest_endpoint)

        cluster.profile_manager.on_down.side_effect = relocate_again

        assert not cluster._force_down_for_endpoint_change(
            host, second_endpoint)

        assert host.endpoint == newest_endpoint
        assert host.is_up is True
        cluster.metadata.update_host.assert_called_once_with(
            host, first_endpoint)

    def test_endpoint_relocation_publishes_down_before_up(self):
        cluster = self._new_transition_cluster()
        host = Host(
            DefaultEndPoint("127.0.0.1"),
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        cluster.metadata = Mock()
        events = []
        listener = Mock()
        listener.on_down.side_effect = lambda _: events.append("down")
        listener.on_up.side_effect = lambda _: events.append("up")
        cluster._listeners = {listener}
        pool_created = Future()
        pool_created.set_result(True)
        session = Mock()
        session.add_or_renew_pool.return_value = pool_created
        cluster.sessions = [session]

        assert cluster._force_down_for_endpoint_change(
            host,
            DefaultEndPoint("127.0.0.2"))

        assert events == ["down", "up"]

    def test_later_down_wins_while_endpoint_relocation_cleans_up(self):
        cluster = self._new_transition_cluster()
        host = Host(
            DefaultEndPoint("127.0.0.1"),
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        cluster.metadata = Mock()
        cleanup_entered = Event()
        release_cleanup = Event()
        relocation_result = []

        def block_relocation_cleanup(_):
            cleanup_entered.set()
            assert release_cleanup.wait(2)

        cluster.profile_manager.on_down.side_effect = \
            block_relocation_cleanup
        relocation_thread = Thread(
            target=lambda: relocation_result.append(
                cluster._force_down_for_endpoint_change(
                    host,
                    DefaultEndPoint("127.0.0.2"))))
        relocation_thread.start()
        assert cleanup_entered.wait(2)

        cluster.on_down(
            host,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)
        release_cleanup.set()
        relocation_thread.join(2)

        assert not relocation_thread.is_alive()
        assert relocation_result == [True]
        assert host.is_up is False

    def test_private_duplicate_down_preempts_relocation_recovery(self):
        cluster = self._new_transition_cluster()
        host = Host(
            DefaultEndPoint("127.0.0.1"),
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        cluster.metadata = Mock()
        cleanup_entered = Event()
        release_cleanup = Event()

        def block_relocation_cleanup(_):
            cleanup_entered.set()
            assert release_cleanup.wait(2)

        cluster.profile_manager.on_down.side_effect = \
            block_relocation_cleanup
        relocation_thread = Thread(
            target=lambda: cluster._force_down_for_endpoint_change(
                host,
                DefaultEndPoint("127.0.0.2")))
        relocation_thread.start()
        assert cleanup_entered.wait(2)

        cluster._on_down_locked(
            host,
            is_host_addition=False)
        release_cleanup.set()
        relocation_thread.join(2)

        assert not relocation_thread.is_alive()
        assert host.is_up is False

    def test_endpoint_relocation_preserves_legacy_on_down_override(self):
        class LegacyCluster(Cluster):

            def on_down(
                    self, host, is_host_addition,
                    expect_host_to_be_down=False):
                self.endpoint_events.append((
                    "down",
                    host.endpoint,
                    is_host_addition,
                    expect_host_to_be_down))
                host.get_and_set_reconnection_handler(
                    self.override_reconnector)

            def on_up(self, host):
                self.endpoint_events.append(("up", host.endpoint))
                return Cluster.on_up(self, host)

        cluster = object.__new__(LegacyCluster)
        cluster.__dict__.update(
            self._new_transition_cluster().__dict__)
        cluster.endpoint_events = []
        cluster.override_reconnector = Mock()
        cluster.metadata = Mock()
        old_endpoint = DefaultEndPoint("127.0.0.1")
        new_endpoint = DefaultEndPoint("127.0.0.2")
        host = Host(
            old_endpoint,
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()

        assert cluster._force_down_for_endpoint_change(
            host,
            new_endpoint)

        assert cluster.endpoint_events == [
            ("down", old_endpoint, False, True),
            ("up", new_endpoint),
        ]
        assert host.is_up is True
        cluster.override_reconnector.cancel.assert_called_once_with()
        cluster.profile_manager.on_down.assert_called_once_with(host)

    def test_endpoint_relocation_with_delegating_down_override_recovers_up(
            self):
        class DelegatingCluster(Cluster):

            def on_down(
                    self, host, is_host_addition,
                    expect_host_to_be_down=False):
                return super().on_down(
                    host,
                    is_host_addition,
                    expect_host_to_be_down)

        cluster = object.__new__(DelegatingCluster)
        cluster.__dict__.update(
            self._new_transition_cluster().__dict__)
        cluster.metadata = Mock()
        host = Host(
            DefaultEndPoint("127.0.0.1"),
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()

        assert cluster._force_down_for_endpoint_change(
            host,
            DefaultEndPoint("127.0.0.2"))

        assert host.is_up is True
        assert host.endpoint == DefaultEndPoint("127.0.0.2")

    def test_endpoint_relocation_aborts_when_down_cleanup_fails(self):
        cluster = self._new_transition_cluster()
        old_endpoint = DefaultEndPoint("127.0.0.1")
        new_endpoint = DefaultEndPoint("127.0.0.2")
        host = Host(
            old_endpoint,
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        old_hash = hash(host)
        cluster.metadata = Mock()
        cluster.profile_manager.on_down.side_effect = RuntimeError(
            "policy cleanup failed")

        assert not cluster._force_down_for_endpoint_change(
            host,
            new_endpoint)

        assert host.endpoint == old_endpoint
        assert hash(host) == old_hash
        assert host.is_up is False
        cluster.metadata.update_host.assert_not_called()
        cluster.profile_manager.on_up.assert_not_called()
        cluster._start_reconnector.assert_called_once_with(
            host,
            is_host_addition=False,
            recovery_epoch=host._recovery_epoch)

    def test_duplicate_deferred_relocation_runs_one_down_up_cycle(self):
        cluster = self._new_transition_cluster()
        old_endpoint = DefaultEndPoint("127.0.0.1")
        new_endpoint = DefaultEndPoint("127.0.0.2")
        host = Host(
            old_endpoint,
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        cluster.metadata = Mock()
        deferred_results = []

        def enqueue_duplicate_relocations():
            deferred_results.append(
                cluster._force_down_for_endpoint_change(
                    host,
                    new_endpoint))
            deferred_results.append(
                cluster._force_down_for_endpoint_change(
                    host,
                    new_endpoint))

        cluster._run_host_transition(
            host,
            enqueue_duplicate_relocations)

        assert len(deferred_results) == 2
        assert host.endpoint == new_endpoint
        assert host.is_up is True
        cluster.metadata.update_host.assert_called_once_with(
            host,
            old_endpoint)
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.profile_manager.on_up.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        cluster.control_connection.on_up.assert_called_once_with(host)

    def test_older_blocked_delegating_relocation_cannot_overwrite_newer(
            self):
        class DelegatingCluster(Cluster):

            def on_down(
                    self, host, is_host_addition,
                    expect_host_to_be_down=False):
                self.delegating_down_calls += 1
                if self.delegating_down_calls == 1:
                    self.first_down_entered.set()
                    assert self.release_first_down.wait(2)
                return super().on_down(
                    host,
                    is_host_addition,
                    expect_host_to_be_down)

        cluster = object.__new__(DelegatingCluster)
        cluster.__dict__.update(
            self._new_transition_cluster().__dict__)
        cluster.metadata = Mock()
        cluster.delegating_down_calls = 0
        cluster.first_down_entered = Event()
        cluster.release_first_down = Event()
        second_reservation = Event()
        reservation_count = []

        def reserve_endpoint_change(host):
            sequence = Cluster._reserve_endpoint_change_event(host)
            reservation_count.append(sequence)
            if len(reservation_count) == 2:
                second_reservation.set()
            return sequence

        cluster._reserve_endpoint_change_event = reserve_endpoint_change
        old_endpoint = DefaultEndPoint("127.0.0.1")
        intermediate_endpoint = DefaultEndPoint("127.0.0.2")
        newest_endpoint = DefaultEndPoint("127.0.0.3")
        host = Host(
            old_endpoint,
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        older_result = []
        newer_result = []

        older_thread = Thread(
            target=lambda: older_result.append(
                cluster._force_down_for_endpoint_change(
                    host,
                    intermediate_endpoint)))
        newer_thread = Thread(
            target=lambda: newer_result.append(
                cluster._force_down_for_endpoint_change(
                    host,
                    newest_endpoint)))
        older_thread.start()
        assert cluster.first_down_entered.wait(2)
        newer_thread.start()
        assert second_reservation.wait(2)
        cluster.release_first_down.set()
        older_thread.join(2)
        newer_thread.join(2)

        assert not older_thread.is_alive()
        assert not newer_thread.is_alive()
        assert older_result == [False]
        assert newer_result == [True]
        assert host.endpoint == newest_endpoint
        assert host.is_up is True
        cluster.metadata.update_host.assert_called_once_with(
            host,
            old_endpoint)
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.profile_manager.on_up.assert_called_once_with(host)

    def test_force_connection_failure_bypasses_conviction_policy(self):
        host = Mock()
        host.signal_connection_failure.return_value = False
        error = ConnectionException("closed during startup")
        cluster = object.__new__(Cluster)
        cluster.on_down = Mock()
        cluster._on_down = Mock()

        is_down = cluster.signal_connection_failure(
            host,
            error,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        assert not is_down
        cluster._on_down.assert_called_once_with(
            host,
            False,
            True,
            force=True)
        cluster.on_down.assert_not_called()

    def test_forced_connection_failure_survives_conviction_policy_error(self):
        host = Mock()
        host.signal_connection_failure.side_effect = RuntimeError(
            "broken policy")
        cluster = object.__new__(Cluster)
        cluster._on_down = Mock()

        is_down = cluster.signal_connection_failure(
            host,
            ConnectionException("closed during startup"),
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        assert not is_down
        cluster._on_down.assert_called_once_with(
            host,
            False,
            True,
            force=True)

    def test_forced_connection_failure_survives_conviction_base_exception(
            self):
        class PolicyCancelled(BaseException):
            pass

        host = Mock()
        cancellation = PolicyCancelled("cancelled")
        host.signal_connection_failure.side_effect = cancellation
        cluster = object.__new__(Cluster)
        cluster._on_down = Mock()

        is_down = cluster.signal_connection_failure(
            host,
            ConnectionException("closed during startup"),
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        assert not is_down
        cluster._on_down.assert_called_once_with(
            host,
            False,
            True,
            force=True)

    def test_down_queued_during_on_up_leaves_host_and_policies_down(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_down()
        entered_on_up = Event()
        release_on_up = Event()
        policy_events = []

        def block_on_up(_):
            policy_events.append("up")
            entered_on_up.set()
            assert release_on_up.wait(2)

        cluster.profile_manager.on_up.side_effect = block_on_up
        cluster.profile_manager.on_down.side_effect = (
            lambda _: policy_events.append("down"))

        up_thread = Thread(target=cluster.on_up, args=(host,))
        up_thread.start()
        assert entered_on_up.wait(2)

        cluster._on_down_locked(
            host,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)
        release_on_up.set()
        up_thread.join(2)

        assert not up_thread.is_alive()
        assert host.is_up is False
        assert policy_events == ["up", "down"]
        cluster.control_connection.on_up.assert_not_called()
        cluster.control_connection.on_down.assert_called_once_with(host)

    def test_unknown_host_failed_up_enters_down_recovery(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        cluster._prepare_all_queries.side_effect = RuntimeError(
            "query preparation failed")

        assert host.is_up is None
        with pytest.raises(RuntimeError, match="query preparation failed"):
            cluster.on_up(host)

        assert host.is_up is False
        assert not host._currently_handling_node_up
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host,
            is_host_addition=False,
            recovery_epoch=host._recovery_epoch)

    def test_up_during_add_preserves_add_callbacks(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        listener = Mock()
        cluster._listeners = {listener}
        entered_add = Event()
        release_add = Event()
        errors = []

        def block_add(_):
            entered_add.set()
            assert release_add.wait(2)

        cluster.profile_manager.on_add.side_effect = block_add

        def run_add():
            try:
                cluster.on_add(host)
            except BaseException as exc:
                errors.append(exc)

        add_thread = Thread(target=run_add)
        add_thread.start()
        assert entered_add.wait(2)

        cluster.on_up(host)
        release_add.set()
        add_thread.join(2)

        assert not add_thread.is_alive()
        assert not errors
        assert host.is_up is True
        cluster.control_connection.on_add.assert_called_once_with(host, True)
        listener.on_add.assert_called_once_with(host)
        cluster.control_connection.on_up.assert_not_called()
        listener.on_up.assert_not_called()

    def test_down_during_add_preserves_add_before_down_callbacks(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        listener = Mock()
        cluster._listeners = {listener}
        entered_add = Event()
        release_add = Event()
        event_lock = Lock()
        events = []
        errors = []

        def record(event):
            with event_lock:
                events.append(event)

        def block_add(_):
            record("profile-add")
            entered_add.set()
            assert release_add.wait(2)

        cluster.profile_manager.on_add.side_effect = block_add
        cluster.profile_manager.on_down.side_effect = \
            lambda _: record("profile-down")
        cluster.control_connection.on_add.side_effect = \
            lambda *_: record("control-add")
        cluster.control_connection.on_down.side_effect = \
            lambda _: record("control-down")
        listener.on_add.side_effect = lambda _: record("listener-add")
        listener.on_down.side_effect = lambda _: record("listener-down")

        def run_add():
            try:
                cluster.on_add(host)
            except BaseException as exc:
                errors.append(exc)

        add_thread = Thread(target=run_add)
        add_thread.start()
        assert entered_add.wait(2)

        cluster.on_down(
            host,
            is_host_addition=True,
            expect_host_to_be_down=True)
        release_add.set()
        add_thread.join(2)

        assert not add_thread.is_alive()
        assert not errors
        assert host.is_up is False
        assert events.index("control-add") < events.index("control-down")
        assert events.index("listener-add") < events.index("listener-down")

    def test_queued_add_then_down_finishes_down(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        lane_entered = Event()
        release_lane = Event()

        def hold_lane():
            lane_entered.set()
            assert release_lane.wait(2)

        lane_thread = Thread(
            target=lambda: cluster._run_host_transition(
                host, hold_lane))
        lane_thread.start()
        assert lane_entered.wait(2)

        cluster.on_add(host)
        cluster.on_down(
            host,
            is_host_addition=True,
            expect_host_to_be_down=True,
            force=True)
        release_lane.set()
        lane_thread.join(2)

        assert not lane_thread.is_alive()
        assert host.is_up is False
        cluster.profile_manager.on_add.assert_called_once_with(host)
        cluster.profile_manager.on_down.assert_called_once_with(host)

    def test_preemptive_private_down_fences_older_queued_add(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        lane_entered = Event()
        release_lane = Event()

        def hold_lane():
            lane_entered.set()
            assert release_lane.wait(2)

        lane_thread = Thread(
            target=lambda: cluster._run_host_transition(
                host, hold_lane))
        lane_thread.start()
        assert lane_entered.wait(2)

        cluster.on_add(host)
        cluster._on_down_locked(
            host,
            is_host_addition=True,
            expect_host_to_be_down=True,
            force=True)
        release_lane.set()
        lane_thread.join(2)

        assert not lane_thread.is_alive()
        assert host.is_up is False
        cluster.profile_manager.on_add.assert_not_called()
        cluster.profile_manager.on_down.assert_called_once_with(host)

    def test_down_during_pending_add_does_not_publish_pool_success(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.conviction_policy = Mock()
        listener = Mock()
        cluster._listeners = {listener}
        listener_events = []
        listener.on_add.side_effect = \
            lambda _: listener_events.append("add")
        listener.on_down.side_effect = \
            lambda _: listener_events.append("down")
        delayed_executor_submissions = []
        cluster.on_down_potentially_blocking = \
            lambda *args: delayed_executor_submissions.append(args)
        pending_add = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pending_add
        cluster.sessions = [session]

        cluster.on_add(host)

        assert host._currently_handling_node_add
        assert host.is_up is None

        cluster.on_down(
            host,
            is_host_addition=True,
            expect_host_to_be_down=True)

        assert host.is_up is False
        assert not host._currently_handling_node_add
        host.conviction_policy.reset.assert_not_called()
        listener.on_add.assert_called_once_with(host)
        listener.on_down.assert_not_called()
        assert listener_events == ["add"]
        assert len(delayed_executor_submissions) == 1

        down_host, is_addition, down_epoch = \
            delayed_executor_submissions.pop()
        cluster._run_host_transition(
            down_host,
            lambda: cluster._on_down_potentially_blocking_serialized(
                down_host,
                is_addition,
                down_epoch))

        listener.on_down.assert_called_once_with(host)
        assert listener_events == ["add", "down"]

        # The superseded pool attempt may finish later, but must not publish
        # the host UP or replace the newer DOWN transition.
        pending_add.set_result(False)

        assert host.is_up is False
        host.conviction_policy.reset.assert_not_called()
        session.add_or_renew_pool.assert_called_once_with(
            host,
            is_host_addition=True,
            recovery_epoch=1)

    def test_discounted_down_does_not_cancel_pending_add(self):
        cluster = self._new_transition_cluster()
        cluster._discount_down_events = True
        cluster.on_down_potentially_blocking = Mock()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        listener = Mock()
        cluster._listeners = {listener}
        pending_add = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pending_add
        session.get_pool_state.return_value = {
            host: {'open_count': 1}}
        cluster.sessions = [session]

        cluster.on_add(host)
        cluster.on_down(
            host,
            is_host_addition=True,
            expect_host_to_be_down=True)

        assert host.is_up is None
        assert host._currently_handling_node_add
        assert host._recovery_epoch == 1
        cluster.on_down_potentially_blocking.assert_not_called()
        listener.on_add.assert_not_called()
        listener.on_down.assert_not_called()

        pending_add.set_result(True)

        assert host.is_up is True
        assert not host._currently_handling_node_add
        listener.on_add.assert_called_once_with(host)
        listener.on_down.assert_not_called()

    def test_external_host_notifications_have_independent_fifo(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        first_notification_entered = Event()
        release_first_notification = Event()
        second_core_done = Event()
        events = []
        errors = []

        def first_notification():
            first_notification_entered.set()
            assert release_first_notification.wait(2)
            events.append("add")

        def run_first():
            try:
                cluster._run_host_transition(
                    host,
                    lambda: _HostTransitionResult(
                        notifications=(first_notification,)))
            except BaseException as exc:
                errors.append(exc)

        def second_notification():
            events.append("down")
            raise RuntimeError("second listener failed")

        def run_second():
            try:
                cluster._run_host_transition(
                    host,
                    lambda: _HostTransitionResult(
                        notifications=(second_notification,)))
            except BaseException as exc:
                errors.append(exc)
            finally:
                second_core_done.set()

        first_thread = Thread(target=run_first)
        second_thread = Thread(target=run_second)
        first_thread.start()
        assert first_notification_entered.wait(2)
        second_thread.start()

        assert second_core_done.wait(1)
        assert events == []

        release_first_notification.set()
        first_thread.join(2)
        second_thread.join(2)

        assert not first_thread.is_alive()
        assert not second_thread.is_alive()
        assert not errors
        assert events == ["add", "down"]

    def test_cross_host_transition_wait_does_not_deadlock(self):
        cluster = self._new_transition_cluster()
        first_host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        second_host = Host(
            "127.0.0.2",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        first_entered = Event()
        second_entered = Event()
        callbacks = []
        errors = []

        def first_work():
            first_entered.set()
            assert second_entered.wait(2)
            cluster._run_host_transition_and_wait(
                second_host,
                lambda: callbacks.append("from-first"))

        def second_work():
            second_entered.set()
            assert first_entered.wait(2)
            cluster._run_host_transition_and_wait(
                first_host,
                lambda: callbacks.append("from-second"))

        def run(host, work):
            try:
                cluster._run_host_transition(host, work)
            except BaseException as exc:
                errors.append(exc)

        first_thread = Thread(
            target=run,
            args=(first_host, first_work),
            daemon=True)
        second_thread = Thread(
            target=run,
            args=(second_host, second_work),
            daemon=True)
        first_thread.start()
        second_thread.start()
        first_thread.join(2)
        second_thread.join(2)

        assert not first_thread.is_alive()
        assert not second_thread.is_alive()
        assert not errors
        assert sorted(callbacks) == ["from-first", "from-second"]

    def test_host_up_reconciles_sessions_before_external_listeners(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_down()
        events = []
        listener = Mock()
        listener.on_up.side_effect = lambda _: events.append("listener")
        cluster._listeners = {listener}
        pool_created = Future()
        pool_created.set_result(True)
        session = Mock()
        session.add_or_renew_pool.return_value = pool_created
        session.update_created_pools.side_effect = \
            lambda: events.append("session")
        cluster.sessions = [session]

        cluster.on_up(host)

        assert events == ["session", "listener"]

    def test_host_up_without_pool_futures_reconciles_and_notifies(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_down()
        listener = Mock()
        cluster._listeners = {listener}
        session = Mock()
        session.add_or_renew_pool.return_value = None
        cluster.sessions = [session]

        cluster.on_up(host)

        assert host.is_up is True
        session.add_or_renew_pool.assert_called_once_with(
            host,
            is_host_addition=False,
            recovery_epoch=host._recovery_epoch)
        session.update_created_pools.assert_called_once_with()
        listener.on_up.assert_called_once_with(host)

    def test_host_add_reconciles_sessions_before_external_listeners(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        events = []
        listener = Mock()
        listener.on_add.side_effect = lambda _: events.append("listener")
        cluster._listeners = {listener}
        pool_created = Future()
        pool_created.set_result(True)
        session = Mock()
        session.add_or_renew_pool.return_value = pool_created
        session.update_created_pools.side_effect = \
            lambda: events.append("session")
        cluster.sessions = [session]

        cluster.on_add(host)

        assert events == ["session", "listener"]

    def test_blocking_up_listener_does_not_block_remove_cleanup(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_down()
        host._recovery_epoch = 4
        host._currently_handling_node_up = True
        session = Mock()
        cluster.sessions = [session]
        listener = Mock()
        cluster._listeners = {listener}
        entered_listener = Event()
        release_listener = Event()
        remove_done = Event()
        errors = []

        def block_on_up(_):
            entered_listener.set()
            assert release_listener.wait(2)

        listener.on_up.side_effect = block_on_up

        def finish_up():
            try:
                cluster._run_host_transition(
                    host,
                    lambda: cluster._finish_on_up(host, 4, [True]))
            except BaseException as exc:
                errors.append(exc)

        def remove():
            try:
                cluster.on_remove(host)
            except BaseException as exc:
                errors.append(exc)
            finally:
                remove_done.set()

        up_thread = Thread(target=finish_up)
        remove_thread = Thread(target=remove)
        up_thread.start()
        assert entered_listener.wait(2)
        remove_thread.start()
        cleanup_completed_while_listener_blocked = remove_done.wait(1)
        release_listener.set()
        up_thread.join(2)
        remove_thread.join(2)

        assert cleanup_completed_while_listener_blocked
        assert not up_thread.is_alive()
        assert not remove_thread.is_alive()
        assert not errors
        assert host._is_removed
        assert host.is_up is False
        session.on_remove.assert_called_once_with(host)
        cluster.control_connection.on_remove.assert_called_once_with(host)

    def test_up_queued_during_on_down_leaves_host_and_policies_up(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        entered_on_down = Event()
        release_on_down = Event()
        policy_events = []

        def block_on_down(_):
            policy_events.append("down")
            entered_on_down.set()
            assert release_on_down.wait(2)

        cluster.profile_manager.on_down.side_effect = block_on_down
        cluster.profile_manager.on_up.side_effect = (
            lambda _: policy_events.append("up"))

        down_thread = Thread(
            target=cluster._on_down_locked,
            kwargs={
                'host': host,
                'is_host_addition': False,
                'expect_host_to_be_down': True,
                'force': True,
            })
        down_thread.start()
        assert entered_on_down.wait(2)

        cluster.on_up(host)
        release_on_down.set()
        down_thread.join(2)

        assert not down_thread.is_alive()
        assert host.is_up is True
        assert policy_events == ["down", "up"]
        cluster.control_connection.on_down.assert_called_once_with(host)
        cluster.control_connection.on_up.assert_called_once_with(host)

    def test_failed_up_preserves_delayed_down_notification(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        listener = Mock()
        cluster._listeners = {listener}
        delayed_down = []
        cluster.on_down_potentially_blocking = (
            lambda *args: delayed_down.append(args))

        cluster.on_down(
            host,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        cluster.sessions = [session]
        cluster.on_up(host)

        assert host._currently_handling_node_up
        assert listener.on_down.call_count == 0
        down_args = delayed_down.pop()
        cluster._run_host_transition(
            host,
            lambda: cluster._on_down_potentially_blocking_serialized(
                *down_args))
        pool_future.set_result(False)

        assert host.is_up is False
        listener.on_down.assert_called_once_with(host)
        listener.on_up.assert_not_called()

    def test_failed_up_does_not_repeat_published_down_notification(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        listener = Mock()
        cluster._listeners = {listener}

        cluster.on_down(
            host,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)
        listener.on_down.assert_called_once_with(host)

        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        cluster.sessions = [session]
        cluster.on_up(host)
        pool_future.set_result(False)

        assert host.is_up is False
        listener.on_down.assert_called_once_with(host)
        listener.on_up.assert_not_called()

    def test_successful_up_suppresses_delayed_down_notification(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        listener = Mock()
        cluster._listeners = {listener}
        delayed_down = []
        cluster.on_down_potentially_blocking = (
            lambda *args: delayed_down.append(args))

        cluster.on_down(
            host,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        cluster.sessions = [session]
        cluster.on_up(host)
        pool_future.set_result(True)

        down_args = delayed_down.pop()
        cluster._run_host_transition(
            host,
            lambda: cluster._on_down_potentially_blocking_serialized(
                *down_args))

        assert host.is_up is True
        listener.on_down.assert_not_called()
        listener.on_up.assert_called_once_with(host)

    def test_reentrant_add_failure_transitions_to_recovery_and_propagates(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        cluster.profile_manager.on_add.side_effect = RuntimeError(
            "policy add failed")

        cluster._run_host_transition(
            host,
            lambda: cluster.on_add(host))

        assert host.is_up is False
        assert not host._currently_handling_node_add
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host,
            True,
            recovery_epoch=host._recovery_epoch)

    def test_add_resets_conviction_without_holding_host_lock(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        acquired = Event()
        reset_thread = []

        def reset_from_another_thread():
            def acquire_host_lock():
                with host.lock:
                    acquired.set()

            thread = Thread(target=acquire_host_lock)
            reset_thread.append(thread)
            thread.start()
            assert acquired.wait(1)
            thread.join(1)

        host.conviction_policy.reset = reset_from_another_thread

        cluster.on_add(host)

        assert acquired.is_set()
        assert not reset_thread[0].is_alive()
        assert host.is_up is True
        assert not host._currently_handling_node_add

    def test_add_reset_failure_transitions_to_recovery(self):
        class ResetCancelled(BaseException):
            pass

        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.conviction_policy.reset = Mock(
            side_effect=ResetCancelled("cancelled reset"))

        with pytest.raises(ResetCancelled):
            cluster.on_add(host)

        assert host.is_up is False
        assert not host._currently_handling_node_add
        cluster._start_reconnector.assert_called_once_with(
            host,
            True,
            recovery_epoch=host._recovery_epoch)

    def test_up_reset_failure_restores_reconnector(self):
        class ResetCancelled(BaseException):
            pass

        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_down()
        host.conviction_policy.reset = Mock(
            side_effect=ResetCancelled("cancelled reset"))

        with pytest.raises(ResetCancelled):
            cluster.on_up(host)

        assert host.is_up is False
        assert not host._currently_handling_node_up
        cluster._start_reconnector.assert_called_once_with(
            host,
            is_host_addition=False,
            recovery_epoch=host._recovery_epoch)

    def test_down_callback_failure_still_starts_reconnector(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        session = Mock()
        listener = Mock()
        cluster.sessions = [session]
        cluster._listeners = {listener}
        cluster.profile_manager.on_down.side_effect = RuntimeError(
            "policy down failed")

        cluster._on_down_locked(
            host,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        assert host.is_up is False
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(host)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host,
            False,
            recovery_epoch=host._recovery_epoch)

    def test_rejected_down_executor_uses_fallback_cleanup(self):
        cluster = self._new_transition_cluster()
        del cluster.on_down_potentially_blocking
        cluster.executor = Mock()
        cluster.executor.submit.side_effect = RuntimeError(
            "executor rejected cleanup")
        host = Host(
            DefaultEndPoint("127.0.0.1"),
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        session = Mock()
        listener = Mock()
        cluster.sessions = [session]
        cluster._listeners = {listener}
        cleanup_completed = Event()
        cluster._start_reconnector.side_effect = \
            lambda *args, **kwargs: cleanup_completed.set()
        startup_error = _ConnectionClosedDuringStartup(
            "closed during startup",
            host.endpoint)

        cluster.signal_connection_failure(
            host,
            startup_error,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        assert cleanup_completed.wait(2)
        assert host.is_up is False
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(host)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host,
            False,
            recovery_epoch=host._recovery_epoch)

    def test_reconnector_cancel_error_does_not_strand_transitions(self):
        for transition in ("up", "add", "remove"):
            with self.subTest(transition=transition):
                cluster = self._new_transition_cluster()
                host = Host(
                    "127.0.0.1",
                    SimpleConvictionPolicy,
                    host_id=uuid.uuid4())
                if transition == "up":
                    host.set_down()
                elif transition == "remove":
                    host.set_up()
                reconnector = Mock()
                reconnector.cancel.side_effect = RuntimeError(
                    "cancel failed")
                host.get_and_set_reconnection_handler(reconnector)

                if transition == "up":
                    cluster.on_up(host)
                    assert host.is_up is True
                    cluster.profile_manager.on_up.assert_called_once_with(
                        host)
                    cluster.control_connection.on_up.assert_called_once_with(
                        host)
                elif transition == "add":
                    cluster.on_add(host)
                    assert host.is_up is True
                    cluster.profile_manager.on_add.assert_called_once_with(
                        host)
                    cluster.control_connection.on_add.assert_called_once_with(
                        host, True)
                else:
                    cluster.on_remove(host)
                    assert host._is_removed
                    assert host.is_up is False
                    cluster.profile_manager.on_remove.assert_called_once_with(
                        host)
                    cluster.control_connection.on_remove.assert_called_once_with(
                        host)

                reconnector.cancel.assert_called_once_with()

    def test_session_invalidation_error_does_not_suppress_down_cleanup(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        failing_session = Mock()
        failing_session._invalidate_pool_attempts.side_effect = RuntimeError(
            "invalidation failed")
        healthy_session = Mock()
        cluster.sessions = [failing_session, healthy_session]

        cluster.on_down(
            host,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        assert host.is_up is False
        failing_session._invalidate_pool_attempts.assert_called_once_with(
            host)
        healthy_session._invalidate_pool_attempts.assert_called_once_with(
            host)
        failing_session.on_down.assert_called_once_with(host)
        healthy_session.on_down.assert_called_once_with(host)
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host,
            False,
            recovery_epoch=host._recovery_epoch)

    def test_session_invalidation_error_does_not_suppress_remove_cleanup(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        failing_session = Mock()
        failing_session._invalidate_pool_attempts.side_effect = RuntimeError(
            "invalidation failed")
        healthy_session = Mock()
        cluster.sessions = [failing_session, healthy_session]

        cluster.on_remove(host)

        assert host._is_removed
        assert host.is_up is False
        failing_session._invalidate_pool_attempts.assert_called_once_with(
            host)
        healthy_session._invalidate_pool_attempts.assert_called_once_with(
            host)
        failing_session.on_remove.assert_called_once_with(host)
        healthy_session.on_remove.assert_called_once_with(host)
        cluster.profile_manager.on_remove.assert_called_once_with(host)
        cluster.control_connection.on_remove.assert_called_once_with(host)

    def test_failed_up_cleanup_error_does_not_skip_pool_teardown(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_down()
        host._recovery_epoch = 3
        first_session = Mock()
        second_session = Mock()
        cluster.sessions = [first_session, second_session]
        cluster.profile_manager.on_down.side_effect = RuntimeError(
            "policy cleanup failed")

        with pytest.raises(RuntimeError, match="policy cleanup failed"):
            cluster._cleanup_failed_on_up_handling(
                host,
                recovery_epoch=3)

        cluster.control_connection.on_down.assert_called_once_with(host)
        first_session.remove_pool.assert_called_once_with(host)
        second_session.remove_pool.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host,
            is_host_addition=False,
            recovery_epoch=3)

    def test_up_listener_error_does_not_skip_remaining_reconciliation(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_down()
        host._recovery_epoch = 4
        host._currently_handling_node_up = True
        failing_listener = Mock()
        healthy_listener = Mock()
        failing_listener.on_up.side_effect = RuntimeError(
            "listener failed")
        cluster._listeners = {failing_listener, healthy_listener}
        session = Mock()
        cluster.sessions = [session]

        cluster._run_host_transition(
            host,
            lambda: cluster._finish_on_up(host, 4, [True]))

        assert host.is_up is True
        healthy_listener.on_up.assert_called_once_with(host)
        session.update_created_pools.assert_called_once_with()

    def test_remove_callback_failure_does_not_skip_pool_cleanup(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        session = Mock()
        listener = Mock()
        cluster.sessions = [session]
        cluster._listeners = {listener}
        cluster.profile_manager.on_remove.side_effect = RuntimeError(
            "policy remove failed")

        cluster.on_remove(host)

        assert host._is_removed
        assert host.is_up is False
        session.on_remove.assert_called_once_with(host)
        listener.on_remove.assert_called_once_with(host)
        cluster.control_connection.on_remove.assert_called_once_with(host)

    def test_async_add_base_exception_transitions_to_recovery(self):
        class PoolSetupCancelled(BaseException):
            pass

        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        cluster.sessions = [session]

        cluster.on_add(host)
        assert host._currently_handling_node_add

        cancellation = PoolSetupCancelled("cancelled")
        pool_future.set_exception(cancellation)

        assert pool_future.exception() is cancellation
        assert host.is_up is False
        assert not host._currently_handling_node_add
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host,
            True,
            recovery_epoch=host._recovery_epoch)

    def test_async_add_false_result_transitions_to_recovery(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        cluster.sessions = [session]

        cluster.on_add(host)
        assert host._currently_handling_node_add

        pool_future.set_result(False)

        assert host.is_up is False
        assert not host._currently_handling_node_add
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host,
            True,
            recovery_epoch=host._recovery_epoch)

    def test_stale_pool_result_is_neutral_for_add(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host._recovery_epoch = 3
        host._currently_handling_node_add = True

        cluster._run_host_transition(
            host,
            lambda: cluster._finish_add(
                host,
                3,
                [True, _STALE_POOL_ATTEMPT]))

        assert host.is_up is True
        cluster._start_reconnector.assert_not_called()
        cluster.profile_manager.on_down.assert_not_called()

    def test_stale_pool_result_is_neutral_for_up(self):
        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_down()
        host._recovery_epoch = 3
        host._currently_handling_node_up = True

        cluster._run_host_transition(
            host,
            lambda: cluster._finish_on_up(
                host,
                3,
                [True, _STALE_POOL_ATTEMPT]))

        assert host.is_up is True
        cluster._start_reconnector.assert_not_called()
        cluster.profile_manager.on_down.assert_not_called()

    def test_async_up_base_exception_restores_reconnector(self):
        class PoolSetupCancelled(BaseException):
            pass

        cluster = self._new_transition_cluster()
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_down()
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        cluster.sessions = [session]

        cluster.on_up(host)
        recovery_epoch = host._recovery_epoch
        assert host._currently_handling_node_up

        cancellation = PoolSetupCancelled("cancelled")
        pool_future.set_exception(cancellation)

        assert pool_future.exception() is cancellation
        assert host.is_up is False
        assert not host._currently_handling_node_up
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        assert session.remove_pool.call_count == 2
        cluster._start_reconnector.assert_called_once_with(
            host,
            is_host_addition=False,
            recovery_epoch=recovery_epoch)

    def test_connection_failure_preserves_legacy_on_down_override(self):
        class LegacyCluster(Cluster):

            def on_down(self, host, is_host_addition,
                        expect_host_to_be_down=False):
                self.down_call = (
                    host, is_host_addition, expect_host_to_be_down)

        host = Mock()
        host.signal_connection_failure.return_value = True
        cluster = object.__new__(LegacyCluster)

        cluster.signal_connection_failure(
            host,
            ConnectionException("boom"),
            is_host_addition=False,
            expect_host_to_be_down=True)

        assert cluster.down_call == (host, False, True)

    def test_prepare_all_queries_bounds_keyspace_setup(self):
        endpoint = Mock(address='127.0.0.1')
        host = Mock(endpoint=endpoint)
        connection = Mock(is_closed=False)
        statement = Mock(keyspace='ks', query_string='SELECT * FROM tbl')
        cluster = Cluster(protocol_version=ProtocolVersion.V4)
        cluster._prepared_statements = {b'query-id': statement}

        with patch.object(
                cluster, 'connection_factory', return_value=connection), \
                patch.object(cluster, '_send_chunks'):
            cluster._prepare_all_queries(host)

        connection.set_keyspace_blocking.assert_called_once_with(
            'ks', timeout=cluster.connect_timeout)


class SessionPoolLifecycleTest(unittest.TestCase):

    @staticmethod
    def _new_session():
        cluster = Mock()
        cluster._lock = RLock()
        cluster.is_shutdown = False
        cluster.allow_control_connection_query_fallback = \
            ControlConnectionQueryFallback.Disabled
        cluster.connect_timeout = 1
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL

        session = object.__new__(Session)
        session.cluster = cluster
        session.keyspace = None
        session.is_shutdown = False
        session._lock = RLock()
        session._keyspace_dispatch_lock = RLock()
        session._keyspace_completion_lock = Lock()
        session._keyspace_completion_queue = []
        session._keyspace_completion_runner_active = False
        session._keyspace_generation = 0
        session._pools = {}
        session._pool_generations = {}
        session._profile_manager = cluster.profile_manager
        session._initial_connect_futures = set()
        session._monitor_reporter = None
        session._pool_repair_schedule = None
        session._pool_repair_scheduled = False
        return session

    @staticmethod
    def _new_host():
        host = Host(
            "127.0.0.1",
            SimpleConvictionPolicy,
            host_id=uuid.uuid4())
        host.set_up()
        return host

    @staticmethod
    def _capture_submissions(session):
        tasks = []

        def submit(fn, *args, **kwargs):
            tasks.append(lambda: fn(*args, **kwargs))
            return Future()

        session.submit = submit
        return tasks

    def test_initial_pool_clean_close_forces_host_recovery(self):
        session = self._new_session()
        host = self._new_host()
        tasks = self._capture_submissions(session)
        startup_error = _ConnectionClosedDuringStartup(
            "closed during startup", host.endpoint)

        with patch(
                'cassandra.cluster.HostConnection',
                side_effect=startup_error):
            session.add_or_renew_pool(host, is_host_addition=False)
            task_result = tasks.pop()()
            assert not task_result

        session.cluster.signal_connection_failure.assert_called_once_with(
            host,
            startup_error,
            False,
            expect_host_to_be_down=True,
            force=True)

    def test_clean_startup_close_recovers_when_conviction_policy_raises(self):
        class PolicyCancelled(BaseException):
            pass

        class ClusterStandin(object):

            def __init__(self):
                self._lock = RLock()
                self.is_shutdown = False
                self.allow_control_connection_query_fallback = (
                    ControlConnectionQueryFallback.Disabled)
                self.connect_timeout = 1
                self.profile_manager = Mock()
                self.profile_manager.distance.return_value = (
                    HostDistance.LOCAL)
                self.down_calls = []

            def _uses_default_failure_hooks(self):
                return True

            def _on_down_locked(
                    self, host, is_host_addition,
                    expect_host_to_be_down=False, force=False):
                self.down_calls.append((
                    host,
                    is_host_addition,
                    expect_host_to_be_down,
                    force))

        session = self._new_session()
        session.cluster = ClusterStandin()
        session._profile_manager = session.cluster.profile_manager
        host = self._new_host()
        host.signal_connection_failure = Mock(
            side_effect=PolicyCancelled("cancelled policy"))
        tasks = self._capture_submissions(session)
        startup_error = _ConnectionClosedDuringStartup(
            "closed during startup", host.endpoint)

        with patch(
                'cassandra.cluster.HostConnection',
                side_effect=startup_error):
            session.add_or_renew_pool(host, is_host_addition=False)
            task_result = tasks.pop()()
            assert not task_result

        assert session.cluster.down_calls == [
            (host, False, True, True),
        ]

    def test_clean_startup_delegated_force_is_not_committed_twice(self):
        calls = []

        class DelegatingCluster(object):
            def __init__(self):
                self._lock = RLock()
                self.is_shutdown = False
                self.allow_control_connection_query_fallback = (
                    ControlConnectionQueryFallback.Disabled)
                self.connect_timeout = 1
                self.profile_manager = Mock()
                self.profile_manager.distance.return_value = (
                    HostDistance.LOCAL)

            def _uses_default_failure_hooks(self):
                return False

            def signal_connection_failure(
                    self, host, exc, is_host_addition,
                    expect_host_to_be_down=False, force=False):
                calls.append(("public", force))
                self._on_down_locked(
                    host,
                    is_host_addition,
                    expect_host_to_be_down,
                    force)
                return False

            def _on_down_locked(
                    self, host, is_host_addition,
                    expect_host_to_be_down=False, force=False):
                calls.append(("private", force))
                host._recovery_epoch += 1

        session = self._new_session()
        session.cluster = DelegatingCluster()
        session._profile_manager = session.cluster.profile_manager
        host = self._new_host()
        starting_epoch = host._recovery_epoch
        tasks = self._capture_submissions(session)
        startup_error = _ConnectionClosedDuringStartup(
            "closed during startup", host.endpoint)

        with patch(
                'cassandra.cluster.HostConnection',
                side_effect=startup_error):
            session.add_or_renew_pool(host, is_host_addition=False)
            task_result = tasks.pop()()
            assert not task_result

        assert calls == [
            ("public", True),
            ("private", True),
        ]
        assert host._recovery_epoch == starting_epoch + 1

    def test_pool_failure_preserves_legacy_cluster_on_down_override(self):
        class LegacyCluster(Cluster):

            def on_down(
                    self, host, is_host_addition,
                    expect_host_to_be_down=False):
                self.down_calls.append((
                    host,
                    is_host_addition,
                    expect_host_to_be_down))

        session = self._new_session()
        session.cluster = object.__new__(LegacyCluster)
        session.cluster._lock = RLock()
        session.cluster.is_shutdown = False
        session.cluster.allow_control_connection_query_fallback = (
            ControlConnectionQueryFallback.Disabled)
        session.cluster.connect_timeout = 1
        session.cluster.profile_manager = Mock()
        session.cluster.profile_manager.distance.return_value = (
            HostDistance.LOCAL)
        session.cluster.down_calls = []
        session.cluster.sessions = [session]
        session.cluster._discount_down_events = False
        session.cluster.on_down_potentially_blocking = Mock()
        session._profile_manager = session.cluster.profile_manager
        host = self._new_host()
        tasks = self._capture_submissions(session)
        startup_error = _ConnectionClosedDuringStartup(
            "closed during startup", host.endpoint)

        with patch(
                'cassandra.cluster.HostConnection',
                side_effect=startup_error):
            session.add_or_renew_pool(host, is_host_addition=False)
            task_result = tasks.pop()()
            assert not task_result

        assert session.cluster.down_calls == [
            (host, False, True),
        ]
        assert host.is_up is False
        session.cluster.on_down_potentially_blocking.assert_called_once_with(
            host,
            False,
            host._recovery_epoch)

    def test_stale_startup_close_does_not_mark_host_down(self):
        session = self._new_session()
        host = self._new_host()
        tasks = self._capture_submissions(session)
        startup_error = _ConnectionClosedDuringStartup(
            "closed during startup", host.endpoint)

        with patch(
                'cassandra.cluster.HostConnection',
                side_effect=startup_error):
            session.add_or_renew_pool(host, is_host_addition=False)
            session.remove_pool(host)
            task_result = tasks.pop()()
            assert not task_result

        session.cluster.signal_connection_failure.assert_not_called()

    def test_invalid_initial_keyspace_does_not_mark_host_down(self):
        session = self._new_session()
        host = self._new_host()
        tasks = self._capture_submissions(session)
        keyspace_error = InvalidRequest("keyspace does not exist")

        with patch(
                'cassandra.cluster.HostConnection',
                side_effect=keyspace_error):
            session.add_or_renew_pool(host, is_host_addition=False)
            task_result = tasks.pop()()
            assert not task_result

        session.cluster.signal_connection_failure.assert_not_called()

    def test_invalid_changed_keyspace_does_not_mark_host_down(self):
        session = self._new_session()
        session.keyspace = 'new_ks'
        host = self._new_host()
        tasks = self._capture_submissions(session)
        keyspace_error = InvalidRequest("keyspace does not exist")
        new_pool = Mock(_keyspace='old_ks')
        new_pool._set_keyspace_for_all_conns.side_effect = \
            lambda keyspace, callback: callback(
                new_pool, [keyspace_error])

        with patch(
                'cassandra.cluster.HostConnection',
                return_value=new_pool):
            session.add_or_renew_pool(host, is_host_addition=False)
            task_result = tasks.pop()()
            assert not task_result

        session.cluster.on_down.assert_not_called()
        new_pool.shutdown.assert_called_once_with()

    def test_session_local_pool_failure_retries_until_success(self):
        session = self._new_session()
        host = self._new_host()
        session.cluster.metadata.all_hosts.return_value = [host]
        session.cluster.reconnection_policy.new_schedule.return_value = iter(
            [0.1, 0.2])

        first_failure = Future()
        first_failure.set_result(_SESSION_LOCAL_POOL_FAILURE)
        second_failure = Future()
        second_failure.set_result(_SESSION_LOCAL_POOL_FAILURE)
        success = Future()
        session.add_or_renew_pool = Mock(
            side_effect=[first_failure, second_failure, success])

        assert session.update_created_pools() == {first_failure}
        first_retry = session.cluster.scheduler.schedule.call_args_list[0]
        assert first_retry.args[0] == 0.1
        first_retry.args[1]()

        assert session._pool_repair_scheduled
        second_retry = session.cluster.scheduler.schedule.call_args_list[1]
        assert second_retry.args[0] == 0.2
        second_retry.args[1]()

        assert not session._pool_repair_scheduled
        assert session._pool_repair_schedule is not None
        success.set_result(True)

        assert session.add_or_renew_pool.call_count == 3
        assert session.cluster.scheduler.schedule.call_count == 2
        assert not session._pool_repair_scheduled
        assert session._pool_repair_schedule is None

    def test_post_connect_cancellation_closes_unpublished_pool(self):
        class SetupCancelled(BaseException):
            pass

        session = self._new_session()
        session.keyspace = 'new_ks'
        host = self._new_host()
        tasks = self._capture_submissions(session)
        new_pool = Mock(_keyspace='old_ks')
        new_pool._set_keyspace_for_all_conns.side_effect = SetupCancelled(
            "cancelled keyspace reconciliation")

        with patch(
                'cassandra.cluster.HostConnection',
                return_value=new_pool):
            session.add_or_renew_pool(host, is_host_addition=False)
            with pytest.raises(SetupCancelled):
                tasks.pop()()

        assert host not in session._pools
        new_pool.shutdown.assert_called_once_with()

    def test_pool_finishing_after_shutdown_is_closed_not_published(self):
        session = self._new_session()
        host = self._new_host()
        tasks = self._capture_submissions(session)
        new_pool = Mock(_keyspace=None)

        with patch(
                'cassandra.cluster.HostConnection',
                return_value=new_pool):
            session.add_or_renew_pool(host, is_host_addition=False)
            session.shutdown()
            task_result = tasks.pop()()
            assert task_result is _STALE_POOL_ATTEMPT

        assert host not in session._pools
        new_pool.shutdown.assert_called_once_with()

    def test_pool_finishing_after_host_down_is_closed_not_published(self):
        session = self._new_session()
        host = self._new_host()
        tasks = self._capture_submissions(session)
        new_pool = Mock(_keyspace=None)

        with patch(
                'cassandra.cluster.HostConnection',
                return_value=new_pool):
            session.add_or_renew_pool(host, is_host_addition=False)
            host.set_down()
            session.on_down(host)
            task_result = tasks.pop()()
            assert task_result is _STALE_POOL_ATTEMPT

        assert host not in session._pools
        new_pool.shutdown.assert_called_once_with()

    def test_unregistered_session_cannot_publish_pool_after_host_down(self):
        session = self._new_session()
        host = self._new_host()
        tasks = self._capture_submissions(session)
        new_pool = Mock(_keyspace=None)

        with patch(
                'cassandra.cluster.HostConnection',
                return_value=new_pool):
            session.add_or_renew_pool(host, is_host_addition=False)
            # Session.__init__ is not yet registered in Cluster.sessions, so a
            # concurrent DOWN cannot invalidate this attempt through
            # Session.on_down. Host state must fence publication itself.
            host.set_down()
            task_result = tasks.pop()()
            assert task_result is _STALE_POOL_ATTEMPT

        assert host not in session._pools
        new_pool.shutdown.assert_called_once_with()

    def test_concurrent_renewals_publish_only_one_pool(self):
        session = self._new_session()
        host = self._new_host()
        old_pool = Mock()
        first_pool = Mock(_keyspace=None)
        second_pool = Mock(_keyspace=None)
        session._pools[host] = old_pool
        tasks = self._capture_submissions(session)

        with patch(
                'cassandra.cluster.HostConnection',
                side_effect=[first_pool, second_pool]):
            session.add_or_renew_pool(host, is_host_addition=False)
            session.add_or_renew_pool(host, is_host_addition=False)
            task_result = tasks.pop(0)()
            assert task_result is True
            task_result = tasks.pop(0)()
            assert task_result is _STALE_POOL_ATTEMPT

        assert session._pools[host] is first_pool
        old_pool.shutdown.assert_called_once_with()
        first_pool.shutdown.assert_not_called()
        second_pool.shutdown.assert_called_once_with()

    def test_remove_pool_closes_synchronously_if_submit_loses_shutdown_race(self):
        session = self._new_session()
        host = self._new_host()
        pool = Mock()
        session._pools[host] = pool
        session.submit = Mock(return_value=None)

        assert session.remove_pool(host) is None

        assert host not in session._pools
        pool.shutdown.assert_called_once_with()

    def test_remove_pool_cancellation_retains_shutdown_ownership(self):
        session = self._new_session()
        host = self._new_host()
        pool = Mock()
        session._pools[host] = pool
        shutdown_future = Future()
        session.submit = Mock(return_value=shutdown_future)

        assert session.remove_pool(host) is shutdown_future
        assert shutdown_future.cancel()

        assert host not in session._pools
        pool.shutdown.assert_called_once_with()

    def test_shutdown_atomically_drains_pool_mapping(self):
        session = self._new_session()
        host = self._new_host()
        pool = Mock()
        session._pools[host] = pool

        session.shutdown()

        assert session._pools == {}
        pool.shutdown.assert_called_once_with()

    def test_keyspace_completion_uses_snapshot_and_is_exactly_once(self):
        session = self._new_session()
        first_pool = Mock(host='host1')
        second_pool = Mock(host='host2')
        callbacks = {}
        first_pool._set_keyspace_for_all_conns.side_effect = \
            lambda keyspace, callback: callbacks.setdefault(
                first_pool, callback)
        second_pool._set_keyspace_for_all_conns.side_effect = \
            lambda keyspace, callback: callbacks.setdefault(
                second_pool, callback)
        session._pools = {
            first_pool.host: first_pool,
            second_pool.host: second_pool,
        }
        completed = Mock()

        session._set_keyspace_for_all_pools('ks', completed)
        session._pools = {}
        callbacks[first_pool](first_pool, [])
        callbacks[first_pool](first_pool, [])
        callbacks[second_pool](second_pool, [])

        completed.assert_called_once_with({})

    def test_keyspace_dispatch_base_exception_completes_generation(self):
        class PoolUpdateCancelled(BaseException):
            pass

        session = self._new_session()
        pool = Mock(host='host1')
        cancellation = PoolUpdateCancelled("cancelled")
        pool._set_keyspace_for_all_conns.side_effect = cancellation
        session._pools = {pool.host: pool}
        completed = Mock()

        session._set_keyspace_for_all_pools('ks', completed)

        completed.assert_called_once_with({
            pool.host: [cancellation],
        })
        assert session._keyspace_completion_queue == []

    def test_keyspace_completion_callbacks_preserve_dispatch_order(self):
        session = self._new_session()
        pool = Mock(host='host1')
        pool_callbacks = []
        pool._set_keyspace_for_all_conns.side_effect = (
            lambda keyspace, callback: pool_callbacks.append(callback))
        session._pools = {pool.host: pool}
        completions = []

        session._set_keyspace_for_all_pools(
            'first',
            lambda errors: completions.append(('first', errors)))
        session._pools = {}
        session._set_keyspace_for_all_pools(
            'second',
            lambda errors: completions.append(('second', errors)))

        # The second generation completed synchronously on an empty snapshot,
        # but it must remain behind the first generation.
        assert completions == []
        pool_callbacks[0](pool, [])

        assert completions == [
            ('first', {}),
            ('second', {}),
        ]


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
    class FakeTime(object):

        def __init__(self):
            self.clock = 0

        def time(self):
            return self.clock

        def sleep(self, amount):
            self.clock += amount

    class MockPool(object):

        def __init__(self, host, connection):
            self.host = host
            self.host_distance = HostDistance.LOCAL
            self.is_shutdown = False
            self.connection = connection

        def _get_connection_for_routing_key(self):
            return self.connection

    class MockSchemaVersionFuture(object):

        def __init__(self, outcome, auto_complete=True):
            self._outcome = outcome
            self._auto_complete = auto_complete
            self._delivered = False
            self._callback_state = None
            self._col_names = ("schema_version",)
            self._col_types = None
            self.has_more_pages = False
            self._continuous_paging_session = None

        def _deliver(self):
            if self._delivered or self._callback_state is None:
                return

            self._delivered = True
            callback, errback, callback_args, callback_kwargs, errback_args, errback_kwargs = self._callback_state
            if isinstance(self._outcome, Exception):
                errback(self._outcome, *errback_args, **errback_kwargs)
            else:
                row = SimpleNamespace(schema_version=self._outcome)
                callback([row], *callback_args, **callback_kwargs)

        def add_callbacks(self, callback, errback,
                          callback_args=(), callback_kwargs=None,
                          errback_args=(), errback_kwargs=None):
            self._callback_state = (
                callback,
                errback,
                callback_args,
                callback_kwargs or {},
                errback_args,
                errback_kwargs or {},
            )
            if self._auto_complete:
                self._deliver()
            return self

        def complete(self):
            self._deliver()

        def result(self):
            if isinstance(self._outcome, Exception):
                raise self._outcome
            return ResultSet(self, [SimpleNamespace(schema_version=self._outcome)])

    def setUp(self):
        if connection_class is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        connection_class.initialize_reactor()

    def _mock_schema_future(self, outcome):
        return self.MockSchemaVersionFuture(outcome)

    def _host_query_count(self, session, target_host):
        return sum(1 for call in session.execute_async.call_args_list if call.kwargs.get('host') is target_host)

    def _new_schema_agreement_session(self, schema_versions, distances=None):
        hosts = []
        connections = {}
        distance_map = {}
        if distances is None:
            distances = [HostDistance.LOCAL] * len(schema_versions)

        for index, schema_version in enumerate(schema_versions):
            host = Host("127.0.0.%d" % (index + 1), SimpleConvictionPolicy, host_id=uuid.uuid4())
            host.set_up()
            hosts.append(host)
            distance_map[host] = distances[index]

        cluster = Cluster(protocol_version=4)
        for host in hosts:
            cluster.metadata.add_or_return_host(host)

        session = Session(cluster, hosts)
        session._profile_manager.distance = Mock(side_effect=lambda host: distance_map.get(host, HostDistance.LOCAL))
        session._pools = {}
        for host, schema_version in zip(hosts, schema_versions):
            connection = Mock(endpoint=host.endpoint)
            connection.future_outcomes = [schema_version]
            session._pools[host] = self.MockPool(host, connection)
            connections[host] = connection

        def execute_async(query, parameters=None, trace=False,
                            custom_payload=None, execution_profile=None,
                            paging_state=None, timeout=None, host=None, execute_as=None):
            connection = connections[host]
            outcome = connection.future_outcomes.pop(0) if len(connection.future_outcomes) > 1 else connection.future_outcomes[0]
            return self._mock_schema_future(outcome)

        session.execute_async = Mock(side_effect=execute_async)

        return session, hosts, connections

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

    @mock_session_pools
    def test_wait_for_schema_agreement_default_scope_queries_all_connected_hosts(self, *_):
        session, hosts, _ = self._new_schema_agreement_session(
            ["a", "a"],
            distances=[HostDistance.LOCAL_RACK, HostDistance.REMOTE])

        assert session.wait_for_schema_agreement(wait_time=1)

        for host in hosts:
            assert self._host_query_count(session, host) == 1

    @mock_session_pools
    def test_wait_for_schema_agreement_retries_until_local_hosts_match(self, *_):
        session, hosts, connections = self._new_schema_agreement_session(["a", "b"])
        clock = self.FakeTime()
        connections[hosts[1]].future_outcomes = ["b", "a"]

        with patch('cassandra.cluster.time', new=clock):
            assert session.wait_for_schema_agreement(wait_time=1)
        for host in hosts:
            assert self._host_query_count(session, host) == 2
        assert clock.clock == 0.2

    @mock_session_pools
    def test_wait_for_schema_agreement_retries_when_local_connection_is_busy(self, *_):
        session, hosts, connections = self._new_schema_agreement_session(["a", "a"])
        clock = self.FakeTime()
        connections[hosts[1]].future_outcomes = [
            ConnectionBusy("connection overloaded"),
            "a"]

        with patch('cassandra.cluster.time', new=clock):
            assert session.wait_for_schema_agreement(wait_time=1)
        for host in hosts:
            assert self._host_query_count(session, host) == 2
        assert clock.clock == 0.2

    @mock_session_pools
    def test_wait_for_schema_agreement_ignores_local_hosts_without_session_pool(self, *_):
        session, hosts, _ = self._new_schema_agreement_session(["a"])

        unconnected_host = Host("127.0.0.2", SimpleConvictionPolicy, host_id=uuid.uuid4())
        unconnected_host.set_up()
        session.cluster.metadata.add_or_return_host(unconnected_host)

        assert session.wait_for_schema_agreement(wait_time=1)
        assert self._host_query_count(session, hosts[0]) == 1

    @mock_session_pools
    def test_wait_for_schema_agreement_queries_hosts_in_order(self, *_):
        session, hosts, _ = self._new_schema_agreement_session(["a"] * 11)

        assert session.wait_for_schema_agreement(wait_time=1)
        assert [call.kwargs['host'] for call in session.execute_async.call_args_list] == list(hosts)

    @mock_session_pools
    def test_wait_for_schema_agreement_rack_scope_only_queries_local_rack_connections(self, *_):
        session, hosts, _ = self._new_schema_agreement_session(
            ["a", "a", "a"],
            distances=[HostDistance.LOCAL_RACK, HostDistance.LOCAL, HostDistance.REMOTE])

        assert session.wait_for_schema_agreement(wait_time=1, scope=SchemaAgreementScope.RACK)

        assert self._host_query_count(session, hosts[0]) == 1
        assert self._host_query_count(session, hosts[1]) == 0
        assert self._host_query_count(session, hosts[2]) == 0

    @mock_session_pools
    def test_wait_for_schema_agreement_cluster_scope_skips_ignored_hosts(self, *_):
        session, hosts, _ = self._new_schema_agreement_session(
            ["a", "a"],
            distances=[HostDistance.IGNORED, HostDistance.LOCAL])

        assert session.wait_for_schema_agreement(wait_time=1, scope=SchemaAgreementScope.CLUSTER)

        assert self._host_query_count(session, hosts[0]) == 0
        assert self._host_query_count(session, hosts[1]) == 1

    @mock_session_pools
    def test_wait_for_schema_agreement_cluster_scope_excludes_hosts_with_unknown_status(self, *_):
        session, hosts, _ = self._new_schema_agreement_session(
            ["a", "a"],
            distances=[HostDistance.LOCAL_RACK, HostDistance.LOCAL])

        hosts[0].is_up = None

        assert session.wait_for_schema_agreement(wait_time=1, scope=SchemaAgreementScope.CLUSTER)

        assert self._host_query_count(session, hosts[0]) == 0
        assert self._host_query_count(session, hosts[1]) == 1

    @mock_session_pools
    def test_wait_for_schema_agreement_rejects_unknown_scope(self, *_):
        session, _, _ = self._new_schema_agreement_session(["a"])

        with pytest.raises(ValueError):
            session.wait_for_schema_agreement(wait_time=1, scope='planet')

    @mock_session_pools
    def test_set_keyspace_for_all_pools_reports_all_errors(self, *_):
        cluster = Cluster()
        session = Session(
            cluster,
            [Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())],
        )

        pool1 = Mock(host='host1')
        pool2 = Mock(host='host2')
        keyspace_error = ConnectionException("boom")

        pool1._set_keyspace_for_all_conns.side_effect = (
            lambda keyspace, callback: callback(pool1, [keyspace_error])
        )
        pool2._set_keyspace_for_all_conns.side_effect = (
            lambda keyspace, callback: callback(pool2, [])
        )
        session._pools = {'host1': pool1, 'host2': pool2}

        callback = Mock()
        session._set_keyspace_for_all_pools('ks', callback)

        callback.assert_called_once()
        assert callback.call_args.args[0] == {'host1': [keyspace_error]}

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
