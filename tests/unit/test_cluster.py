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
from threading import Event, Lock, RLock, Thread
from unittest.mock import patch, Mock, ANY
import uuid

from cassandra import ConsistencyLevel, DriverException, Timeout, Unavailable, RequestExecutionException, ReadTimeout, WriteTimeout, CoordinationFailure, ReadFailure, WriteFailure, FunctionFailure, AlreadyExists,\
    InvalidRequest, Unauthorized, AuthenticationFailed, OperationTimedOut, UnsupportedOperation, RequestValidationException, ConfigurationException, ProtocolVersion
from cassandra.cluster import _Scheduler, Session, Cluster, default_lbp_factory, \
    ExecutionProfile, _ConfigMode, EXEC_PROFILE_DEFAULT
from cassandra.connection import ClientRoutesEndPoint, ConnectionException, DefaultEndPoint, SniEndPoint
from cassandra.metadata import Metadata
from cassandra.pool import Host, HostConnection, _HostReconnectionHandler
from cassandra.policies import HostDistance, RetryPolicy, RoundRobinPolicy, DowngradingConsistencyRetryPolicy, SimpleConvictionPolicy
from cassandra.query import SimpleStatement, named_tuple_factory, tuple_factory
from tests.unit.utils import mock_session_pools
from tests import connection_class
import pytest


log = logging.getLogger(__name__)


class _ImmediateExecutor(object):

    def submit(self, fn, *args, **kwargs):
        future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future


class _QueuedExecutor(object):

    def __init__(self):
        self.submissions = []

    def submit(self, fn, *args, **kwargs):
        future = Future()
        self.submissions.append((future, fn, args, kwargs))
        return future

    def run_next(self, index=0):
        future, fn, args, kwargs = self.submissions.pop(index)
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future


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

    def test_reconnector_connection_factory_recomputes_authenticator_after_endpoint_swap(self):
        old_endpoint = DefaultEndPoint('127.0.0.1')
        new_endpoint = DefaultEndPoint('127.0.0.2')
        auth_provider = Mock()
        auth_provider.new_authenticator.side_effect = lambda address: 'auth-%s' % (address,)

        with patch.object(Cluster.connection_class, 'factory', autospec=True, return_value='connection') as factory:
            cluster = Cluster(auth_provider=auth_provider)
            host = Host(old_endpoint, SimpleConvictionPolicy, host_id=uuid.uuid4())
            connection_factory = cluster._make_connection_factory(host)
            handler = _HostReconnectionHandler(
                host, connection_factory, False, Mock(), Mock(), Mock(), iter([0]),
                host.get_and_set_reconnection_handler, new_handler=None)

            host.endpoint = new_endpoint
            conn = handler.try_reconnect()

        assert conn == 'connection'
        assert factory.call_args.args[0] == new_endpoint
        assert factory.call_args.kwargs['authenticator'] == 'auth-127.0.0.2'


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

    @patch('cassandra.cluster._Scheduler.run')  # don't actually run the thread
    def test_schedule_unique_keeps_client_route_events_for_distinct_ports(self, *_):
        host_id = uuid.uuid4()
        old_endpoint = ClientRoutesEndPoint(
            host_id, Mock(), "127.0.0.1", original_port=9042)
        new_endpoint = ClientRoutesEndPoint(
            host_id, Mock(), "127.0.0.1", original_port=9142)
        assert old_endpoint == new_endpoint
        assert not Cluster._endpoints_match(old_endpoint, new_endpoint)
        host = Host(new_endpoint, SimpleConvictionPolicy, host_id=host_id)
        scheduled_fn = Mock()

        sched = _Scheduler(Mock())
        sched.schedule_unique(
            30, scheduled_fn, host, expected_endpoint=old_endpoint)
        sched.schedule_unique(
            30, scheduled_fn, host, expected_endpoint=new_endpoint)

        assert len(sched._scheduled_tasks) == 2


class SessionPoolRaceTest(unittest.TestCase):

    class _EndpointSwapIfPoolUnpublishedOnFirstExitLock(object):

        def __init__(self, host, new_endpoint, pool_is_published):
            self._lock = RLock()
            self._host = host
            self._new_endpoint = new_endpoint
            self._pool_is_published = pool_is_published
            self._exits = 0
            self.pool_was_published_on_first_exit = None

        def __enter__(self):
            self._lock.acquire()
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._lock.release()
            self._exits += 1
            if self._exits == 1:
                self.pool_was_published_on_first_exit = self._pool_is_published()
                if not self.pool_was_published_on_first_exit:
                    self._host.endpoint = self._new_endpoint

    class _DuplicatePoolEntries(object):

        def __init__(self, entries):
            self._entries = entries

        def __len__(self):
            return len(self._entries)

        def items(self):
            return list(self._entries)

    @staticmethod
    def _make_host(address):
        return Host(address, SimpleConvictionPolicy, host_id=uuid.uuid4())

    @staticmethod
    def _make_pool(host, distance, session, endpoint=None):
        pool = Mock()
        pool.host = host
        pool.endpoint = endpoint if endpoint is not None else host.endpoint
        pool.host_distance = distance
        pool._keyspace = session.keyspace
        pool.is_shutdown = False
        return pool

    @staticmethod
    def _make_cluster_and_session(hosts):
        executor = _QueuedExecutor()

        cluster = Cluster.__new__(Cluster)
        cluster.is_shutdown = False
        cluster.profile_manager = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        cluster.control_connection = Mock()
        cluster.metadata = Mock()
        cluster.metadata.all_hosts.return_value = hosts
        cluster._listeners = set()
        cluster._listener_lock = Lock()
        cluster.executor = executor
        cluster._prepare_all_queries = Mock()

        session = Session.__new__(Session)
        session.cluster = cluster
        session.is_shutdown = False
        session.keyspace = None
        session._lock = RLock()
        session._pools = {}
        session._profile_manager = cluster.profile_manager
        cluster.sessions = set([session])

        return cluster, session, executor

    def test_update_created_pools_reuses_in_flight_add_for_issue_317(self):
        first_host = self._make_host("127.0.0.1")
        second_host = self._make_host("127.0.0.2")
        cluster, session, executor = self._make_cluster_and_session(
            [first_host, second_host])
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            Cluster.on_add(cluster, first_host)
            Cluster.on_add(cluster, second_host)

            assert len(executor.submissions) == 2

            executor.run_next(1)

            assert second_host.is_up
            assert len(executor.submissions) == 1

            executor.run_next()

        assert len(created_pools) == 2
        assert session._pools[first_host].host is first_host
        assert session._pools[second_host].host is second_host
        for pool in created_pools:
            pool.shutdown.assert_not_called()

    def test_add_or_renew_pool_returns_in_flight_creation(self):
        host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session([host])

        with patch("cassandra.cluster.HostConnection",
                   side_effect=self._make_pool):
            first_future = session.add_or_renew_pool(host, is_host_addition=True)
            second_future = session.add_or_renew_pool(host, is_host_addition=False)

            assert second_future is first_future
            assert len(executor.submissions) == 1

            executor.run_next()

        assert session._pools[host].host is host

    def test_update_created_pools_keeps_in_flight_up_pool_for_down_host(self):
        host = self._make_host("127.0.0.1")
        host.set_down()
        cluster, session, executor = self._make_cluster_and_session([host])
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(
                host, is_host_addition=False)

            assert session.update_created_pools() == set([future])

            executor.run_next()

        assert future.result() is True
        assert session._pools[host].host is host
        created_pools[0].shutdown.assert_not_called()

    def test_update_created_pools_rechecks_distance_before_invalidating_pool_creation(self):
        host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session([host])
        created_pools = []
        distance_calls = []
        future_holder = {}

        def distance(changed_host):
            distance_calls.append(changed_host)
            if len(distance_calls) == 1:
                future_holder["future"] = session.add_or_renew_pool(
                    host, is_host_addition=False)
                return HostDistance.IGNORED
            return HostDistance.LOCAL

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        cluster.profile_manager.distance.side_effect = distance
        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            assert session.update_created_pools() == set([future_holder["future"]])

            executor.run_next()

        assert future_holder["future"].result() is True
        assert session._pools[host].host is host
        created_pools[0].shutdown.assert_not_called()

    def test_update_created_pools_rechecks_distance_before_reusing_pool_creation(self):
        host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session([host])
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(
                host, is_host_addition=False)
            cluster.profile_manager.distance.side_effect = [
                HostDistance.LOCAL, HostDistance.IGNORED]

            assert session.update_created_pools() == set()

            executor.run_next()

        assert future.result() is False
        assert session._pools == {}
        created_pools[0].shutdown.assert_called_once_with()

    def test_update_created_pools_invalidates_creation_after_endpoint_change(self):
        host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session([host])
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(
                host, is_host_addition=False)
            host.endpoint = DefaultEndPoint("127.0.0.2")

            assert session.update_created_pools() == set()

            executor.run_next()

        assert future.result() is False
        assert session._pools == {}
        created_pools[0].shutdown.assert_called_once_with()

    def test_add_or_renew_pool_invalidates_creation_after_endpoint_change(self):
        host = self._make_host("127.0.0.1")
        old_endpoint = host.endpoint
        new_endpoint = DefaultEndPoint("127.0.0.2")
        _, session, executor = self._make_cluster_and_session([host])
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            old_future = session.add_or_renew_pool(
                host, is_host_addition=False)
            host.endpoint = new_endpoint

            new_future = session.add_or_renew_pool(
                host, is_host_addition=False)

            assert new_future is not old_future
            assert len(executor.submissions) == 2

            executor.run_next()
            executor.run_next()

        assert old_future.result() is False
        assert new_future.result() is True
        assert created_pools[0].endpoint == old_endpoint
        assert created_pools[1].endpoint == new_endpoint
        created_pools[0].shutdown.assert_called_once_with()
        created_pools[1].shutdown.assert_not_called()
        assert session._pools[host] is created_pools[1]

    def test_on_up_does_not_publish_replacement_endpoint_pool_after_endpoint_swap(self):
        host = self._make_host("127.0.0.1")
        host.set_down()
        old_endpoint = host.endpoint
        new_endpoint = DefaultEndPoint("127.0.0.2")
        cluster, session, executor = self._make_cluster_and_session([host])
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        original_add_or_renew_pool = Session.add_or_renew_pool.__get__(
            session, Session)

        def add_after_endpoint_swap(host, *args, **kwargs):
            host.endpoint = new_endpoint
            return original_add_or_renew_pool(host, *args, **kwargs)

        session.add_or_renew_pool = Mock(side_effect=add_after_endpoint_swap)

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            Cluster.on_up(cluster, host)
            while executor.submissions:
                executor.run_next()

        assert created_pools == []
        assert session._pools == {}
        session.add_or_renew_pool.assert_called_once_with(
            host, is_host_addition=False,
            allow_retry_after_auth_failure=True,
            expected_endpoint=old_endpoint)

    def test_pool_creation_publishes_before_endpoint_lock_is_released(self):
        host = self._make_host("127.0.0.1")
        new_endpoint = DefaultEndPoint("127.0.0.2")
        cluster, session, executor = self._make_cluster_and_session([host])
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(
                host, is_host_addition=False)
            host.lock = self._EndpointSwapIfPoolUnpublishedOnFirstExitLock(
                host, new_endpoint, lambda: bool(session._pools))

            executor.run_next()

        assert host.lock.pool_was_published_on_first_exit is True
        assert future.result() is True
        assert session._pools[host] is created_pools[0]
        created_pools[0].shutdown.assert_not_called()

    def test_update_created_pools_removes_stale_pool_for_down_host_after_endpoint_change(self):
        host = self._make_host("127.0.0.1")
        host.set_down()
        cluster, session, executor = self._make_cluster_and_session([host])
        stale_pool = self._make_pool(host, HostDistance.LOCAL, session)
        session._pools[host] = stale_pool

        host.endpoint = DefaultEndPoint("127.0.0.2")

        futures = session.update_created_pools()

        assert len(futures) == 1
        executor.run_next()
        assert session._pools == {}
        stale_pool.shutdown.assert_called_once_with()

    def test_update_created_pools_replaces_pool_after_endpoint_change(self):
        host = self._make_host("127.0.0.1")
        old_endpoint = host.endpoint
        cluster, session, executor = self._make_cluster_and_session([host])
        stale_pool = self._make_pool(
            host, HostDistance.LOCAL, session, endpoint=old_endpoint)
        session._pools[host] = stale_pool
        created_pools = []

        host.endpoint = DefaultEndPoint("127.0.0.2")

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            futures = session.update_created_pools()

            assert len(futures) == 1
            executor.run_next()

        assert created_pools[0].endpoint == host.endpoint
        assert session._pools[host] is created_pools[0]
        stale_pool.shutdown.assert_called_once_with()

    def test_removed_in_flight_pool_is_not_published(self):
        host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session([host])
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(host, is_host_addition=True)
            session.remove_pool(host)

            executor.run_next()

        assert future.result() is False
        assert session._pools == {}
        created_pools[0].shutdown.assert_called_once_with()

    def test_stale_host_pool_creation_does_not_publish_to_replacement_host(self):
        host_id = uuid.uuid4()
        stale_host = Host(DefaultEndPoint("127.0.0.1"), SimpleConvictionPolicy,
                          host_id=host_id)
        replacement_host = Host(DefaultEndPoint("127.0.0.2"),
                                SimpleConvictionPolicy, host_id=host_id)
        cluster, session, executor = self._make_cluster_and_session(
            [replacement_host])
        cluster.metadata = Metadata()
        cluster.metadata.add_or_return_host(replacement_host)
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(
                stale_host, is_host_addition=False)

            executor.run_next()

        assert future.result() is False
        assert session._pools == {}
        created_pools[0].shutdown.assert_called_once_with()

    def test_remove_pool_expected_host_mismatch_invalidates_stale_creation(self):
        stale_host = self._make_host("127.0.0.1")
        replacement_host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session(
            [replacement_host])
        replacement_pool = self._make_pool(
            replacement_host, HostDistance.LOCAL, session)
        created_pools = []
        session._pools[replacement_host] = replacement_pool

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(
                stale_host, is_host_addition=False)

            assert session.remove_pool(
                stale_host, expected_host=stale_host) is None

            executor.run_next()

        assert future.result() is False
        assert session._pools[replacement_host] is replacement_pool
        replacement_pool.shutdown.assert_not_called()
        created_pools[0].shutdown.assert_called_once_with()

    def test_remove_pool_finds_pool_after_host_endpoint_changes(self):
        host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session([host])
        pool = self._make_pool(host, HostDistance.LOCAL, session)
        session._pools[host] = pool

        host.endpoint = DefaultEndPoint("127.0.0.2")

        future = session.remove_pool(host, expected_host=host)
        executor.run_next()

        assert session._pools == {}
        pool.shutdown.assert_called_once_with()
        assert future.done()

    def test_remove_pool_prefers_identity_after_endpoint_rewrite(self):
        stale_host = self._make_host("127.0.0.1")
        replacement_host = self._make_host("127.0.0.2")
        cluster, session, executor = self._make_cluster_and_session(
            [replacement_host])
        stale_pool = self._make_pool(stale_host, HostDistance.LOCAL, session)
        replacement_pool = self._make_pool(
            replacement_host, HostDistance.LOCAL, session)
        session._pools[stale_host] = stale_pool

        stale_host.endpoint = replacement_host.endpoint
        session._pools[replacement_host] = replacement_pool

        assert stale_host == replacement_host

        session.remove_pool(stale_host, expected_host=stale_host)
        executor.run_next()

        assert session._pools[replacement_host] is replacement_pool
        stale_pool.shutdown.assert_called_once_with()
        replacement_pool.shutdown.assert_not_called()

    def test_remove_pool_expected_endpoint_preserves_replacement_pool(self):
        host = self._make_host("127.0.0.1")
        old_endpoint = host.endpoint
        cluster, session, executor = self._make_cluster_and_session([host])
        stale_pool = self._make_pool(host, HostDistance.LOCAL, session)

        host.endpoint = DefaultEndPoint("127.0.0.2")
        replacement_pool = self._make_pool(host, HostDistance.LOCAL, session)
        session._pools = self._DuplicatePoolEntries([
            (host, stale_pool),
            (host, replacement_pool),
        ])

        assert len(session._pools) == 2

        session.remove_pool(
            host, expected_host=host, expected_endpoint=old_endpoint)
        executor.run_next()

        assert session._pools[host] is replacement_pool
        stale_pool.shutdown.assert_called_once_with()
        replacement_pool.shutdown.assert_not_called()

    def test_remove_pool_expected_endpoint_preserves_replacement_creation(self):
        host = self._make_host("127.0.0.1")
        old_endpoint = host.endpoint
        cluster, session, executor = self._make_cluster_and_session([host])
        created_pools = []

        host.endpoint = DefaultEndPoint("127.0.0.2")

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(
                host, is_host_addition=False)

            assert session.remove_pool(
                host, expected_host=host,
                expected_endpoint=old_endpoint) is None

            executor.run_next()

        assert future.result() is True
        assert session._pools[host] is created_pools[0]
        assert created_pools[0].endpoint == host.endpoint
        created_pools[0].shutdown.assert_not_called()

    def test_stale_host_connection_cleanup_after_endpoint_flip_back_preserves_current_pool(self):
        host = self._make_host("127.0.0.1")
        old_endpoint = host.endpoint
        replacement_endpoint = DefaultEndPoint("127.0.0.2")
        cluster, session, executor = self._make_cluster_and_session([host])

        host.endpoint = replacement_endpoint
        host.endpoint = old_endpoint
        current_pool = self._make_pool(
            host, HostDistance.LOCAL, session, endpoint=old_endpoint)
        session._pools[host] = current_pool

        stale_pool = HostConnection.__new__(HostConnection)
        stale_pool.host = host
        stale_pool.endpoint = old_endpoint
        stale_pool._session = session

        stale_pool._remove_stale_pool(old_endpoint)

        assert session._get_pool_by_host_identity(host) is current_pool
        current_pool.shutdown.assert_not_called()

    def test_add_or_renew_pool_tags_pool_with_creation_endpoint(self):
        host = self._make_host("127.0.0.1")
        old_endpoint = host.endpoint
        cluster, session, executor = self._make_cluster_and_session([host])
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            host.endpoint = DefaultEndPoint("127.0.0.2")
            pool = self._make_pool(host, distance, pool_session, endpoint)
            created_pools.append(pool)
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool) as host_connection:
            future = session.add_or_renew_pool(
                host, is_host_addition=False)

            executor.run_next()

        assert future.result() is False
        host_connection.assert_called_once_with(
            host, HostDistance.LOCAL, session, endpoint=old_endpoint)
        assert created_pools[0].endpoint == old_endpoint
        created_pools[0].shutdown.assert_called_once_with()

    def test_add_or_renew_pool_auth_failure_reports_creation_endpoint(self):
        host = self._make_host("127.0.0.1")
        old_endpoint = host.endpoint
        cluster, session, executor = self._make_cluster_and_session([host])
        cluster.signal_connection_failure = Mock()

        def fail_pool(host, distance, pool_session, endpoint=None):
            host.endpoint = DefaultEndPoint("127.0.0.2")
            raise AuthenticationFailed("failed")

        with patch("cassandra.cluster.HostConnection", side_effect=fail_pool):
            future = session.add_or_renew_pool(host, is_host_addition=False)
            executor.run_next()

        assert future.result() is False
        args, kwargs = cluster.signal_connection_failure.call_args
        conn_exc = args[1]
        assert isinstance(conn_exc, ConnectionException)
        assert conn_exc.endpoint == old_endpoint
        assert isinstance(conn_exc.__cause__, AuthenticationFailed)
        assert args == (host, conn_exc, False)
        assert kwargs == {"expected_endpoint": old_endpoint}

    def test_add_or_renew_pool_failure_reports_creation_endpoint(self):
        host = self._make_host("127.0.0.1")
        old_endpoint = host.endpoint
        cluster, session, executor = self._make_cluster_and_session([host])
        cluster.signal_connection_failure = Mock()
        pool_error = RuntimeError("failed")

        def fail_pool(host, distance, pool_session, endpoint=None):
            host.endpoint = DefaultEndPoint("127.0.0.2")
            raise pool_error

        with patch("cassandra.cluster.HostConnection", side_effect=fail_pool):
            future = session.add_or_renew_pool(host, is_host_addition=True)
            executor.run_next()

        assert future.result() is False
        args, kwargs = cluster.signal_connection_failure.call_args
        conn_exc = args[1]
        assert conn_exc is pool_error
        assert conn_exc.endpoint == old_endpoint
        assert args == (host, conn_exc, True)
        assert kwargs == {
            "expect_host_to_be_down": True,
            "expected_endpoint": old_endpoint
        }

    def test_removed_in_flight_pool_failure_does_not_signal_down(self):
        host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session([host])
        cluster.signal_connection_failure = Mock()

        with patch("cassandra.cluster.HostConnection",
                   side_effect=ConnectionException("failed")):
            future = session.add_or_renew_pool(host, is_host_addition=True)
            session.remove_pool(host)

            executor.run_next()

        assert future.result() is False
        cluster.signal_connection_failure.assert_not_called()

    def test_removed_in_flight_pool_auth_failure_does_not_signal_down(self):
        host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session([host])
        cluster.signal_connection_failure = Mock()

        with patch("cassandra.cluster.HostConnection",
                   side_effect=AuthenticationFailed("failed")):
            future = session.add_or_renew_pool(host, is_host_addition=True)
            session.remove_pool(host)

            executor.run_next()

        assert future.result() is False
        cluster.signal_connection_failure.assert_not_called()

    def test_stale_keyspace_failure_does_not_signal_down(self):
        host = self._make_host("127.0.0.1")
        cluster, session, executor = self._make_cluster_and_session([host])
        cluster.connect_timeout = 1
        cluster.on_down = Mock()
        session.keyspace = "ks"

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            pool._keyspace = None

            def set_keyspace(keyspace, callback):
                session.remove_pool(host)
                callback(pool, [Exception("failed")])

            pool._set_keyspace_for_all_conns.side_effect = set_keyspace
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(host, is_host_addition=True)

            executor.run_next()

        assert future.result() is False
        assert session._pools == {}
        cluster.on_down.assert_not_called()

    def test_keyspace_failure_signals_down_for_creation_endpoint(self):
        host = self._make_host("127.0.0.1")
        old_endpoint = host.endpoint
        cluster, session, executor = self._make_cluster_and_session([host])
        cluster.connect_timeout = 1
        cluster.on_down = Mock()
        session.keyspace = "ks"
        created_pools = []

        def make_pool(host, distance, pool_session, endpoint=None):
            pool = self._make_pool(host, distance, pool_session, endpoint)
            pool._keyspace = None
            created_pools.append(pool)

            def set_keyspace(keyspace, callback):
                host.endpoint = DefaultEndPoint("127.0.0.2")
                callback(pool, [Exception("failed")])

            pool._set_keyspace_for_all_conns.side_effect = set_keyspace
            return pool

        with patch("cassandra.cluster.HostConnection", side_effect=make_pool):
            future = session.add_or_renew_pool(host, is_host_addition=True)

            executor.run_next()

        assert future.result() is False
        assert session._pools == {}
        cluster.on_down.assert_called_once_with(
            host, True, expected_endpoint=old_endpoint)
        created_pools[0].shutdown.assert_called_once_with()


class HostStateRaceTest(unittest.TestCase):

    class _EndpointSwapOnFirstExitLock(object):

        def __init__(self, host, new_endpoint):
            self._lock = RLock()
            self._host = host
            self._new_endpoint = new_endpoint
            self._exits = 0

        def __enter__(self):
            self._lock.acquire()
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self._lock.release()
            self._exits += 1
            if self._exits == 1:
                self._host.endpoint = self._new_endpoint


    @staticmethod
    def _make_host():
        return Host("127.0.0.1", SimpleConvictionPolicy, host_id=uuid.uuid4())

    @staticmethod
    def _make_cluster(session=None, listener=None):
        cluster = Cluster.__new__(Cluster)
        cluster.is_shutdown = False
        cluster.profile_manager = Mock()
        cluster.control_connection = Mock()
        cluster.sessions = set([session] if session else [])
        cluster._listeners = set([listener] if listener else [])
        cluster._listener_lock = Lock()
        cluster.executor = _ImmediateExecutor()
        cluster._start_reconnector = Mock()
        cluster._discount_down_events = False
        return cluster

    @staticmethod
    def _make_session_with_pool(host, pool):
        session = Session.__new__(Session)
        session._lock = Lock()
        session._pools = {host: pool}
        session.submit = _ImmediateExecutor().submit
        return session

    def test_discount_down_event_does_not_hold_host_lock_while_scanning_pools(self):
        host = self._make_host()
        host.set_up()
        old_endpoint = host.endpoint
        pool = Mock()
        pool.host = host
        pool.endpoint = old_endpoint
        pool.open_count = 1
        session = self._make_session_with_pool(host, pool)
        session._lock = RLock()
        cluster = self._make_cluster(session=session)
        cluster._discount_down_events = True
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        cluster.on_down_potentially_blocking = Mock(return_value=None)
        session.cluster = cluster

        # Mutating the endpoint after insertion forces an identity scan under
        # session._lock; the down path must not hold host.lock during that scan.
        host.endpoint = DefaultEndPoint("127.0.0.2")
        entered_distance = Event()
        release_distance = Event()
        blocked_on_host = []
        thread_errors = []

        def distance(_host):
            entered_distance.set()
            release_distance.wait(1)
            return HostDistance.LOCAL

        def hold_session_then_try_host():
            with session._lock:
                entered_distance.wait(1)
                acquired = host.lock.acquire(timeout=0.2)
                blocked_on_host.append(not acquired)
                if acquired:
                    host.lock.release()
                release_distance.set()

        def run_on_down():
            try:
                Cluster.on_down(cluster, host, is_host_addition=False)
            except Exception as exc:
                thread_errors.append(exc)

        cluster.profile_manager.distance.side_effect = distance
        worker = Thread(target=hold_session_then_try_host)
        runner = Thread(target=run_on_down)
        worker.start()
        runner.start()
        worker.join(2)
        runner.join(2)

        assert not thread_errors
        assert not runner.is_alive()
        assert blocked_on_host == [False]

    def test_signal_connection_failure_does_not_hold_host_lock_while_scanning_pools(self):
        host = self._make_host()
        host.set_up()
        old_endpoint = host.endpoint
        pool = Mock()
        pool.host = host
        pool.endpoint = old_endpoint
        pool.open_count = 1
        session = self._make_session_with_pool(host, pool)
        session._lock = RLock()
        cluster = self._make_cluster(session=session)
        cluster._discount_down_events = True
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        cluster.on_down_potentially_blocking = Mock(return_value=None)
        session.cluster = cluster

        # Mutating the endpoint after insertion forces an identity scan under
        # session._lock. signal_connection_failure() must not keep host.lock
        # held while it delegates to the down path that performs that scan.
        host.endpoint = DefaultEndPoint("127.0.0.2")
        entered_distance = Event()
        release_distance = Event()
        blocked_on_host = []
        thread_errors = []

        def distance(_host):
            entered_distance.set()
            release_distance.wait(1)
            return HostDistance.LOCAL

        def hold_session_then_try_host():
            with session._lock:
                entered_distance.wait(1)
                acquired = host.lock.acquire(timeout=0.2)
                blocked_on_host.append(not acquired)
                if acquired:
                    host.lock.release()
                release_distance.set()

        def run_signal_connection_failure():
            try:
                Cluster.signal_connection_failure(
                    cluster, host, ConnectionException("failed"),
                    is_host_addition=False)
            except Exception as exc:
                thread_errors.append(exc)

        cluster.profile_manager.distance.side_effect = distance
        worker = Thread(target=hold_session_then_try_host)
        runner = Thread(target=run_signal_connection_failure)
        worker.start()
        runner.start()
        worker.join(2)
        runner.join(2)

        assert not thread_errors
        assert not runner.is_alive()
        assert blocked_on_host == [False]

    def test_discount_down_event_applies_to_current_expected_endpoint(self):
        host = self._make_host()
        host.set_up()
        endpoint = host.endpoint
        pool = Mock()
        pool.host = host
        pool.endpoint = endpoint
        pool.open_count = 1
        session = self._make_session_with_pool(host, pool)
        cluster = self._make_cluster(session=session)
        cluster._discount_down_events = True
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        cluster.on_down_potentially_blocking = Mock(return_value=None)
        session.cluster = cluster

        Cluster.on_down(
            cluster, host, is_host_addition=False,
            expected_endpoint=endpoint)

        assert host.is_up
        cluster.on_down_potentially_blocking.assert_not_called()

    def test_forced_down_is_not_discounted_by_connected_pool(self):
        host = self._make_host()
        host.set_up()
        endpoint = host.endpoint
        pool = Mock()
        pool.host = host
        pool.endpoint = endpoint
        pool.open_count = 1
        session = self._make_session_with_pool(host, pool)
        cluster = self._make_cluster(session=session)
        cluster._discount_down_events = True
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        cluster.on_down_potentially_blocking = Mock(return_value=None)
        session.cluster = cluster

        Cluster.on_down(
            cluster, host, is_host_addition=False,
            expect_host_to_be_down=True, expected_endpoint=endpoint)

        assert not host.is_up
        cluster.on_down_potentially_blocking.assert_called_once_with(
            host, False, ANY, endpoint, False, False)

    @staticmethod
    def _state(cluster, host):
        return cluster._get_host_liveness_state(host)

    @classmethod
    def _reserve_down_handling(cls, cluster, host):
        with host.lock:
            state = cls._state(cluster, host)
            state.epoch += 1
            state.pending_up_epoch = None
            host.set_down()
            state.down_epoch = state.epoch
            return state.down_epoch

    def test_stale_down_handling_is_ignored_after_host_is_up(self):
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        host = self._make_host()
        host.set_up()
        down_epoch = self._reserve_down_handling(cluster, host)
        with host.lock:
            state = self._state(cluster, host)
            state.down_epoch = None
            state.epoch += 1
            host.set_up()

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False, down_epoch=down_epoch)

        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        session.on_down.assert_not_called()
        listener.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()

    def test_stale_generic_down_handling_uses_original_endpoint_after_endpoint_swap(self):
        executor = _QueuedExecutor()
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster.executor = executor
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_up()
        old_endpoint = host.endpoint
        new_endpoint = DefaultEndPoint("127.0.0.2")

        Cluster.on_down(cluster, host, is_host_addition=False)
        host.endpoint = new_endpoint

        executor.run_next()

        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        session.on_down.assert_called_once_with(
            host, expected_endpoint=old_endpoint)
        listener.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()
        assert self._state(cluster, host).down_epoch is None

    def test_unreserved_down_handling_is_ignored_during_host_up_handling(self):
        session = Mock()
        cluster = self._make_cluster(session=session)
        host = self._make_host()
        host.set_down()
        self._state(cluster, host).up_epoch = 0

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False)

        session.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()

    def test_reserved_down_handling_after_endpoint_swap_only_removes_stale_pool(self):
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        host = self._make_host()
        host.set_up()
        old_endpoint = host.endpoint
        down_epoch = self._reserve_down_handling(cluster, host)

        host.endpoint = DefaultEndPoint("127.0.0.2")

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False, down_epoch=down_epoch,
            expected_endpoint=old_endpoint)

        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        session.on_down.assert_called_once_with(
            host, expected_endpoint=old_endpoint)
        listener.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()
        assert self._state(cluster, host).down_epoch is None

    def test_reserved_down_handling_after_endpoint_swap_removes_stale_pool(self):
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        host = self._make_host()
        host_id = uuid.uuid4()
        old_endpoint = ClientRoutesEndPoint(
            host_id, Mock(), "127.0.0.1", original_port=9042)
        new_endpoint = ClientRoutesEndPoint(
            host_id, Mock(), "127.0.0.1", original_port=9142)
        assert old_endpoint == new_endpoint
        assert not Cluster._endpoints_match(old_endpoint, new_endpoint)
        host.endpoint = old_endpoint
        host.set_up()
        down_epoch = self._reserve_down_handling(cluster, host)
        host.lock = self._EndpointSwapOnFirstExitLock(host, new_endpoint)

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False, down_epoch=down_epoch)

        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        session.on_down.assert_called_once_with(
            host, expected_endpoint=old_endpoint)
        listener.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()
        assert self._state(cluster, host).down_epoch is None

    def test_expected_endpoint_down_listener_is_not_called_under_host_lock(self):
        session = Mock()
        cluster = self._make_cluster(session=session)
        host = self._make_host()
        host.set_up()
        expected_endpoint = host.endpoint
        down_epoch = self._reserve_down_handling(cluster, host)
        blocked_on_host = []

        class Listener(object):

            def on_down(self, _host):
                def try_host_lock():
                    acquired = host.lock.acquire(timeout=0.2)
                    blocked_on_host.append(not acquired)
                    if acquired:
                        host.lock.release()

                worker = Thread(target=try_host_lock)
                worker.start()
                worker.join(1)

        cluster._listeners = set([Listener()])

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False, down_epoch=down_epoch,
            expected_endpoint=expected_endpoint)

        self.assertEqual(blocked_on_host, [False])

    def test_endpoint_match_preserves_endpoint_specific_identity(self):
        proxy_endpoint = SniEndPoint("proxy.example.com", "node-a", port=9042)
        other_proxy_endpoint = SniEndPoint("proxy.example.com", "node-b", port=9042)
        assert not Cluster._endpoints_match(proxy_endpoint, other_proxy_endpoint)

        old_endpoint = ClientRoutesEndPoint(
            uuid.uuid4(), Mock(), "127.0.0.1", original_port=9042)
        new_endpoint = ClientRoutesEndPoint(
            uuid.uuid4(), Mock(), "127.0.0.1", original_port=9042)
        assert not Cluster._endpoints_match(old_endpoint, new_endpoint)

    def test_auth_failure_quarantine_preserves_endpoint_specific_identity(self):
        host = self._make_host()
        host.endpoint = ClientRoutesEndPoint(
            uuid.uuid4(), Mock(), "127.0.0.1", original_port=9042)

        Cluster._set_non_retryable_auth_failure(host, True)
        host.endpoint = ClientRoutesEndPoint(
            uuid.uuid4(), Mock(), "127.0.0.1", original_port=9042)

        assert not Cluster._has_non_retryable_auth_failure(host)

    def test_noop_down_during_up_handling_does_not_supersede_up(self):
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()

        Cluster.on_up(cluster, host)
        state = self._state(cluster, host)
        up_epoch = state.up_epoch

        Cluster.on_down(cluster, host, is_host_addition=False)

        assert state.epoch == up_epoch
        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        session.on_down.assert_not_called()
        listener.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()

        pool_future.set_result(True)

        listener.on_up.assert_called_once_with(host)
        assert host.is_up
        assert state.up_epoch is None

    def test_on_up_waits_for_all_pool_futures_when_one_is_already_done(self):
        completed_pool_future = Future()
        completed_pool_future.set_result(True)
        pending_pool_future = Future()
        completed_session = Mock()
        completed_session.add_or_renew_pool.return_value = completed_pool_future
        pending_session = Mock()
        pending_session.add_or_renew_pool.return_value = pending_pool_future
        listener = Mock()
        cluster = self._make_cluster(listener=listener)
        cluster.sessions = [completed_session, pending_session]
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()

        Cluster.on_up(cluster, host)

        assert not host.is_up
        listener.on_up.assert_not_called()

        pending_pool_future.set_result(False)

        assert not host.is_up
        listener.on_up.assert_not_called()
        cluster._start_reconnector.assert_called_once_with(
            host, is_host_addition=False, expected_endpoint=host.endpoint)

    def test_newer_forced_down_during_up_handling_is_preserved(self):
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()

        Cluster.on_up(cluster, host)
        state = self._state(cluster, host)
        first_up_epoch = state.up_epoch
        assert session.remove_pool.call_count == 1

        Cluster.on_down(
            cluster, host, is_host_addition=False, expect_host_to_be_down=True)

        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(
            host, expected_endpoint=host.endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)
        assert state.epoch > first_up_epoch
        assert state.up_epoch == first_up_epoch
        assert not host.is_up

        pool_future.set_result(True)

        listener.on_up.assert_not_called()
        assert session.remove_pool.call_count == 1
        assert not host.is_up
        assert state.up_epoch is None

    def test_stale_failed_up_callback_does_not_cleanup_newer_down(self):
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()

        Cluster.on_up(cluster, host)
        Cluster.on_down(
            cluster, host, is_host_addition=False, expect_host_to_be_down=True)

        pool_future.set_exception(RuntimeError("pool failed after newer down"))

        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(
            host, expected_endpoint=host.endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)
        listener.on_up.assert_not_called()
        assert not host.is_up
        assert self._state(cluster, host).up_epoch is None

    def test_failed_up_callback_rechecks_before_cleanup(self):
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()

        Cluster.on_up(cluster, host)
        state = self._state(cluster, host)

        def force_down_before_cleanup(message, *args, **kwargs):
            if message.startswith("Connection pool could not be created"):
                Cluster.on_down(
                    cluster, host, is_host_addition=False,
                    expect_host_to_be_down=True)

        with patch("cassandra.cluster.log.debug", side_effect=force_down_before_cleanup):
            pool_future.set_result(False)

        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(
            host, expected_endpoint=host.endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)
        assert session.remove_pool.call_count == 1
        listener.on_up.assert_not_called()
        assert not host.is_up
        assert state.up_epoch is None
        assert state.down_epoch is None

    def test_failed_up_callback_after_endpoint_swap_does_not_signal_down(self):
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        cluster = self._make_cluster(session=session)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()

        Cluster.on_up(cluster, host)
        state = self._state(cluster, host)
        old_endpoint = host.endpoint

        host.endpoint = DefaultEndPoint("127.0.0.2")
        pool_future.set_exception(RuntimeError("pool failed after endpoint swap"))

        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()
        assert session.remove_pool.call_count == 2
        session.remove_pool.assert_any_call(
            host, expected_host=host, expected_endpoint=old_endpoint)
        assert not host.is_up
        assert state.up_epoch is None

    def test_forced_down_during_up_handling_is_not_hidden_by_reconnector(self):
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        host = self._make_host()
        host.set_down()
        old_reconnector = Mock()
        host._reconnection_handler = old_reconnector
        original_get_reconnector = Cluster._get_reconnector_for_current_up_handling

        def force_down_before_reconnector_is_cleared(h, up_epoch, **kwargs):
            Cluster.on_down(
                cluster, h, is_host_addition=False, expect_host_to_be_down=True)
            return original_get_reconnector(cluster, h, up_epoch, **kwargs)

        cluster._get_reconnector_for_current_up_handling = Mock(
            side_effect=force_down_before_reconnector_is_cleared)

        Cluster.on_up(cluster, host)

        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(
            host, expected_endpoint=host.endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)
        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()
        old_reconnector.cancel.assert_called_once_with()
        assert not host.is_up
        assert self._state(cluster, host).up_epoch is None
        assert self._state(cluster, host).down_epoch is None

    def test_forced_down_while_reconnecting_runs_new_down_handling(self):
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        host = self._make_host()
        host.set_down()
        host._reconnection_handler = Mock()

        Cluster.on_down(
            cluster, host, is_host_addition=False, expect_host_to_be_down=True)

        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(
            host, expected_endpoint=host.endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)
        assert self._state(cluster, host).down_epoch is None

    def test_newer_down_before_up_side_effects_suppresses_stale_up(self):
        cluster = self._make_cluster()
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        host = self._make_host()
        host.set_down()
        original_superseded_check = Cluster._up_handling_was_superseded
        checked = []

        def force_down_before_first_superseded_check(h, up_epoch):
            if not checked:
                checked.append(True)
                Cluster.on_down(
                    cluster, h, is_host_addition=False, expect_host_to_be_down=True)
            return original_superseded_check(cluster, h, up_epoch)

        cluster._up_handling_was_superseded = Mock(
            side_effect=force_down_before_first_superseded_check)

        Cluster.on_up(cluster, host)

        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)
        assert not host.is_up
        assert self._state(cluster, host).up_epoch is None
        assert self._state(cluster, host).down_epoch is None

    def test_up_during_down_superseding_in_flight_up_is_replayed(self):
        first_pool_future = Future()
        second_pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.side_effect = [first_pool_future, second_pool_future]
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()

        Cluster.on_up(cluster, host)
        state = self._state(cluster, host)
        first_up_epoch = state.up_epoch
        listener.on_down.side_effect = lambda h: Cluster.on_up(cluster, h)

        Cluster.on_down(
            cluster, host, is_host_addition=False, expect_host_to_be_down=True)

        assert state.epoch > first_up_epoch
        assert state.up_epoch == first_up_epoch
        assert state.pending_up_epoch == state.epoch

        first_pool_future.set_result(True)

        assert session.add_or_renew_pool.call_count == 2
        assert state.up_epoch == state.epoch
        assert state.pending_up_epoch is None
        listener.on_up.assert_not_called()

        second_pool_future.set_result(True)

        listener.on_up.assert_called_once_with(host)
        assert host.is_up
        assert state.up_epoch is None

    def test_stale_superseded_up_cleanup_does_not_run_after_newer_down(self):
        first_pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = first_pool_future
        cluster = self._make_cluster(session=session)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()

        Cluster.on_up(cluster, host)
        Cluster.on_down(
            cluster, host, is_host_addition=False, expect_host_to_be_down=True)

        cleanup_calls = []

        def signal_up_during_stale_cleanup(h, **kwargs):
            cleanup_calls.append(h)
            return None

        session.remove_pool.side_effect = signal_up_during_stale_cleanup

        first_pool_future.set_result(True)

        assert cleanup_calls == []
        assert session.add_or_renew_pool.call_count == 1
        assert self._state(cluster, host).up_epoch is None
        assert self._state(cluster, host).pending_up_epoch is None
        assert not host.is_up

    def test_sync_up_failure_replays_queued_up(self):
        session = Mock()
        session.add_or_renew_pool.return_value = None
        cluster = self._make_cluster(session=session)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()
        fail_first_on_up = [True]

        def queue_up_then_fail(h):
            if not fail_first_on_up[0]:
                return
            fail_first_on_up[0] = False
            Cluster.on_down(
                cluster, h, is_host_addition=False, expect_host_to_be_down=True)
            Cluster.on_up(cluster, h)
            state = self._state(cluster, h)
            assert state.pending_up_epoch == state.epoch
            raise RuntimeError("up failed")

        cluster.profile_manager.on_up.side_effect = queue_up_then_fail

        with pytest.raises(RuntimeError):
            Cluster.on_up(cluster, host)

        assert cluster.profile_manager.on_up.call_count == 2
        cluster.control_connection.on_up.assert_called_once_with(host)
        session.add_or_renew_pool.assert_called_once_with(
            host, is_host_addition=False,
            allow_retry_after_auth_failure=True,
            expected_endpoint=host.endpoint)
        assert host.is_up
        assert self._state(cluster, host).up_epoch is None
        assert self._state(cluster, host).pending_up_epoch is None

    def test_old_up_callback_does_not_clear_replayed_up_handling(self):
        first_pool_future = Future()
        second_pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.side_effect = [first_pool_future, second_pool_future]
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()

        listener.on_up.side_effect = lambda h: Cluster.on_down(
            cluster, h, is_host_addition=False)
        listener.on_down.side_effect = lambda h: Cluster.on_up(cluster, h)

        Cluster.on_up(cluster, host)
        state = self._state(cluster, host)
        first_up_epoch = state.up_epoch

        first_pool_future.set_result(True)

        assert session.add_or_renew_pool.call_count == 2
        assert not host.is_up
        assert state.up_epoch != first_up_epoch

        listener.on_up.side_effect = None
        second_pool_future.set_result(True)

        assert listener.on_up.call_count == 2
        assert host.is_up
        assert state.up_epoch is None

    def test_stale_reconnector_success_does_not_clear_newer_reconnector(self):
        old_endpoint = DefaultEndPoint('127.0.0.1')
        new_endpoint = DefaultEndPoint('127.0.0.2')
        cluster = self._make_cluster()
        host = Host(old_endpoint, SimpleConvictionPolicy, host_id=uuid.uuid4())
        host.endpoint = new_endpoint
        new_reconnector = Mock()
        host._reconnection_handler = new_reconnector
        connection = Mock(endpoint=old_endpoint)
        handler = _HostReconnectionHandler(
            host, Mock(return_value=connection), False, Mock(), cluster.on_up, Mock(),
            iter([0]), host.get_and_set_reconnection_handler, new_handler=None)

        handler.run()

        assert host._reconnection_handler is new_reconnector
        assert not host.is_up

    def test_superseded_up_cleanup_preserves_replacement_host_pool(self):
        stale_host = self._make_host()
        replacement_host = self._make_host()
        replacement_pool = Mock(host=replacement_host)
        session = self._make_session_with_pool(replacement_host, replacement_pool)
        cluster = self._make_cluster(session=session)

        assert stale_host == replacement_host
        assert stale_host.host_id != replacement_host.host_id

        cluster._cleanup_superseded_up_handling(stale_host)

        assert session._pools.get(replacement_host) is replacement_pool
        replacement_pool.shutdown.assert_not_called()

    def test_superseded_up_cleanup_removes_matching_host_pool(self):
        host = self._make_host()
        pool = Mock(host=host)
        session = self._make_session_with_pool(host, pool)
        cluster = self._make_cluster(session=session)

        cluster._cleanup_superseded_up_handling(host)

        assert session._pools == {}
        pool.shutdown.assert_called_once_with()

    def test_down_during_up_listener_is_handled(self):
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_down()
        listener.on_up.side_effect = lambda h: Cluster.on_down(
            cluster, h, is_host_addition=False)

        Cluster.on_up(cluster, host)
        assert self._state(cluster, host).up_epoch is not None

        pool_future.set_result(True)

        listener.on_up.assert_called_once_with(host)
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(
            host, expected_endpoint=host.endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)
        assert not host.is_up
        assert self._state(cluster, host).up_epoch is None
        assert self._state(cluster, host).down_epoch is None

    def test_current_down_handling_still_removes_pools_and_reconnects(self):
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        host = self._make_host()
        host.set_up()
        down_epoch = self._reserve_down_handling(cluster, host)

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False, down_epoch=down_epoch)

        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(
            host, expected_endpoint=host.endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(host, False, expected_down_epoch=ANY)
        assert self._state(cluster, host).down_epoch is None

    def test_remove_during_down_listener_does_not_start_reconnector(self):
        listener = Mock()
        cluster = self._make_cluster(listener=listener)
        cluster.metadata = Mock()
        cluster.metadata.remove_host.return_value = True
        host = self._make_host()
        host.set_up()
        down_epoch = self._reserve_down_handling(cluster, host)

        listener.on_down.side_effect = lambda h: Cluster.remove_host(cluster, h)

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False, down_epoch=down_epoch)

        cluster.metadata.remove_host.assert_called_once_with(host)
        listener.on_down.assert_called_once_with(host)
        listener.on_remove.assert_called_once_with(host)
        cluster.profile_manager.on_remove.assert_called_once_with(host)
        cluster._start_reconnector.assert_not_called()
        assert self._state(cluster, host).down_epoch is None

    def test_queued_down_handling_after_remove_does_not_start_reconnector(self):
        executor = _QueuedExecutor()
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster.executor = executor
        cluster.metadata = Mock()
        cluster.metadata.remove_host.return_value = True
        host = self._make_host()
        host.set_up()

        Cluster.on_down(cluster, host, is_host_addition=False)
        assert len(executor.submissions) == 1

        Cluster.remove_host(cluster, host)
        executor.run_next()

        cluster.metadata.remove_host.assert_called_once_with(host)
        cluster.profile_manager.on_remove.assert_called_once_with(host)
        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        session.on_down.assert_not_called()
        listener.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()
        assert self._state(cluster, host).down_epoch is None

    def test_start_reconnector_rechecks_down_epoch_before_installing_handler(self):
        cluster = self._make_cluster()
        cluster.reconnection_policy = Mock()
        cluster.reconnection_policy.new_schedule.return_value = iter([0])
        cluster.scheduler = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        state = self._state(cluster, host)
        state.down_epoch = 1

        def clear_down_epoch(h):
            with h.lock:
                self._state(cluster, h).down_epoch = None
            return Mock()

        cluster._make_connection_factory = Mock(side_effect=clear_down_epoch)
        Cluster._start_reconnector(
            cluster, host, is_host_addition=False, expected_down_epoch=1)

        assert host._reconnection_handler is None
        cluster.scheduler.schedule.assert_not_called()

    def test_on_up_queues_after_down_is_submitted_before_worker_runs(self):
        executor = _QueuedExecutor()
        session = Mock()
        session.add_or_renew_pool.return_value = None
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster.executor = executor
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        host = self._make_host()
        host.set_up()

        Cluster.on_down(cluster, host, is_host_addition=False)
        state = self._state(cluster, host)

        assert len(executor.submissions) == 1
        assert state.down_epoch == state.epoch

        Cluster.on_up(cluster, host)

        assert state.pending_up_epoch == state.epoch
        assert state.up_epoch is None
        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()

        executor.run_next()

        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.on_down.assert_called_once_with(
            host, expected_endpoint=host.endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)
        cluster.profile_manager.on_up.assert_called_once_with(host)
        cluster.control_connection.on_up.assert_called_once_with(host)
        assert host.is_up
        assert state.down_epoch is None
        assert state.up_epoch is None
        assert state.pending_up_epoch is None

    def test_on_up_stays_queued_after_endpoint_update_before_down_worker_runs(self):
        executor = _QueuedExecutor()
        session = Mock()
        session.add_or_renew_pool.return_value = None
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster.executor = executor
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        host = self._make_host()
        host.set_up()

        Cluster.on_down(
            cluster, host, is_host_addition=False, expect_host_to_be_down=True)
        state = self._state(cluster, host)
        old_endpoint = host.endpoint

        host.endpoint = DefaultEndPoint("127.0.0.2")

        assert self._state(cluster, host) is state

        Cluster.on_up(cluster, host)

        assert state.down_epoch == state.epoch
        assert state.pending_up_epoch == state.epoch
        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()

        executor.run_next()

        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        session.on_down.assert_called_once_with(
            host, expected_endpoint=old_endpoint)
        listener.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()
        cluster.profile_manager.on_up.assert_called_once_with(host)
        cluster.control_connection.on_up.assert_called_once_with(host)
        assert host.is_up
        assert state.down_epoch is None
        assert state.up_epoch is None
        assert state.pending_up_epoch is None

    def test_down_for_replacement_endpoint_during_pending_old_down_is_handled(self):
        executor = _QueuedExecutor()
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster.executor = executor
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_up()
        old_endpoint = host.endpoint
        new_endpoint = DefaultEndPoint("127.0.0.2")

        Cluster.on_down(
            cluster, host, is_host_addition=False,
            expected_endpoint=old_endpoint)
        state = self._state(cluster, host)
        assert state.down_epoch == state.epoch

        host.endpoint = new_endpoint
        Cluster.on_up(cluster, host)
        assert state.pending_up_epoch == state.epoch

        Cluster.on_down(
            cluster, host, is_host_addition=False,
            expected_endpoint=new_endpoint)

        executor.run_next()

        assert len(executor.submissions) == 1
        executor.run_next()

        session.on_down.assert_any_call(
            host, expected_endpoint=old_endpoint)
        session.on_down.assert_any_call(
            host, expected_endpoint=new_endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=new_endpoint)
        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()
        assert not host.is_up
        assert state.down_epoch is None
        assert state.up_epoch is None
        assert state.pending_up_epoch is None

    def test_forced_down_for_replacement_endpoint_during_old_down_is_handled(self):
        executor = _QueuedExecutor()
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster.executor = executor
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        host.set_up()
        old_endpoint = host.endpoint
        new_endpoint = DefaultEndPoint("127.0.0.2")

        Cluster.on_down(
            cluster, host, is_host_addition=False,
            expected_endpoint=old_endpoint)
        state = self._state(cluster, host)
        assert state.down_epoch == state.epoch

        host.endpoint = new_endpoint
        Cluster.on_down(
            cluster, host, is_host_addition=False,
            expect_host_to_be_down=True, expected_endpoint=new_endpoint)

        executor.run_next()

        assert len(executor.submissions) == 1
        executor.run_next()

        session.on_down.assert_any_call(
            host, expected_endpoint=old_endpoint)
        session.on_down.assert_any_call(
            host, expected_endpoint=new_endpoint)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=new_endpoint)
        assert not host.is_up
        assert state.down_epoch is None
        assert state.up_epoch is None
        assert state.pending_up_epoch is None

    def test_later_down_before_worker_runs_does_not_skip_pool_cleanup(self):
        executor = _QueuedExecutor()
        host = self._make_host()
        host.set_up()
        pool = Mock()
        pool.host = host
        pool.endpoint = host.endpoint
        session = self._make_session_with_pool(host, pool)
        session._lock = RLock()
        cluster = self._make_cluster(session=session)
        cluster.executor = executor
        cluster.metadata = Mock()
        cluster.metadata.all_hosts.return_value = [host]
        session.cluster = cluster
        session._profile_manager = cluster.profile_manager

        Cluster.on_down(cluster, host, is_host_addition=False)
        Cluster.on_up(cluster, host)
        Cluster.on_down(cluster, host, is_host_addition=False)

        future = executor.run_next()

        assert future.exception() is None
        assert session._pools == {}
        pool.shutdown.assert_called_once_with()

    def test_up_signal_waits_until_submitted_down_handling_finishes(self):
        executor = _QueuedExecutor()
        events = []
        session = Mock()
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster.executor = executor
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        host = self._make_host()
        host.set_up()

        cluster.profile_manager.on_down.side_effect = lambda h: events.append("profile_down")
        cluster.control_connection.on_down.side_effect = lambda h: events.append("control_down")
        session.on_down.side_effect = lambda h, **kwargs: events.append("session_down")
        listener.on_down.side_effect = lambda h: events.append("listener_down")
        cluster._start_reconnector.side_effect = lambda h, is_host_addition, **kwargs: events.append("reconnector")
        session.remove_pool.side_effect = lambda h, **kwargs: events.append("remove_pool")
        cluster.profile_manager.on_up.side_effect = lambda h: events.append("profile_up")
        cluster.control_connection.on_up.side_effect = lambda h: events.append("control_up")
        session.add_or_renew_pool.side_effect = lambda h, is_host_addition, **kwargs: events.append("add_pool")

        Cluster.on_down(cluster, host, is_host_addition=False)
        Cluster.on_up(cluster, host)

        assert events == []

        executor.run_next()

        assert events == [
            "profile_down",
            "control_down",
            "session_down",
            "listener_down",
            "reconnector",
            "remove_pool",
            "profile_up",
            "control_up",
            "add_pool",
        ]
        assert host.is_up

    def test_on_up_queues_when_down_handling_is_active(self):
        cluster = self._make_cluster()
        cluster._prepare_all_queries = Mock()
        host = self._make_host()
        host.set_down()
        down_epoch = self._reserve_down_handling(cluster, host)

        Cluster.on_up(cluster, host)

        cluster._prepare_all_queries.assert_not_called()
        state = self._state(cluster, host)
        assert state.down_epoch == down_epoch
        assert state.up_epoch is None
        assert state.pending_up_epoch == state.epoch

    def test_on_up_during_down_handling_is_replayed_for_ignored_host(self):
        listener = Mock()
        cluster = self._make_cluster(listener=listener)
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        host = self._make_host()
        host.set_up()
        down_epoch = self._reserve_down_handling(cluster, host)
        listener.on_down.side_effect = lambda h: Cluster.on_up(cluster, h)

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False, down_epoch=down_epoch)

        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        listener.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(host, False, expected_down_epoch=ANY)
        cluster.profile_manager.on_up.assert_called_once_with(host)
        cluster.control_connection.on_up.assert_called_once_with(host)
        assert host.is_up
        assert self._state(cluster, host).down_epoch is None
        assert self._state(cluster, host).up_epoch is None
        assert self._state(cluster, host).pending_up_epoch is None

    def test_later_down_during_down_handling_invalidates_queued_up(self):
        listener = Mock()
        cluster = self._make_cluster(listener=listener)
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        host = self._make_host()
        host.set_up()
        down_epoch = self._reserve_down_handling(cluster, host)

        def queue_up_then_down(h):
            Cluster.on_up(cluster, h)
            assert self._state(cluster, h).pending_up_epoch == self._state(cluster, h).epoch
            Cluster.on_down(cluster, h, is_host_addition=False)

        listener.on_down.side_effect = queue_up_then_down

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False, down_epoch=down_epoch)

        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()
        assert not host.is_up
        assert self._state(cluster, host).down_epoch is None
        assert self._state(cluster, host).up_epoch is None
        assert self._state(cluster, host).pending_up_epoch is None

    def test_remove_during_down_handling_invalidates_queued_up(self):
        listener = Mock()
        cluster = self._make_cluster(listener=listener)
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        host = self._make_host()
        host.set_up()
        down_epoch = self._reserve_down_handling(cluster, host)

        def queue_up_then_remove(h):
            Cluster.on_up(cluster, h)
            assert self._state(cluster, h).pending_up_epoch == self._state(cluster, h).epoch
            Cluster.on_remove(cluster, h)

        listener.on_down.side_effect = queue_up_then_remove

        Cluster.on_down_potentially_blocking(
            cluster, host, is_host_addition=False, down_epoch=down_epoch)

        cluster.profile_manager.on_remove.assert_called_once_with(host)
        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()
        assert not host.is_up
        assert self._state(cluster, host).up_epoch is None
        assert self._state(cluster, host).pending_up_epoch is None

    def test_stale_queued_up_replay_is_ignored_after_newer_down_event(self):
        cluster = self._make_cluster()
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        host = self._make_host()
        host.set_down()
        state = self._state(cluster, host)
        state.epoch = 1
        state.pending_up_epoch = 0

        cluster._handle_pending_node_up(host, 0)

        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()
        assert not host.is_up
        assert state.up_epoch is None

    def test_stale_queued_up_replay_preserves_newer_pending_up(self):
        cluster = self._make_cluster()
        host = self._make_host()
        host.set_down()
        state = self._state(cluster, host)
        state.epoch = 2
        state.down_epoch = 2
        state.pending_up_epoch = 2

        cluster._handle_pending_node_up(host, 1)

        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()
        assert state.pending_up_epoch == 2
        assert state.up_epoch is None

    def test_down_after_pending_up_pop_invalidates_replay(self):
        cluster = self._make_cluster()
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        host = self._make_host()
        host.set_down()
        state = self._state(cluster, host)
        state.epoch = 2
        state.pending_up_epoch = 2

        with host.lock:
            pending_up_epoch = cluster._pop_pending_node_up_if_ready(host)

        assert pending_up_epoch == (2, None)
        assert state.pending_up_epoch == 2

        Cluster.on_down(cluster, host, is_host_addition=False)

        assert state.epoch == 3
        assert state.pending_up_epoch is None

        cluster._handle_pending_node_up(host, pending_up_epoch)

        cluster.profile_manager.on_up.assert_not_called()
        cluster.control_connection.on_up.assert_not_called()
        assert not host.is_up

    def test_down_for_down_host_with_pending_up_only_invalidates_pending_up(self):
        cluster = self._make_cluster()
        cluster.executor = Mock()
        host = self._make_host()
        host.set_down()
        state = self._state(cluster, host)
        state.epoch = 2
        state.down_epoch = 2
        state.pending_up_epoch = 2

        Cluster.on_down(cluster, host, is_host_addition=False)

        cluster.executor.submit.assert_not_called()
        assert state.down_epoch == 2
        assert state.pending_up_epoch is None
        assert state.epoch == 3

    def test_auth_failure_for_unknown_host_does_not_start_down_handling(self):
        cluster = self._make_cluster()
        host = self._make_host()

        is_down = cluster.signal_connection_failure(
            host, AuthenticationFailed("bad credentials"), is_host_addition=False)

        assert is_down
        assert host.is_up is None
        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()

    def test_wrapped_auth_failure_for_unknown_host_does_not_start_down_handling(self):
        cluster = self._make_cluster()
        host = self._make_host()
        auth_exc = AuthenticationFailed("bad credentials")
        conn_exc = ConnectionException(str(auth_exc), endpoint=host)
        conn_exc.__cause__ = auth_exc

        is_down = cluster.signal_connection_failure(
            host, conn_exc, is_host_addition=False)

        assert is_down
        assert host.is_up is None
        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()

    def test_connection_failure_after_endpoint_swap_is_ignored(self):
        cluster = self._make_cluster()
        host = self._make_host()
        host.set_up()
        old_endpoint = host.endpoint
        host.endpoint = DefaultEndPoint("127.0.0.2")

        is_down = cluster.signal_connection_failure(
            host, ConnectionException("failed", endpoint=old_endpoint),
            is_host_addition=False, expected_endpoint=old_endpoint)

        assert not is_down
        assert host.is_up
        cluster.profile_manager.on_down.assert_not_called()
        cluster.control_connection.on_down.assert_not_called()
        cluster._start_reconnector.assert_not_called()

    def test_auth_failed_up_pool_for_unknown_host_rolls_back_without_reconnector(self):
        pool_future = Future()
        session = Mock()
        session.add_or_renew_pool.return_value = pool_future
        listener = Mock()
        cluster = self._make_cluster(session=session, listener=listener)
        cluster._prepare_all_queries = Mock()
        cluster.profile_manager.distance.return_value = HostDistance.LOCAL
        host = self._make_host()
        auth_exc = AuthenticationFailed("bad credentials")
        conn_exc = ConnectionException(str(auth_exc), endpoint=host)
        conn_exc.__cause__ = auth_exc

        Cluster.on_up(cluster, host)
        is_down = cluster.signal_connection_failure(
            host, conn_exc, is_host_addition=False)

        assert is_down
        assert host.is_up is None
        session.remove_pool.reset_mock()

        pool_future.set_result(False)

        assert host.is_up is None
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        session.remove_pool.assert_called_once_with(
            host, expected_host=host, expected_endpoint=host.endpoint)
        listener.on_up.assert_not_called()
        cluster._start_reconnector.assert_not_called()
        assert self._state(cluster, host).up_epoch is None

    def test_real_down_for_unknown_host_marks_host_down(self):
        cluster = self._make_cluster()
        host = self._make_host()

        Cluster.on_down(cluster, host, is_host_addition=False)

        assert host.is_up is False
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)

    def test_expected_down_for_unknown_host_marks_host_down(self):
        cluster = self._make_cluster()
        host = self._make_host()

        Cluster.on_down(
            cluster, host, is_host_addition=False, expect_host_to_be_down=True)

        assert host.is_up is False
        cluster.profile_manager.on_down.assert_called_once_with(host)
        cluster.control_connection.on_down.assert_called_once_with(host)
        cluster._start_reconnector.assert_called_once_with(
            host, False, expected_down_epoch=ANY,
            expected_endpoint=host.endpoint)


class SessionTest(unittest.TestCase):
    def setUp(self):
        if connection_class is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        connection_class.initialize_reactor()

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
