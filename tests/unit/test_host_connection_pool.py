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
from concurrent.futures import ThreadPoolExecutor
import logging
import time
import uuid
from cassandra.protocol_features import ProtocolFeatures

from cassandra.shard_info import _ShardingInfo

import unittest
from threading import Thread, Event, Lock, Condition
from unittest.mock import Mock, NonCallableMagicMock, MagicMock

from cassandra.cluster import Cluster, Session, ShardAwareOptions
from cassandra.connection import ClientRoutesEndPoint, Connection, DefaultEndPoint
from cassandra.metadata import Metadata
from cassandra.pool import HostConnection
from cassandra.pool import Host, NoConnectionsAvailable
from cassandra.policies import HostDistance, SimpleConvictionPolicy
import pytest

from tests.unit.util import HashableMock

LOGGER = logging.getLogger(__name__)


class _PoolTests(unittest.TestCase):
    __test__ = False
    PoolImpl = None
    uses_single_connection = None

    def make_session(self):
        session = NonCallableMagicMock(spec=Session, keyspace='foobarkeyspace', _trash=[])
        return session

    def test_borrow_and_return(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        c, request_id = pool.borrow_connection(timeout=0.01)
        assert c is conn
        assert 1 == conn.in_flight
        conn.set_keyspace_blocking.assert_called_once_with('foobarkeyspace')

        pool.return_connection(conn)
        assert 0 == conn.in_flight
        if not self.uses_single_connection:
            assert conn not in pool._trash

    def test_failed_wait_for_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        assert 1 == conn.in_flight

        conn.in_flight = conn.max_request_id

        # we're already at the max number of requests for this connection,
        # so we this should fail
        with pytest.raises(NoConnectionsAvailable):
            pool.borrow_connection(0)

    def test_successful_wait_for_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100,
                                    lock=Lock())
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        assert 1 == conn.in_flight

        def get_second_conn():
            c, request_id = pool.borrow_connection(1.0)
            assert conn is c
            pool.return_connection(c)

        t = Thread(target=get_second_conn)
        t.start()

        pool.return_connection(conn)
        t.join()
        assert 0 == conn.in_flight

    def test_spawn_when_at_max(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100)
        conn.max_request_id = 100
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        assert 1 == conn.in_flight

        # make this conn full
        conn.in_flight = conn.max_request_id

        # we don't care about making this borrow_connection call succeed for the
        # purposes of this test, as long as it results in a new connection
        # creation being scheduled
        with pytest.raises(NoConnectionsAvailable):
            pool.borrow_connection(0)
        if not self.uses_single_connection:
            session.submit.assert_called_once_with(pool._create_new_connection)

    def test_return_defunct_connection(self):
        host = Mock(spec=Host, address='ip1')
        host.lock = Lock()
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
                                    max_request_id=100, signaled_error=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        session.cluster.signal_connection_failure.return_value = False
        host.signal_connection_failure.return_value = False
        pool.return_connection(conn)

        # the connection should be closed a new creation scheduled
        assert session.submit.call_args
        assert not pool.is_shutdown

    def test_return_defunct_connection_on_down_host(self):
        host = Mock(spec=Host, address='ip1')
        host.lock = Lock()
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
                                    max_request_id=100, signaled_error=False,
                                    orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn
        session.cluster.shard_aware_options = ShardAwareOptions()

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        session.cluster.signal_connection_failure.return_value = True
        host.signal_connection_failure.return_value = True
        pool.return_connection(conn)

        # the connection should be closed a new creation scheduled
        assert conn.close.call_args
        if self.PoolImpl is HostConnection:
            # on shard aware implementation we use submit function regardless
            assert host.signal_connection_failure.call_args
            session.cluster.on_down.assert_called_once_with(
                host, is_host_addition=False, expected_endpoint=pool.endpoint)
            assert session.submit.called
        else:
            assert not session.submit.called
            assert session.cluster.signal_connection_failure.call_args
        assert pool.is_shutdown

    def test_return_closed_connection(self):
        host = Mock(spec=Host, address='ip1')
        host.lock = Lock()
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=True, max_request_id=100,
                                    signaled_error=False, orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(host.endpoint, on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        conn.is_closed = True
        session.cluster.signal_connection_failure.return_value = False
        host.signal_connection_failure.return_value = False
        pool.return_connection(conn)

        # a new creation should be scheduled
        assert session.submit.call_args
        assert not pool.is_shutdown

    def test_return_defunct_connection_after_endpoint_swap_is_ignored(self):
        old_endpoint = DefaultEndPoint('127.0.0.1')
        new_endpoint = DefaultEndPoint('127.0.0.2')
        host = Mock(spec=Host, address='ip1')
        host.endpoint = old_endpoint
        host.lock = Lock()
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
                                    max_request_id=100, signaled_error=False,
                                    orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)

        pool.borrow_connection(timeout=0.01)
        host.endpoint = new_endpoint
        conn.is_defunct = True
        host.signal_connection_failure.return_value = True
        pool.return_connection(conn)

        host.signal_connection_failure.assert_not_called()
        session.cluster.on_down.assert_not_called()
        session.submit.assert_not_called()
        conn.close.assert_called_once_with()
        assert not pool.is_shutdown

    def test_return_defunct_connection_after_client_route_endpoint_port_swap_is_ignored(self):
        host_id = uuid.uuid4()
        old_endpoint = ClientRoutesEndPoint(
            host_id, Mock(), '127.0.0.1', original_port=9042)
        new_endpoint = ClientRoutesEndPoint(
            host_id, Mock(), '127.0.0.1', original_port=9142)
        assert old_endpoint == new_endpoint
        assert not Cluster._endpoints_match(old_endpoint, new_endpoint)
        host = Mock(spec=Host, address='ip1')
        host.endpoint = old_endpoint
        host.lock = Lock()
        session = self.make_session()
        session.cluster._endpoints_match.side_effect = Cluster._endpoints_match
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
                                    max_request_id=100, signaled_error=False,
                                    orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)

        pool.borrow_connection(timeout=0.01)
        host.endpoint = new_endpoint
        conn.is_defunct = True
        host.signal_connection_failure.return_value = True
        pool.return_connection(conn)

        host.signal_connection_failure.assert_not_called()
        session.cluster.on_down.assert_not_called()
        session.submit.assert_not_called()
        conn.close.assert_called_once_with()
        assert not pool.is_shutdown

    def test_return_defunct_connection_after_endpoint_reassignment_is_ignored(self):
        endpoint = DefaultEndPoint('127.0.0.1')
        stale_host = Host(endpoint, SimpleConvictionPolicy, host_id=uuid.uuid4())
        replacement_host = Host(endpoint, SimpleConvictionPolicy, host_id=uuid.uuid4())
        stale_host.signal_connection_failure = Mock(return_value=True)

        session = self.make_session()
        session.remove_pool.return_value = None
        session.cluster.metadata = Metadata()
        session.cluster.metadata.add_or_return_host(replacement_host)
        session.cluster._endpoints_match.side_effect = Cluster._endpoints_match
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100,
                            signaled_error=False,
                            orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(stale_host, HostDistance.LOCAL, session)

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        pool.return_connection(conn)

        stale_host.signal_connection_failure.assert_not_called()
        session.cluster.on_down.assert_not_called()
        session.submit.assert_not_called()
        session.remove_pool.assert_called_once_with(
            stale_host, expected_host=stale_host,
            expected_endpoint=endpoint, expected_pool=pool)
        conn.close.assert_called_once_with()
        assert not pool.is_shutdown

    def test_host_instantiations(self):
        """
        Ensure Host fails if not initialized properly
        """

        with pytest.raises(ValueError):
            Host(None, None, host_id=uuid.uuid4())
        with pytest.raises(ValueError):
            Host('127.0.0.1', None, host_id=uuid.uuid4())
        with pytest.raises(ValueError):
            Host(None, SimpleConvictionPolicy, host_id=uuid.uuid4())

    def test_host_equality(self):
        """
        Test host equality has correct logic
        """

        a = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
        b = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
        c = Host('127.0.0.2', SimpleConvictionPolicy, host_id=uuid.uuid4())

        assert a == b, 'Two Host instances should be equal when sharing.'
        assert a != c, 'Two Host instances should NOT be equal when using two different addresses.'
        assert b != c, 'Two Host instances should NOT be equal when using two different addresses.'


class HostConnectionTests(_PoolTests):
    __test__ = True
    PoolImpl = HostConnection
    uses_single_connection = True

    def test_fast_shutdown(self):
        class MockSession(MagicMock):
            is_shutdown = False
            keyspace = "reprospace"

            def __init__(self, *args, **kwargs):
                super(MockSession, self).__init__(*args, **kwargs)
                self.cluster = MagicMock()
                self.connection_created = Event()
                self.cluster.executor = ThreadPoolExecutor(max_workers=2)
                self.cluster.signal_connection_failure = lambda *args, **kwargs: False
                self.cluster.connection_factory = self.mock_connection_factory
                self.connection_counter = 0

            def submit(self, fn, *args, **kwargs):
                LOGGER.info("Scheduling %s with args: %s, kwargs: %s", fn, args, kwargs)
                if not self.is_shutdown:
                    return self.cluster.executor.submit(fn, *args, **kwargs)

            def mock_connection_factory(self, *args, **kwargs):
                connection = HashableMock()
                connection.is_shutdown = False
                connection.is_defunct = False
                connection.is_closed = False
                connection.features = ProtocolFeatures(shard_id=self.connection_counter,
                                                       sharding_info=_ShardingInfo(shard_id=1, shards_count=14,
                                                                    partitioner="", sharding_algorithm="", sharding_ignore_msb=0,
                                                                    shard_aware_port="", shard_aware_port_ssl=""))
                self.connection_counter += 1
                self.connection_created.set()

                return connection

        for attempt_num in range(3):
            LOGGER.info("Testing fast shutdown %d / 3 times", attempt_num + 1)
            host = MagicMock()
            host.endpoint = "1.2.3.4"
            session = MockSession()

            pool = HostConnection(host=host, host_distance=HostDistance.REMOTE, session=session)
            LOGGER.info("Initialized pool %s", pool)

            # Wait for initial connection to be created (with timeout)
            if not session.connection_created.wait(timeout=2.0):
                pytest.fail("Initial connection failed to be created within 2 seconds")

            LOGGER.info("Connections: %s", pool._connections)

            # Shutdown the pool
            pool.shutdown()

            # Verify pool is shut down
            assert pool.is_shutdown, "Pool should be marked as shutdown"

            # Cleanup executor with proper wait
            session.cluster.executor.shutdown(wait=True)

    def test_replace_retries_when_replacement_keyspace_set_fails(self):
        host = Host(DefaultEndPoint('127.0.0.1'), SimpleConvictionPolicy,
                    host_id=uuid.uuid4())
        session = NonCallableMagicMock(spec=Session, keyspace='ks')
        session.cluster = MagicMock()
        session.cluster.shard_aware_options = ShardAwareOptions()
        session.cluster._endpoints_match.side_effect = Cluster._endpoints_match
        initial_connection = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, signaled_error=False,
            orphaned_threshold_reached=False,
            features=ProtocolFeatures(shard_id=0))
        replacement_connection = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, signaled_error=False,
            orphaned_threshold_reached=False,
            features=ProtocolFeatures(shard_id=0))
        replacement_connection.set_keyspace_blocking.side_effect = RuntimeError(
            "keyspace failed")
        session.cluster.connection_factory.side_effect = [
            initial_connection, replacement_connection]

        pool = HostConnection(host, HostDistance.LOCAL, session)
        pool._is_replacing = True

        pool._replace(initial_connection)

        assert session.submit.call_count == 1
        submitted_fn, submitted_connection = session.submit.call_args.args
        assert submitted_fn == pool._replace
        assert submitted_connection is initial_connection

    def test_replace_discards_replacement_when_endpoint_changes_during_keyspace_set(self):
        old_endpoint = DefaultEndPoint('127.0.0.1')
        new_endpoint = DefaultEndPoint('127.0.0.2')
        host = Host(old_endpoint, SimpleConvictionPolicy, host_id=uuid.uuid4())
        session = NonCallableMagicMock(spec=Session, keyspace='ks')
        session.cluster = MagicMock()
        session.cluster.shard_aware_options = ShardAwareOptions()
        session.cluster._endpoints_match.side_effect = Cluster._endpoints_match
        session.remove_pool.return_value = None
        initial_connection = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, signaled_error=False,
            orphaned_threshold_reached=False,
            features=ProtocolFeatures(shard_id=0))
        replacement_connection = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, signaled_error=False,
            orphaned_threshold_reached=False,
            features=ProtocolFeatures(shard_id=0))
        replacement_connection.set_keyspace_blocking.side_effect = (
            lambda keyspace: setattr(host, 'endpoint', new_endpoint))
        session.cluster.connection_factory.side_effect = [
            initial_connection, replacement_connection]

        pool = HostConnection(host, HostDistance.LOCAL, session)
        pool._is_replacing = True

        pool._replace(initial_connection)

        replacement_connection.close.assert_called_once_with()
        session.remove_pool.assert_called_once_with(
            host, expected_host=host, expected_endpoint=old_endpoint,
            expected_pool=pool)
        assert pool._connections == {}
        assert not pool._is_replacing

    def test_missing_shard_discards_connection_when_endpoint_changes_during_keyspace_set(self):
        old_endpoint = DefaultEndPoint('127.0.0.1')
        new_endpoint = DefaultEndPoint('127.0.0.2')
        host = Host(old_endpoint, SimpleConvictionPolicy, host_id=uuid.uuid4())
        host.sharding_info = _ShardingInfo(
            shard_id=0, shards_count=1, partitioner='',
            sharding_algorithm='', sharding_ignore_msb=0,
            shard_aware_port='', shard_aware_port_ssl='')
        session = NonCallableMagicMock(spec=Session, keyspace='ks')
        session.cluster = MagicMock()
        session.cluster.shard_aware_options = ShardAwareOptions()
        session.cluster.ssl_options = None
        session.cluster._endpoints_match.side_effect = Cluster._endpoints_match
        session.remove_pool.return_value = None
        connection = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, signaled_error=False,
            orphaned_threshold_reached=False,
            features=ProtocolFeatures(shard_id=0))
        connection.set_keyspace_blocking.side_effect = (
            lambda keyspace: setattr(host, 'endpoint', new_endpoint))
        session.cluster.connection_factory.return_value = connection

        pool = HostConnection.__new__(HostConnection)
        pool.host = host
        pool.endpoint = old_endpoint
        pool.host_distance = HostDistance.LOCAL
        pool.is_shutdown = False
        pool._session = session
        pool._lock = Lock()
        pool._stream_available_condition = Condition(Lock())
        pool._connections = {}
        pool._pending_connections = []
        pool._connecting = {0}
        pool._excess_connections = set()
        pool._trash = set()
        pool._shard_connections_futures = []
        pool._keyspace = 'ks'
        pool.advanced_shardaware_block_until = 0
        pool.tablets_routing_v1 = False

        pool._open_connection_to_missing_shard(0)

        connection.close.assert_called_once_with()
        session.remove_pool.assert_called_once_with(
            host, expected_host=host, expected_endpoint=old_endpoint,
            expected_pool=pool)
        assert pool._connections == {}
        assert pool._connecting == set()
