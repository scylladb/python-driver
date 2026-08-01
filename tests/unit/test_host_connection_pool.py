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
from concurrent.futures import Future, ThreadPoolExecutor
import logging
import uuid
from cassandra import InvalidRequest
from cassandra.protocol_features import ProtocolFeatures

from cassandra.shard_info import _ShardingInfo

import unittest
from threading import Thread, Event, Lock, RLock
from unittest.mock import Mock, NonCallableMagicMock, MagicMock

from cassandra.cluster import Session, ShardAwareOptions
from cassandra.connection import (
    Connection, ConnectionException, ConnectionShutdown)
from cassandra.pool import HostConnection, _signal_connection_failure
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
        session.cluster.connect_timeout = 5
        return session

    def test_borrow_and_return(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(
            host.endpoint,
            host_conn=pool,
            on_orphaned_stream_released=pool.on_orphaned_stream_released)

        c, request_id = pool.borrow_connection(timeout=0.01)
        assert c is conn
        assert 1 == conn.in_flight
        conn.set_keyspace_blocking.assert_called_once_with(
            'foobarkeyspace',
            timeout=session.cluster.connect_timeout)

        pool.return_connection(conn)
        assert 0 == conn.in_flight
        if not self.uses_single_connection:
            assert conn not in pool._trash

    def test_failed_wait_for_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(
            host.endpoint,
            host_conn=pool,
            on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        assert 1 == conn.in_flight

        conn.in_flight = conn.max_request_id

        # we're already at the max number of requests for this connection,
        # so we this should fail
        with pytest.raises(NoConnectionsAvailable):
            pool.borrow_connection(0)

    def test_orphan_threshold_connection_remains_borrowable_during_repair(
            self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        connection = HashableMock(
            spec=Connection,
            in_flight=0,
            is_defunct=False,
            is_closed=False,
            max_request_id=100,
            orphaned_threshold_reached=True,
            orphaned_request_ids=set(),
            lock=RLock())
        connection.features = ProtocolFeatures(shard_id=0)
        session.cluster.connection_factory.return_value = connection
        session.submit.return_value = Future()
        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.submit.reset_mock()

        borrowed, request_id = pool.borrow_connection(0.1)

        assert borrowed is connection
        assert connection.in_flight == 1
        if self.uses_single_connection:
            session.submit.assert_called_once_with(
                pool._replace, connection)

    def test_successful_wait_for_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, lock=Lock(),
            orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(
            host.endpoint,
            host_conn=pool,
            on_orphaned_stream_released=pool.on_orphaned_stream_released)

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
        conn = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, orphaned_threshold_reached=False)
        conn.max_request_id = 100
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(
            host.endpoint,
            host_conn=pool,
            on_orphaned_stream_released=pool.on_orphaned_stream_released)

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
        session = self.make_session()
        conn = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, signaled_error=False,
            orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(
            host.endpoint,
            host_conn=pool,
            on_orphaned_stream_released=pool.on_orphaned_stream_released)

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
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
                                    max_request_id=100, signaled_error=False,
                                    orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn
        session.cluster.shard_aware_options = ShardAwareOptions()

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(
            host.endpoint,
            host_conn=pool,
            on_orphaned_stream_released=pool.on_orphaned_stream_released)

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
            assert session.submit.called
        else:
            assert not session.submit.called
            assert session.cluster.signal_connection_failure.call_args
        assert pool.is_shutdown

    def test_return_closed_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100,
                                    signaled_error=False, orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.assert_called_once_with(
            host.endpoint,
            host_conn=pool,
            on_orphaned_stream_released=pool.on_orphaned_stream_released)

        pool.borrow_connection(timeout=0.01)
        conn.is_closed = True
        session.cluster.signal_connection_failure.return_value = False
        host.signal_connection_failure.return_value = False
        pool.return_connection(conn)

        # a new creation should be scheduled
        assert session.submit.call_args
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

    def test_get_connections_takes_pool_lock(self):
        pool = object.__new__(HostConnection)
        pool._lock = Lock()
        connection = object()
        pool._connections = {0: connection}
        started = Event()
        finished = Event()
        result = []

        def get_connections():
            started.set()
            result.extend(pool.get_connections())
            finished.set()

        pool._lock.acquire()
        thread = Thread(target=get_connections)
        thread.start()
        try:
            assert started.wait(2)
            assert not finished.wait(0.05)
        finally:
            pool._lock.release()
        thread.join(2)

        assert not thread.is_alive()
        assert result == [connection]

    def make_connection(
            self, shard_id=0, sharding_info=None, in_flight=0,
            orphaned_threshold_reached=False):
        connection = HashableMock(
            spec=Connection,
            in_flight=in_flight,
            is_defunct=False,
            is_closed=False,
            max_request_id=100,
            signaled_error=False,
            orphaned_threshold_reached=orphaned_threshold_reached,
            orphaned_request_ids=set(),
            lock=RLock())
        connection.features = ProtocolFeatures(
            shard_id=shard_id,
            sharding_info=sharding_info)
        return connection

    def make_pool(self, shard_aware=False, shards_count=2):
        host = Mock(spec=Host, address='ip1')
        host.lock = RLock()
        host.sharding_info = None
        session = self.make_session()
        session.cluster.shard_aware_options = ShardAwareOptions(disable=True)
        first_connection = self.make_connection()
        session.cluster.connection_factory.return_value = first_connection
        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.reset_mock()
        session.submit.reset_mock()

        if shard_aware:
            host.sharding_info = _ShardingInfo(
                shard_id=0,
                shards_count=shards_count,
                partitioner="",
                sharding_algorithm="",
                sharding_ignore_msb=0,
                shard_aware_port="",
                shard_aware_port_ssl="")
            session.cluster.shard_aware_options = ShardAwareOptions(
                disable=False,
                disable_shardaware_port=True)

        return pool, host, session, first_connection

    @staticmethod
    def factory_returning_registered(candidate):
        """
        Model Connection.factory's successful ownership handoff.

        The factory leaves the candidate registered with HostConnection until
        the pool either adopts it or shutdown drains it.
        """
        def factory(endpoint, host_conn=None, **kwargs):
            assert host_conn._register_pending_connection(candidate)
            return candidate

        return factory

    @staticmethod
    def factory_returning_registered_with_adoption_barrier(candidate):
        """
        Pause the caller's adoption immediately after the factory returns.

        This exposes the exact handoff interval to a concurrent shutdown.
        """
        factory_returned = Event()
        adoption_started = Event()
        release_adoption = Event()
        captured_pools = []

        def factory(endpoint, host_conn=None, **kwargs):
            original_register = host_conn._register_pending_connection
            assert original_register(candidate)
            captured_pools.append(host_conn)

            def block_adoption(connection):
                assert connection is candidate
                assert factory_returned.is_set()
                adoption_started.set()
                assert release_adoption.wait(2)
                return original_register(connection)

            host_conn._register_pending_connection = block_adoption
            factory_returned.set()
            return candidate

        return (
            factory,
            adoption_started,
            release_adoption,
            captured_pools,
        )

    def test_initial_factory_registered_connection_is_adopted_once(self):
        host = Mock(spec=Host, address='ip1')
        host.lock = RLock()
        host.sharding_info = None
        session = self.make_session()
        session.cluster.shard_aware_options = ShardAwareOptions(disable=True)
        connection = self.make_connection()
        session.cluster.connection_factory.side_effect = \
            self.factory_returning_registered(connection)

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)

        assert pool._connections == {0: connection}
        assert pool._pending_connections == []
        connection.close.assert_not_called()

    def test_replacement_factory_registered_connection_is_adopted_once(self):
        pool, host, session, old_connection = self.make_pool()
        replacement = self.make_connection()
        session.cluster.connection_factory.side_effect = \
            self.factory_returning_registered(replacement)
        pool._is_replacing = True

        pool._replace(old_connection)

        assert pool._connections == {0: replacement}
        assert pool._pending_connections == []
        replacement.close.assert_not_called()
        old_connection.close.assert_called_once_with()

    def test_missing_shard_factory_registered_connection_is_adopted_once(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        connection = self.make_connection(shard_id=1)
        session.cluster.connection_factory.side_effect = \
            self.factory_returning_registered(connection)

        pool._open_connection_to_missing_shard(1)

        assert pool._connections == {
            0: first_connection,
            1: connection,
        }
        assert pool._pending_connections == []
        connection.close.assert_not_called()

    def test_shutdown_owns_registered_initial_connection_before_adoption(
            self):
        host = Mock(spec=Host, address='ip1')
        host.lock = RLock()
        host.sharding_info = None
        session = self.make_session()
        session.cluster.shard_aware_options = ShardAwareOptions(disable=True)
        connection = self.make_connection()
        connection_closed = Event()

        def close_connection():
            connection.is_closed = True
            connection_closed.set()

        connection.close.side_effect = close_connection
        (
            factory,
            adoption_started,
            release_adoption,
            captured_pools,
        ) = self.factory_returning_registered_with_adoption_barrier(connection)
        session.cluster.connection_factory.side_effect = factory
        constructed_pools = []
        construction_errors = []

        def construct_pool():
            try:
                constructed_pools.append(
                    self.PoolImpl(host, HostDistance.LOCAL, session))
            except BaseException as exc:
                construction_errors.append(exc)

        constructor = Thread(target=construct_pool)
        constructor.start()
        try:
            assert adoption_started.wait(2)
            captured_pools[0].shutdown()
            assert connection_closed.wait(2)
        finally:
            release_adoption.set()
            constructor.join(2)

        assert not constructor.is_alive()
        assert constructed_pools == []
        assert len(construction_errors) == 1
        assert isinstance(construction_errors[0], ConnectionException)
        assert captured_pools[0]._connections == {}
        assert captured_pools[0]._pending_connections == []
        connection.close.assert_called_once_with()

    def test_shutdown_owns_registered_replacement_before_adoption(self):
        pool, host, session, old_connection = self.make_pool()
        replacement = self.make_connection()
        replacement_closed = Event()

        def close_replacement():
            replacement.is_closed = True
            replacement_closed.set()

        replacement.close.side_effect = close_replacement
        (
            factory,
            adoption_started,
            release_adoption,
            captured_pools,
        ) = self.factory_returning_registered_with_adoption_barrier(
            replacement)
        session.cluster.connection_factory.side_effect = factory
        pool._is_replacing = True
        replacement_errors = []

        def replace():
            try:
                pool._replace(old_connection)
            except BaseException as exc:
                replacement_errors.append(exc)

        replacement_thread = Thread(target=replace)
        replacement_thread.start()
        try:
            assert adoption_started.wait(2)
            assert captured_pools == [pool]
            pool.shutdown()
            assert replacement_closed.wait(2)
        finally:
            release_adoption.set()
            replacement_thread.join(2)

        assert not replacement_thread.is_alive()
        assert replacement_errors == []
        assert pool._connections == {}
        assert pool._pending_connections == []
        replacement.close.assert_called_once_with()

    def test_shutdown_owns_registered_missing_shard_before_adoption(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        connection = self.make_connection(shard_id=1)
        connection_closed = Event()

        def close_connection():
            connection.is_closed = True
            connection_closed.set()

        connection.close.side_effect = close_connection
        (
            factory,
            adoption_started,
            release_adoption,
            captured_pools,
        ) = self.factory_returning_registered_with_adoption_barrier(connection)
        session.cluster.connection_factory.side_effect = factory
        opening_errors = []

        def open_missing_shard():
            try:
                pool._open_connection_to_missing_shard(1)
            except BaseException as exc:
                opening_errors.append(exc)

        opening_thread = Thread(target=open_missing_shard)
        opening_thread.start()
        try:
            assert adoption_started.wait(2)
            assert captured_pools == [pool]
            pool.shutdown()
            assert connection_closed.wait(2)
        finally:
            release_adoption.set()
            opening_thread.join(2)

        assert not opening_thread.is_alive()
        assert opening_errors == []
        assert pool._connections == {}
        assert pool._pending_connections == []
        connection.close.assert_called_once_with()

    def test_initial_closed_connection_is_rejected(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        connection = HashableMock(
            spec=Connection,
            is_closed=True,
            is_defunct=False,
            in_flight=0,
            max_request_id=100)
        session.cluster.connection_factory.return_value = connection

        with pytest.raises(ConnectionShutdown) as exc_info:
            self.PoolImpl(host, HostDistance.LOCAL, session)

        assert "closed during the startup handshake" in str(exc_info.value)
        connection.set_keyspace_blocking.assert_not_called()

    def test_replace_tracks_pending_connection(self):
        host = Mock(spec=Host, address='ip1')
        host.sharding_info = None
        session = self.make_session()
        first_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                  is_closed=False, max_request_id=100)
        first_conn.features = ProtocolFeatures(shard_id=0)
        replacement_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                        is_closed=False, max_request_id=100)
        replacement_conn.features = ProtocolFeatures(shard_id=0)
        session.cluster.connection_factory.side_effect = [first_conn, replacement_conn]

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.reset_mock()
        pool._is_replacing = True

        pool._replace(first_conn)

        session.cluster.connection_factory.assert_called_once_with(
            host.endpoint,
            host_conn=pool,
            on_orphaned_stream_released=pool.on_orphaned_stream_released)
        assert pool._pending_connections == []
        assert pool._connections[0] is replacement_conn
        assert not pool._is_replacing

    def test_regular_replacement_keeps_old_mapping_until_candidate_is_ready(
            self):
        pool, host, session, old_connection = self.make_pool()
        old_connection.orphaned_threshold_reached = True
        replacement = self.make_connection()
        keyspace_started = Event()
        release_keyspace = Event()
        replacement_errors = []

        def set_keyspace(keyspace, timeout=None):
            keyspace_started.set()
            assert release_keyspace.wait(2)

        replacement.set_keyspace_blocking.side_effect = set_keyspace
        session.cluster.connection_factory.return_value = replacement
        pool._is_replacing = True

        def replace():
            try:
                pool._replace(old_connection)
            except BaseException as exc:
                replacement_errors.append(exc)

        replace_thread = Thread(target=replace)
        replace_thread.start()
        try:
            assert keyspace_started.wait(2)
            assert pool._connections[0] is old_connection

            borrowed, _ = pool.borrow_connection(timeout=0)
            assert borrowed is old_connection
            pool.return_connection(borrowed)
        finally:
            release_keyspace.set()
            replace_thread.join(2)

        assert not replace_thread.is_alive()
        assert replacement_errors == []
        assert pool._connections[0] is replacement
        old_connection.close.assert_called_once_with()

    def test_replace_closed_connection_hands_off_to_host_reconnector(self):
        host = Mock(spec=Host, address='ip1')
        host.sharding_info = None
        session = self.make_session()
        first_conn = HashableMock(
            spec=Connection,
            in_flight=0,
            is_defunct=False,
            is_closed=False,
            max_request_id=100)
        first_conn.features = ProtocolFeatures(shard_id=0)
        replacement_conn = HashableMock(
            spec=Connection,
            in_flight=0,
            is_defunct=False,
            is_closed=True,
            max_request_id=100)
        session.cluster.connection_factory.side_effect = [
            first_conn, replacement_conn]
        session.cluster.signal_connection_failure.return_value = True

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.reset_mock()
        session.submit.reset_mock()
        pool._is_replacing = True

        pool._replace(first_conn)

        replacement_conn.close.assert_called_once_with()
        replacement_conn.set_keyspace_blocking.assert_not_called()
        assert pool._pending_connections == []
        assert pool._connections == {0: first_conn}
        first_conn.close.assert_not_called()
        assert pool._is_replacing
        session.submit.assert_not_called()
        session.cluster.signal_connection_failure.assert_called_once()
        failure_args = session.cluster.signal_connection_failure.call_args
        assert failure_args.args[0] is host
        assert isinstance(failure_args.args[1], ConnectionShutdown)
        assert failure_args.args[2] is False
        assert failure_args.kwargs == {
            'expect_host_to_be_down': True,
            'force': True,
        }

    def test_replace_startup_close_during_shutdown_does_not_mark_host_down(self):
        host = Mock(spec=Host, address='ip1')
        host.sharding_info = None
        session = self.make_session()
        first_conn = HashableMock(
            spec=Connection,
            in_flight=0,
            is_defunct=False,
            is_closed=False,
            max_request_id=100)
        first_conn.features = ProtocolFeatures(shard_id=0)
        replacement_conn = HashableMock(
            spec=Connection,
            in_flight=0,
            is_defunct=False,
            is_closed=True,
            max_request_id=100)
        session.cluster.connection_factory.return_value = first_conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.reset_mock()
        pool._is_replacing = True

        def close_pool_during_factory(*args, **kwargs):
            pool.is_shutdown = True
            return replacement_conn

        session.cluster.connection_factory.side_effect = close_pool_during_factory
        pool._replace(first_conn)

        replacement_conn.close.assert_called_once_with()
        session.cluster.signal_connection_failure.assert_not_called()
        session.cluster.scheduler.schedule.assert_not_called()

    def test_replace_allows_shutdown_to_close_pending_connection(self):
        host = Mock(spec=Host, address='ip1')
        host.sharding_info = None
        session = self.make_session()
        first_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                  is_closed=False, max_request_id=100)
        first_conn.features = ProtocolFeatures(shard_id=0)
        session.cluster.connection_factory.return_value = first_conn

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.reset_mock()
        pool._is_replacing = True

        factory_entered = Event()
        release_factory = Event()
        pending_closed = Event()

        pending_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                    is_closed=False, max_request_id=100)
        pending_conn.features = ProtocolFeatures(shard_id=0)

        def close_pending():
            pending_conn.is_closed = True
            pending_closed.set()

        pending_conn.close.side_effect = close_pending

        replacement_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                        is_closed=False, max_request_id=100)
        replacement_conn.features = ProtocolFeatures(shard_id=0)

        def blocking_factory(endpoint, host_conn=None, **kwargs):
            host_conn._pending_connections.append(pending_conn)
            factory_entered.set()
            try:
                release_factory.wait(2)
                return replacement_conn
            finally:
                try:
                    host_conn._pending_connections.remove(pending_conn)
                except ValueError:
                    pass

        session.cluster.connection_factory.side_effect = blocking_factory

        replace_thread = Thread(target=pool._replace, args=(first_conn,))
        replace_thread.start()
        assert factory_entered.wait(2)

        shutdown_thread = Thread(target=pool.shutdown)
        shutdown_thread.start()
        try:
            assert pending_closed.wait(2)
        finally:
            release_factory.set()
            replace_thread.join(2)
            shutdown_thread.join(2)

        assert not replace_thread.is_alive()
        assert not shutdown_thread.is_alive()
        assert pool.is_shutdown
        replacement_conn.close.assert_called_once()

    def test_replace_allows_shutdown_during_keyspace_setup(self):
        host = Mock(spec=Host, address='ip1')
        host.sharding_info = None
        session = self.make_session()
        first_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                  is_closed=False, max_request_id=100)
        first_conn.features = ProtocolFeatures(shard_id=0)
        replacement_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                        is_closed=False, max_request_id=100)
        replacement_conn.features = ProtocolFeatures(shard_id=0)
        session.cluster.connection_factory.side_effect = [first_conn, replacement_conn]

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session.cluster.connection_factory.reset_mock()
        pool._is_replacing = True

        keyspace_started = Event()
        release_keyspace = Event()
        replacement_closed = Event()

        def set_keyspace_blocking(keyspace, timeout=None):
            keyspace_started.set()
            release_keyspace.wait(2)

        def close_replacement():
            replacement_conn.is_closed = True
            replacement_closed.set()
            release_keyspace.set()

        replacement_conn.set_keyspace_blocking.side_effect = set_keyspace_blocking
        replacement_conn.close.side_effect = close_replacement

        replace_thread = Thread(target=pool._replace, args=(first_conn,))
        replace_thread.start()
        try:
            assert keyspace_started.wait(2)
            assert replacement_conn in pool._pending_connections

            pool.shutdown()

            assert replacement_closed.is_set()
        finally:
            release_keyspace.set()
            replace_thread.join(2)

        assert not replace_thread.is_alive()
        assert pool.is_shutdown
        assert pool._pending_connections == []
        assert pool._connections == {}
        replacement_conn.close.assert_called_once_with()

    def test_initial_keyspace_failure_closes_unpublished_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        connection = self.make_connection()
        connection.set_keyspace_blocking.side_effect = RuntimeError(
            "keyspace setup failed")
        session.cluster.connection_factory.return_value = connection

        with pytest.raises(RuntimeError, match="keyspace setup failed"):
            self.PoolImpl(host, HostDistance.LOCAL, session)

        connection.close.assert_called_once_with()

    def test_initial_keyspace_cancellation_closes_unpublished_connection(self):
        class SetupCancelled(BaseException):
            pass

        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        connection = self.make_connection()
        connection.set_keyspace_blocking.side_effect = SetupCancelled()
        session.cluster.connection_factory.return_value = connection

        with pytest.raises(SetupCancelled):
            self.PoolImpl(host, HostDistance.LOCAL, session)

        connection.close.assert_called_once_with()

    def test_initial_stale_keyspace_validation_retries_current_generation(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        session._lock = RLock()
        session._keyspace_generation = 0
        session.keyspace = "old_keyspace"
        connection = self.make_connection()
        seen_keyspaces = []

        def set_keyspace(keyspace, timeout=None):
            seen_keyspaces.append((keyspace, timeout))
            if keyspace == "old_keyspace":
                with session._lock:
                    session.keyspace = "new_keyspace"
                    session._keyspace_generation += 1
                raise InvalidRequest("old keyspace was dropped")

        connection.set_keyspace_blocking.side_effect = set_keyspace
        session.cluster.connection_factory.return_value = connection

        pool = self.PoolImpl(host, HostDistance.LOCAL, session)

        assert seen_keyspaces == [
            ("old_keyspace", session.cluster.connect_timeout),
            ("new_keyspace", session.cluster.connect_timeout),
        ]
        assert pool._keyspace == "new_keyspace"
        assert pool._connections[0] is connection
        connection.close.assert_not_called()

    def test_initial_current_keyspace_validation_still_fails(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        session._lock = RLock()
        session._keyspace_generation = 0
        connection = self.make_connection()
        connection.set_keyspace_blocking.side_effect = InvalidRequest(
            "current keyspace is invalid")
        session.cluster.connection_factory.return_value = connection

        with pytest.raises(
                InvalidRequest, match="current keyspace is invalid"):
            self.PoolImpl(host, HostDistance.LOCAL, session)

        connection.close.assert_called_once_with()

    def test_failure_adapter_preserves_pre_force_override_signature(self):
        calls = []

        class LegacyCluster(object):
            def signal_connection_failure(
                    self, host, exc, adding, expected_down=False, **kwargs):
                calls.append(
                    (host, exc, adding, expected_down, kwargs))
                return "legacy-result"

        host = object()
        error = ConnectionShutdown("startup closed")

        result = _signal_connection_failure(
            LegacyCluster(),
            host,
            error,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        assert result == "legacy-result"
        assert calls == [(
            host,
            error,
            False,
            True,
            {'force': True},
        )]

    def test_failure_adapter_supports_keyword_only_override(self):
        calls = []

        class KeywordOnlyCluster(object):
            def signal_connection_failure(
                    self, host, exc, *, is_host_addition,
                    expect_host_to_be_down=False, force=False):
                calls.append((
                    host,
                    exc,
                    is_host_addition,
                    expect_host_to_be_down,
                    force))

        host = object()
        error = ConnectionShutdown("startup closed")
        _signal_connection_failure(
            KeywordOnlyCluster(),
            host,
            error,
            is_host_addition=False,
            expect_host_to_be_down=True,
            force=True)

        assert calls == [(host, error, False, True, True)]

    def test_replacement_handoff_forces_recovery_after_legacy_false(self):
        pool, host, session, connection = self.make_pool()
        calls = []

        class LegacyCluster(object):
            def __init__(self):
                self._lock = RLock()

            def _uses_default_failure_hooks(self):
                return False

            def signal_connection_failure(
                    self, failed_host, exc, adding, expected_down=False):
                calls.append(
                    ("public", failed_host, exc, adding, expected_down))
                return False

            def _on_down_locked(
                    self, failed_host, is_host_addition,
                    expect_host_to_be_down=False, force=False):
                calls.append((
                    "private",
                    failed_host,
                    is_host_addition,
                    expect_host_to_be_down,
                    force))

        cluster = LegacyCluster()
        session.cluster = cluster
        session._lock = RLock()
        session.is_shutdown = False
        session._pools = {host: pool}
        error = ConnectionShutdown("replacement closed")

        assert pool._handoff_replacement_failure(error)

        assert calls == [
            ("public", host, error, False, True),
            ("private", host, False, True, True),
        ]

    def test_replacement_handoff_does_not_duplicate_delegated_force(self):
        pool, host, session, connection = self.make_pool()
        calls = []
        host._recovery_epoch = 3

        class DelegatingCluster(object):
            def __init__(self):
                self._lock = RLock()

            def _uses_default_failure_hooks(self):
                return False

            def signal_connection_failure(
                    self, failed_host, exc, is_host_addition,
                    expect_host_to_be_down=False, force=False):
                calls.append(("public", force))
                self._on_down_locked(
                    failed_host,
                    is_host_addition,
                    expect_host_to_be_down,
                    force)
                return False

            def _on_down_locked(
                    self, failed_host, is_host_addition,
                    expect_host_to_be_down=False, force=False):
                calls.append(("private", force))
                failed_host._recovery_epoch += 1

        session.cluster = DelegatingCluster()
        session._lock = RLock()
        session.is_shutdown = False
        session._pools = {host: pool}

        assert pool._handoff_replacement_failure(
            ConnectionShutdown("replacement closed"))

        assert calls == [
            ("public", True),
            ("private", True),
        ]
        assert host._recovery_epoch == 4

    def test_replacement_handoff_survives_conviction_base_exception(self):
        class PolicyCancelled(BaseException):
            pass

        pool, host, session, connection = self.make_pool()
        calls = []

        class DefaultClusterStandin(object):
            def __init__(self):
                self._lock = RLock()

            def _uses_default_failure_hooks(self):
                return True

            def _on_down_locked(
                    self, failed_host, is_host_addition,
                    expect_host_to_be_down=False, force=False):
                calls.append((
                    failed_host,
                    is_host_addition,
                    expect_host_to_be_down,
                    force))

        session.cluster = DefaultClusterStandin()
        session._lock = RLock()
        session.is_shutdown = False
        session._pools = {host: pool}
        host.signal_connection_failure.side_effect = PolicyCancelled(
            "cancelled policy")

        assert pool._handoff_replacement_failure(
            ConnectionShutdown("replacement closed"))

        assert calls == [(host, False, True, True)]

    def test_replace_invalid_keyspace_quarantines_open_connection(self):
        pool, host, session, first_connection = self.make_pool()
        replacement = self.make_connection()
        replacement.set_keyspace_blocking.side_effect = InvalidRequest(
            "keyspace was dropped")
        session.cluster.connection_factory.return_value = replacement
        pool._is_replacing = True

        pool._replace(first_connection)

        assert pool._connections[0] is replacement
        assert pool._pending_connections == []
        assert not pool._is_replacing
        assert replacement._pool_keyspace_mismatch is True
        replacement.close.assert_not_called()
        session.cluster.signal_connection_failure.assert_not_called()
        session.submit.reset_mock()
        session.cluster.connection_factory.reset_mock()
        for _ in range(2):
            with pytest.raises(NoConnectionsAvailable):
                pool.borrow_connection(timeout=0)
        session.submit.assert_not_called()
        session.cluster.connection_factory.assert_not_called()
        borrowed, _ = pool.borrow_connection(
            timeout=0,
            allow_keyspace_mismatch=True)
        assert borrowed is replacement
        pool.return_connection(borrowed)

    def test_quarantined_connection_counts_as_open(self):
        pool, host, session, connection = self.make_pool()
        connection._pool_keyspace_mismatch = True

        assert pool.open_count == 1
        assert pool.get_state()['open_count'] == 1

    def test_replace_revalidates_keyspace_before_adoption(self):
        pool, host, session, first_connection = self.make_pool()
        replacement = self.make_connection()
        seen_keyspaces = []
        first_connection.set_keyspace_async.side_effect = (
            lambda keyspace, callback:
            callback(first_connection, None))

        def set_keyspace(keyspace, timeout=None):
            seen_keyspaces.append((keyspace, timeout))
            if len(seen_keyspaces) == 1:
                keyspace_set = Mock()
                pool._set_keyspace_for_all_conns("new_keyspace", keyspace_set)
                keyspace_set.assert_called_once_with(pool, [])

        replacement.set_keyspace_blocking.side_effect = set_keyspace
        session.cluster.connection_factory.return_value = replacement
        pool._is_replacing = True

        pool._replace(first_connection)

        assert seen_keyspaces == [
            ("foobarkeyspace", session.cluster.connect_timeout),
            ("new_keyspace", session.cluster.connect_timeout),
        ]
        assert pool._connections[0] is replacement
        assert pool._keyspace == "new_keyspace"

    def test_regular_replacement_failure_uses_host_recovery_backoff(self):
        pool, host, session, first_connection = self.make_pool()
        replacement = self.make_connection()
        replacement.set_keyspace_blocking.side_effect = RuntimeError(
            "socket failed")
        session.cluster.connection_factory.return_value = replacement
        pool._is_replacing = True

        pool._replace(first_connection)

        replacement.close.assert_called_once_with()
        assert pool._connections == {0: first_connection}
        first_connection.close.assert_not_called()
        assert pool._pending_connections == []
        assert pool._is_replacing
        session.submit.assert_not_called()
        session.cluster.signal_connection_failure.assert_called_once()

    def test_regular_replacement_cancellation_cleans_socket_and_marker(self):
        class SetupCancelled(BaseException):
            pass

        pool, host, session, first_connection = self.make_pool()
        replacement = self.make_connection()
        replacement.set_keyspace_blocking.side_effect = SetupCancelled()
        session.cluster.connection_factory.return_value = replacement
        pool._is_replacing = True

        with pytest.raises(SetupCancelled):
            pool._replace(first_connection)

        replacement.close.assert_called_once_with()
        assert pool._pending_connections == []
        assert not pool._is_replacing
        session.cluster.signal_connection_failure.assert_called_once()

    def test_regular_replacement_submit_cancellation_hands_off_recovery(self):
        class SubmitCancelled(BaseException):
            pass

        pool, host, session, connection = self.make_pool()
        cancellation = SubmitCancelled("cancelled")
        with pool._lock:
            del pool._connections[connection.features.shard_id]
            pool._is_replacing = True
        session.submit.side_effect = cancellation

        with pytest.raises(SubmitCancelled):
            pool._submit_regular_replacement(connection, claimed=True)

        assert not pool._is_replacing
        session.cluster.signal_connection_failure.assert_called_once()

    def test_cancelled_regular_replacement_future_hands_off_recovery(self):
        pool, host, session, connection = self.make_pool()
        replacement_future = Future()
        session.submit.return_value = replacement_future

        assert pool._submit_regular_replacement(connection) is \
            replacement_future
        assert pool._is_replacing

        assert replacement_future.cancel()

        assert not pool._is_replacing
        session.cluster.signal_connection_failure.assert_called_once()

    def test_clean_shard_replacement_hands_off_to_host_recovery(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        submitted = Future()
        session.submit.return_value = submitted
        pool._replace(first_connection)
        attempt = pool._shard_connection_attempts[0]

        closed_replacement = self.make_connection()
        closed_replacement.is_closed = True
        session.cluster.connection_factory.return_value = closed_replacement

        pool._open_connection_to_missing_shard(0, attempt['token'])

        assert pool._connections == {}
        assert 0 not in pool._connecting
        assert 0 not in pool._shard_connection_attempts
        closed_replacement.close.assert_called_once_with()
        session.cluster.signal_connection_failure.assert_called_once()
        failure = session.cluster.signal_connection_failure.call_args
        assert failure.kwargs['force'] is True
        assert failure.kwargs['expect_host_to_be_down'] is True

    def test_cancelled_shard_replacement_future_hands_off_recovery(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        replacement_future = Future()
        session.submit.return_value = replacement_future

        pool._replace(first_connection)
        assert 0 in pool._shard_connection_attempts

        assert replacement_future.cancel()

        assert 0 not in pool._shard_connection_attempts
        assert 0 not in pool._connecting
        session.cluster.signal_connection_failure.assert_called_once()

    def test_interrupted_shard_replacement_with_live_shard_hands_off_recovery(
            self):
        class SetupCancelled(BaseException):
            pass

        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        live_connection = self.make_connection(shard_id=1)
        pool._connections[1] = live_connection
        replacement_future = Future()
        session.submit.return_value = replacement_future
        pool._replace(first_connection)
        attempt = pool._shard_connection_attempts[0]
        candidate = self.make_connection(shard_id=0)
        candidate.set_keyspace_blocking.side_effect = SetupCancelled(
            "cancelled replacement setup")
        session.cluster.connection_factory.return_value = candidate

        with pytest.raises(SetupCancelled):
            pool._open_connection_to_missing_shard(
                0,
                attempt['token'])

        assert pool._connections == {1: live_connection}
        assert 0 not in pool._connecting
        assert 0 not in pool._shard_connection_attempts
        candidate.close.assert_called_once_with()
        session.cluster.signal_connection_failure.assert_called_once()

    def test_missing_shard_setup_failure_closes_unadopted_socket(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        connection = self.make_connection(shard_id=1)
        connection.set_keyspace_blocking.side_effect = RuntimeError(
            "USE failed")
        session.cluster.connection_factory.return_value = connection
        pool._connections.pop(0)

        with pytest.raises(RuntimeError, match="USE failed"):
            pool._open_connection_to_missing_shard(1)

        connection.close.assert_called_once_with()
        assert pool._connections == {}
        assert pool._pending_connections == []
        assert 1 not in pool._connecting
        session.cluster.signal_connection_failure.assert_not_called()

    def test_missing_shard_setup_cancellation_cleans_socket_and_marker(self):
        class SetupCancelled(BaseException):
            pass

        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        connection = self.make_connection(shard_id=1)
        connection.set_keyspace_blocking.side_effect = SetupCancelled()
        session.cluster.connection_factory.return_value = connection

        with pytest.raises(SetupCancelled):
            pool._open_connection_to_missing_shard(1)

        connection.close.assert_called_once_with()
        assert pool._pending_connections == []
        assert 1 not in pool._connecting
        assert 1 not in pool._shard_connection_attempts

    def test_shard_submit_cancellation_clears_attempt_marker(self):
        class SubmitCancelled(BaseException):
            pass

        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        session.submit.side_effect = SubmitCancelled()

        with pytest.raises(SubmitCancelled):
            pool._schedule_connection_to_missing_shard(1)

        assert 1 not in pool._connecting
        assert 1 not in pool._shard_connection_attempts

    def test_missing_shard_invalid_keyspace_quarantines_open_socket(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        connection = self.make_connection(shard_id=1)
        connection.set_keyspace_blocking.side_effect = InvalidRequest(
            "keyspace was dropped")
        session.cluster.connection_factory.return_value = connection

        pool._open_connection_to_missing_shard(1)

        assert pool._connections[1] is connection
        assert connection._pool_keyspace_mismatch is True
        assert 1 not in pool._connecting
        assert 1 not in pool._shard_connection_attempts
        connection.close.assert_not_called()
        session.cluster.signal_connection_failure.assert_not_called()
        pool._connections.pop(0)
        session.submit.reset_mock()
        session.cluster.connection_factory.reset_mock()
        for _ in range(2):
            with pytest.raises(NoConnectionsAvailable):
                pool.borrow_connection(timeout=0)
        session.submit.assert_not_called()
        session.cluster.connection_factory.assert_not_called()
        borrowed, _ = pool.borrow_connection(
            timeout=0,
            allow_keyspace_mismatch=True)
        assert borrowed is connection
        pool.return_connection(borrowed)
        pool.shutdown()
        connection.close.assert_called_once_with()

    def test_shard_attempt_tokens_are_identity_safe(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        session.submit.side_effect = lambda *args, **kwargs: Future()

        pool._schedule_connection_to_missing_shard(1)
        first_token = pool._shard_connection_attempts[1]['token']
        pool._finish_shard_connection_attempt(1, first_token)
        pool._schedule_connection_to_missing_shard(1)
        second_token = pool._shard_connection_attempts[1]['token']

        assert second_token is not first_token
        pool._finish_shard_connection_attempt(1, first_token)
        assert pool._shard_connection_attempts[1]['token'] is second_token
        assert 1 in pool._connecting

        pool._finish_shard_connection_attempt(1, second_token)
        assert 1 not in pool._connecting

    def test_shard_failure_and_promotion_are_linearizable(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        session.submit.side_effect = lambda *args, **kwargs: Future()

        # Promotion wins the lock first: the failure remains published for
        # forced host recovery.
        pool._schedule_connection_to_missing_shard(1)
        promoted_token = pool._shard_connection_attempts[1]['token']
        pool._schedule_connection_to_missing_shard(
            1,
            is_replacement=True)
        assert pool._claim_failed_shard_attempt(1, promoted_token)
        assert pool._shard_connection_attempts[1]['token'] is promoted_token
        pool._finish_shard_connection_attempt(1, promoted_token)

        # Failure wins first: it clears only its token, and the later
        # replacement receives a distinct attempt.
        pool._schedule_connection_to_missing_shard(1)
        failed_token = pool._shard_connection_attempts[1]['token']
        assert not pool._claim_failed_shard_attempt(1, failed_token)
        pool._schedule_connection_to_missing_shard(
            1,
            is_replacement=True)
        replacement_token = pool._shard_connection_attempts[1]['token']
        assert replacement_token is not failed_token
        assert pool._shard_connection_attempts[1]['is_replacement']

    def test_ordinary_shard_failure_clears_marker_for_later_retry(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        session.submit.side_effect = lambda *args, **kwargs: Future()
        session.cluster.connection_factory.side_effect = RuntimeError(
            "temporary failure")
        pool._schedule_connection_to_missing_shard(1)
        token = pool._shard_connection_attempts[1]['token']

        with pytest.raises(RuntimeError, match="temporary failure"):
            pool._open_connection_to_missing_shard(1, token)

        assert 1 not in pool._connecting
        assert 1 not in pool._shard_connection_attempts
        session.cluster.signal_connection_failure.assert_not_called()

        session.cluster.connection_factory.side_effect = None
        session.cluster.connection_factory.return_value = self.make_connection(
            shard_id=1)
        pool._schedule_connection_to_missing_shard(1)
        assert 1 in pool._connecting

    def test_failed_shards_schedule_independent_replacements(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        second_connection = self.make_connection(shard_id=1, in_flight=1)
        pool._connections[1] = second_connection
        first_connection.in_flight = 1
        session.submit.side_effect = lambda *args, **kwargs: Future()
        host.signal_connection_failure.return_value = False

        first_connection.is_closed = True
        second_connection.is_closed = True
        pool.return_connection(first_connection)
        pool.return_connection(second_connection)

        assert set(pool._shard_connection_attempts) == {0, 1}
        assert pool._connecting == {0, 1}
        assert session.submit.call_count == 2

    def test_stale_mapping_removal_preserves_new_connection(self):
        pool, host, session, old_connection = self.make_pool(
            shard_aware=True)
        new_connection = self.make_connection(shard_id=0)
        pool._connections[0] = new_connection

        pool._replace(old_connection)
        assert pool._connections[0] is new_connection
        session.cluster.connection_factory.assert_not_called()

        old_connection.in_flight = 1
        old_connection.is_closed = True
        old_connection.signaled_error = True
        pool._trash.add(old_connection)
        pool.return_connection(old_connection)

        assert pool._connections[0] is new_connection
        assert old_connection not in pool._trash
        session.submit.assert_not_called()

    def test_mapped_closed_shard_schedules_repair_and_uses_fallback(self):
        pool, host, session, closed_connection = self.make_pool(
            shard_aware=True)
        fallback = self.make_connection(shard_id=1)
        pool._connections[1] = fallback
        closed_connection.is_closed = True
        host.signal_connection_failure.return_value = False
        session.submit.side_effect = lambda *args, **kwargs: Future()
        token = Mock(value=123)
        session.cluster.metadata.token_map.token_class.from_key.return_value = (
            token)
        host.sharding_info.shard_id_from_token = Mock(return_value=0)

        selected = pool._get_connection_for_routing_key(b"routing-key")

        assert selected is fallback
        attempt = pool._shard_connection_attempts[0]
        assert attempt['is_replacement']
        assert pool.num_missing_or_needing_replacement == 1

    def test_routed_healthy_shard_does_not_scan_all_connections(self):
        pool, host, session, target = self.make_pool(
            shard_aware=True,
            shards_count=64)

        class GetOnlyConnections(dict):

            def values(self):
                raise AssertionError(
                    "healthy routed selection scanned every connection")

        pool._connections = GetOnlyConnections({0: target})
        token = Mock(value=123)
        session.cluster.metadata.token_map.token_class.from_key.return_value = (
            token)
        host.sharding_info.shard_id_from_token = Mock(return_value=0)

        selected = pool._get_connection_for_routing_key(b"routing-key")

        assert selected is target
        session.submit.assert_not_called()

    def test_synchronous_regular_replacement_is_selected_immediately(self):
        pool, host, session, old_connection = self.make_pool()
        old_connection.orphaned_threshold_reached = True
        replacement = self.make_connection(shard_id=1)
        session.cluster.connection_factory.return_value = replacement
        session.submit.side_effect = (
            lambda function, *args, **kwargs:
            function(*args, **kwargs))

        selected = pool._get_connection_for_routing_key()

        assert selected is replacement
        assert pool._connections == {1: replacement}
        old_connection.close.assert_called_once_with()

    def test_synchronous_optional_shard_failure_uses_healthy_fallback(self):
        pool, host, session, fallback = self.make_pool(shard_aware=True)
        token = Mock(value=123)
        session.cluster.metadata.token_map.token_class.from_key.return_value = (
            token)
        host.sharding_info.shard_id_from_token = Mock(return_value=1)
        session.cluster.connection_factory.side_effect = RuntimeError(
            "optional shard failed")
        session.submit.side_effect = (
            lambda function, *args, **kwargs:
            function(*args, **kwargs))

        selected = pool._get_connection_for_routing_key(b"routing-key")

        assert selected is fallback
        assert 1 not in pool._connecting
        session.cluster.signal_connection_failure.assert_not_called()

    def test_last_return_before_retirement_still_closes_old_connection(self):
        pool, host, session, old_connection = self.make_pool(
            shard_aware=True)
        old_connection.in_flight = 1
        old_connection.orphaned_threshold_reached = True

        # The final request returns just before replacement adoption.  At this
        # point the old connection is not yet in trash.
        pool.return_connection(old_connection)
        assert old_connection.in_flight == 0
        assert old_connection not in pool._trash

        replacement = self.make_connection(shard_id=0)
        session.cluster.connection_factory.return_value = replacement
        pool._open_connection_to_missing_shard(0)

        assert pool._connections[0] is replacement
        old_connection.close.assert_called_once_with()
        assert old_connection not in pool._trash

    def test_retired_connection_closes_on_last_return_without_evicting_new(self):
        pool, host, session, old_connection = self.make_pool(
            shard_aware=True)
        old_connection.in_flight = 1
        old_connection.orphaned_threshold_reached = True
        replacement = self.make_connection(shard_id=0)
        session.cluster.connection_factory.return_value = replacement

        pool._open_connection_to_missing_shard(0)

        assert pool._connections[0] is replacement
        assert old_connection in pool._trash
        old_connection.close.assert_not_called()

        pool.return_connection(old_connection)

        assert pool._connections[0] is replacement
        assert old_connection not in pool._trash
        old_connection.close.assert_called_once_with()

    def test_retirement_waits_for_active_continuous_paging_session(self):
        pool, host, session, old_connection = self.make_pool(
            shard_aware=True)
        old_connection.orphaned_threshold_reached = True
        old_connection._continuous_paging_sessions = {7: Mock()}
        replacement = self.make_connection(shard_id=0)
        session.cluster.connection_factory.return_value = replacement

        pool._open_connection_to_missing_shard(0)

        assert pool._connections[0] is replacement
        assert old_connection in pool._trash
        old_connection.close.assert_not_called()

        old_connection._continuous_paging_sessions.clear()
        pool.on_connection_released(old_connection)

        assert old_connection not in pool._trash
        old_connection.close.assert_called_once_with()

    def test_direct_release_closes_idle_retired_connection(self):
        pool, host, session, old_connection = self.make_pool(
            shard_aware=True)
        old_connection.in_flight = 1
        old_connection.orphaned_threshold_reached = True
        replacement = self.make_connection(shard_id=0)
        session.cluster.connection_factory.return_value = replacement

        pool._open_connection_to_missing_shard(0)

        assert old_connection in pool._trash
        old_connection.close.assert_not_called()

        with old_connection.lock:
            old_connection.in_flight -= 1
        pool.on_connection_released(old_connection)

        assert old_connection not in pool._trash
        old_connection.close.assert_called_once_with()

    def test_defunct_retired_connection_does_not_convict_healthy_replacement(
            self):
        pool, host, session, old_connection = self.make_pool(
            shard_aware=True)
        old_connection.in_flight = 1
        old_connection.orphaned_threshold_reached = True
        replacement = self.make_connection(shard_id=0)
        session.cluster.connection_factory.return_value = replacement

        pool._open_connection_to_missing_shard(0)
        old_connection.is_defunct = True
        old_connection.is_closed = False
        pool.return_connection(old_connection)

        assert pool._connections[0] is replacement
        assert old_connection not in pool._trash
        old_connection.close.assert_called_once_with()
        host.signal_connection_failure.assert_not_called()
        session.cluster.on_down.assert_not_called()

    def test_return_racing_deliberate_retirement_does_not_convict_host(self):
        pool, host, session, connection = self.make_pool()
        connection.in_flight = 1
        decremented = Event()
        release_return = Event()
        thread_errors = []

        class PauseAfterDecrementLock(object):

            def __init__(self):
                self._lock = RLock()
                self._exits = 0

            def __enter__(self):
                self._lock.acquire()
                return self

            def __exit__(self, *args):
                self._lock.release()
                self._exits += 1
                if self._exits == 1:
                    decremented.set()
                    assert release_return.wait(2)

        connection.lock = PauseAfterDecrementLock()

        def return_connection():
            try:
                pool.return_connection(connection)
            except BaseException as exc:
                thread_errors.append(exc)

        return_thread = Thread(target=return_connection)
        return_thread.start()
        assert decremented.wait(2)

        with pool._lock:
            del pool._connections[connection.features.shard_id]
            close_connection, remaining = (
                pool._retire_connection_locked(connection))
        assert close_connection
        assert remaining == 0
        connection.is_closed = True
        connection.close()

        release_return.set()
        return_thread.join(2)

        assert not return_thread.is_alive()
        assert thread_errors == []
        host.signal_connection_failure.assert_not_called()
        session.cluster.on_down.assert_not_called()
        session.cluster.signal_connection_failure.assert_not_called()

    def test_blocked_conviction_cannot_down_healthy_replacement(self):
        pool, host, session, connection = self.make_pool()
        connection.in_flight = 1
        connection.is_closed = True
        conviction_entered = Event()
        release_conviction = Event()
        thread_errors = []

        def blocking_conviction(_):
            conviction_entered.set()
            assert release_conviction.wait(2)
            return True

        host.signal_connection_failure.side_effect = blocking_conviction

        def return_connection():
            try:
                pool.return_connection(connection)
            except BaseException as exc:
                thread_errors.append(exc)

        return_thread = Thread(target=return_connection)
        return_thread.start()
        assert conviction_entered.wait(2)

        # In non-shard-aware mode Scylla may assign the replacement a
        # different shard id.
        replacement = self.make_connection(shard_id=1)
        with pool._lock:
            del pool._connections[0]
            pool._connections[1] = replacement

        release_conviction.set()
        return_thread.join(2)

        assert not return_thread.is_alive()
        assert thread_errors == []
        assert not pool.is_shutdown
        assert pool._connections[1] is replacement
        replacement.close.assert_not_called()
        session.cluster.on_down.assert_not_called()

    def test_conviction_base_exception_releases_signal_claim(self):
        class PolicyCancelled(BaseException):
            pass

        pool, host, session, connection = self.make_pool()
        connection.in_flight = 1
        connection.is_closed = True
        connection.last_error = ConnectionShutdown("closed")
        host.signal_connection_failure.side_effect = PolicyCancelled(
            "cancelled policy")

        with pytest.raises(PolicyCancelled):
            pool.return_connection(connection)

        assert not connection.signaled_error
        host.signal_connection_failure.side_effect = None
        host.signal_connection_failure.return_value = False
        pool.return_connection(connection, stream_was_orphaned=True)
        assert host.signal_connection_failure.call_count == 2

    def test_shutdown_closes_adopted_and_retired_connections_once(self):
        pool, host, session, old_connection = self.make_pool(
            shard_aware=True)
        old_connection.in_flight = 1
        old_connection.orphaned_threshold_reached = True
        replacement = self.make_connection(shard_id=0)
        session.cluster.connection_factory.return_value = replacement

        pool._open_connection_to_missing_shard(0)
        assert old_connection in pool._trash

        pool.shutdown()

        replacement.close.assert_called_once_with()
        old_connection.close.assert_called_once_with()
        assert pool._connections == {}
        assert pool._trash == set()

    def test_missing_shard_keyspace_setup_does_not_block_shutdown(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        connection = self.make_connection(shard_id=1)
        keyspace_started = Event()
        release_keyspace = Event()
        connection_closed = Event()

        def set_keyspace(keyspace, timeout=None):
            keyspace_started.set()
            release_keyspace.wait(2)

        def close_connection():
            connection.is_closed = True
            connection_closed.set()
            release_keyspace.set()

        connection.set_keyspace_blocking.side_effect = set_keyspace
        connection.close.side_effect = close_connection
        session.cluster.connection_factory.return_value = connection

        open_thread = Thread(
            target=pool._open_connection_to_missing_shard,
            args=(1,))
        open_thread.start()
        assert keyspace_started.wait(2)
        assert connection in pool._pending_connections

        shutdown_thread = Thread(target=pool.shutdown)
        shutdown_thread.start()
        try:
            assert connection_closed.wait(2)
        finally:
            release_keyspace.set()
            open_thread.join(2)
            shutdown_thread.join(2)

        assert not open_thread.is_alive()
        assert not shutdown_thread.is_alive()
        connection.close.assert_called_once_with()
        assert pool._pending_connections == []

    def test_missing_shard_revalidates_keyspace_before_adoption(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        connection = self.make_connection(shard_id=1)
        seen_keyspaces = []

        def set_keyspace(keyspace, timeout=None):
            seen_keyspaces.append((keyspace, timeout))
            if len(seen_keyspaces) == 1:
                keyspace_set = Mock()
                pool._set_keyspace_for_all_conns("new_keyspace", keyspace_set)
                callbacks = [
                    call.args[1]
                    for call in first_connection.set_keyspace_async.call_args_list
                ]
                assert len(callbacks) == 1
                first_connection.in_flight = 1
                callbacks[0](first_connection, None)
                keyspace_set.assert_called_once_with(pool, [])

        connection.set_keyspace_blocking.side_effect = set_keyspace
        session.cluster.connection_factory.return_value = connection

        pool._open_connection_to_missing_shard(1)

        assert seen_keyspaces == [
            ("foobarkeyspace", session.cluster.connect_timeout),
            ("new_keyspace", session.cluster.connect_timeout),
        ]
        assert pool._connections[1] is connection

    def test_empty_pool_keyspace_update_changes_pool_state(self):
        pool, host, session, first_connection = self.make_pool()
        pool._connections.clear()
        callback = Mock()
        previous_generation = pool._keyspace_generation

        pool._set_keyspace_for_all_conns("new_keyspace", callback)

        assert pool._keyspace == "new_keyspace"
        assert pool._keyspace_generation == previous_generation + 1
        callback.assert_called_once_with(pool, [])

    def test_keyspace_completion_callback_is_exactly_once(self):
        pool, host, session, first_connection = self.make_pool()
        second_connection = self.make_connection(shard_id=1)
        pool._connections[1] = second_connection
        callbacks = {}

        def capture_callback(connection):
            def set_keyspace_async(keyspace, callback):
                connection.in_flight += 1
                callbacks[connection] = callback
            connection.set_keyspace_async.side_effect = set_keyspace_async

        capture_callback(first_connection)
        capture_callback(second_connection)
        finished = Mock()
        pool._set_keyspace_for_all_conns("new_keyspace", finished)

        callbacks[first_connection](first_connection, None)
        callbacks[first_connection](first_connection, RuntimeError("late"))
        callbacks[second_connection](second_connection, None)
        callbacks[second_connection](second_connection, None)

        finished.assert_called_once_with(pool, [])
        assert first_connection.in_flight == 0
        assert second_connection.in_flight == 0
        assert first_connection._pool_keyspace_mismatch is False

    def test_late_keyspace_callback_cannot_clear_newer_quarantine(self):
        pool, host, session, connection = self.make_pool()
        callbacks = []

        def capture_callback(keyspace, callback):
            connection.in_flight += 1
            callbacks.append(callback)

        connection.set_keyspace_async.side_effect = capture_callback
        first_finished = Mock()
        second_finished = Mock()

        pool._set_keyspace_for_all_conns("first_keyspace", first_finished)
        callbacks[0](connection, None)
        pool._set_keyspace_for_all_conns("second_keyspace", second_finished)
        validation_error = InvalidRequest("schema has not converged")
        callbacks[1](connection, validation_error)

        assert connection._pool_keyspace_mismatch is True
        callbacks[0](connection, None)

        assert connection._pool_keyspace_mismatch is True
        first_finished.assert_called_once_with(pool, [])
        second_finished.assert_called_once_with(
            pool,
            [validation_error])

    def test_keyspace_completion_survives_sync_and_return_errors(self):
        pool, host, session, connection = self.make_pool()
        connection.in_flight = 1
        sync_error = RuntimeError("send failed")
        connection.set_keyspace_async.side_effect = sync_error
        pool.return_connection = Mock(side_effect=RuntimeError("return failed"))
        finished = Mock()

        pool._set_keyspace_for_all_conns("new_keyspace", finished)

        finished.assert_called_once()
        errors = finished.call_args.args[1]
        assert errors == [sync_error]
        assert connection._pool_keyspace_mismatch is True
        pool.return_connection.assert_called_once_with(connection)

    def test_keyspace_completion_survives_sync_base_exception(self):
        class SetupCancelled(BaseException):
            pass

        pool, host, session, connection = self.make_pool()
        connection.in_flight = 1
        cancellation = SetupCancelled("cancelled")
        connection.set_keyspace_async.side_effect = cancellation
        finished = Mock()

        pool._set_keyspace_for_all_conns("new_keyspace", finished)

        finished.assert_called_once_with(pool, [cancellation])
        assert connection.in_flight == 0
        assert pool._keyspace_update_current is None
        assert not pool._keyspace_update_in_progress

    def test_keyspace_callback_base_exception_does_not_strand_next_update(
            self):
        class CompletionCancelled(BaseException):
            pass

        pool, host, session, connection = self.make_pool()
        pool._connections = {}
        completions = []
        cancellation = CompletionCancelled("cancelled completion")

        def first_completed(_, errors):
            pool._set_keyspace_for_all_conns(
                "second",
                lambda _, second_errors: completions.append(
                    ("second", second_errors)))
            raise cancellation

        with pytest.raises(CompletionCancelled) as raised:
            pool._set_keyspace_for_all_conns(
                "first",
                first_completed)

        assert raised.value is cancellation
        assert completions == [("second", [])]
        assert not pool._keyspace_update_queue
        assert pool._keyspace_update_current is None
        assert not pool._keyspace_update_in_progress
        assert not pool._keyspace_update_runner_active

    def test_shutdown_completes_keyspace_updates_in_fifo_order(self):
        pool, host, session, connection = self.make_pool()
        callbacks = []
        completions = []

        def delay_keyspace_update(keyspace, callback):
            connection.in_flight += 1
            callbacks.append(callback)

        connection.set_keyspace_async.side_effect = delay_keyspace_update
        pool._set_keyspace_for_all_conns(
            "first",
            lambda _, errors: completions.append(("first", errors)))
        pool._set_keyspace_for_all_conns(
            "second",
            lambda _, errors: completions.append(("second", errors)))

        assert len(callbacks) == 1
        assert completions == []

        pool.shutdown()

        assert [name for name, _ in completions] == ["first", "second"]
        assert all(errors for _, errors in completions)

        # A reactor may deliver the close callback after shutdown returns.
        # It must neither repeat the first completion nor disturb FIFO order.
        callbacks[0](connection, ConnectionShutdown("closed"))
        assert [name for name, _ in completions] == ["first", "second"]

    def test_shutdown_does_not_overtake_active_keyspace_callback(self):
        pool, host, session, connection = self.make_pool()
        connection_callbacks = []
        completions = []
        first_callback_entered = Event()
        release_first_callback = Event()

        def delay_keyspace_update(keyspace, callback):
            connection.in_flight += 1
            connection_callbacks.append(callback)

        def finish_first(_, errors):
            first_callback_entered.set()
            assert release_first_callback.wait(2)
            completions.append(("first", errors))

        connection.set_keyspace_async.side_effect = delay_keyspace_update
        pool._set_keyspace_for_all_conns("first", finish_first)
        pool._set_keyspace_for_all_conns(
            "second",
            lambda _, errors: completions.append(("second", errors)))

        callback_thread = Thread(
            target=connection_callbacks[0],
            args=(connection, None))
        callback_thread.start()
        assert first_callback_entered.wait(2)

        pool.shutdown()
        assert completions == []

        release_first_callback.set()
        callback_thread.join(2)

        assert not callback_thread.is_alive()
        assert [name for name, _ in completions] == ["first", "second"]
        assert completions[1][1]

    def test_last_keyspace_response_wins_before_return_during_shutdown(self):
        pool, host, session, connection = self.make_pool()
        connection_callbacks = []
        completions = []
        return_entered = Event()
        release_return = Event()

        def capture_keyspace_callback(keyspace, callback):
            connection.in_flight += 1
            connection_callbacks.append(callback)

        def block_return(conn):
            return_entered.set()
            assert release_return.wait(2)
            conn.in_flight -= 1

        connection.set_keyspace_async.side_effect = \
            capture_keyspace_callback
        pool.return_connection = block_return
        pool._set_keyspace_for_all_conns(
            "new_keyspace",
            lambda _, errors: completions.append(errors))

        response_thread = Thread(
            target=connection_callbacks[0],
            args=(connection, None))
        response_thread.start()
        assert return_entered.wait(2)

        # The final response removed the final pending connection while
        # holding the update lock. Shutdown must not replace that success
        # with its own error while in_flight is being balanced.
        pool.shutdown()
        assert completions == []

        release_return.set()
        response_thread.join(2)

        assert not response_thread.is_alive()
        assert completions == [[]]

    def test_shutdown_stops_dispatch_to_remaining_connections(self):
        pool, host, session, first_connection = self.make_pool()
        second_connection = self.make_connection(shard_id=1)
        pool._connections[1] = second_connection
        first_dispatch_entered = Event()
        release_first_dispatch = Event()
        completed = Mock()

        def block_first_dispatch(keyspace, callback):
            first_dispatch_entered.set()
            assert release_first_dispatch.wait(2)

        first_connection.set_keyspace_async.side_effect = block_first_dispatch

        dispatch_thread = Thread(
            target=pool._set_keyspace_for_all_conns,
            args=("new_keyspace", completed))
        dispatch_thread.start()
        assert first_dispatch_entered.wait(2)

        pool.shutdown()
        release_first_dispatch.set()
        dispatch_thread.join(2)

        assert not dispatch_thread.is_alive()
        second_connection.set_keyspace_async.assert_not_called()
        completed.assert_called_once()
        assert completed.call_args.args[1]

    def test_forced_handoff_uses_global_lock_order_and_fences_stale_pool(self):
        pool, host, session, first_connection = self.make_pool()
        events = []

        class RecordingLock(object):
            def __init__(self, name):
                self.name = name

            def __enter__(self):
                events.append("enter_" + self.name)

            def __exit__(self, *args):
                events.append("exit_" + self.name)

        session.cluster._lock = RecordingLock("cluster")
        host.lock = RecordingLock("host")
        session._lock = RecordingLock("session")
        session._pools = {host: pool}
        session.is_shutdown = False
        session.cluster.signal_connection_failure.side_effect = (
            lambda *args, **kwargs: events.append("signal"))

        assert pool._handoff_replacement_failure(RuntimeError("failed"))
        # User-extensible conviction callbacks run outside driver locks.
        assert events == [
            "enter_cluster",
            "enter_host",
            "enter_session",
            "exit_session",
            "exit_host",
            "exit_cluster",
            "signal",
        ]

        events[:] = []
        session.cluster.signal_connection_failure.reset_mock()
        session._pools[host] = object()
        assert not pool._handoff_replacement_failure(
            RuntimeError("stale failure"))
        session.cluster.signal_connection_failure.assert_not_called()

    def test_open_all_shards_closes_saved_trash_snapshot(self):
        pool, host, session, first_connection = self.make_pool(
            shard_aware=True)
        trashed = self.make_connection(shard_id=1)
        pool._trash.add(trashed)
        pool._schedule_connection_to_missing_shard = Mock()

        pool._open_connections_for_all_shards(skip_shard_id=0)

        trashed.close.assert_called_once_with()
        assert pool._trash == set()

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
