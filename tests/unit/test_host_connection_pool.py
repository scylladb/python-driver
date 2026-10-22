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
import uuid
from cassandra.protocol_features import ProtocolFeatures

from cassandra.shard_info import _ShardingInfo

import unittest
from threading import Thread, Event, Lock
from unittest.mock import Mock, NonCallableMagicMock, MagicMock, patch

from cassandra.cluster import Session, ShardAwareOptions
from cassandra.connection import Connection, ConnectionBusy, ConnectionException, ConnectionSendError, DefaultEndPoint
from cassandra.pool import HostConnection
from cassandra.pool import Host, NoConnectionsAvailable
from cassandra.policies import HostDistance, SimpleConvictionPolicy
import pytest

from tests.unit.util import HashableMock

LOGGER = logging.getLogger(__name__)


class _ExpectedStaleSocketFailure(Exception):
    pass


@pytest.mark.parametrize('ownership, convicted, postcondition', [
    pytest.param('current', False, None),
    pytest.param('current', True, None),
] + [
    pytest.param(
        ownership, convicted, postcondition,
        marks=pytest.mark.xfail(
            strict=True, raises=_ExpectedStaleSocketFailure,
            reason='#858: stale socket failures still affect current ownership'
        ) if postcondition in expected_failures else ())
    for ownership, convicted, expected_failures in [
        ('draining', False, ('conviction', 'replacement_scheduled',
                             'shard_connection')),
        ('draining', True, ('conviction', 'down', 'pool_open',
                            'shard_connection', 'replacement_open')),
        ('retired_pool', False, ('conviction',)),
        ('retired_pool', True, ('conviction', 'down')),
    ]
    for postcondition in [
        'conviction', 'down', 'replacement_scheduled', 'pool_open',
        'shard_connection', 'replacement_open',
    ]
])
def test_connection_failure_respects_socket_ownership(
        ownership, convicted, postcondition):
    host = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
    host.signal_connection_failure = Mock(return_value=convicted)
    session = NonCallableMagicMock(spec=Session, keyspace=None)
    session.cluster.shard_aware_options.disable = True

    def connection():
        return HashableMock(
            spec=Connection, lock=Lock(), in_flight=0, max_request_id=100,
            is_closed=False, is_defunct=False, signaled_error=False,
            orphaned_threshold_reached=False, orphaned_request_ids=set(),
            keyspace=None, features=ProtocolFeatures(shard_id=0))

    original = connection()
    replacement = connection()
    session.cluster.connection_factory.side_effect = [original, replacement]
    pool = HostConnection(host, HostDistance.LOCAL, session)
    session._pools = {host: pool}
    borrowed, _ = pool.borrow_connection(timeout=0)
    assert borrowed is original
    assert original.in_flight == 1

    if ownership == 'draining':
        # An overloaded socket keeps its outstanding requests after its shard
        # slot has been given to a healthy replacement.
        pool._connections[0] = replacement
        pool._trash.add(original)
    elif ownership == 'retired_pool':
        winner = HostConnection(host, HostDistance.LOCAL, session)
        session._pools = {host: winner}
        pool.shutdown()

    original.is_defunct = True
    original.last_error = ConnectionException('old socket failed', host.endpoint)
    pool.return_connection(original)

    # Even stale completions still release their own connection's accounting.
    assert original.in_flight == 0
    assert replacement.in_flight == 0
    if ownership == 'current':
        host.signal_connection_failure.assert_called_once_with(original.last_error)
        assert pool.is_shutdown is convicted
        if convicted:
            session.cluster.on_down.assert_called_once_with(
                host, is_host_addition=False)
        else:
            session.submit.assert_called_once_with(pool._replace, original)
    else:
        current = session._pools[host]
        try:
            if postcondition == 'conviction':
                host.signal_connection_failure.assert_not_called()
            elif postcondition == 'down':
                session.cluster.on_down.assert_not_called()
            elif postcondition == 'replacement_scheduled':
                session.submit.assert_not_called()
            elif postcondition == 'pool_open':
                assert not current.is_shutdown
            elif postcondition == 'shard_connection':
                assert current._connections.get(0) is replacement
            elif postcondition == 'replacement_open':
                replacement.close.assert_not_called()
        except AssertionError as error:
            raise _ExpectedStaleSocketFailure from error


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

    def test_init_logs_none_shard_id(self):
        """shard_id is None off Scylla; the rendered debug line must say so."""
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100)
        conn.features = ProtocolFeatures(shard_id=None, sharding_info=None)
        session.cluster.connection_factory.return_value = conn

        with self.assertLogs('cassandra.pool', level='DEBUG') as logs:
            self.PoolImpl(host, HostDistance.LOCAL, session)

        shard_logs = [r.getMessage() for r in logs.records
                      if "First connection created" in r.getMessage()]
        self.assertTrue(shard_logs, "no connection initialization log was emitted")
        self.assertTrue(all(m.endswith("for shard_id=None") for m in shard_logs), shard_logs)

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

    def test_replace_only_does_not_mark_host_down(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100,
                            signaled_error=False)
        session.cluster.connection_factory.return_value = conn
        pool = self.PoolImpl(host, HostDistance.LOCAL, session)

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        conn.signaled_error = True  # Other callbacks may return before replacement.
        pool.return_connection(conn, replace_only=True)

        host.signal_connection_failure.assert_not_called()
        assert conn.signaled_error
        assert not pool.is_shutdown
        session.submit.assert_called_once_with(pool._replace, conn)

    def test_signaled_error_still_shuts_down_when_configured(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100,
                            signaled_error=False)
        session.cluster.connection_factory.return_value = conn
        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        pool.shutdown_on_error = True

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        conn.signaled_error = True
        conn.last_error = ConnectionException('remote failure')
        pool.return_connection(conn)

        assert pool.is_shutdown

    def test_local_send_error_skips_shutdown_when_configured(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100,
                            signaled_error=False)
        session.cluster.connection_factory.return_value = conn
        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        pool.shutdown_on_error = True

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        conn.signaled_error = True
        conn.last_error = ConnectionSendError('local queue failure')
        pool.return_connection(conn)
        pool.return_connection(conn, replace_only=True)

        assert not pool.is_shutdown
        host.signal_connection_failure.assert_not_called()
        session.cluster.on_down.assert_not_called()
        session.submit.assert_called_once_with(pool._replace, conn)

    def test_heartbeat_failure_shuts_down_after_local_send_error(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100,
                            signaled_error=False)
        session.cluster.connection_factory.return_value = conn
        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        pool.shutdown_on_error = True

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        conn.signaled_error = True
        conn.last_error = ConnectionSendError('local queue failure')
        pool.return_connection(conn, heartbeat_failed=True)

        assert pool.is_shutdown
        host.signal_connection_failure.assert_not_called()
        session.cluster.on_down.assert_called_once_with(host, is_host_addition=False)

    def test_return_defunct_connection_on_down_host(self):
        host = Mock(spec=Host, address='ip1')
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
            assert session.submit.called
        else:
            assert not session.submit.called
            assert session.cluster.signal_connection_failure.call_args
        assert pool.is_shutdown

    def test_return_closed_connection(self):
        host = Mock(spec=Host, address='ip1')
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

        for host_id in (None, "not-a-uuid", object()):
            with pytest.raises(
                    TypeError, match=r"^host_id must be a uuid\.UUID$"):
                Host('127.0.0.1', SimpleConvictionPolicy, host_id=host_id)

        with pytest.raises(ValueError):
            Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.UUID(int=0))

        with pytest.raises(
                TypeError, match=r"^host_id must be a uuid\.UUID$"):
            Host('127.0.0.1', SimpleConvictionPolicy)

    def test_host_equality(self):
        """
        Test host equality has correct logic
        """

        shared_id = uuid.uuid4()
        a = Host('127.0.0.1', SimpleConvictionPolicy, host_id=shared_id)
        b = Host('127.0.0.2', SimpleConvictionPolicy, host_id=shared_id)
        c = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())

        assert a == b, 'Two Host instances with the same host ID should be equal.'
        assert a != c, 'Two Host instances with different host IDs should not be equal.'
        assert a != a.address, 'A Host should not compare equal to its address.'

    def test_host_id_is_read_only(self):
        host_id = uuid.uuid4()
        host = Host('127.0.0.1', SimpleConvictionPolicy, host_id=host_id)

        with pytest.raises(AttributeError):
            host.host_id = uuid.uuid4()

        assert host.host_id == host_id

    def test_host_hash_is_stable_when_endpoint_changes(self):
        host = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
        hosts_by_id = {host: "pool"}
        hosts = {host}

        host.endpoint = DefaultEndPoint('127.0.0.2')

        assert hosts_by_id[host] == "pool"
        assert host in hosts

    def test_host_ordering_uses_host_id(self):
        first = Host('127.0.0.2', SimpleConvictionPolicy, host_id=uuid.UUID(int=1))
        second = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.UUID(int=2))

        assert first < second

    def test_host_id_is_set_before_conviction_policy_is_created(self):
        host_id = uuid.UUID(int=1)
        hosts = set()

        def conviction_policy_factory(host):
            assert host.host_id == host_id
            hosts.add(host)
            return SimpleConvictionPolicy(host)

        host = Host(
            '127.0.0.1', conviction_policy_factory, host_id=host_id)

        assert host in hosts


class HostConnectionTests(_PoolTests):
    __test__ = True
    PoolImpl = HostConnection
    uses_single_connection = True

    def test_replacement_connection_uses_pool_endpoint(self):
        host = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
        session = self.make_session()
        session.keyspace = None
        first = HashableMock(spec=Connection)
        first.features.shard_id = 0
        first.features.sharding_info = None
        first.features.tablets_routing_v1 = False
        first.features.tablets_routing_v2 = False
        replacement = HashableMock(spec=Connection)
        replacement.features.shard_id = 0
        session.cluster.connection_factory.side_effect = [first, replacement]
        pool = HostConnection(host, HostDistance.LOCAL, session)

        original_endpoint = host.endpoint
        host.endpoint = DefaultEndPoint('127.0.0.2')
        pool._replace(first)

        assert pool.endpoint == original_endpoint
        assert session.cluster.connection_factory.call_args.args[0] == original_endpoint
        pool.shutdown()

    def test_borrow_rejects_unsynchronized_connection_without_waiting(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100, lock=Lock())
        session.cluster.connection_factory.return_value = conn
        pool = HostConnection(host, HostDistance.LOCAL, session)
        pool._keyspace = 'newks'
        conn.keyspace = 'foobarkeyspace'
        conn.set_keyspace_blocking.side_effect = \
            lambda keyspace: setattr(conn, 'keyspace', keyspace)
        with patch.object(pool._stream_available_condition, 'wait',
                          side_effect=AssertionError('borrow blocked the reactor')):
            with pytest.raises(NoConnectionsAvailable, match='current session keyspace'):
                pool.borrow_connection(timeout=1)

        pool._sync_keyspace(conn)
        borrowed, _ = pool.borrow_connection(timeout=1)
        assert borrowed is conn
        pool.return_connection(borrowed)

    def test_failed_keyspace_change_can_be_corrected_by_use(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100, lock=Lock())
        session.cluster.connection_factory.return_value = conn
        pool = HostConnection(host, HostDistance.LOCAL, session)

        errors = []
        pool._set_keyspace_for_all_conns(
            'missing_ks', lambda _pool, pool_errors: errors.extend(pool_errors))
        conn.in_flight = 1
        conn.set_keyspace_async.call_args.args[1](
            conn, ConnectionException('keyspace unavailable', host.endpoint))
        assert errors
        assert conn.keyspace == 'foobarkeyspace'
        with pytest.raises(NoConnectionsAvailable):
            pool.borrow_connection(timeout=0)

        borrowed, _ = pool.borrow_connection(
            timeout=0, allow_keyspace_mismatch=True)
        assert borrowed is conn
        pool.return_connection(borrowed)

    def test_failed_keyspace_send_still_completes_pool_callback(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100, lock=Lock())
        session.cluster.connection_factory.return_value = conn
        pool = HostConnection(host, HostDistance.LOCAL, session)
        error = ConnectionBusy('send blocked')
        conn.set_keyspace_async.side_effect = error
        callback = Mock()

        pool._set_keyspace_for_all_conns('newks', callback)

        callback.assert_called_once_with(pool, [error])
        assert conn.in_flight == 0
        assert pool._keyspace == 'newks'

    def test_stale_shard_uses_aligned_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        stale = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                             is_closed=False, max_request_id=100, lock=Lock())
        stale.features.shard_id = 0
        session.cluster.connection_factory.return_value = stale
        pool = HostConnection(host, HostDistance.LOCAL, session)
        aligned = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                               is_closed=False, max_request_id=100, lock=Lock())
        aligned.features.shard_id = 1
        aligned.keyspace = 'newks'
        pool._connections[1] = aligned
        pool._keyspace = 'newks'
        pool._get_connection_for_routing_key = Mock(return_value=stale)

        borrowed, _ = pool.borrow_connection(timeout=0)
        assert borrowed is aligned
        pool.return_connection(borrowed)

    def test_published_connection_catches_newer_keyspace(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100, lock=Lock())
        session.cluster.connection_factory.return_value = conn
        pool = HostConnection(host, HostDistance.LOCAL, session)
        conn.keyspace = None

        def set_keyspace(keyspace):
            if keyspace == 'foobarkeyspace':
                pool._keyspace = 'newks'
            conn.keyspace = keyspace

        conn.set_keyspace_blocking.side_effect = set_keyspace
        pool._sync_keyspace(conn)
        assert [call.args[0] for call in conn.set_keyspace_blocking.call_args_list[-2:]] == \
            ['foobarkeyspace', 'newks']
        assert conn.keyspace == pool._keyspace

    def test_async_keyspace_reply_reapplies_current_target(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100, lock=Lock())
        session.cluster.connection_factory.return_value = conn
        pool = HostConnection(host, HostDistance.LOCAL, session)
        finished = []

        pool._set_keyspace_for_all_conns(
            'older', lambda _pool, errors: finished.append(errors))
        older_callback = conn.set_keyspace_async.call_args.args[1]
        pool._keyspace = 'newer'
        conn.keyspace = 'older'
        conn.in_flight = 1
        older_callback(conn, None)

        assert not finished
        assert conn.in_flight == 0
        assert conn.set_keyspace_async.call_args.args[0] == 'newer'
        newer_callback = conn.set_keyspace_async.call_args.args[1]
        conn.keyspace = 'newer'
        conn.in_flight = 1
        newer_callback(conn, None)
        assert finished == [[]]
        assert conn.in_flight == 0
        assert conn.keyspace == pool._keyspace

    def test_failed_replacement_keyspace_sync_removes_connection(self):
        host = Mock(spec=Host, address='ip1', sharding_info=None)
        session = self.make_session()
        session.cluster.shard_aware_options.disable = True
        old_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                is_closed=False, max_request_id=100, lock=Lock())
        old_conn.features.shard_id = 0
        old_conn.features.sharding_info = None
        new_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                is_closed=False, max_request_id=100, lock=Lock())
        new_conn.features.shard_id = 0
        session.cluster.connection_factory.side_effect = [old_conn, new_conn]
        pool = HostConnection(host, HostDistance.LOCAL, session)

        def set_keyspace(keyspace):
            if keyspace == 'foobarkeyspace':
                new_conn.keyspace = keyspace
                pool._keyspace = 'newks'
            else:
                raise ConnectionException('sync failed', host.endpoint)

        new_conn.set_keyspace_blocking.side_effect = set_keyspace
        pool._replace(old_conn)

        assert 0 not in pool._connections
        new_conn.close.assert_called_once()
        session.submit.assert_called_once_with(pool._replace, new_conn)

    def test_failed_shard_keyspace_sync_allows_retry(self):
        host = Mock(spec=Host, address='ip1', sharding_info=None)
        session = self.make_session()
        first_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                  is_closed=False, max_request_id=100, lock=Lock())
        first_conn.features.shard_id = 0
        first_conn.features.sharding_info = None
        shard_conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                                  is_closed=False, max_request_id=100, lock=Lock())
        shard_conn.features.shard_id = 1
        session.cluster.connection_factory.side_effect = [first_conn, shard_conn]
        pool = HostConnection(host, HostDistance.LOCAL, session)
        host.sharding_info = Mock(shards_count=2)
        pool._get_shard_aware_endpoint = Mock(return_value=None)
        pool._connecting.add(1)

        def set_keyspace(keyspace):
            if keyspace == 'foobarkeyspace':
                shard_conn.keyspace = keyspace
                pool._keyspace = 'newks'
            else:
                raise ConnectionException('sync failed', host.endpoint)

        shard_conn.set_keyspace_blocking.side_effect = set_keyspace
        with pytest.raises(ConnectionException, match='sync failed'):
            pool._open_connection_to_missing_shard(1)

        assert 1 not in pool._connections
        assert 1 not in pool._connecting
        shard_conn.close.assert_called_once()

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
