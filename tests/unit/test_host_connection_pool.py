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
from cassandra.protocol_features import ProtocolFeatures

from cassandra.shard_info import _ShardingInfo

import unittest
from threading import Barrier, Thread, Event, Lock, RLock
from unittest.mock import Mock, NonCallableMagicMock, MagicMock, patch

from cassandra import AuthenticationFailed
from cassandra.cluster import Cluster, Session, ShardAwareOptions
from cassandra._lifecycle import ConnectionFailureReservation, PoolOwnershipReservation
from cassandra.connection import (ClientRoutesEndPoint, Connection, ConnectionBusy,
                                  ConnectionException, ConnectionSendError,
                                  ConnectionShutdown,
                                  DefaultEndPoint, SniEndPoint, UnixSocketEndPoint)
from cassandra.pool import HostConnection
from cassandra.pool import Host, NoConnectionsAvailable
from cassandra.policies import HostDistance, SimpleConvictionPolicy
import pytest

from tests.unit.util import HashableMock
from tests.unit.utils import move_host, new_session_with_pool_state

LOGGER = logging.getLogger(__name__)


def _make_socket_pool(session=None):
    host = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
    host.signal_connection_failure = Mock(return_value=False)
    if session is None:
        session = NonCallableMagicMock(
            spec=Session, keyspace=None, _lock=RLock(), is_shutdown=False)
    session.cluster.is_shutdown = False
    session.cluster.shard_aware_options.disable = True

    def connection():
        conn = HashableMock(
            spec=Connection, lock=Lock(), in_flight=0, max_request_id=100,
            is_closed=False, is_defunct=False, signaled_error=False,
            orphaned_threshold_reached=False, orphaned_request_ids=set(),
            keyspace=None, features=ProtocolFeatures(shard_id=0))
        conn.set_keyspace_blocking.side_effect = \
            lambda keyspace: setattr(conn, 'keyspace', keyspace)
        return conn

    original = connection()
    replacement = connection()
    session.cluster.connection_factory.side_effect = [original, replacement]
    pool = HostConnection(host, HostDistance.LOCAL, session)
    session._pools = {host: pool}
    return host, session, pool, original, replacement


@pytest.fixture
def socket_pool():
    return _make_socket_pool()


@pytest.mark.parametrize('ownership', [
    'current', 'draining', 'retired_pool', 'unpublished_pool',
    'retired_host', 'moved_endpoint', 'shutdown_session',
])
@pytest.mark.parametrize('convicted', [False, True])
@pytest.mark.parametrize('shutdown_on_error', [False, True])
def test_connection_failure_respects_socket_ownership(
        socket_pool, ownership, convicted, shutdown_on_error):
    host, session, pool, original, replacement = socket_pool
    host.signal_connection_failure.return_value = convicted
    pool.shutdown_on_error = shutdown_on_error
    borrowed, _ = pool.borrow_connection(timeout=0)
    assert borrowed is original
    assert original.in_flight == 1

    if ownership == 'draining':
        # An overloaded socket keeps its outstanding requests after its shard
        # slot has been given to a healthy replacement.
        pool._connections[0] = replacement
        pool._trash.add(original)
    elif ownership in ('retired_pool', 'unpublished_pool'):
        winner = HostConnection(host, HostDistance.LOCAL, session)
        session._pools = {host: winner}
        if ownership == 'retired_pool':
            pool.shutdown()
    elif ownership == 'retired_host':
        host._is_removed = True
    elif ownership == 'moved_endpoint':
        moved = move_host(host, DefaultEndPoint('127.0.0.2'))
        winner = HostConnection(moved, HostDistance.LOCAL, session)
        session._pools = {moved: winner}
    elif ownership == 'shutdown_session':
        session.is_shutdown = True

    original.is_defunct = True
    original.last_error = ConnectionException('old socket failed', host.endpoint)
    pool.return_connection(original)

    # Even stale completions still release their own connection's accounting.
    assert original.in_flight == 0
    assert replacement.in_flight == 0
    if ownership == 'current':
        host.signal_connection_failure.assert_called_once_with(original.last_error)
        assert pool.is_shutdown is (convicted or shutdown_on_error)
        if convicted or shutdown_on_error:
            session.cluster.on_down.assert_called_once()
            args, kwargs = session.cluster.on_down.call_args
            assert args == (host,)
            assert kwargs['is_host_addition'] is False
        else:
            session.submit.assert_called_once_with(pool._replace, original)
    else:
        current = session._pools[host]
        host.signal_connection_failure.assert_not_called()
        session.cluster.on_down.assert_not_called()
        session.submit.assert_not_called()
        assert not current.is_shutdown
        if ownership in ('draining', 'retired_pool', 'unpublished_pool', 'moved_endpoint'):
            assert current._connections.get(0) is replacement
            replacement.close.assert_not_called()


@pytest.mark.parametrize('convicted', [False, True])
@pytest.mark.parametrize('change', ['socket', 'pool', 'endpoint', 'retirement'])
def test_failure_policy_cannot_affect_replacement(socket_pool, convicted, change):
    host, session, pool, original, replacement = socket_pool
    pool.borrow_connection(timeout=0)
    original.is_defunct = True
    original.last_error = ConnectionException('socket failed', host.endpoint)
    changed = Event()
    errors = []

    def replace():
        try:
            winner = (HostConnection(host, HostDistance.LOCAL, session)
                      if change == 'pool' else None)
            with original.lock, session._lock, host.lock, pool._lock:
                if change == 'socket':
                    pool._connections[0] = replacement
                    pool._trash.add(original)
                elif change == 'pool':
                    session._pools[host] = winner
                elif change == 'endpoint':
                    moved = move_host(host, DefaultEndPoint('127.0.0.2'))
                    winner = HostConnection(moved, HostDistance.LOCAL, session)
                    session._pools = {moved: winner}
                else:
                    host._is_removed = True
        except Exception as error:
            errors.append(error)
        finally:
            changed.set()

    worker = Thread(target=replace)

    def convict(error):
        worker.start()
        assert changed.wait(5), 'policy ran while ownership locks were held'
        assert not errors
        return convicted

    host.signal_connection_failure.side_effect = convict
    try:
        pool.return_connection(original)
    finally:
        worker.join(5)
    assert not worker.is_alive()
    assert original.in_flight == 0
    session.cluster.on_down.assert_not_called()
    session.submit.assert_not_called()
    assert not session._pools[host].is_shutdown
    if change in ('socket', 'pool', 'endpoint'):
        assert session._pools[host]._connections[0] is replacement
        replacement.close.assert_not_called()


@pytest.mark.parametrize('change', ['pool', 'endpoint', 'health'])
def test_down_admission_rechecks_failed_socket(socket_pool, change):
    host, session, pool, original, replacement = socket_pool
    cluster = Cluster(contact_points=[])
    session.cluster = cluster
    cluster.sessions = (session,)
    cluster.profile_manager = Mock()
    cluster.on_down_potentially_blocking = Mock(return_value=Future())
    host.set_up()
    host.signal_connection_failure.return_value = True
    pool.borrow_connection(timeout=0)
    original.is_defunct = True
    original.last_error = ConnectionException('socket failed', host.endpoint)

    def distance(current):
        with session._lock, host.lock, pool._lock:
            if change == 'pool':
                session._pools[host] = Mock(
                    host=host, endpoint=host.endpoint, is_shutdown=False)
            elif change == 'endpoint':
                move_host(host, DefaultEndPoint('127.0.0.2'))
            else:
                host._health_generation += 1
        return HostDistance.IGNORED

    cluster.profile_manager.distance.side_effect = distance
    try:
        pool.return_connection(original)
        assert host.is_up is (change != 'endpoint')
        assert pool.is_shutdown
        if change == 'pool':
            assert not session._pools[host].is_shutdown
        replacement.close.assert_not_called()
        cluster.on_down_potentially_blocking.assert_not_called()
    finally:
        pool.shutdown()
        cluster.shutdown()


@pytest.mark.parametrize('change', ['pool', 'endpoint', 'host', 'session', 'cluster'])
def test_retirement_cleanup_cannot_down_superseding_owner(socket_pool, change):
    host, session, pool, original, replacement = socket_pool
    host.signal_connection_failure.return_value = True
    pool.borrow_connection(timeout=0)
    original.is_defunct = True
    successor = Mock(is_shutdown=False)

    def supersede():
        # Socket cleanup is outside ownership guards, so it can synchronously
        # change topology or publish a successor before DOWN inspection starts.
        with session._lock, host.lock:
            assert pool._lock.acquire(False), 'cleanup retained the pool work lock'
            try:
                if change == 'pool':
                    session._pools[host] = successor
                elif change == 'endpoint':
                    move_host(host, DefaultEndPoint('127.0.0.2'))
                elif change == 'host':
                    host._is_removed = True
                elif change == 'session':
                    session.is_shutdown = True
                else:
                    session.cluster.is_shutdown = True
            finally:
                pool._lock.release()

    original.close.side_effect = supersede
    pool.return_connection(original)
    assert pool.is_shutdown
    assert original.in_flight == 0
    original.close.assert_called_once()
    session.cluster.on_down.assert_not_called()
    replacement.close.assert_not_called()
    successor.shutdown.assert_not_called()


@pytest.mark.parametrize('retirement', ['owned', 'generic', 'already_shutdown', 'stale', 'other_failure'])
def test_only_own_guarded_retirement_can_admit_down(socket_pool, retirement):
    host, session, pool, original, replacement = socket_pool
    cluster = Cluster(contact_points=[])
    session.cluster = cluster
    cluster.sessions = (session,)
    cluster.profile_manager.distance = Mock(return_value=HostDistance.IGNORED)
    cluster.on_down_potentially_blocking = Mock(return_value=Future())
    host.set_up()
    failure = ConnectionFailureReservation(pool, original)
    try:
        if retirement == 'generic':
            pool.shutdown()
        elif retirement == 'already_shutdown':
            pool.shutdown()
            pool.shutdown(_failure=failure)
        elif retirement == 'stale':
            pool._connections[0] = replacement
            pool.shutdown(_failure=failure)
        elif retirement == 'other_failure':
            pool.shutdown(_failure=ConnectionFailureReservation(pool, original))
            pool.shutdown(_failure=failure)
        else:
            pool.shutdown(_failure=failure)

        admitted = cluster.on_down(host, False, _pool_failure=failure)
        assert admitted is (retirement == 'owned')
        assert host.is_up is (retirement != 'owned')
        assert cluster.on_down_potentially_blocking.call_count == int(retirement == 'owned')
    finally:
        pool.shutdown()
        cluster.shutdown()


def test_racing_first_callbacks_preserve_convicting_owner(socket_pool):
    host, session, pool, original, _ = socket_pool
    original.in_flight = 2
    original.is_defunct = True
    start_callbacks = Barrier(3)
    policy_entered, release_policy, duplicate_done = Event(), Event(), Event()
    errors = []

    def convict(error):
        policy_entered.set()
        assert release_policy.wait(5)
        return True

    def fail():
        try:
            start_callbacks.wait(timeout=5)
            pool.return_connection(original)
        except BaseException as exc:
            errors.append(exc)
        finally:
            duplicate_done.set()

    host.signal_connection_failure.side_effect = convict
    workers = (Thread(target=fail), Thread(target=fail))
    try:
        for worker in workers:
            worker.start()
        start_callbacks.wait(timeout=5)
        assert policy_entered.wait(5)
        assert duplicate_done.wait(2), 'duplicate callback waited for conviction'
        assert pool._connections[0] is original
    finally:
        release_policy.set()
        start_callbacks.abort()
        for worker in workers:
            if worker.ident is not None:
                worker.join(5)
        pool.shutdown()
    assert not errors
    assert all(not worker.is_alive() for worker in workers)
    assert original.in_flight == 0
    host.signal_connection_failure.assert_called_once()
    session.cluster.on_down.assert_called_once()
    session.submit.assert_not_called()


@pytest.mark.parametrize('independent_pool', [False, True])
def test_failed_pool_cannot_discount_its_own_down(independent_pool):
    session = new_session_with_pool_state()
    session.cluster = Mock()
    session.keyspace = None
    session.is_shutdown = False
    host, session, pool, original, sibling = _make_socket_pool(session)
    sibling.features = ProtocolFeatures(shard_id=1)
    pool._connections[1] = sibling

    other_session = new_session_with_pool_state()
    other_session.cluster = session.cluster
    other_session.keyspace = None
    other_session.is_shutdown = False
    healthy = HashableMock(
        spec=Connection, in_flight=0, is_closed=False, is_defunct=False,
        orphaned_request_ids=set(), features=ProtocolFeatures(shard_id=0))
    session.cluster.connection_factory.side_effect = [healthy]
    other_pool = HostConnection(host, HostDistance.LOCAL, other_session)
    other_session._pools[host] = other_pool

    cluster = Cluster(contact_points=[])
    session.cluster = other_session.cluster = cluster
    cluster.sessions = ((session, other_session) if independent_pool
                        else (session,))
    cluster.profile_manager.distance = Mock(return_value=HostDistance.LOCAL)
    cluster.on_down_potentially_blocking = Mock(return_value=Future())
    host.set_up()
    host.signal_connection_failure.return_value = True
    original.in_flight = 1
    original.is_defunct = True
    original.last_error = ConnectionException('one shard failed', host.endpoint)
    try:
        assert cluster._discount_down_events
        assert session.get_pool_state()[host]['open_count'] == 1
        pool.return_connection(original)
        assert original.in_flight == 0
        assert pool.is_shutdown
        assert host.is_up is independent_pool
        assert (cluster.on_down_potentially_blocking.call_count ==
                (0 if independent_pool else 1))
        assert not other_pool.is_shutdown
        healthy.close.assert_not_called()
    finally:
        pool.shutdown()
        other_pool.shutdown()
        cluster.shutdown()


@pytest.fixture
def discounted_pool_recovery(request):
    """Retire one pool while a second session keeps the host available."""
    session = new_session_with_pool_state()
    session.cluster = Mock()
    session.keyspace = None
    session.is_shutdown = False
    host, session, pool, original, replacement = _make_socket_pool(session)
    other_session = new_session_with_pool_state()
    other_session.cluster = session.cluster
    other_session.keyspace = None
    other_session.is_shutdown = False
    healthy = HashableMock(
        spec=Connection, in_flight=0, is_closed=False, is_defunct=False,
        orphaned_request_ids=set(), features=ProtocolFeatures(shard_id=0))
    session.cluster.connection_factory.side_effect = [healthy]
    other_pool = HostConnection(host, HostDistance.LOCAL, other_session)
    other_session._pools[host] = other_pool

    cluster = Cluster(contact_points=[])
    try:
        session.cluster = other_session.cluster = cluster
        cluster.sessions = (session, other_session)
        cluster.metadata.add_or_return_host(host)
        cluster.shard_aware_options.disable = True
        cluster.profile_manager.distance = Mock(
            return_value=HostDistance.LOCAL)
        session._profile_manager = cluster.profile_manager
        cluster.on_down_potentially_blocking = Mock(return_value=Future())
        cluster.connection_factory = Mock(return_value=replacement)
        cluster.reconnection_policy = Mock()
        # Reconnection policies may return an iterable rather than an iterator.
        cluster.reconnection_policy.new_schedule.return_value = [1, 2, 4]
        scheduled = []
        cluster.scheduler.schedule = Mock(
            side_effect=lambda delay, fn: scheduled.append((delay, fn)))
        work = []

        def submit(fn, *args, **kwargs):
            """Queue work so tests can control creation and completion."""
            future = Future()
            work.append((fn, args, kwargs, future))
            return future

        session.submit = Mock(side_effect=submit)
        initial_state = getattr(request, 'param', True)
        if initial_state is True:
            host.set_up()
        host.signal_connection_failure.return_value = True
        original.in_flight = 1
        original.is_defunct = True
        original.last_error = ConnectionException(
            'failed socket', host.endpoint)
        pool.return_connection(original)
        assert pool.is_shutdown
        assert host.is_up is initial_state
        assert len(scheduled) == 1
        cluster.on_down_potentially_blocking.assert_not_called()
        yield host, session, pool, other_pool, scheduled, work
    finally:
        cluster.shutdown()


def _finish_pool_recovery_work(work):
    """Run one queued task and notify its completion callbacks."""
    fn, args, kwargs, future = work.pop(0)
    future.set_result(fn(*args, **kwargs))


def test_discounted_down_recovers_only_failed_session(
        discounted_pool_recovery):
    """Restore the retired pool without closing the healthy peer pool."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    delay, recover = scheduled.pop(0)
    assert delay == 1
    recover()
    assert len(work) == 1
    _finish_pool_recovery_work(work)
    assert session._pools[host] is not failed
    assert not session._pools[host].is_shutdown
    assert not healthy.is_shutdown
    assert host.is_up
    assert not scheduled


@pytest.mark.parametrize('creation_error', [
    ConnectionException, AuthenticationFailed,
])
def test_discounted_down_retries_failed_pool_creation(
        discounted_pool_recovery, creation_error):
    """Keep retrying a transient creation failure with the next delay."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    session.cluster.connection_factory.side_effect = [
        creation_error('still unavailable'),
        session.cluster.connection_factory.return_value]
    scheduled.pop(0)[1]()
    _finish_pool_recovery_work(work)
    assert session._pools[host] is failed
    assert host.is_up
    assert len(scheduled) == 1
    delay, recover = scheduled.pop(0)
    assert delay == 2
    recover()
    _finish_pool_recovery_work(work)
    assert session._pools[host] is not failed
    assert not session._pools[host].is_shutdown
    assert not healthy.is_shutdown
    assert not scheduled


@pytest.mark.parametrize('discounted_pool_recovery', [None], indirect=True)
def test_discounted_down_recovers_unknown_host(discounted_pool_recovery):
    """Recover a usable host whose health has not been marked up yet."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    assert host.is_up is None
    scheduled.pop(0)[1]()
    _finish_pool_recovery_work(work)
    assert session._pools[host] is not failed
    assert not session._pools[host].is_shutdown
    assert not healthy.is_shutdown
    assert not scheduled


@pytest.mark.parametrize('failure', ['is_closed', 'is_defunct'])
def test_discounted_pool_recovery_retries_unusable_candidate(
        discounted_pool_recovery, failure):
    """Retain the failed-pool token when its candidate has no usable socket."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    candidate = session.cluster.connection_factory.return_value
    setattr(candidate, failure, True)
    scheduled.pop(0)[1]()
    _finish_pool_recovery_work(work)
    assert session._pools[host] is failed
    assert not healthy.is_shutdown
    assert host.is_up
    candidate.close.assert_called()
    assert len(scheduled) == 1
    assert scheduled[0][0] == 2


@pytest.mark.parametrize('caller', ['reconcile', 'direct_up', 'direct_add'])
@pytest.mark.parametrize('failure', ['is_closed', 'is_defunct'])
def test_retired_pool_renewal_rejects_unusable_candidate(
        discounted_pool_recovery, caller, failure):
    """Reject unusable renewals outside the session recovery callback."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    cluster = session.cluster
    # Leave the delayed recovery untouched; use ordinary creation entry points.
    candidate = cluster.connection_factory.return_value
    setattr(candidate, failure, True)
    cluster.signal_connection_failure = Mock(
        wraps=cluster.signal_connection_failure)

    def renew():
        """Use reconciliation or the direct creation used by UP and ADD."""
        if caller == 'reconcile':
            futures = session.update_created_pools()
            assert len(futures) == 1
            return futures.pop()
        return session.add_or_renew_pool(
            host, is_host_addition=caller == 'direct_add')

    completion = renew()
    assert len(work) == 1
    _finish_pool_recovery_work(work)
    assert completion.result(timeout=0) is False
    assert session._pools[host] is failed
    assert not healthy.is_shutdown
    candidate.close.assert_called_once()
    cluster.signal_connection_failure.assert_called_once()
    args, kwargs = cluster.signal_connection_failure.call_args
    assert args[0] is host
    assert isinstance(args[1], ConnectionShutdown)
    assert args[2] is (caller == 'direct_add')
    assert kwargs['expect_host_to_be_down'] is True
    cluster.on_down_potentially_blocking.assert_not_called()
    assert host.is_up
    # A normal caller still succeeds when its next candidate is usable.
    setattr(candidate, failure, False)
    completion = renew()
    _finish_pool_recovery_work(work)
    assert completion.result(timeout=0) is True
    assert session._pools[host] is not failed
    assert not session._pools[host].is_shutdown
    assert not healthy.is_shutdown
    cluster.signal_connection_failure.assert_called_once()
    assert len(scheduled) == 1


@pytest.mark.parametrize('removal_point', ['before_reconcile', 'distance'])
def test_discounted_recovery_cannot_reopen_removed_pool(
        discounted_pool_recovery, removal_point):
    """Stop when removal races recovery admission or distance inspection."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery

    def remove_failed_pool(*args):
        """Remove the pool at the selected reconciliation boundary."""
        session.remove_pool(host)
        return HostDistance.LOCAL

    if removal_point == 'before_reconcile':
        reconcile = session.update_created_pools

        def remove_then_reconcile(*args, **kwargs):
            """Remove the token between retry admission and reconciliation."""
            remove_failed_pool()
            return reconcile(*args, **kwargs)

        session.update_created_pools = remove_then_reconcile
    else:
        distance = session.cluster.profile_manager.distance
        distance.side_effect = remove_failed_pool

    scheduled.pop(0)[1]()
    # The only submitted work closes the removed pool; no replacement opens.
    assert len(work) == 1
    _finish_pool_recovery_work(work)
    session.cluster.connection_factory.assert_not_called()
    assert host not in session._pools
    assert not healthy.is_shutdown
    assert not scheduled


def test_discounted_pool_recovery_respects_disabled_remote_connections(
        discounted_pool_recovery):
    """Allow an empty remote pool when remote connections are disabled."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    session.cluster.profile_manager.distance.return_value = HostDistance.REMOTE
    session.cluster.connect_to_remote_hosts = False
    scheduled.pop(0)[1]()
    _finish_pool_recovery_work(work)
    assert session._pools[host] is not failed
    assert session._pools[host].open_count == 0
    session.cluster.connection_factory.assert_not_called()
    assert not healthy.is_shutdown
    assert not scheduled


@pytest.mark.parametrize('supersession', [
    'pool_replaced', 'pool_removed', 'host_replaced', 'host_down',
    'session_shutdown', 'cluster_shutdown',
])
def test_discounted_pool_recovery_stops_after_supersession(
        discounted_pool_recovery, supersession):
    """Discard a queued retry after its pool or lifecycle loses ownership."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    if supersession == 'pool_replaced':
        session._pools[host] = healthy
    elif supersession == 'pool_removed':
        session._pools.pop(host)
    elif supersession == 'host_replaced':
        replacement = Host(host.endpoint, SimpleConvictionPolicy,
                           host_id=host.host_id)
        session.cluster.metadata.remove_host(host)
        session.cluster.metadata.add_or_return_host(replacement)
    elif supersession == 'host_down':
        host.set_down()
    elif supersession == 'session_shutdown':
        session.shutdown()
    elif supersession == 'cluster_shutdown':
        session.cluster.shutdown()
    scheduled.pop(0)[1]()
    assert not work
    assert not scheduled


def test_discounted_pool_recovery_honors_exhausted_schedule(
        discounted_pool_recovery, caplog):
    """Stop and warn after a custom finite policy consumes all attempts."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    session.cluster.connection_factory.side_effect = \
        ConnectionException('still unavailable', host.endpoint)
    for delay in (1, 2, 4):
        actual_delay, recover = scheduled.pop(0)
        assert actual_delay == delay
        recover()
        _finish_pool_recovery_work(work)
    assert session._pools[host] is failed
    assert not scheduled
    assert not healthy.is_shutdown
    assert 'Session pool recovery schedule exhausted' in caplog.text


def test_discounted_recovery_discards_work_after_host_replacement(
        discounted_pool_recovery):
    """Reject queued creation when metadata has replaced its original host."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    scheduled.pop(0)[1]()
    replacement = Host(DefaultEndPoint('127.0.0.2'), SimpleConvictionPolicy,
                       host_id=host.host_id)
    session.cluster.metadata.remove_host(host)
    session.cluster.metadata.add_or_return_host(replacement)
    _finish_pool_recovery_work(work)
    session.cluster.connection_factory.assert_not_called()
    assert session._pools[host] is failed
    assert not scheduled


def test_discounted_pool_recovery_retries_cancelled_work(
        discounted_pool_recovery):
    """Schedule another attempt when pending creation work is cancelled."""
    host, session, failed, healthy, scheduled, work = discounted_pool_recovery
    scheduled.pop(0)[1]()
    work.pop(0)[3].cancel()
    assert len(scheduled) == 1
    delay, recover = scheduled.pop(0)
    assert delay == 2
    recover()
    _finish_pool_recovery_work(work)
    assert session._pools[host] is not failed
    assert not session._pools[host].is_shutdown
    assert not scheduled


def test_queued_replacement_cannot_remove_new_socket(socket_pool):
    host, session, pool, original, replacement = socket_pool
    pool.borrow_connection(timeout=0)
    original.is_defunct = True
    pool.return_connection(original)
    session.submit.assert_called_once_with(pool._replace, original)

    with pool._lock:
        pool._connections[0] = replacement
    pool._replace(original)
    assert pool._connections[0] is replacement
    assert session.cluster.connection_factory.call_count == 1
    replacement.close.assert_not_called()

    # The stale worker must neither suppress a new failure nor clear its owner.
    pool.borrow_connection(timeout=0)
    replacement.is_defunct = True
    pool.return_connection(replacement)
    session.submit.assert_called_with(pool._replace, replacement)
    pool._replace(original)
    healthy = HashableMock(spec=Connection)
    healthy.features = ProtocolFeatures(shard_id=0)
    session.cluster.connection_factory.side_effect = [healthy]
    pool._replace(replacement)
    assert pool._connections[0] is healthy


def test_current_pool_still_shuts_down_when_down_dispatch_raises(socket_pool):
    host, session, pool, original, _ = socket_pool
    host.signal_connection_failure.return_value = True
    session.cluster.on_down.side_effect = RuntimeError('executor stopped')
    pool.borrow_connection(timeout=0)
    original.is_defunct = True

    with pytest.raises(RuntimeError, match='executor stopped'):
        pool.return_connection(original)

    assert pool.is_shutdown
    assert original.in_flight == 0
    original.close.assert_called_once()


def test_return_after_session_collection_releases_original_stream():
    import gc
    import weakref

    host, session, pool, original, _ = _make_socket_pool()
    pool.borrow_connection(timeout=0)
    original.is_defunct = True
    session_ref = weakref.ref(session)
    del session
    gc.collect()
    assert session_ref() is None

    pool.return_connection(original)

    assert original.in_flight == 0
    original.close.assert_called_once()
    host.signal_connection_failure.assert_not_called()


class _PoolTests(unittest.TestCase):
    __test__ = False
    PoolImpl = None
    uses_single_connection = None

    def make_session(self):
        session = NonCallableMagicMock(
            spec=Session, keyspace='foobarkeyspace', _trash=[],
            _lock=RLock(), _pools={}, is_shutdown=False)
        session.cluster.is_shutdown = False
        return session

    def make_pool(self, host, session):
        host.lock = RLock()
        host._is_removed = False
        conn = session.cluster.connection_factory.return_value
        conn.set_keyspace_blocking.side_effect = \
            lambda keyspace: setattr(conn, 'keyspace', keyspace)
        pool = self.PoolImpl(host, HostDistance.LOCAL, session)
        session._pools[host] = pool
        return pool

    def test_borrow_and_return(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100)
        session.cluster.connection_factory.return_value = conn

        pool = self.make_pool(host, session)
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
            self.make_pool(host, session)

        shard_logs = [r.getMessage() for r in logs.records
                      if "First connection created" in r.getMessage()]
        self.assertTrue(shard_logs, "no connection initialization log was emitted")
        self.assertTrue(all(m.endswith("for shard_id=None") for m in shard_logs), shard_logs)

    def test_failed_wait_for_connection(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False, max_request_id=100)
        session.cluster.connection_factory.return_value = conn

        pool = self.make_pool(host, session)
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

        pool = self.make_pool(host, session)
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

        pool = self.make_pool(host, session)
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

        pool = self.make_pool(host, session)
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
        pool = self.make_pool(host, session)

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
        pool = self.make_pool(host, session)
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
        pool = self.make_pool(host, session)
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
        pool = self.make_pool(host, session)
        pool.shutdown_on_error = True

        pool.borrow_connection(timeout=0.01)
        conn.is_defunct = True
        conn.signaled_error = True
        conn.last_error = ConnectionSendError('local queue failure')
        pool.return_connection(conn, heartbeat_failed=True)

        assert pool.is_shutdown
        host.signal_connection_failure.assert_not_called()
        session.cluster.on_down.assert_called_once_with(
            host, is_host_addition=False, _pool_failure=unittest.mock.ANY)

    def test_return_defunct_connection_on_down_host(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
                                    max_request_id=100, signaled_error=False,
                                    orphaned_threshold_reached=False)
        session.cluster.connection_factory.return_value = conn
        session.cluster.shard_aware_options = ShardAwareOptions()

        pool = self.make_pool(host, session)
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

        pool = self.make_pool(host, session)
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

    def test_host_identity_and_endpoint_are_read_only(self):
        host_id = uuid.uuid4()
        host = Host('127.0.0.1', SimpleConvictionPolicy, host_id=host_id)

        endpoint = host.endpoint
        original_hash = hash(host)
        hosts_by_id = {host: 'pool'}
        hosts = {host}
        for name, value in (('host_id', uuid.uuid4()),
                            ('endpoint', DefaultEndPoint('127.0.0.2'))):
            for operation in (setattr, delattr):
                with self.subTest(attribute=name, operation=operation.__name__):
                    with pytest.raises(AttributeError):
                        if operation is setattr:
                            operation(host, name, value)
                        else:
                            operation(host, name)
                    assert hash(host) == original_hash
                    assert hosts_by_id[host] == 'pool'
                    assert host in hosts
                    assert host.host_id == host_id
                    assert host.endpoint is endpoint

    def test_host_hash_is_shared_by_endpoint_replacements(self):
        host = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
        hosts_by_id = {host: "pool"}
        hosts = {host}

        replacement = Host('127.0.0.2', SimpleConvictionPolicy,
                           host_id=host.host_id)

        assert hosts_by_id[host] == "pool"
        assert hosts_by_id[replacement] == "pool"
        assert hash(replacement) == hash(host)
        assert host in hosts
        assert replacement in hosts
        assert host.endpoint == DefaultEndPoint('127.0.0.1')

    def test_host_ordering_uses_host_id(self):
        first = Host('127.0.0.2', SimpleConvictionPolicy, host_id=uuid.UUID(int=1))
        second = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.UUID(int=2))

        assert first < second

    def test_identity_and_endpoint_are_set_before_conviction_policy_is_created(self):
        host_id = uuid.UUID(int=1)
        hosts = set()
        endpoints = ('127.0.0.1', DefaultEndPoint('127.0.0.1', 9142),
                     UnixSocketEndPoint('/tmp/maintenance.sock'),
                     SniEndPoint('proxy.example', 'node', 9042),
                     ClientRoutesEndPoint(host_id, Mock(), '127.0.0.1', 9042))
        for endpoint in endpoints:
            with self.subTest(endpoint=endpoint):
                normalized = (DefaultEndPoint(endpoint)
                              if isinstance(endpoint, str) else endpoint)

                def conviction_policy_factory(host):
                    assert host.host_id == host_id
                    assert host.endpoint == normalized
                    if not isinstance(endpoint, str):
                        assert host.endpoint is endpoint
                    hosts.add(host)
                    return SimpleConvictionPolicy(host)

                host = Host(endpoint, conviction_policy_factory, host_id=host_id)
                assert host in hosts

    def test_health_policy_and_reconnection_state_remain_mutable(self):
        host = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid.uuid4())
        policy = Mock()
        host.conviction_policy = policy
        host.set_up()
        assert host.is_up
        policy.reset.assert_called_once_with()
        error = ConnectionException('socket failed')
        result = host.signal_connection_failure(error)
        assert result is policy.add_failure.return_value
        policy.add_failure.assert_called_once_with(error)
        host.set_down()
        assert not host.is_up
        handler = Mock()
        assert host.get_and_set_reconnection_handler(handler) is None
        assert host.is_currently_reconnecting()
        assert host.get_and_set_reconnection_handler(None) is handler
        assert not host.is_currently_reconnecting()


class HostConnectionTests(_PoolTests):
    __test__ = True
    PoolImpl = HostConnection
    uses_single_connection = True

    def test_replacement_skips_pool_after_endpoint_changes(self):
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
        pool = self.make_pool(host, session)

        original_endpoint = host.endpoint
        move_host(host, DefaultEndPoint('127.0.0.2'))
        pool._replace(first)

        assert pool.endpoint == original_endpoint
        assert session.cluster.connection_factory.call_count == 1
        assert pool._connections[0] is first
        replacement.close.assert_not_called()
        pool.shutdown()

    def test_borrow_rejects_unsynchronized_connection_without_waiting(self):
        host = Mock(spec=Host, address='ip1')
        session = self.make_session()
        conn = HashableMock(spec=Connection, in_flight=0, is_defunct=False,
                            is_closed=False, max_request_id=100, lock=Lock())
        session.cluster.connection_factory.return_value = conn
        pool = self.make_pool(host, session)
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
        pool = self.make_pool(host, session)

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
        pool = self.make_pool(host, session)
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
        pool = self.make_pool(host, session)
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
        pool = self.make_pool(host, session)
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
        pool = self.make_pool(host, session)
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
        host, session, pool, old_conn, new_conn = _make_socket_pool()
        pool._keyspace = 'foobarkeyspace'
        # Regular-port replacements can land on a different shard.
        new_conn.features = ProtocolFeatures(shard_id=1)
        healthy = HashableMock(
            spec=Connection, in_flight=0, is_defunct=False, is_closed=False,
            max_request_id=100, lock=Lock(), keyspace=None,
            features=ProtocolFeatures(shard_id=0))
        healthy.set_keyspace_blocking.side_effect = \
            lambda keyspace: setattr(healthy, 'keyspace', keyspace)
        session.cluster.connection_factory.side_effect = [new_conn, healthy]

        def set_keyspace(keyspace):
            if keyspace == 'foobarkeyspace':
                new_conn.keyspace = keyspace
                pool._keyspace = 'newks'
            else:
                raise ConnectionException('sync failed', host.endpoint)

        new_conn.set_keyspace_blocking.side_effect = set_keyspace
        old_conn.is_defunct = True
        old_conn.in_flight = 1
        try:
            pool.return_connection(old_conn)
            replace, retired = session.submit.call_args.args
            replace(retired)

            assert not pool._connections
            new_conn.close.assert_called_once()
            replace, retired = session.submit.call_args.args
            replace(retired)

            borrowed, _ = pool.borrow_connection(timeout=0)
            assert borrowed is healthy
            assert healthy.keyspace == pool._keyspace == 'newks'
            assert not pool._replacement_connections
            healthy.close.assert_not_called()
            pool.return_connection(borrowed)
        finally:
            pool.shutdown()

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
        pool = self.make_pool(host, session)
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
                self._lock = RLock()
                self._pools = {}
                self.cluster = MagicMock(is_shutdown=False)
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
            host.lock = RLock()
            host._is_removed = False
            session = MockSession()

            ownership = PoolOwnershipReservation(session, host, host.endpoint)
            pool = HostConnection(host=host, host_distance=HostDistance.REMOTE,
                                  session=session, _ownership=ownership)
            with pool._membership_lock, session._lock, host.lock:
                session._pools[host] = pool
                ownership.published = True
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


@pytest.mark.parametrize('accepted_shards', [0, 1])
def test_shard_fanout_failure_retires_candidate(socket_pool, accepted_shards):
    host, session, original_pool, original, candidate = socket_pool
    session.cluster.shard_aware_options.disable = False
    candidate.features = ProtocolFeatures(
        shard_id=0, sharding_info=Mock(shards_count=3))
    pending = []
    initialized = []
    failure = RuntimeError('executor stopped during shard fan-out')

    def submit(fn, *args):
        pool = fn.__self__
        if not initialized:
            initialized.append(pool)
        if len(pending) == accepted_shards:
            raise failure
        future = Future()
        pending.append(future)
        return future

    session.submit.side_effect = submit
    try:
        with pytest.raises(RuntimeError) as raised:
            HostConnection(host, HostDistance.LOCAL, session)
        assert raised.value is failure
        pool = initialized[0]
        assert pool.is_shutdown
        assert pool._ownership.retired
        assert not pool._connections
        assert not pool._connection_attempts
        assert not pool._connecting
        assert not pool._shard_connections_futures
        assert all(future.cancelled() for future in pending)
        candidate.close.assert_called_once()
        original.close.assert_not_called()
        assert session._pools[host] is original_pool
    finally:
        for pool in initialized:
            pool.shutdown()
        original_pool.shutdown()


@pytest.mark.parametrize('change', ['endpoint', 'host', 'session', 'cluster'])
def test_first_connection_retired_after_keyspace_publication(
        socket_pool, change):
    host, session, original_pool, original, candidate = socket_pool
    session.cluster.shard_aware_options.disable = False
    candidate.features = ProtocolFeatures(
        shard_id=0, sharding_info=Mock(shards_count=2))
    sync_keyspace = HostConnection._sync_keyspace
    initialized = []

    def sync_then_retire(pool, conn, **kwargs):
        initialized.append(pool)
        assert sync_keyspace(pool, conn, **kwargs)
        assert pool._connections == {0: candidate}
        with session._lock, host.lock:
            if change == 'endpoint':
                move_host(host, DefaultEndPoint('127.0.0.2'))
            elif change == 'host':
                host._is_removed = True
            elif change == 'session':
                session.is_shutdown = True
            else:
                session.cluster.is_shutdown = True
        return True

    try:
        with patch.object(HostConnection, '_sync_keyspace', sync_then_retire):
            pool = HostConnection(host, HostDistance.LOCAL, session)
        assert pool.is_shutdown
        assert pool._ownership.retired
        assert not pool._connections
        candidate.close.assert_called_once()
        assert host.sharding_info is None
        assert session._pools[host] is original_pool
        session.submit.assert_not_called()
        session.cluster.on_down.assert_not_called()
        host.signal_connection_failure.assert_not_called()
    finally:
        for pool in initialized:
            pool.shutdown()
        original_pool.shutdown()


@pytest.mark.parametrize('pause', ['connect', 'keyspace'])
@pytest.mark.parametrize('outcome', [
    'success', 'current_failure', 'stale_failure'])
def test_shard_connection_diagnostics(socket_pool, caplog, pause, outcome):
    host, session, pool, original, candidate = socket_pool
    host.sharding_info = Mock(shards_count=2)
    candidate.features = ProtocolFeatures(shard_id=1)
    pool._get_shard_aware_endpoint = Mock(
        return_value=DefaultEndPoint('127.0.0.1', 19042))
    pool._keyspace = 'ks'

    def fail_if_requested(stage):
        if stage == pause and outcome != 'success':
            if outcome == 'stale_failure':
                with session._lock:
                    session.is_shutdown = True
            raise ConnectionException(
                'shard listener unavailable', pool.endpoint)

    def connect(*args, **kwargs):
        fail_if_requested('connect')
        return candidate

    def keyspace(target):
        fail_if_requested('keyspace')
        candidate.keyspace = target

    session.cluster.connection_factory.side_effect = connect
    candidate.set_keyspace_blocking.side_effect = keyspace
    try:
        with caplog.at_level(logging.ERROR, logger='cassandra.pool'):
            if outcome == 'current_failure':
                with pytest.raises(
                        ConnectionException, match='shard listener unavailable'):
                    pool._open_connection_to_missing_shard(1)
            else:
                pool._open_connection_to_missing_shard(1)

        records = [record for record in caplog.records
                   if record.name == 'cassandra.pool']
        if outcome == 'current_failure':
            assert len(records) == 1
            assert records[0].levelno == logging.ERROR
            assert str(host) in records[0].getMessage()
            assert 'shard_id=1' in records[0].getMessage()
            assert 'shard listener unavailable' in records[0].getMessage()
        else:
            assert not records
        assert not pool._connecting
        if outcome == 'success':
            assert pool._connections == {0: original, 1: candidate}
            candidate.close.assert_not_called()
        else:
            assert pool._connections == {0: original}
            if pause == 'keyspace':
                candidate.close.assert_called_once()
            else:
                candidate.close.assert_not_called()
        session.cluster.on_down.assert_not_called()
        host.signal_connection_failure.assert_not_called()
    finally:
        pool.shutdown()


@pytest.mark.parametrize('operation', ['replace', 'shard'])
@pytest.mark.parametrize('pause', ['connect', 'keyspace'])
@pytest.mark.parametrize('change', ['endpoint', 'pool', 'host', 'session', 'cluster'])
@pytest.mark.parametrize('fail', [False, True])
def test_connection_completion_rechecks_ownership(
        socket_pool, request, operation, pause, change, fail):
    host, session, pool, original, candidate = socket_pool
    request.addfinalizer(pool.shutdown)
    host.sharding_info = Mock(shards_count=2)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    if operation == 'shard':
        candidate.features = ProtocolFeatures(shard_id=1)
    pool._keyspace = 'ks'
    entered, release = Event(), Event()
    errors = []

    def wait():
        entered.set()
        assert release.wait(5)
        if fail:
            raise ConnectionException('late failure', pool.endpoint)

    def connect(*args, **kwargs):
        if pause == 'connect':
            wait()
        return candidate

    def keyspace(target):
        if pause == 'keyspace':
            wait()
        candidate.keyspace = target

    session.cluster.connection_factory.side_effect = connect
    candidate.set_keyspace_blocking.side_effect = keyspace

    def complete():
        try:
            if operation == 'replace':
                pool._replace(original)
            else:
                pool._open_connection_to_missing_shard(1)
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=complete)
    winner = None
    try:
        worker.start()
        assert entered.wait(5)
        with session._lock, host.lock:
            if change == 'endpoint':
                moved = move_host(host, DefaultEndPoint('127.0.0.2'))
                winner = HostConnection(moved, HostDistance.IGNORED, session)
                request.addfinalizer(winner.shutdown)
                session._pools = {moved: winner}
            elif change == 'pool':
                winner = HostConnection(host, HostDistance.IGNORED, session)
                request.addfinalizer(winner.shutdown)
                session._pools[host] = winner
            elif change == 'host':
                host._is_removed = True
            elif change == 'session':
                session.is_shutdown = True
            else:
                session.cluster.is_shutdown = True
        before = pool._connections.copy()
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not errors
    assert pool._connections == before
    if pause == 'keyspace' or not fail:
        candidate.close.assert_called_once()
    else:
        candidate.close.assert_not_called()  # factory never returned a socket
    assert not pool._connecting
    assert not pool._connection_attempts
    session.submit.assert_not_called()
    session.cluster.on_down.assert_not_called()
    host.signal_connection_failure.assert_not_called()
    if winner is not None:
        assert session._pools[host] is winner
        assert not winner.is_shutdown


@pytest.mark.parametrize('fail', [False, True])
def test_old_attempt_cleanup_preserves_new_attempt(socket_pool, request, fail):
    host, session, pool, original, candidate = socket_pool
    request.addfinalizer(pool.shutdown)
    host.sharding_info = Mock(shards_count=2)
    candidate.features = ProtocolFeatures(shard_id=1)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    pool._keyspace = 'ks'
    entered, release = Event(), Event()

    def keyspace(target):
        entered.set()
        assert release.wait(5)
        if fail:
            raise ConnectionException('stale USE failed')
        candidate.keyspace = target

    candidate.set_keyspace_blocking.side_effect = keyspace
    errors = []

    def complete():
        try:
            pool._open_connection_to_missing_shard(1)
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=complete)
    try:
        worker.start()
        assert entered.wait(5)
        newer = pool._begin_shard_attempt(1)
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not errors
    assert pool._connections == {0: original}
    candidate.close.assert_called_once()
    assert pool._connection_attempts[1] is newer[1]
    assert 1 in pool._connecting

    # The retained token must still be able to publish its own socket.
    healthy = HashableMock(
        spec=Connection, keyspace='ks', orphaned_threshold_reached=False,
        features=ProtocolFeatures(shard_id=1))
    session.cluster.connection_factory.side_effect = [healthy]
    pool._open_connection_to_missing_shard(1, newer)
    assert pool._connections == {0: original, 1: healthy}
    assert not pool._connecting
    healthy.close.assert_not_called()


@pytest.mark.parametrize('fail', [False, True])
def test_competing_shard_completion_cannot_overwrite_winner(
        socket_pool, request, fail):
    host, session, pool, original, candidate = socket_pool
    request.addfinalizer(pool.shutdown)
    host.sharding_info = Mock(shards_count=3)
    candidate.features = ProtocolFeatures(shard_id=1)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    pool._keyspace = 'ks'
    entered, release = Event(), Event()
    errors = []

    def keyspace(target):
        entered.set()
        assert release.wait(5)
        if fail:
            raise ConnectionException('losing USE failed')
        candidate.keyspace = target

    candidate.set_keyspace_blocking.side_effect = keyspace

    def complete():
        try:
            # This requested shard 2, but the server assigned shard 1.
            pool._open_connection_to_missing_shard(2)
        except Exception as exc:
            errors.append(exc)

    winner = HashableMock(
        spec=Connection, keyspace='ks', orphaned_threshold_reached=False,
        features=ProtocolFeatures(shard_id=1))
    worker = Thread(target=complete)
    try:
        worker.start()
        assert entered.wait(5)
        winning_attempt = pool._begin_shard_attempt(1)
        assert pool._sync_keyspace(winner, winning_attempt, expected=None)
        pool._finish_shard_attempt(winning_attempt)
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not errors
    assert pool._connections == {0: original, 1: winner}
    candidate.close.assert_called_once()
    winner.close.assert_not_called()
    assert not pool._connecting


def test_shutdown_during_keyspace_sync_revokes_publication(socket_pool):
    host, session, pool, original, candidate = socket_pool
    host.sharding_info = Mock(shards_count=2)
    candidate.features = ProtocolFeatures(shard_id=1)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    pool._keyspace = 'ks'
    entered, release, retiring = Event(), Event(), Event()
    errors = []
    work_lock = pool._lock

    class ObservedLock:
        def __enter__(self):
            if pool._ownership.retired:
                retiring.set()
            work_lock.acquire()

        def __exit__(self, *args):
            work_lock.release()

    pool._lock = ObservedLock()

    def keyspace(target):
        entered.set()
        assert release.wait(5)
        candidate.keyspace = target

    candidate.set_keyspace_blocking.side_effect = keyspace

    def run(fn, *args):
        try:
            fn(*args)
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=run, args=(pool._open_connection_to_missing_shard, 1))
    shutdown = Thread(target=run, args=(pool.shutdown,))
    try:
        worker.start()
        assert entered.wait(5)
        shutdown.start()
        assert retiring.wait(5)
    finally:
        release.set()
        for thread in (worker, shutdown):
            if thread.ident is not None:
                thread.join(5)
        pool._lock = work_lock
    assert not worker.is_alive() and not shutdown.is_alive()
    assert not errors
    assert pool.is_shutdown
    assert not pool._connections
    candidate.close.assert_called_once()
    assert not pool._excess_connections


def test_excess_connection_retention_rechecks_pool_owner(socket_pool, request):
    host, session, pool, original, candidate = socket_pool
    request.addfinalizer(pool.shutdown)
    host.sharding_info = Mock(shards_count=2)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    pool.max_excess_connections_per_shard_multiplier = 0
    retained_excess = HashableMock(spec=Connection)
    pool._excess_connections.add(retained_excess)
    entered, release = Event(), Event()
    close_excess = pool._close_excess_connections
    errors = []

    def paused_close(_attempt=None):
        entered.set()
        assert release.wait(5)
        close_excess(_attempt=_attempt)

    pool._close_excess_connections = paused_close

    def complete():
        try:
            pool._open_connection_to_missing_shard(1)
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=complete)
    try:
        worker.start()
        assert entered.wait(5)
        winner = HostConnection(host, HostDistance.IGNORED, session)
        request.addfinalizer(winner.shutdown)
        with session._lock:
            session._pools[host] = winner
    finally:
        release.set()
        worker.join(5)
        pool._close_excess_connections = close_excess
    assert not worker.is_alive()
    assert not errors
    assert pool._excess_connections == {retained_excess}
    retained_excess.close.assert_not_called()
    assert pool._connections == {0: original}
    candidate.close.assert_called_once()
    assert not winner.is_shutdown
    pool.shutdown()
    retained_excess.close.assert_called_once()


@pytest.mark.parametrize('failure', [
    'current', 'retry', 'retry_retired', 'retired', 'newer_marker'])
def test_sharded_replacement_submission_failure(socket_pool, caplog, failure):
    host, session, pool, original, candidate = socket_pool
    host.sharding_info = Mock(shards_count=2)
    session.cluster.shard_aware_options.disable = False
    retry = Future()

    def submit(*args, **kwargs):
        if session.submit.call_count == 1:
            if failure == 'retired':
                with session._lock, host.lock:
                    host._is_removed = True
            elif failure == 'newer_marker':
                pool._replacement_connections[0] = candidate
            raise RuntimeError('scheduler unavailable')
        if failure in ('retry', 'retry_retired'):
            if failure == 'retry_retired':
                with session._lock, host.lock:
                    host._is_removed = True
            raise RuntimeError('retry unavailable')
        return retry

    session.submit.side_effect = submit
    try:
        with caplog.at_level(logging.WARNING, logger='cassandra.pool'):
            pool._replace(original)
        if failure in ('current', 'retry', 'retry_retired'):
            assert 'scheduler unavailable' in caplog.text
            assert str(host) in caplog.text
            assert session.submit.call_count == 2
        else:
            assert not caplog.records
            session.submit.assert_called_once()
        if failure == 'current':
            assert pool._replacement_connections == {0: original}
        elif failure == 'newer_marker':
            assert pool._replacement_connections == {0: candidate}
        else:
            assert not pool._replacement_connections
        if failure == 'retry':
            assert 'retry unavailable' in caplog.text
        elif failure == 'retry_retired':
            assert 'retry unavailable' not in caplog.text
        assert not pool._connecting
        assert not pool._connection_attempts
        session.cluster.connection_factory.assert_called_once()
        session.cluster.on_down.assert_not_called()
        host.signal_connection_failure.assert_not_called()
    finally:
        retry.cancel()
        pool.shutdown()


def test_concurrent_requests_coalesce_shard_recovery(socket_pool):
    host, session, pool, original, candidate = socket_pool
    host.sharding_info = Mock(shards_count=2)
    host.sharding_info.shard_id_from_token.return_value = 1
    session.cluster.shard_aware_options.disable = False
    candidate.features = ProtocolFeatures(shard_id=1)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    admitted = Barrier(2)
    begin_attempt = pool._begin_shard_attempt
    futures, borrowed, errors = [], [], []

    def begin(shard_id, **kwargs):
        admitted.wait(timeout=5)
        return begin_attempt(shard_id, **kwargs)

    def submit(*args, **kwargs):
        future = Future()
        futures.append(future)
        return future

    def borrow():
        try:
            conn, _ = pool.borrow_connection(
                timeout=0, routing_key=b'key', routing_token=Mock(value=1))
            borrowed.append(conn)
        except Exception as exc:
            errors.append(exc)

    session.submit.side_effect = submit
    workers = [Thread(target=borrow) for _ in range(2)]
    try:
        with patch.object(pool, '_begin_shard_attempt', begin):
            try:
                for worker in workers:
                    worker.start()
            finally:
                for worker in workers:
                    if worker.ident is not None:
                        worker.join(5)
        assert all(not worker.is_alive() for worker in workers)
        assert not errors
        assert borrowed == [original, original]
        session.submit.assert_called_once()
        queued_attempt = session.submit.call_args.args[2]
        assert pool._connection_attempts[1] is queued_attempt[1]
        assert futures[0].set_running_or_notify_cancel()
        pool._open_connection_to_missing_shard(1, queued_attempt)
        futures[0].set_result(None)
        assert pool._connections == {0: original, 1: candidate}
        candidate.close.assert_not_called()
        newer = pool._begin_shard_attempt(1)
        newest = pool._begin_shard_attempt(1)
        assert newest[1] is not newer[1]
        assert pool._connection_attempts[1] is newest[1]
    finally:
        for conn in borrowed:
            pool.return_connection(conn)
        for future in futures:
            future.cancel()
        pool.shutdown()


@pytest.mark.parametrize('change', ['none', 'owner', 'attempt', 'slot'])
def test_shard_error_after_publication_checks_current_owner(
        socket_pool, caplog, change):
    host, session, pool, original, candidate = socket_pool
    host.sharding_info = Mock(shards_count=2)
    candidate.features = ProtocolFeatures(shard_id=1)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    winner = HashableMock(
        spec=Connection, features=ProtocolFeatures(shard_id=1))

    def close_excess(_attempt=None):
        assert pool._connections[1] is candidate
        if change == 'owner':
            with session._lock, host.lock:
                host._is_removed = True
        elif change == 'attempt':
            pool._begin_shard_attempt(1)
        elif change == 'slot':
            with pool._membership_lock:
                pool._connections[1] = winner
        raise RuntimeError('excess close failed')

    try:
        with patch.object(pool, '_close_excess_connections', close_excess):
            with caplog.at_level(logging.ERROR, logger='cassandra.pool'):
                if change == 'none':
                    with pytest.raises(
                            RuntimeError, match='excess close failed'):
                        pool._open_connection_to_missing_shard(1)
                else:
                    pool._open_connection_to_missing_shard(1)
        if change == 'none':
            assert 'excess close failed' in caplog.text
            assert str(host) in caplog.text
            assert 'shard_id=1' in caplog.text
        else:
            assert not caplog.records
        candidate.close.assert_not_called()
        if change == 'attempt':
            assert 1 in pool._connection_attempts
        else:
            assert not pool._connection_attempts
    finally:
        pool.shutdown()


def test_duplicate_shard_retained_until_coverage_completes(socket_pool):
    host, session, pool, original, duplicate = socket_pool
    host.sharding_info = Mock(shards_count=2)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    last_shard = HashableMock(
        spec=Connection, keyspace=None, orphaned_threshold_reached=False,
        features=ProtocolFeatures(shard_id=1))
    session.cluster.connection_factory.side_effect = [duplicate, last_shard]
    try:
        pool._open_connection_to_missing_shard(1)
        assert pool._connections == {0: original}
        assert pool._excess_connections == {duplicate}
        duplicate.close.assert_not_called()
        pool._open_connection_to_missing_shard(1)
        assert pool._connections == {0: original, 1: last_shard}
        assert not pool._excess_connections
        duplicate.close.assert_called_once()
        original.close.assert_not_called()
        last_shard.close.assert_not_called()
    finally:
        pool.shutdown()
    duplicate.close.assert_called_once()


def test_current_shard_replacement_preserves_draining_requests(socket_pool):
    host, session, pool, original, candidate = socket_pool
    host.sharding_info = Mock(shards_count=1)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    original.orphaned_threshold_reached = True
    original.in_flight = 2
    original.orphaned_request_ids = {1}

    pool._open_connection_to_missing_shard(0)

    assert pool._connections == {0: candidate}
    assert original in pool._trash
    original.close.assert_not_called()
    assert candidate.in_flight == 0
    pool.return_connection(original)
    assert original.in_flight == 1  # only its orphaned request remains
    assert candidate.in_flight == 0
    original.close.assert_called_once()
    assert not pool._trash
    pool.shutdown()


def test_replace_skips_slot_taken_after_reservation_check(socket_pool):
    host, session, pool, original, candidate = socket_pool
    winner = HashableMock(spec=Connection, features=ProtocolFeatures(shard_id=0))
    # Another socket takes the slot between is_current() and the work lock.
    pool._connections[0] = winner
    connects = session.cluster.connection_factory.call_count
    with patch.object(ConnectionFailureReservation, 'is_current',
                      return_value=True):
        pool._replace(original)

    assert pool._connections == {0: winner}
    assert not pool._replacement_connections
    assert session.cluster.connection_factory.call_count == connects
    assert not pool._connecting
    pool._connections.clear()
    pool.shutdown()


def test_current_shard_replacement_closes_drained_connection(socket_pool):
    host, session, pool, original, candidate = socket_pool
    host.sharding_info = Mock(shards_count=1)
    pool._get_shard_aware_endpoint = Mock(return_value=None)
    original.orphaned_threshold_reached = True
    original.in_flight = 1
    original.orphaned_request_ids = {1}

    pool._open_connection_to_missing_shard(0)

    # Only orphaned requests remain: nothing to drain, close it right away.
    assert pool._connections == {0: candidate}
    assert original not in pool._trash
    original.close.assert_called_once()
    candidate.close.assert_not_called()
    pool.shutdown()


@pytest.mark.parametrize('completed_before_submit', [False, True])
@pytest.mark.parametrize('outcome', ['success', 'failure', 'cancelled'])
def test_completed_shard_work_is_not_retained(
        socket_pool, completed_before_submit, outcome):
    host, session, pool, original, candidate = socket_pool

    def complete(future):
        if outcome == 'success':
            future.set_result(None)
        elif outcome == 'failure':
            future.set_exception(ConnectionException('shard attempt failed'))
        else:
            assert future.cancel()

    for _ in range(3):
        future = Future()
        session.submit.return_value = future
        if completed_before_submit:
            complete(future)
        pool._submit_shard_attempt(1)
        if not completed_before_submit:
            assert future in pool._shard_connections_futures
            complete(future)
        assert not pool._shard_connections_futures

    pending = Future()
    session.submit.return_value = pending
    pool._submit_shard_attempt(1)
    pool.shutdown()
    assert pending.cancelled()
    assert not pool._shard_connections_futures


@pytest.mark.parametrize('running', [False, True])
def test_shard_work_registered_after_shutdown(socket_pool, running):
    host, session, pool, original, candidate = socket_pool
    future = Future()
    if running:
        assert future.set_running_or_notify_cancel()
    submitted, release = Event(), Event()
    errors = []

    def submit(*args, **kwargs):
        submitted.set()
        assert release.wait(5)
        return future

    session.submit.side_effect = submit

    def schedule():
        try:
            pool._submit_shard_attempt(1)
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=schedule)
    try:
        try:
            worker.start()
            assert submitted.wait(5)
            assert not pool._shard_connections_futures
            pool.shutdown()
            assert not future.cancelled()
        finally:
            release.set()
            worker.join(5)
        assert not worker.is_alive()
        assert not errors
        assert future.cancelled() is not running
        if running:
            assert pool._shard_connections_futures == [future]
            future.set_result(None)
        assert not pool._shard_connections_futures
        assert not pool._connecting
        assert not pool._connection_attempts
        original.close.assert_called_once()
        candidate.close.assert_not_called()
    finally:
        pool.shutdown()


@pytest.mark.parametrize('newer_marker', [False, True])
def test_rejected_replacement_clears_only_its_marker(
        socket_pool, newer_marker):
    host, session, pool, original, candidate = socket_pool
    begin_attempt = pool._begin_shard_attempt

    def retire_before_attempt(shard_id):
        assert pool._replacement_connections[shard_id] is original
        with session._lock, host.lock:
            host._is_removed = True
        if newer_marker:
            pool._replacement_connections[shard_id] = candidate
        return begin_attempt(shard_id)

    try:
        with patch.object(pool, '_begin_shard_attempt', retire_before_attempt):
            pool._replace(original)
        if newer_marker:
            assert pool._replacement_connections == {0: candidate}
        else:
            assert not pool._replacement_connections
        assert not pool._connections
        assert not pool._connecting
        assert not pool._connection_attempts
        session.cluster.connection_factory.assert_called_once()
        session.submit.assert_not_called()
        session.cluster.on_down.assert_not_called()
        host.signal_connection_failure.assert_not_called()
    finally:
        pool.shutdown()


@pytest.mark.parametrize('newer_attempt', [False, True])
def test_cancelled_shard_work_only_clears_its_own_marker(
        socket_pool, request, newer_attempt):
    host, session, pool, original, candidate = socket_pool
    request.addfinalizer(pool.shutdown)
    future = Future()
    session.submit.return_value = future
    pool._submit_shard_attempt(1)
    if newer_attempt:
        newer = pool._begin_shard_attempt(1)
    assert future.cancel()
    assert (1 in pool._connecting) is newer_attempt
    if newer_attempt:
        assert pool._connection_attempts[1] is newer[1]
    else:
        assert 1 not in pool._connection_attempts


def test_failure_waiting_for_membership_does_not_hold_session(socket_pool):
    host, session, pool, original, candidate = socket_pool
    original.is_defunct = True
    original.in_flight = 1
    original.last_error = ConnectionException('failed transport')
    waiting, session_available = Event(), Event()
    membership_lock = pool._membership_lock
    errors = []

    class ObservedLock:
        def __enter__(self):
            waiting.set()
            membership_lock.acquire()

        def __exit__(self, *args):
            membership_lock.release()

    pool._membership_lock = ObservedLock()

    def fail():
        try:
            pool.return_connection(original)
        except Exception as exc:
            errors.append(exc)

    def acquire_session():
        with session._lock:
            session_available.set()

    failure = Thread(target=fail)
    publication = Thread(target=acquire_session)
    try:
        with membership_lock:
            failure.start()
            assert waiting.wait(5)
            publication.start()
            assert session_available.wait(2), 'failure inverted membership/Session order'
    finally:
        for worker in (failure, publication):
            if worker.ident is not None:
                worker.join(5)
        pool._membership_lock = membership_lock
    for worker in (failure, publication):
        assert not worker.is_alive()
    assert not errors
    pool.shutdown()
