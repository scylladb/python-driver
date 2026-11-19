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
"""Pool admission and publication against authoritative metadata (#860)."""

from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import nullcontext
from threading import Event, Lock, RLock, Thread
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from cassandra import AuthenticationFailed
from cassandra.cluster import Cluster
from cassandra.connection import (
    Connection, ConnectionException, DefaultEndPoint)
from cassandra.policies import HostDistance, SimpleConvictionPolicy
from cassandra.pool import Host, HostConnection
from cassandra.protocol_features import ProtocolFeatures
from tests.unit.util import HashableMock
from tests.unit.utils import new_session_with_pool_state


def make_pool(host, keyspace=None):
    pool = Mock(host=host, endpoint=host.endpoint, is_shutdown=False,
                host_distance=HostDistance.LOCAL, _keyspace=keyspace,
                _membership_lock=RLock(), _initially_disconnected=False,
                _promotion_recovery_claimed=False)
    pool._lock_usable_connection.side_effect = lambda: nullcontext(True)
    pool.shutdown.side_effect = lambda: setattr(pool, 'is_shutdown', True)
    return pool


@pytest.fixture
def pool_session():
    cluster = Cluster(contact_points=[])
    host = Host('127.0.0.1', SimpleConvictionPolicy, host_id=uuid4())
    cluster.metadata.add_or_return_host(host)
    cluster.on_add = Mock()
    cluster.control_connection.on_remove = Mock()
    session = new_session_with_pool_state()
    session.cluster = cluster
    session._profile_manager = Mock()
    session._profile_manager.distance.return_value = HostDistance.LOCAL
    session.keyspace = None
    session.is_shutdown = False
    session._initial_connect_futures = set()
    cluster.sessions.add(session)
    try:
        yield session, host
    finally:
        cluster.shutdown()


def make_connection(is_closed=False, is_defunct=False):
    return HashableMock(
        spec=Connection, lock=Lock(), in_flight=0, max_request_id=100,
        is_closed=is_closed, is_defunct=is_defunct, signaled_error=False,
        orphaned_threshold_reached=False, orphaned_request_ids=set(),
        keyspace=None, features=ProtocolFeatures(shard_id=0))


@pytest.mark.parametrize('connect_to_remote_hosts', [False, True])
@pytest.mark.parametrize('advertise_local', [False, True])
@pytest.mark.parametrize('local_distance', [HostDistance.LOCAL,
                                           HostDistance.LOCAL_RACK])
def test_remote_pool_promotion_opens_local_connection(
        pool_session, connect_to_remote_hosts, advertise_local,
        local_distance):
    session, host = pool_session
    cluster = session.cluster
    cluster.connect_to_remote_hosts = connect_to_remote_hosts
    conn = make_connection()
    cluster.connection_factory = Mock(return_value=conn)
    session._profile_manager.distance.return_value = HostDistance.REMOTE

    assert session.add_or_renew_pool(host, True).result(5) is True
    remote_pool = session._pools[host]
    assert remote_pool.open_count == int(connect_to_remote_hosts)
    assert session.update_created_pools() == set()
    assert session._pools[host] is remote_pool
    assert cluster.connection_factory.call_count == int(
        connect_to_remote_hosts)

    if advertise_local:
        # A concurrent direct ADD can update a retained pool's distance.
        remote_pool.host_distance = local_distance
    session._profile_manager.distance.return_value = local_distance
    futures = session.update_created_pools()
    assert len(futures) == int(not connect_to_remote_hosts)
    for future in futures:
        assert future.result(5) is True
    local_pool = session._pools[host]
    assert local_pool.host_distance == local_distance
    assert local_pool.open_count == 1
    assert cluster.connection_factory.call_count == 1
    assert remote_pool.is_shutdown is not connect_to_remote_hosts
    assert (local_pool is remote_pool) is connect_to_remote_hosts
    borrowed, _ = local_pool.borrow_connection(timeout=0)
    assert borrowed is conn
    local_pool.return_connection(borrowed)
    assert session.update_created_pools() == set()
    assert session._pools[host] is local_pool
    assert cluster.connection_factory.call_count == 1


@pytest.mark.parametrize('local_distance', [HostDistance.LOCAL,
                                           HostDistance.LOCAL_RACK])
@pytest.mark.parametrize(
    'connect_to_remote_hosts, publish_remote, local_creation_fails', [
        (False, 'before_distance', False),
        (True, 'before_distance', False),
        (False, 'before_admission', False),
        (True, 'before_admission', False),
        (False, 'before_local_worker', False),
        (True, 'before_local_worker', False),
        (False, 'before_admission', True),
        (False, 'before_local_worker', True),
    ])
def test_queued_remote_creation_cannot_satisfy_local_promotion(
        pool_session, connect_to_remote_hosts, local_distance,
        publish_remote, local_creation_fails):
    session, host = pool_session
    cluster = session.cluster
    cluster.connect_to_remote_hosts = connect_to_remote_hosts
    conn = make_connection()
    cluster.connection_factory = Mock(return_value=conn)
    if local_creation_fails:
        host.set_up()
        cluster.connection_factory.side_effect = ConnectionException(
            'local creation failed', host.endpoint)
        cluster.signal_connection_failure = Mock(return_value=False)
        session._recover_pool = Mock()
    work = []
    created = []

    def submit(fn, *args, **kwargs):
        completion = Future()
        work.append((fn, args, kwargs, completion))
        return completion

    def run_next(expected_result):
        fn, args, kwargs, completion = work.pop(0)
        result = fn(*args, **kwargs)
        completion.set_result(result)
        assert result is expected_result

    def create_pool(*args, **kwargs):
        pool = HostConnection(*args, **kwargs)
        created.append(pool)
        return pool

    session.submit = submit
    session._profile_manager.distance.return_value = HostDistance.REMOTE
    remote_completion = session.add_or_renew_pool(host, True)
    assert len(work) == 1
    assert host not in session._pools
    session._profile_manager.distance.return_value = local_distance

    with patch('cassandra.cluster.HostConnection', side_effect=create_pool):
        if publish_remote == 'before_distance':
            def distance(current_host):
                assert current_host is host
                run_next(True)
                return local_distance

            session._profile_manager.distance.side_effect = distance
            futures = session.update_created_pools()
            session._profile_manager.distance.side_effect = None
        elif publish_remote == 'before_admission':
            add = session.add_or_renew_pool

            def admit(*args, **kwargs):
                run_next(connect_to_remote_hosts)
                return add(*args, **kwargs)

            with patch.object(session, 'add_or_renew_pool', side_effect=admit):
                futures = session.update_created_pools()
        else:
            futures = session.update_created_pools()
            assert len(work) == 2
            run_next(connect_to_remote_hosts)

        immediate_success = (
            connect_to_remote_hosts and
            publish_remote != 'before_local_worker')
        assert len(futures) == int(not immediate_success)
        assert len(work) == int(not immediate_success)
        local_result = not local_creation_fails
        if work:
            assert not remote_completion.done()
            run_next(local_result)
        assert not work
        assert remote_completion.result(5) is local_result
        for future in futures:
            assert future.result(5) is local_result

    if local_creation_fails:
        assert host not in session._pools
        assert host.is_up
        assert len(created) == 1
        assert created[0]._initially_disconnected
        assert created[0].is_shutdown
        cluster.connection_factory.assert_called_once()
        session._recover_pool.assert_not_called()
        return

    pool = session._pools[host]
    assert pool.host_distance == local_distance
    assert pool.open_count == 1
    assert cluster.connection_factory.call_count == 1
    assert len(created) == 2 - int(connect_to_remote_hosts)
    if not connect_to_remote_hosts:
        assert created[0].is_shutdown
        assert pool is not created[0]
    borrowed, _ = pool.borrow_connection(timeout=0)
    assert borrowed is conn
    pool.return_connection(borrowed)
    assert session.update_created_pools() == set()
    assert session._pools[host] is pool
    assert cluster.connection_factory.call_count == 1


@pytest.mark.parametrize('connect_to_remote_hosts', [False, True])
def test_local_pool_reconciliation_preserves_inflight_replacement(
        pool_session, connect_to_remote_hosts):
    session, host = pool_session
    cluster = session.cluster
    cluster.connect_to_remote_hosts = connect_to_remote_hosts

    original, replacement = make_connection(), make_connection()
    cluster.connection_factory = Mock(return_value=original)
    assert session.add_or_renew_pool(host, True).result(5) is True
    pool = session._pools[host]
    original.is_defunct = True
    connecting = Event()
    release_connection = Event()

    def connect(*args, **kwargs):
        connecting.set()
        assert release_connection.wait(5)
        return replacement

    cluster.connection_factory.side_effect = connect
    with ThreadPoolExecutor(max_workers=1) as background:
        try:
            work = background.submit(pool._replace, original)
            assert connecting.wait(5)
            assert pool.open_count == 0
            assert session.update_created_pools() == set()
            assert session._pools[host] is pool
            assert not pool.is_shutdown
            assert cluster.connection_factory.call_count == 2
        finally:
            release_connection.set()
        work.result(5)
    assert session._pools[host] is pool
    assert pool.get_connections() == [replacement]
    assert pool.open_count == 1


@pytest.mark.parametrize('renewal_fails', [False, True])
def test_concurrent_remote_promotions_share_recovery(
        pool_session, renewal_fails):
    session, host = pool_session
    cluster = session.cluster
    cluster.connect_to_remote_hosts = False
    cluster.signal_connection_failure = Mock(return_value=False)
    cluster.reconnection_policy = Mock()
    cluster.reconnection_policy.new_schedule.return_value = [1, 2]
    scheduled = []
    cluster.scheduler.schedule = Mock(
        side_effect=lambda delay, fn: scheduled.append((delay, fn)))
    cluster.executor.submit = Mock(return_value=Future())

    def submit(fn, *args, **kwargs):
        future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future

    session.submit = submit
    session._profile_manager.distance.return_value = HostDistance.REMOTE
    assert session.add_or_renew_pool(host, True).result(5) is True
    remote_pool = session._pools[host]
    shutdown = remote_pool.shutdown
    entered, release = Event(), Event()
    calls = 0
    counter_lock = Lock()

    def hold_first_shutdown():
        nonlocal calls
        with counter_lock:
            calls += 1
            first = calls == 1
        if first:
            entered.set()
            assert release.wait(5)
        shutdown()

    remote_pool.shutdown = hold_first_shutdown
    conn = make_connection()
    cluster.connection_factory = Mock(return_value=conn)
    if renewal_fails:
        cluster.connection_factory.side_effect = ConnectionException('retry')
    session._profile_manager.distance.return_value = HostDistance.LOCAL
    with ThreadPoolExecutor(max_workers=1) as background:
        try:
            work = background.submit(session.update_created_pools)
            assert entered.wait(5)
            futures = session.update_created_pools()
            assert len(futures) == 1
            expected_result = not renewal_fails
            assert next(iter(futures)).result(5) is expected_result
            winner = session._pools[host]
        finally:
            release.set()
        work.result(5)
    cluster._lifecycle.drain()
    assert cluster.reconnection_policy.new_schedule.call_count == 1
    assert cluster.connection_factory.call_count == 1
    assert len(scheduled) == int(renewal_fails)
    if renewal_fails:
        cluster.connection_factory.side_effect = None
        scheduled.pop(0)[1]()
        assert cluster.connection_factory.call_count == 2
    else:
        assert session._pools[host] is winner
    assert session._pools[host] is not remote_pool
    assert session._pools[host].open_count == 1
    assert not session._pools[host].is_shutdown
    assert not scheduled


@pytest.mark.parametrize('renewal_fails', [False, True])
def test_remote_promotion_supports_synchronous_submission(
        pool_session, renewal_fails, caplog):
    session, host = pool_session
    cluster = session.cluster
    cluster.connect_to_remote_hosts = False
    cluster.signal_connection_failure = Mock(return_value=False)
    cluster.reconnection_policy = Mock()
    cluster.reconnection_policy.new_schedule.return_value = [1, 2]
    scheduled = []
    cluster.scheduler.schedule = Mock(
        side_effect=lambda delay, fn: scheduled.append((delay, fn)))
    cluster.executor.submit = Mock(return_value=Future())
    session.submit = lambda fn, *args, **kwargs: fn(*args, **kwargs)
    session._profile_manager.distance.return_value = HostDistance.REMOTE
    assert session.add_or_renew_pool(host, True) is True
    remote_pool = session._pools[host]
    conn = make_connection()
    cluster.connection_factory = Mock(return_value=conn)
    if renewal_fails:
        cluster.connection_factory.side_effect = ConnectionException('retry')
    session._profile_manager.distance.return_value = HostDistance.LOCAL
    assert session.update_created_pools() == (
        set() if renewal_fails else {True})
    cluster._lifecycle.drain()
    assert len(scheduled) == int(renewal_fails)
    if renewal_fails:
        cluster.connection_factory.side_effect = None
        scheduled.pop(0)[1]()
    assert not scheduled
    assert session._pools[host] is not remote_pool
    assert session._pools[host].open_count == 1
    assert not session._pools[host].is_shutdown
    assert 'Error delivering host lifecycle notification' not in caplog.text


@pytest.fixture
def failed_pool_promotion(pool_session, request):
    session, host = pool_session
    cluster = session.cluster
    cluster.connect_to_remote_hosts = False
    cluster.signal_connection_failure = Mock(return_value=False)
    cluster.reconnection_policy = Mock()
    cluster.reconnection_policy.new_schedule.return_value = [1, 2]
    scheduled = []

    def schedule(delay, fn):
        scheduled.append((delay, fn))

    cluster.scheduler.schedule = Mock(side_effect=schedule)

    def submit(fn, *args, **kwargs):
        future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as exc:
            future.set_exception(exc)
        return future

    # Hold executor delivery so this test drives the lifecycle FIFO.
    cluster.executor.submit = Mock(return_value=Future())
    session.submit = submit
    session._profile_manager.distance.return_value = HostDistance.REMOTE
    assert session.add_or_renew_pool(host, True).result(5) is True
    remote_pool = session._pools[host]
    session._profile_manager.distance.return_value = HostDistance.LOCAL
    failure = getattr(request, 'param', 'connect')
    if failure == 'connect':
        cluster.connection_factory = Mock(
            side_effect=ConnectionException('retry'))
    else:
        conn = make_connection(
            is_closed=failure == 'closed', is_defunct=failure == 'defunct')
        cluster.connection_factory = Mock(return_value=conn)
    futures = session.update_created_pools()
    assert len(futures) == 1
    assert next(iter(futures)).result(5) is False
    cluster._lifecycle.drain()
    assert session._pools[host] is remote_pool
    assert remote_pool.is_shutdown
    cluster.signal_connection_failure.assert_called_once()
    return session, host, remote_pool, scheduled


@pytest.mark.parametrize(
    'failed_pool_promotion', ['connect', 'closed', 'defunct'], indirect=True)
def test_empty_pool_promotion_retries_discounted_connection_failure(
        failed_pool_promotion):
    session, host, remote_pool, scheduled = failed_pool_promotion
    conn = make_connection()
    session.cluster.connection_factory.side_effect = None
    session.cluster.connection_factory.return_value = conn
    delay, retry = scheduled.pop(0)
    assert delay == 1
    # Deliver the configured timer without explicit reconciliation.
    retry()
    assert session._pools[host] is not remote_pool
    assert session._pools[host].open_count == 1
    assert not scheduled


@pytest.mark.parametrize('supersession', ['removed', 'replaced', 'down',
                                          'demoted', 'shutdown'])
def test_empty_pool_promotion_retry_respects_current_lifecycle(
        failed_pool_promotion, supersession):
    session, host, remote_pool, scheduled = failed_pool_promotion
    factory = session.cluster.connection_factory
    factory.reset_mock()
    if supersession == 'removed':
        session._pools.pop(host)
    elif supersession == 'replaced':
        winner = make_pool(host)
        session._pools[host] = winner
    elif supersession == 'down':
        host.set_down()
    elif supersession == 'demoted':
        session._profile_manager.distance.return_value = HostDistance.REMOTE
    elif supersession == 'shutdown':
        session.is_shutdown = True
    scheduled.pop(0)[1]()
    factory.assert_not_called()
    assert not scheduled
    if supersession == 'replaced':
        assert session._pools[host] is winner
        winner.shutdown.assert_not_called()


def displace(session, host, change):
    metadata = session.cluster.metadata
    if change == 'remove':
        assert metadata.remove_host(host)
        return None
    replacement = Host(
        '127.0.0.2' if change == 'move' else host.endpoint,
        SimpleConvictionPolicy,
        host_id=uuid4() if change == 'different_id' else host.host_id)
    if change == 'move':
        metadata.replace_host_endpoint(host, replacement)
    elif change == 'different_id':
        metadata._add_or_replace_host(replacement)
    else:
        assert metadata.remove_host(host)
        metadata.add_or_return_host(replacement)
    assert not host._is_removed  # Exercise the interval before retirement.
    return replacement


def displace_in_thread(session, host, change):
    result = Future()

    def publish():
        try:
            result.set_result(displace(session, host, change))
        except Exception as exc:
            result.set_exception(exc)

    # A lock-order regression must fail locally even if this caller holds the
    # metadata lock needed by the worker. An executor context would join it
    # before the caller could unwind and release that lock.
    worker = Thread(target=publish, daemon=True)
    worker.start()
    worker.join(5)
    assert not worker.is_alive(), 'metadata publication blocked by caller'
    return result.result(timeout=0)


@pytest.mark.parametrize('publish_first', [False, True])
def test_endpoint_move_orders_pool_and_metadata_publication(pool_session, publish_first):
    session, host = pool_session
    cluster = session.cluster
    candidate = make_pool(host)
    constructor_started = Event()
    release_constructor = Event()
    metadata_published = Event()
    release_retirement = Event()
    replace_endpoints = cluster.metadata.replace_host_endpoints

    def construct(*args, **kwargs):
        constructor_started.set()
        assert release_constructor.wait(5)
        return candidate

    def publish_then_pause(replacements):
        replace_endpoints(replacements)
        metadata_published.set()
        assert release_retirement.wait(5)

    with ThreadPoolExecutor(max_workers=1) as topology:
        try:
            with patch('cassandra.cluster.HostConnection', side_effect=construct), \
                    patch.object(cluster.metadata, 'replace_host_endpoints',
                                 side_effect=publish_then_pause):
                ready = session.add_or_renew_pool(host, False)
                assert constructor_started.wait(5)
                if publish_first:
                    release_constructor.set()
                    assert ready.result(timeout=5) is True
                moved = topology.submit(cluster.replace_host_endpoint, host,
                                        DefaultEndPoint('127.0.0.2'))
                assert metadata_published.wait(5)
                assert not host._is_removed
                replacement = cluster.metadata.get_host_by_host_id(host.host_id)
                if not publish_first:
                    release_constructor.set()
                    assert ready.result(timeout=5) is False
                    assert session._pools == {}
                    candidate.shutdown.assert_called_once_with()
                winner = make_pool(replacement)
                with patch('cassandra.cluster.HostConnection', return_value=winner):
                    assert session.add_or_renew_pool(replacement, True).result(5)
                intent = session._pool_intents[replacement]
                generation = session._pool_generations[replacement]
                # Neither a stale direct renewal nor a captured host list may
                # supersede the new lifecycle during the retirement pause.
                assert session.add_or_renew_pool(host, False).result(5) is False
                assert session.update_created_pools(hosts=(host,)) == set()
                assert session._pool_intents[replacement] is intent
                assert session._pool_generations[replacement] is generation
                assert session._pools == {replacement: winner}
                winner.shutdown.assert_not_called()
                release_retirement.set()
                assert moved.result(timeout=5) is replacement
                assert session._pools == {replacement: winner}
                winner.shutdown.assert_not_called()
                candidate.shutdown.assert_called_once_with()
        finally:
            release_constructor.set()
            release_retirement.set()


@pytest.mark.parametrize('change', ['move', 'different_id', 'same_id', 'remove'])
@pytest.mark.parametrize('reconcile', [False, True])
def test_displaced_host_cannot_claim_pool_intent(pool_session, change, reconcile):
    session, host = pool_session
    replacement = displace(session, host, change)
    if replacement is not None:
        winner = make_pool(replacement)
        with patch('cassandra.cluster.HostConnection', return_value=winner):
            assert session.add_or_renew_pool(replacement, True).result(5)
    pools = dict(session._pools)
    intents = dict(session._pool_intents)
    generations = dict(session._pool_generations)
    session._profile_manager.distance.reset_mock()
    with patch('cassandra.cluster.HostConnection') as construct:
        if reconcile:
            assert session.update_created_pools(hosts=(host,)) == set()
        else:
            assert session.add_or_renew_pool(host, False).result(5) is False
    construct.assert_not_called()
    session._profile_manager.distance.assert_not_called()
    assert session._pools == pools
    assert session._pool_intents == intents
    assert session._pool_generations == generations
    for pool in pools.values():
        pool.shutdown.assert_not_called()


@pytest.mark.parametrize('reconcile', [False, True])
@pytest.mark.parametrize('distance', [HostDistance.LOCAL, HostDistance.REMOTE,
                                      HostDistance.IGNORED])
@pytest.mark.parametrize('change', ['move', 'different_id', 'same_id', 'remove'])
def test_distance_callback_displaces_owner(pool_session, reconcile, distance, change):
    session, host = pool_session
    state = {}

    def evaluate(current):
        if current is host:
            state['discarded_intent'] = session._pool_intents[host]
            # A separate thread also proves the metadata guard is released
            # before invoking arbitrary load-balancing policy code.
            replacement = displace_in_thread(session, host, change)
            if replacement is not None:
                winner = make_pool(replacement)
                with patch('cassandra.cluster.HostConnection', return_value=winner):
                    assert session.add_or_renew_pool(replacement, True).result(5)
                state['winner'] = winner
            state['pools'] = dict(session._pools)
            state['intents'] = dict(session._pool_intents)
            if state['intents'].get(host) is state['discarded_intent']:
                state['intents'].pop(host)
            state['generations'] = dict(session._pool_generations)
            return distance
        return HostDistance.LOCAL

    session._profile_manager.distance.side_effect = evaluate
    with patch('cassandra.cluster.HostConnection') as construct:
        if reconcile:
            assert session.update_created_pools(hosts=(host,)) == set()
        else:
            assert session.add_or_renew_pool(host, False).result(5) is False
    construct.assert_not_called()
    assert session._pools == state['pools']
    assert session._pool_intents == state['intents']
    assert session._pool_generations == state['generations']
    assert state['discarded_intent'].completion.result(timeout=0) is False
    assert all(intent.host is not host for intent in session._pool_intents.values())
    if 'winner' in state:
        assert state['winner'].host_distance == HostDistance.LOCAL
        state['winner'].shutdown.assert_not_called()


@pytest.mark.parametrize('change', ['move', 'remove'])
@pytest.mark.parametrize('failure', [ConnectionException('connect'),
                                    AuthenticationFailed('authenticate')])
def test_displaced_constructor_failure_is_discounted(pool_session, change, failure):
    session, host = pool_session
    session.cluster.signal_connection_failure = Mock()
    state = {}

    def construct(*args, **kwargs):
        displace(session, host, change)
        state['generations'] = dict(session._pool_generations)
        raise failure

    with patch('cassandra.cluster.HostConnection', side_effect=construct):
        assert session.add_or_renew_pool(host, False).result(5) is False
    assert session._pool_generations == state['generations']
    assert session._pools == {}
    session.cluster.signal_connection_failure.assert_not_called()


@pytest.mark.parametrize('change', ['move', 'remove'])
@pytest.mark.parametrize('errors', [[], [ConnectionException('USE failed')]])
def test_keyspace_sync_displaces_owner(pool_session, change, errors):
    session, host = pool_session
    session.keyspace = 'ks'
    candidate = make_pool(host)
    session.cluster.on_down = Mock()
    state = {}

    def synchronize(keyspace, callback):
        # Topology and replacement pool creation must be able to finish while
        # the original pool is waiting for its USE response.
        replacement = displace(session, host, change)
        if replacement is not None:
            winner = make_pool(replacement, keyspace)
            with patch('cassandra.cluster.HostConnection', return_value=winner):
                assert session.add_or_renew_pool(replacement, True).result(5)
            state['winner'] = winner
        state['pools'] = dict(session._pools)
        state['intents'] = dict(session._pool_intents)
        state['generations'] = dict(session._pool_generations)
        candidate._keyspace = keyspace
        callback(candidate, errors)

    candidate._set_keyspace_for_all_conns.side_effect = synchronize
    with patch('cassandra.cluster.HostConnection', return_value=candidate):
        assert session.add_or_renew_pool(host, False).result(5) is False
    assert session._pools == state['pools']
    assert session._pool_intents == state['intents']
    assert session._pool_generations == state['generations']
    candidate.shutdown.assert_called_once_with()
    session.cluster.on_down.assert_not_called()
    if 'winner' in state:
        state['winner'].shutdown.assert_not_called()


def test_connection_lock_precedes_final_metadata_validation(pool_session):
    session, host = pool_session
    candidate = make_pool(host)

    def lock_connection():
        # Acquiring a connection may wait for an active request while metadata
        # changes. Publication must validate ownership after that wait.
        displace_in_thread(session, host, 'move')
        return nullcontext(True)

    candidate._lock_usable_connection.side_effect = lock_connection
    with patch('cassandra.cluster.HostConnection', return_value=candidate):
        assert session.add_or_renew_pool(host, False).result(5) is False
    assert session._pools == {}
    candidate.shutdown.assert_called_once_with()


def test_pool_assignment_holds_metadata_ownership(pool_session):
    session, host = pool_session
    candidate = make_pool(host)
    attempted = Event()
    work = []
    acquired = []
    metadata = session.cluster.metadata

    def move_endpoint():
        owns_lock = metadata._hosts_lock.acquire(False)
        acquired.append(owns_lock)
        if owns_lock:
            metadata._hosts_lock.release()
        attempted.set()
        return displace(session, host, 'move')

    with ThreadPoolExecutor(max_workers=1) as topology:
        class ObservedPools(dict):
            def __setitem__(self, key, value):
                work.append(topology.submit(move_endpoint))
                assert attempted.wait(5)
                assert acquired == [False]
                super().__setitem__(key, value)

        session._pools = ObservedPools()
        try:
            with patch('cassandra.cluster.HostConnection', return_value=candidate):
                assert session.add_or_renew_pool(host, False).result(5) is True
            assert work[0].result(timeout=5) is not host
            assert session._pools[host] is candidate
            candidate.shutdown.assert_not_called()
        finally:
            # No constructor or network wait owns this topology worker; the
            # publication guard unwinds even if an assertion fails.
            for future in work:
                future.result(timeout=5)
