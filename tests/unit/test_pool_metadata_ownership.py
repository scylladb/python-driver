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
from threading import Event, RLock, Thread
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest

from cassandra import AuthenticationFailed
from cassandra.cluster import Cluster
from cassandra.connection import ConnectionException, DefaultEndPoint
from cassandra.policies import HostDistance, SimpleConvictionPolicy
from cassandra.pool import Host
from tests.unit.utils import new_session_with_pool_state


def make_pool(host, keyspace=None):
    pool = Mock(host=host, endpoint=host.endpoint, is_shutdown=False,
                host_distance=HostDistance.LOCAL, _keyspace=keyspace,
                _membership_lock=RLock())
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
