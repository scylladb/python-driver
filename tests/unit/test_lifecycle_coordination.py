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

from concurrent.futures import Future
from threading import Event, Lock, Thread
from unittest.mock import Mock

import pytest

from cassandra.cluster import Cluster, ControlConnection
from cassandra._lifecycle import LifecycleCoordinator
from cassandra.connection import DefaultEndPoint
from tests.unit.test_control_connection import (
    HOST_ID_1, HOST_ID_2, HOST_ID_3, MockConnection, _node_meta_results)


@pytest.fixture
def topology():
    cluster = Cluster()
    cluster.control_connection.shutdown()
    cluster._prepare_all_queries = Mock()
    cluster.profile_manager = Mock()
    cluster.profile_manager.distance.return_value = 0
    cluster._start_reconnector = Mock()
    for host_id, address in ((HOST_ID_1, '192.168.1.0'),
                             (HOST_ID_2, '192.168.1.1'),
                             (HOST_ID_3, '192.168.1.2')):
        host, _ = cluster.add_host(DefaultEndPoint(address), signal=False,
                                   host_id=host_id)
        host.set_up()
    connection = MockConnection()
    control = ControlConnection(cluster, 1, 2, 0, 0)
    control._connection = connection
    cluster.control_connection = control
    yield cluster, connection
    cluster.shutdown()


@pytest.mark.parametrize('event', ['up', 'down', 'add'])
def test_listener_refresh_during_concurrent_removal(topology, event):
    cluster, connection = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    entered = Event()
    published = Event()
    refreshed = Event()
    order = []
    errors = []
    listener = Mock()

    def callback(current):
        assert current is host
        entered.set()
        assert published.wait(5)
        assert current._is_removed
        cluster.refresh_nodes()
        refreshed.set()
        order.append(event)

    getattr(listener, 'on_' + event).side_effect = callback
    listener.on_remove.side_effect = lambda host: order.append('remove')
    cluster.register_listener(listener)

    def deliver():
        try:
            if event == 'up':
                result = Future()
                result.set_result(True)
                cluster._on_up_future_completed(host, {result}, [], Lock(), result)
            elif event == 'down':
                Cluster.on_down_potentially_blocking.__wrapped__(
                    cluster, host, False, host._down_event_generation)
            else:
                host._endpoint_add_retry_pending = True
                cluster.retry_failed_endpoint_add(host)
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=deliver)
    worker.start()
    try:
        assert entered.wait(5)
        del connection.peer_results[1][1]
        cluster.refresh_nodes()
        assert cluster.metadata.get_host_by_host_id(HOST_ID_3) is None
        assert host._is_removed
        assert order == []
    finally:
        published.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not errors
    assert refreshed.is_set()
    assert order == [event, 'remove']
    cluster._start_reconnector.assert_not_called()


def test_reentrant_removal_publishes_before_delivering_replacement(topology):
    cluster, connection = topology
    old = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    old_row = connection.peer_results[1].pop()
    order = []
    replacements = []

    def remove(host):
        assert host is old
        order.append('remove-start')
        connection.peer_results[1].append(old_row)
        cluster.refresh_nodes()
        replacement = cluster.metadata.get_host_by_host_id(HOST_ID_3)
        assert replacement is not old
        assert replacement._lifecycle_pending
        replacements.append(replacement)
        assert order == ['remove-start']
        order.append('remove-end')

    def add(host):
        assert host is replacements[0]
        order.append('add')

    cluster.profile_manager.on_remove.side_effect = remove
    cluster.profile_manager.on_add.side_effect = add
    cluster.refresh_nodes()
    assert order == ['remove-start', 'remove-end', 'add']
    assert old._is_removed
    assert not replacements[0]._lifecycle_pending
    assert replacements[0].is_up


def test_older_query_cannot_overwrite_newer_topology(topology):
    cluster, connection = topology
    queried = Event()
    release = Event()
    old_connection = MockConnection()
    del old_connection.peer_results[1][1]
    old_result = _node_meta_results(old_connection.local_results,
                                    old_connection.peer_results)
    errors = []

    def query(*args, **kwargs):
        queried.set()
        assert release.wait(5)
        return old_result

    old_connection.wait_for_responses.side_effect = query

    def refresh_old():
        try:
            cluster.control_connection._refresh_node_list_and_token_map(old_connection)
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=refresh_old)
    worker.start()
    try:
        assert queried.wait(5)
        current = cluster.metadata.get_host_by_host_id(HOST_ID_3)
        cluster.refresh_nodes()
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not errors
    assert cluster.metadata.get_host_by_host_id(HOST_ID_3) is current
    assert not current._is_removed
    cluster.profile_manager.on_remove.assert_not_called()


def test_dispatcher_releases_ownership_after_callback_errors():
    coordinator = LifecycleCoordinator()
    observed = []

    def fail():
        coordinator.submit(observed.append, 'nested')
        raise RuntimeError('listener failed')

    with coordinator.transaction():
        coordinator.submit(fail)
        coordinator.submit(observed.append, 'next')
    coordinator.submit(observed.append, 'last')
    assert observed == ['next', 'nested', 'last']


def test_publication_exception_still_delivers_committed_notifications():
    coordinator = LifecycleCoordinator()
    observed = []
    with pytest.raises(ValueError):
        with coordinator.transaction():
            coordinator.submit(observed.append, 'committed')
            raise ValueError('later row rejected')
    assert observed == ['committed']


def test_notification_does_not_hold_coordination_mutex():
    coordinator = LifecycleCoordinator()
    acquired = Event()
    observations = []

    def acquire():
        with coordinator.transaction():
            acquired.set()

    def callback():
        thread = Thread(target=acquire)
        thread.start()
        observations.append(acquired.wait(5))
        thread.join(5)
        observations.append(not thread.is_alive())

    coordinator.submit(callback)
    assert acquired.is_set()
    assert observations == [True, True]


@pytest.mark.parametrize('with_future', [False, True])
def test_successful_up_releases_pending_addition_reservation(topology, with_future):
    cluster, connection = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_down()
    host._lifecycle_pending = True
    host._pending_host_addition = True
    if with_future:
        completed = Future()
        completed.set_result(True)
        cluster._on_up_future_completed(host, {completed}, [], Lock(), completed)
    else:
        cluster.on_up(host)
    assert host.is_up
    assert not host._lifecycle_pending
    assert not host._pending_host_addition


@pytest.mark.parametrize('policy_kind', ['dc', 'rack'])
def test_location_transition_removes_old_policy_membership(topology, policy_kind):
    from cassandra.policies import DCAwareRoundRobinPolicy, RackAwareRoundRobinPolicy
    cluster, connection = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_location_info('dc1', 'rack1')
    policy = (DCAwareRoundRobinPolicy('dc1', used_hosts_per_remote_dc=1)
              if policy_kind == 'dc' else
              RackAwareRoundRobinPolicy('dc1', 'rack1', used_hosts_per_remote_dc=1))
    policy.populate(cluster, [host])
    cluster.profile_manager.on_down.side_effect = policy.on_down
    cluster.profile_manager.on_up.side_effect = policy.on_up
    cluster.profile_manager.on_remove.side_effect = policy.on_remove
    with cluster._lifecycle.transaction():
        assert cluster.control_connection._update_location_info(host, 'dc2', 'rack2')
        assert host.datacenter == 'dc1'
    assert host.datacenter == 'dc2'
    assert host.rack == 'rack2'
    assert list(policy.make_query_plan()).count(host) == 1
    cluster.remove_host(host, refresh_nodes=False)
    assert host not in list(policy.make_query_plan())


@pytest.mark.parametrize('custom_callback', ['distance', 'reset'])
def test_custom_policy_refresh_does_not_hold_host_state_lock(topology, custom_callback):
    cluster, connection = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    entered = Event()
    published = Event()
    returned = Event()
    errors = []

    def callback(*args):
        entered.set()
        assert published.wait(5)
        cluster.refresh_nodes()
        returned.set()
        return 0

    if custom_callback == 'distance':
        cluster.profile_manager.distance.side_effect = callback
        del cluster._start_reconnector
        operation = lambda: Cluster.on_down_potentially_blocking.__wrapped__(
            cluster, host, False, host._down_event_generation)
    else:
        host.conviction_policy.reset = callback
        operation = lambda: cluster._finalize_add(host)

    def run():
        try:
            operation()
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=run)
    worker.start()
    try:
        assert entered.wait(5)
        del connection.peer_results[1][1]
        cluster.refresh_nodes()
        assert host._is_removed
    finally:
        published.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not errors
    assert returned.is_set()
    assert host._is_removed and not host.is_up
    assert host._reconnection_handler is None


def test_newer_location_reversal_supersedes_queued_change(topology):
    cluster, connection = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_location_info('dc1', 'rack1')
    control = cluster.control_connection
    with cluster._lifecycle.transaction():
        assert control._update_location_info(host, 'dc2', 'rack2')
        assert control._update_location_info(host, 'dc1', 'rack1')
    assert (host.datacenter, host.rack) == ('dc1', 'rack1')
    assert host._pending_location is None


def test_location_delivery_replaces_stale_replica_cache(topology):
    from cassandra.metadata import KeyspaceMetadata, Murmur3Token
    cluster, connection = topology
    metadata = cluster.metadata
    host = metadata.get_host_by_host_id(HOST_ID_3)
    host.set_location_info('dc1', 'rack1')
    metadata.keyspaces['ks'] = KeyspaceMetadata(
        'ks', True, 'NetworkTopologyStrategy', {'dc2': 1})
    with cluster._lifecycle.transaction():
        cluster.control_connection._update_location_info(host, 'dc2', 'rack2')
        metadata.rebuild_token_map('Murmur3Partitioner', {host: ['0']})
        old_map = metadata.token_map
        assert old_map.get_replicas('ks', Murmur3Token(0)) == []
    assert metadata.token_map is not old_map
    assert metadata.token_map.get_replicas('ks', Murmur3Token(0)) == [host]
    # Even late publication of old calculated caches cannot poison the new map.
    old_map.tokens_to_hosts_by_ks['ks'] = {Murmur3Token(0): []}
    assert metadata.token_map.get_replicas('ks', Murmur3Token(0)) == [host]


def test_repeated_location_destination_keeps_latest_reservation(topology):
    cluster, connection = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_location_info('dc1', 'rack1')
    control = cluster.control_connection
    requested = []

    def on_up(current):
        if current.datacenter == 'dc3' and not requested:
            requested.append(True)
            assert control._update_location_info(host, 'dc3', 'rack1')

    cluster.profile_manager.on_up.side_effect = on_up
    with cluster._lifecycle.transaction():
        for dc in ('dc2', 'dc3', 'dc2'):
            assert control._update_location_info(host, dc, 'rack1')
    assert host.datacenter == 'dc3'
    assert host._pending_location is None


@pytest.mark.parametrize('deferred', [False, True])
def test_notification_submission_never_returns_callback_result(deferred):
    coordinator = LifecycleCoordinator()
    observed = []

    def callback():
        observed.append('delivered')
        return 'not a synchronous result'

    if deferred:
        with coordinator.transaction():
            assert coordinator.submit(callback) is None
            assert observed == []
    else:
        assert coordinator.submit(callback) is None
    assert observed == ['delivered']


def test_queued_addition_checks_identity_at_delivery(topology):
    cluster, _ = topology
    old = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    callback = Mock()
    with cluster._lifecycle.transaction():
        cluster._lifecycle.submit_current(old, callback)
        replacement = cluster.replace_host_endpoint(
            old, DefaultEndPoint('192.168.1.9'))
        assert replacement.host_id == old.host_id
        assert old._is_removed
    callback.assert_not_called()
    assert cluster.metadata.get_host_by_host_id(old.host_id) is replacement


def test_retirement_during_readiness_reset_cannot_mark_host_ready(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    cluster._lifecycle.reserve_addition(host)
    host.conviction_policy.reset = Mock(
        side_effect=lambda: cluster.remove_host(host))
    assert not cluster._lifecycle.mark_ready(host)
    assert host._is_removed
    assert not host.is_up
    assert not host._lifecycle_pending


@pytest.mark.parametrize('event', ['location', 'up', 'down', 'add'])
def test_shutdown_discards_pending_notifications(topology, event):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    original_location = (host.datacenter, host.rack)
    host.set_down()
    with cluster._lifecycle.transaction():
        if event == 'location':
            cluster._lifecycle.update_location(host, 'later_dc', 'later_rack')
        elif event == 'up':
            cluster.on_up(host)
        elif event == 'down':
            Cluster.on_down_potentially_blocking.__wrapped__(
                cluster, host, False, host._down_event_generation)
        else:
            cluster.on_add(host)
        cluster.shutdown()
    cluster.profile_manager.on_up.assert_not_called()
    cluster.profile_manager.on_down.assert_not_called()
    cluster.profile_manager.on_add.assert_not_called()
    assert (host.datacenter, host.rack) == original_location
    assert host._pending_location is None
    assert not host._lifecycle_pending
    assert not cluster._lifecycle._events
    callback = Mock()
    cluster._lifecycle.submit(callback)
    callback.assert_not_called()


def test_shutdown_from_executor_listener_does_not_join_itself(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    listener = Mock()
    shutdown_returned = Event()

    def shutdown_from_listener(_host):
        cluster.shutdown()
        shutdown_returned.set()

    listener.on_up.side_effect = shutdown_from_listener
    cluster.register_listener(listener)
    result = Future()
    result.set_result(True)
    finished = cluster.executor.submit(
        cluster._on_up_future_completed, host, {result}, [], Lock(), result)
    finished.result(timeout=5)
    assert cluster.is_shutdown
    assert shutdown_returned.is_set()
    assert cluster._lifecycle.closed
    assert not host._currently_handling_node_up
    listener.on_up.assert_called_once_with(host)


def test_location_listener_shutdown_stops_location_publication(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    original_location = (host.datacenter, host.rack)
    cluster.profile_manager.on_down.side_effect = lambda _: cluster.shutdown()
    cluster._lifecycle.update_location(host, 'later_dc', 'later_rack')
    assert (host.datacenter, host.rack) == original_location
    cluster.profile_manager.on_up.assert_not_called()
    assert host._pending_location is None


def test_late_up_pool_completion_after_shutdown_is_discarded(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_down()
    session = Mock()
    pool_ready = Future()
    session.add_or_renew_pool.return_value = pool_ready
    cluster.sessions.add(session)
    listener = Mock()
    cluster.register_listener(listener)
    cluster.on_up(host)
    assert host._currently_handling_node_up
    cluster.shutdown()
    pool_ready.set_result(True)
    listener.on_up.assert_not_called()
    assert not host.is_up
    assert not host._currently_handling_node_up
    session.update_created_pools.assert_not_called()


def test_repeated_retirement_delivers_removal_once(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    listener = Mock()
    cluster.register_listener(listener)
    with cluster._lifecycle.transaction():
        cluster.remove_host(host, refresh_nodes=False)
        cluster.on_remove(host, refresh_nodes=False)
        cluster.remove_host(host, refresh_nodes=False)
    listener.on_remove.assert_called_once_with(host)
    assert host._is_removed
    assert not host._lifecycle_pending


def test_shutdown_cancels_retired_reconnector_before_queued_removal(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    handler = Mock()
    host._reconnection_handler = handler
    with cluster._lifecycle.transaction():
        cluster.remove_host(host, refresh_nodes=False)
        cluster.shutdown()
    handler.cancel.assert_called_once_with()
    assert host._reconnection_handler is None


def test_close_does_not_wait_for_active_notification():
    coordinator = LifecycleCoordinator()
    entered = Event()
    release = Event()
    later = Mock()

    def callback():
        entered.set()
        assert release.wait(5)
        coordinator.submit(later)

    worker = Thread(target=coordinator.submit, args=(callback,))
    worker.start()
    try:
        assert entered.wait(5)
        coordinator.submit(later)
        coordinator.close()
        assert worker.is_alive()
        assert coordinator.closed
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    later.assert_not_called()


def test_refresh_result_after_shutdown_cannot_publish(topology):
    cluster, connection = topology
    queried = Event()
    release = Event()
    old_query = connection.wait_for_responses.side_effect
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    del connection.peer_results[1][1]
    result = _node_meta_results(connection.local_results, connection.peer_results)

    def query(*args, **kwargs):
        queried.set()
        assert release.wait(5)
        return result

    connection.wait_for_responses.side_effect = query
    apply_topology = Mock(
        wraps=cluster.control_connection._apply_node_list_and_token_map)
    cluster.control_connection._apply_node_list_and_token_map = apply_topology
    errors = []

    def refresh():
        try:
            cluster.control_connection._refresh_node_list_and_token_map(connection)
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=refresh)
    worker.start()
    try:
        assert queried.wait(5)
        cluster.shutdown()
    finally:
        release.set()
        worker.join(5)
        connection.wait_for_responses.side_effect = old_query
    assert not worker.is_alive()
    assert not errors
    apply_topology.assert_not_called()
    assert cluster.metadata.get_host_by_host_id(host.host_id) is host
    cluster.profile_manager.on_remove.assert_not_called()


@pytest.mark.parametrize('event', ['up', 'add'])
def test_policy_shutdown_prevents_remaining_lifecycle_work(topology, event):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_down()
    cluster.profile_manager.distance.side_effect = lambda _: (cluster.shutdown() or 0)
    cluster.control_connection.on_add = Mock()
    getattr(cluster, 'on_' + event)(host)
    cluster._prepare_all_queries.assert_not_called()
    cluster.profile_manager.on_up.assert_not_called()
    assert not host._currently_handling_node_up


def test_failed_ordinary_add_releases_reservation_without_dropping_healthy_pool(topology):
    from tests.unit.utils import new_session_with_pool_state

    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.is_up = None
    cluster.control_connection.on_add = Mock()
    healthy_pool = Mock(host=host, is_shutdown=False)
    good = new_session_with_pool_state({host: healthy_pool})
    bad = new_session_with_pool_state()
    for session, succeeded in ((good, True), (bad, False)):
        session.cluster = cluster
        session._profile_manager = cluster.profile_manager
        session.is_shutdown = False
        session.shutdown = Mock()
        result = Future()
        result.set_result(succeeded)
        session.add_or_renew_pool = Mock(return_value=result)
        session.get_pool_state = Mock(
            return_value={host: {'open_count': int(succeeded)}})
        cluster.sessions.add(session)
    cluster.on_add(host)
    assert not host._lifecycle_pending
    assert not cluster.on_down(host, True, expect_host_to_be_down=True)
    bad.add_or_renew_pool.reset_mock()
    bad.update_created_pools(hosts=(host,))
    assert bad.add_or_renew_pool.call_count == 1
    assert good._pools[host] is healthy_pool
    healthy_pool.shutdown.assert_not_called()


def test_newer_refresh_preserves_pending_forced_token_rebuild(topology):
    cluster, connection = topology
    cluster.refresh_nodes(force_token_rebuild=True)
    old_ring = list(cluster.metadata.token_map.ring)
    queried = Event()
    release = Event()
    older = MockConnection()
    old_result = _node_meta_results(older.local_results, older.peer_results)

    def query(*args, **kwargs):
        queried.set()
        assert release.wait(5)
        return old_result

    older.wait_for_responses.side_effect = query
    errors = []

    def force_refresh():
        try:
            cluster.control_connection._refresh_node_list_and_token_map(
                older, force_token_rebuild=True)
        except Exception as exc:
            errors.append(exc)

    worker = Thread(target=force_refresh)
    worker.start()
    try:
        assert queried.wait(5)
        connection.peer_results[1][1][5] = ['300', '400', '500']
        cluster.refresh_nodes()
        new_ring = list(cluster.metadata.token_map.ring)
        assert new_ring != old_ring
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not errors
    assert list(cluster.metadata.token_map.ring) == new_ring


def test_stale_unix_candidate_binds_without_republishing_topology(topology):
    from cassandra.connection import UnixSocketEndPoint

    cluster, _ = topology
    candidate = MockConnection()
    candidate.original_endpoint = candidate.endpoint = UnixSocketEndPoint(
        '/tmp/lifecycle-candidate.sock')
    revision = cluster._lifecycle.reserve_refresh()
    cluster.refresh_nodes()
    current = cluster.metadata.get_host_by_host_id(HOST_ID_1)
    token_map = cluster.metadata.token_map
    cluster.control_connection._refresh_node_list_and_token_map(
        candidate, preloaded_results=_node_meta_results(
            candidate.local_results, candidate.peer_results), revision=revision)
    assert candidate._control_connection_host is current
    assert cluster.control_connection._connection_matches_host(candidate, current)
    assert cluster.metadata.token_map is token_map


def test_stale_candidate_cannot_bind_to_new_route(topology):
    from cassandra.connection import ConnectionException

    cluster, _ = topology
    candidate = MockConnection()
    revision = cluster._lifecycle.reserve_refresh()
    cluster.refresh_nodes()
    old = cluster.metadata.get_host_by_host_id(HOST_ID_1)
    replacement = cluster.replace_host_endpoint(
        old, DefaultEndPoint('192.168.1.100'))
    with pytest.raises(ConnectionException, match='snapshot is stale'):
        cluster.control_connection._refresh_node_list_and_token_map(
            candidate, preloaded_results=_node_meta_results(
                candidate.local_results, candidate.peer_results), revision=revision)
    assert vars(candidate).get('_control_connection_host') is None
    assert cluster.metadata.get_host_by_host_id(HOST_ID_1) is replacement


def test_duplicate_up_does_not_schedule_another_pool_creation(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_down()
    cluster.control_connection.on_up = Mock()
    pool_ready = Future()
    session = Mock()
    session.add_or_renew_pool.return_value = pool_ready
    cluster.sessions.add(session)
    cluster.on_up(host)
    cluster.on_up(host)
    session.add_or_renew_pool.assert_called_once_with(host, is_host_addition=False)
    assert host._currently_handling_node_up
    pool_ready.set_result(True)
    assert host.is_up
    assert not host._currently_handling_node_up


def test_superseded_down_does_not_reach_later_callbacks(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    generation = host._down_event_generation
    cluster.profile_manager.on_down.side_effect = (
        lambda current: cluster._lifecycle.supersede_down(current))
    cluster.control_connection.on_down = Mock()
    session = Mock()
    cluster.sessions.add(session)
    listener = Mock()
    cluster.register_listener(listener)
    Cluster.on_down_potentially_blocking.__wrapped__(
        cluster, host, False, generation)
    cluster.profile_manager.on_down.assert_called_once_with(host)
    cluster.control_connection.on_down.assert_not_called()
    session.on_down.assert_not_called()
    listener.on_down.assert_not_called()
    cluster._start_reconnector.assert_not_called()


def test_later_failed_force_does_not_hide_earlier_force_intent(topology):
    cluster, connection = topology
    cluster.refresh_nodes(force_token_rebuild=True)
    old_ring = list(cluster.metadata.token_map.ring)
    older_force = cluster._lifecycle.reserve_refresh(force_token_rebuild=True)
    middle_refresh = cluster._lifecycle.reserve_refresh()
    failing_connection = MockConnection()
    failing_connection.wait_for_responses.side_effect = RuntimeError('query failed')
    with pytest.raises(RuntimeError, match='query failed'):
        cluster.control_connection._refresh_node_list_and_token_map(
            failing_connection, force_token_rebuild=True)
    connection.peer_results[1][1][5] = ['300', '400', '500']
    cluster.control_connection._refresh_node_list_and_token_map(
        connection, preloaded_results=_node_meta_results(
            connection.local_results, connection.peer_results), revision=middle_refresh)
    middle_ring = list(cluster.metadata.token_map.ring)
    assert middle_ring != old_ring
    cluster.control_connection._refresh_node_list_and_token_map(
        connection, preloaded_results=_node_meta_results(
            connection.local_results, connection.peer_results), revision=older_force)
    assert list(cluster.metadata.token_map.ring) == middle_ring
    connection.peer_results[1][1][5] = ['600', '700', '800']
    cluster.refresh_nodes()
    assert list(cluster.metadata.token_map.ring) != middle_ring


def test_pool_failure_policy_refresh_runs_without_session_or_host_lock(topology):
    from tests.unit.utils import new_session_with_pool_state

    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    session = new_session_with_pool_state()
    session.cluster = cluster
    session.shutdown = Mock()
    session.get_pool_state = Mock(return_value={})
    cluster.sessions.add(session)
    cluster.on_down_potentially_blocking = Mock(return_value=Future())
    with session._lock:
        intent = session._claim_pool_intent_locked(host)
        generation = object()
        session._pool_generations[host] = generation
    observations = []

    def distance(_host):
        def publication():
            with cluster._lifecycle.transaction():
                session_locked = session._lock.acquire(timeout=2)
                host_locked = host.lock.acquire(timeout=2)
                observations.extend((session_locked, host_locked))
                if host_locked:
                    host.lock.release()
                if session_locked:
                    session._lock.release()

        worker = Thread(target=publication)
        worker.start()
        worker.join(5)
        observations.append(not worker.is_alive())
        cluster.refresh_nodes()
        return 0

    cluster.profile_manager.distance.side_effect = distance
    session._handle_pool_creation_failure(
        host, generation, None, intent, 0,
        lambda reservation: cluster.on_down(
            host, False, _pool_failure=reservation), False)
    assert observations == [True, True, True]
    cluster.on_down_potentially_blocking.assert_called_once()


def test_pool_failure_rechecks_ownership_after_policy_callback(topology):
    from tests.unit.utils import new_session_with_pool_state

    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    session = new_session_with_pool_state()
    session.cluster = cluster
    session.shutdown = Mock()
    session.get_pool_state = Mock(return_value={})
    cluster.sessions.add(session)
    cluster.on_down_potentially_blocking = Mock(return_value=Future())
    with session._lock:
        intent = session._claim_pool_intent_locked(host)
        generation = object()
        session._pool_generations[host] = generation
    winner = Mock(host=host, is_shutdown=False)

    def publish_new_pool(_host):
        with session._lock:
            with host.lock:
                session._pools[host] = winner
                session._pool_generations[host] = object()
        return 0

    cluster.profile_manager.distance.side_effect = publish_new_pool
    session._handle_pool_creation_failure(
        host, generation, None, intent, 0,
        lambda reservation: cluster.on_down(
            host, False, _pool_failure=reservation), False)
    assert host.is_up
    assert session._pools[host] is winner
    cluster.on_down_potentially_blocking.assert_not_called()
    winner.shutdown.assert_not_called()


def test_shutdown_flag_racing_replacement_does_not_skip_old_retirement(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    handler = Mock()
    host._reconnection_handler = handler
    with cluster._lifecycle.transaction():
        cluster.metadata.remove_host(host)
        cluster.is_shutdown = True
        cluster.on_remove(host)
        assert host._is_removed
        cluster._shutdown(wait_for_executor=True)
    handler.cancel.assert_called_once_with()


def test_removal_cancels_detached_reconnector_even_if_shutdown_wins(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    handler = Mock()
    host._reconnection_handler = handler
    take_handler = host.get_and_set_reconnection_handler

    def take_then_shutdown(replacement):
        previous = take_handler(replacement)
        if previous is handler:
            cluster.shutdown()
        return previous

    host.get_and_set_reconnection_handler = take_then_shutdown
    cluster.remove_host(host, refresh_nodes=False)
    handler.cancel.assert_called_once_with()


def test_down_is_queued_before_executor_handoff(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    cluster._discount_down_events = False
    cluster.control_connection.on_down = Mock()
    cluster.control_connection.on_up = Mock()
    original_submit = cluster.executor.submit
    scheduled = []

    def delayed_submit(fn, *args, **kwargs):
        scheduled.append((fn, args, kwargs))
        return Future()

    cluster.executor.submit = delayed_submit
    order = []
    cluster.profile_manager.on_down.side_effect = lambda _: order.append('down')
    cluster.profile_manager.on_up.side_effect = lambda _: order.append('up')
    try:
        assert cluster.on_down(host, False)
        assert order == []
        cluster.on_up(host)
        assert order == ['down', 'up']
        for fn, args, kwargs in scheduled:
            fn(*args, **kwargs)
        assert order == ['down', 'up']
        assert host.is_up
    finally:
        cluster.executor.submit = original_submit


def test_rejected_executor_handoff_removes_queued_down(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    cluster._discount_down_events = False
    original_submit = cluster.executor.submit
    cluster.executor.submit = Mock(side_effect=RuntimeError('executor stopped'))
    try:
        assert not cluster.on_down(host, False)
        assert not host._currently_handling_node_down
        cluster._lifecycle.drain()
        cluster.profile_manager.on_down.assert_not_called()
    finally:
        cluster.executor.submit = original_submit
