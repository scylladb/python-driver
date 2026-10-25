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
from threading import Barrier, Event, Lock, Thread, get_ident
from unittest.mock import Mock, patch

import pytest

from cassandra.cluster import Cluster, ControlConnection
from cassandra._lifecycle import (
    ConnectionFailureReservation, LifecycleCoordinator, PoolFailureReservation)
from cassandra.connection import (
    Connection, ConnectionException, DefaultEndPoint)
from cassandra.policies import HostDistance
from cassandra.pool import HostConnection
from cassandra.protocol_features import ProtocolFeatures
from tests.unit.test_control_connection import (
    HOST_ID_1, HOST_ID_2, HOST_ID_3, MockConnection, _node_meta_results)
from tests.unit.utils import new_session_with_pool_state


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


@pytest.mark.parametrize('initial_state', [None, False, True])
def test_ignored_add_during_down_inspection_does_not_establish_health(
        topology, initial_state):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.is_up = initial_state
    cluster.control_connection.on_add = Mock()
    cluster.on_down_potentially_blocking = Mock(return_value=Future())
    cluster._restart_reconnector = Mock(return_value=Future())
    listener = Mock()
    cluster.register_listener(listener)

    def distance(current):
        assert current is host
        cluster.profile_manager.distance.side_effect = None
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
        cluster.on_add(host, refresh_nodes=False)
        listener.on_add.assert_called_once_with(host)
        assert host.is_up is initial_state
        return HostDistance.IGNORED

    cluster.profile_manager.distance.side_effect = distance

    dispatched = cluster.on_down(host, is_host_addition=False)

    cluster.profile_manager.distance.assert_called()
    listener.on_add.assert_called_once_with(host)
    assert host.is_up is False
    assert dispatched is (initial_state is True)
    if initial_state:
        cluster.on_down_potentially_blocking.assert_called_once()
        cluster._restart_reconnector.assert_not_called()
    else:
        cluster.on_down_potentially_blocking.assert_not_called()
        cluster._restart_reconnector.assert_called_once()


@pytest.mark.parametrize('inspection', ['distance', 'pool_state'])
@pytest.mark.parametrize('event', ['up', 'add'])
@pytest.mark.parametrize('healthy', [False, True])
def test_down_health_inspection_respects_aggregate_pool_health(
        topology, inspection, event, healthy):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_down()
    session = new_session_with_pool_state()
    session.cluster = cluster
    session._profile_manager = cluster.profile_manager
    session.keyspace = None
    session.is_shutdown = False
    session.shutdown = Mock()
    session.update_created_pools = Mock()
    cluster.sessions = (session,)
    cluster.control_connection.on_up = Mock()
    cluster.control_connection.on_add = Mock()
    cluster._prepare_host = lambda host, success, failure: success()
    cluster.on_down_potentially_blocking = Mock(return_value=Future())
    connection = _PublicationConnection(host.endpoint, compression=False)
    constructing = Event()
    release = Event()
    completed = Event()
    listener = Mock()
    getattr(listener, 'on_' + event).side_effect = lambda _: completed.set()
    cluster.register_listener(listener)

    def construct(*args, **kwargs):
        constructing.set()
        assert release.wait(5)
        return connection

    cluster.connection_factory = Mock(side_effect=construct)
    owns = session._owns_pool_generation_locked

    def check_ownership(*args):
        if cluster.connection_factory.called and not healthy:
            connection.defunct(ConnectionException('candidate failed'))
        return owns(*args)

    session._owns_pool_generation_locked = check_ownership
    if event == 'up':
        cluster.on_up(host)
    else:
        cluster.on_add(host, refresh_nodes=False)
    assert constructing.wait(5)

    def finish():
        release.set()
        assert completed.wait(5)
        assert host.is_up

    if inspection == 'distance':
        def distance(_host):
            finish()
            return HostDistance.IGNORED
        cluster.profile_manager.distance.side_effect = distance
    else:
        def pool_state():
            observed = {host: {'open_count': 0}}
            finish()
            return observed
        session.get_pool_state = Mock(side_effect=pool_state)

    try:
        assert cluster.on_down(host, False) is (not healthy)
        assert host.is_up is healthy
    finally:
        release.set()
    if healthy:
        cluster.on_down_potentially_blocking.assert_not_called()
        cluster.profile_manager.distance.side_effect = None
        session.get_pool_state = Mock(return_value={host: {'open_count': 0}})
        assert cluster.on_down(host, False)
    cluster.on_down_potentially_blocking.assert_called_once()


@pytest.mark.parametrize('event', ['up', 'add'])
@pytest.mark.parametrize('ignored', [False, True])
def test_readiness_without_pool_work_retains_health_authority(
        topology, event, ignored):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_down()
    cluster.sessions = ()
    if ignored:
        cluster.profile_manager.distance.return_value = HostDistance.IGNORED
    cluster._prepare_host = lambda host, success, failure: success()
    cluster.control_connection.on_up = Mock()
    cluster.control_connection.on_add = Mock()
    before = host._health_generation
    if event == 'up':
        cluster.on_up(host)
    else:
        cluster.on_add(host, refresh_nodes=False, set_up_if_ignored=True)
    assert host.is_up
    assert host._health_generation > before


@pytest.mark.parametrize('inspection', ['distance', 'pool_state'])
@pytest.mark.parametrize('pool_failure', [False, True])
@pytest.mark.parametrize('pool_distance, connect_remote, connection_healthy', [
    pytest.param(HostDistance.REMOTE, False, True, id='disabled_remote'),
    pytest.param(HostDistance.REMOTE, True, True, id='healthy_remote'),
    pytest.param(HostDistance.LOCAL, False, True, id='healthy_local'),
    pytest.param(HostDistance.REMOTE, True, False, id='closed_remote'),
    pytest.param(HostDistance.LOCAL, False, False, id='closed_local'),
])
def test_down_health_inspection_cannot_undo_pool_publication(
        topology, inspection, pool_failure, pool_distance, connect_remote,
        connection_healthy):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    cluster.on_down_potentially_blocking = Mock(return_value=Future())
    session = new_session_with_pool_state()
    session.cluster = cluster
    session._profile_manager = cluster.profile_manager
    session.keyspace = None
    session.is_shutdown = False
    session.shutdown = Mock()
    cluster.sessions = (session,)
    cluster.connect_to_remote_hosts = connect_remote
    cluster.profile_manager.distance.return_value = pool_distance
    connection = Mock(
        features=ProtocolFeatures(shard_id=0),
        is_closed=not connection_healthy,
        is_defunct=False, lock=Lock())
    cluster.connection_factory = Mock(return_value=connection)
    has_connections = pool_distance != HostDistance.REMOTE or connect_remote
    establishes_health = has_connections and connection_healthy

    def publish_pool():
        assert session.add_or_renew_pool(
            host, is_host_addition=False).result(timeout=5) is True

    if inspection == 'distance':
        def distance(_host):
            cluster.profile_manager.distance.side_effect = None
            publish_pool()
            return HostDistance.IGNORED
        cluster.profile_manager.distance.side_effect = distance
    else:
        def pool_state():
            publish_pool()
            return {host: {'open_count': 0}}
        session.get_pool_state = Mock(side_effect=pool_state)

    # Keep a failing session's reservation current while another session
    # publishes a pool. The host observation must fence both admission paths.
    reservation = None
    if pool_failure:
        failed = new_session_with_pool_state()
        failed.is_shutdown = False
        intent = object()
        generation = object()
        failed._pool_intents[host] = intent
        failed._pool_generations[host] = generation
        reservation = PoolFailureReservation(
            failed, host, generation, None, intent, host.endpoint)

    dispatched = cluster.on_down(
        host, is_host_addition=False, _pool_failure=reservation)

    candidate = session._pools[host]
    assert candidate.open_count == int(establishes_health)
    assert cluster.connection_factory.call_count == int(has_connections)
    assert not candidate.is_shutdown
    assert host.is_up is establishes_health
    assert dispatched is (not establishes_health)
    if establishes_health:
        cluster.on_down_potentially_blocking.assert_not_called()
    else:
        cluster.on_down_potentially_blocking.assert_called_once()
    if reservation is not None:
        assert reservation.is_current()

    if not establishes_health:
        return

    # A later genuine failure must still be admitted with a fresh observation.
    cluster.profile_manager.distance.side_effect = None
    cluster.profile_manager.distance.return_value = HostDistance.LOCAL
    session.get_pool_state = Mock(return_value={host: {'open_count': 0}})
    assert cluster.on_down(
        host, is_host_addition=False, _pool_failure=reservation)
    assert host.is_up is False
    cluster.on_down_potentially_blocking.assert_called_once()


class _PublicationConnection(Connection):
    """Real connection state transitions without a network transport."""

    def close(self):
        with self.lock:
            self.is_closed = True


def test_duplicate_failed_callback_allows_same_pool_use_response(topology):
    from cassandra.protocol import ResultMessage, RESULT_KIND_SET_KEYSPACE
    from cassandra.shard_info import _ShardingInfo

    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    session = new_session_with_pool_state()
    session.cluster = cluster
    session.keyspace = None
    session.is_shutdown = False
    session.submit = Mock()
    original = _PublicationConnection(host.endpoint, compression=False)
    original.features = ProtocolFeatures(shard_id=0)
    candidate = _PublicationConnection(host.endpoint, compression=False)
    candidate.features = ProtocolFeatures(shard_id=0)
    cluster.connection_factory = Mock(side_effect=[original, candidate])
    cluster.shard_aware_options.disable_shardaware_port = True
    pool = HostConnection(host, HostDistance.LOCAL, session)
    session._pools[host] = pool
    session.keyspace = pool._keyspace = original.keyspace = 'ks'
    host.sharding_info = _ShardingInfo(0, 2, '', '', 0, None, None)
    host.signal_connection_failure = Mock(return_value=False)
    original.in_flight = 2
    original.is_defunct = True
    original.last_error = ConnectionException('socket failed', host.endpoint)
    pool.return_connection(original)
    assert original.in_flight == 1
    session.submit.assert_called_once_with(pool._replace, original)
    pool._replace(original)
    assert session.submit.call_count == 2
    session.submit.assert_called_with(pool._open_connection_to_missing_shard, 0)

    io_waiting = Event()
    reactor_done = Event()
    response_lock = Lock()
    responses = []
    errors = []

    def send(message, request_id, cb, **kwargs):
        with response_lock:
            responses.append(cb)
        io_waiting.set()
        return 0

    def respond():
        with response_lock:
            callbacks = list(responses)
            responses.clear()
        for callback in callbacks:
            callback(ResultMessage(RESULT_KIND_SET_KEYSPACE))

    def run(fn):
        try:
            fn()
        except BaseException as exc:
            errors.append(exc)

    def process_responses():
        # The same reactor must finish this redundant failure callback before
        # delivering the USE response that releases the replacement work lock.
        pool.return_connection(original)
        respond()
        reactor_done.set()

    candidate.send_msg = send
    worker = Thread(target=run, args=(lambda: pool._open_connection_to_missing_shard(0),))
    reactor = Thread(target=run, args=(process_responses,))
    try:
        worker.start()
        assert io_waiting.wait(5)
        reactor.start()
        assert reactor_done.wait(2), 'duplicate failure blocked same-pool USE response'
    finally:
        respond()
        for thread in (worker, reactor):
            if thread.ident is not None:
                thread.join(5)
        pool.shutdown()
    assert all(not thread.is_alive() for thread in (worker, reactor))
    assert not errors
    assert original.in_flight == candidate.in_flight == 0
    assert candidate.keyspace == 'ks'
    host.signal_connection_failure.assert_called_once_with(original.last_error)
    assert session.submit.call_count == 2


@pytest.mark.parametrize('independent_healthy_pool', [False, True])
def test_concurrent_session_failures_retire_before_health_inspection(
        topology, independent_healthy_pool):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    sessions, pools, failed, siblings = [], [], [], []
    for index in range(2 + int(independent_healthy_pool)):
        session = new_session_with_pool_state()
        session.cluster = cluster
        session.keyspace = None
        session.is_shutdown = False
        session.shutdown = Mock()
        session.on_down = Mock()
        connection = _PublicationConnection(host.endpoint, compression=False)
        connection.features = ProtocolFeatures(shard_id=0)
        cluster.connection_factory = Mock(return_value=connection)
        pool = HostConnection(host, HostDistance.LOCAL, session)
        session._pools[host] = pool
        sessions.append(session)
        pools.append(pool)
        if index < 2:
            sibling = _PublicationConnection(host.endpoint, compression=False)
            sibling.features = ProtocolFeatures(shard_id=1)
            pool._connections[1] = sibling
            siblings.append(sibling)
            connection.is_defunct = True
            connection.in_flight = 1
            connection.last_error = ConnectionException('shard failed', host.endpoint)
            failed.append(connection)
    cluster.sessions = tuple(sessions)
    host.signal_connection_failure = Mock(return_value=True)
    cluster.control_connection.on_down = Mock()
    snapshots = Barrier(2)
    for session in sessions[:2]:
        get_state = session.get_pool_state

        def inspect(get_state=get_state):
            state = get_state()
            # Both failures capture the other session's real pool health
            # before either can finish DOWN admission or its old cleanup.
            snapshots.wait(timeout=5)
            return state

        session.get_pool_state = inspect

    errors, scheduled, order = [], [], []
    cluster.profile_manager.on_down.side_effect = lambda _: order.append('down')
    cluster._start_reconnector.side_effect = lambda *a, **kw: order.append('recovery')

    def delayed_submit(fn, *args, **kwargs):
        scheduled.append((fn, args, kwargs))
        return Future()

    def fail(index):
        try:
            pools[index].return_connection(failed[index])
        except BaseException as exc:
            errors.append(exc)

    workers = [Thread(target=fail, args=(index,)) for index in range(2)]
    try:
        with patch.object(cluster.executor, 'submit', side_effect=delayed_submit):
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join(7)
            assert all(not worker.is_alive() for worker in workers)
            assert not errors
            assert all(pool.is_shutdown for pool in pools[:2])
            assert all(connection.is_closed for connection in failed + siblings)
            assert all(connection.in_flight == 0 for connection in failed)
            assert host.is_up is independent_healthy_pool
            assert len(scheduled) == int(not independent_healthy_pool)
            # Check actual FIFO delivery and recovery, including a later
            # notification. A stale second failure must not duplicate DOWN.
            with cluster._lifecycle.transaction(drain=False):
                cluster._lifecycle.submit(order.append, 'later')
            cluster._lifecycle.drain()
            for fn, args, kwargs in scheduled:
                fn(*args, **kwargs)
            assert order == (['later'] if independent_healthy_pool else
                             ['down', 'recovery', 'later'])
            assert cluster._start_reconnector.call_count == int(not independent_healthy_pool)
            if independent_healthy_pool:
                healthy, _ = pools[2].borrow_connection(timeout=0)
                assert not healthy.is_closed
                pools[2].return_connection(healthy)
                assert not pools[2].is_shutdown
    finally:
        snapshots.abort()
        for worker in workers:
            if worker.ident is not None:
                worker.join(5)
        for pool in pools:
            pool.shutdown()


@pytest.mark.parametrize('supersede', [
    None, 'pool', 'endpoint', 'host', 'session', 'cluster', 'health',
])
def test_retired_failure_admission_does_not_wait_for_pool_work(
        topology, supersede):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    session = new_session_with_pool_state()
    session.cluster = cluster
    session.keyspace = None
    session.is_shutdown = False
    session.shutdown = Mock()
    connection = _PublicationConnection(host.endpoint, compression=False)
    connection.features = ProtocolFeatures(shard_id=0)
    cluster.connection_factory = Mock(return_value=connection)
    pool = HostConnection(host, HostDistance.LOCAL, session)
    session._pools[host] = pool
    cluster.sessions = (session,)
    successor = HostConnection(host, HostDistance.IGNORED, session)
    reservation = ConnectionFailureReservation(pool, connection)
    pool.shutdown(_failure=reservation)
    cluster.on_down_potentially_blocking = Mock(return_value=Future())

    def distance(_host):
        with session._lock, host.lock:
            if supersede == 'pool':
                session._pools[host] = successor
            elif supersede == 'endpoint':
                host.endpoint = DefaultEndPoint('127.0.0.2')
            elif supersede == 'host':
                host._is_removed = True
            elif supersede == 'session':
                session.is_shutdown = True
            elif supersede == 'cluster':
                cluster.is_shutdown = True
            elif supersede == 'health':
                assert cluster._lifecycle.mark_ready(host)
        return HostDistance.LOCAL

    cluster.profile_manager.distance.side_effect = distance
    done = Event()
    admitted, errors = [], []

    def admit():
        try:
            admitted.append(cluster.on_down(host, False, _pool_failure=reservation))
        except BaseException as exc:
            errors.append(exc)
        finally:
            done.set()

    worker = Thread(target=admit)
    try:
        # After retirement the shard map has no authority left to inspect.
        # Its work lock must not delay admission or hide superseding topology.
        with pool._lock:
            worker.start()
            assert done.wait(2), 'retired failure waited for pool work'
        worker.join(5)
        assert not worker.is_alive()
        assert not errors
        assert admitted == [supersede is None]
        assert host.is_up is (supersede is not None)
        assert cluster.on_down_potentially_blocking.call_count == int(supersede is None)
        assert not successor.is_shutdown
    finally:
        worker.join(5)
        cluster.is_shutdown = False
        pool.shutdown()
        successor.shutdown()


@pytest.mark.parametrize('failure', ['close', 'defunct'])
def test_failed_candidate_cannot_fence_down_before_publication(
        topology, failure):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    session = new_session_with_pool_state()
    session.cluster = cluster
    session._profile_manager = cluster.profile_manager
    session.keyspace = None
    session.is_shutdown = False
    session.shutdown = Mock()
    cluster.sessions = (session,)
    cluster.on_down_potentially_blocking = Mock(return_value=Future())
    connection = _PublicationConnection(host.endpoint, compression=False)
    cluster.connection_factory = Mock(return_value=connection)
    publishing = Event()
    failed = Event()
    errors = []
    owns = session._owns_pool_generation_locked

    def fail():
        try:
            assert publishing.wait(5)
            if failure == 'close':
                connection.close()
            else:
                connection.defunct(ConnectionException('candidate failed'))
        except BaseException as exc:
            errors.append(exc)
        finally:
            failed.set()

    def check_ownership(*args):
        if cluster.connection_factory.called and not publishing.is_set():
            # The candidate exists, and publication owns Session/Host state.
            # A positive observation made before these locks is now obsolete.
            publishing.set()
            assert failed.wait(5)
        return owns(*args)

    session._owns_pool_generation_locked = check_ownership

    def inspect():
        observed = {host: {'open_count': 0}}
        assert session.add_or_renew_pool(host, False).result(timeout=5) is True
        return observed

    session.get_pool_state = Mock(side_effect=inspect)
    worker = Thread(target=fail)
    worker.start()
    try:
        assert cluster.on_down(host, False)
        assert host.is_up is False
        assert session._pools[host].open_count == 0
        cluster.on_down_potentially_blocking.assert_called_once()
    finally:
        publishing.set()
        worker.join(5)
    assert not worker.is_alive()
    assert not errors


def test_publication_guard_releases_connection_and_membership(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    session = new_session_with_pool_state()
    session.cluster = cluster
    session.keyspace = None
    connection = _PublicationConnection(host.endpoint, compression=False)
    cluster.connection_factory = Mock(return_value=connection)
    pool = HostConnection(host, HostDistance.LOCAL, session)
    attempting = Event()
    failed = Event()

    def fail():
        attempting.set()
        connection.defunct(ConnectionException('failure after publication'))
        pool.shutdown()
        failed.set()

    worker = Thread(target=fail)
    try:
        with pool._membership_lock:
            with session._lock:
                with host.lock:
                    with pool._lock_usable_connection() as connected:
                        assert connected
                        worker.start()
                        assert attempting.wait(5)
                        assert not failed.wait(0.05)
                        assert not connection.is_defunct
                        assert pool._connections.get(
                            connection.features.shard_id) is connection
        assert failed.wait(5)
    finally:
        if worker.ident is not None:
            worker.join(5)
        pool.shutdown()
    assert not worker.is_alive()
    assert connection.is_defunct
    assert pool.is_shutdown


def test_publication_contention_keeps_session_available(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    session = new_session_with_pool_state()
    session.cluster = cluster
    session._profile_manager = cluster.profile_manager
    session.keyspace = None
    session.is_shutdown = False
    session.shutdown = Mock()
    connection = _PublicationConnection(host.endpoint, compression=False)
    cluster.connection_factory = Mock(return_value=connection)
    pool = HostConnection(host, HostDistance.LOCAL, session)
    membership = pool._membership_lock
    attempting = Event()
    replied = Event()

    class ObservedLock:
        def __enter__(self):
            attempting.set()
            membership.acquire()

        def __exit__(self, *args):
            membership.release()

    pool._membership_lock = ObservedLock()
    reply_thread = None
    errors = []

    def reply():
        try:
            session._set_keyspace_for_all_pools(
                None, lambda errors: replied.set())
        except BaseException as exc:
            errors.append(exc)

    try:
        membership.acquire()
        try:
            with patch('cassandra.cluster.HostConnection', return_value=pool):
                publication = session.add_or_renew_pool(host, False)
                assert attempting.wait(5)
                reply_thread = Thread(target=reply)
                reply_thread.start()
                assert replied.wait(2)
        finally:
            membership.release()
            if reply_thread is not None:
                reply_thread.join(5)
                assert not reply_thread.is_alive()
        assert not errors
        assert publication.result(timeout=5) is True
    finally:
        pool.shutdown()


def test_unpublished_failure_keeps_session_available_for_publication(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    session = new_session_with_pool_state()
    session.cluster = cluster
    session._profile_manager = cluster.profile_manager
    session.keyspace = None
    session.is_shutdown = False
    session.shutdown = Mock()
    connection = _PublicationConnection(host.endpoint, compression=False)
    cluster.connection_factory = Mock(return_value=connection)
    pool = HostConnection(host, HostDistance.LOCAL, session)
    connection.is_defunct = True
    host.signal_connection_failure = Mock()
    work_lock = pool._lock
    attempting = Event()
    published = Event()
    errors = []

    class ObservedLock:
        def __enter__(self):
            attempting.set()
            work_lock.acquire()

        def __exit__(self, *args):
            work_lock.release()

    pool._lock = ObservedLock()

    def fail():
        try:
            pool.return_connection(connection, stream_was_orphaned=True)
        except BaseException as exc:
            errors.append(exc)

    def publish():
        try:
            with patch('cassandra.cluster.HostConnection', return_value=pool):
                assert session.add_or_renew_pool(host, False).result(timeout=5)
            published.set()
        except BaseException as exc:
            errors.append(exc)

    failure = Thread(target=fail)
    publication = Thread(target=publish)
    try:
        # A background shard may own the work lock while publication takes
        # membership then Session. A stale failure must release Session before
        # waiting for work-lock cleanup, or all three operations can deadlock.
        work_lock.acquire()
        try:
            failure.start()
            assert attempting.wait(5)
            publication.start()
            assert published.wait(2)
        finally:
            work_lock.release()
    finally:
        for worker in (failure, publication):
            if worker.ident is not None:
                worker.join(5)
        pool.shutdown()
    assert not failure.is_alive()
    assert not publication.is_alive()
    assert not errors
    assert session._pools[host] is pool
    host.signal_connection_failure.assert_not_called()


@pytest.mark.parametrize('supersede', [None, 'pool'])
def test_current_failure_contention_allows_other_host_response(
        topology, supersede):
    from cassandra.protocol import ResultMessage, RESULT_KIND_SET_KEYSPACE
    from cassandra.shard_info import _ShardingInfo

    cluster, _ = topology
    hosts = [cluster.metadata.get_host_by_host_id(host_id)
             for host_id in (HOST_ID_1, HOST_ID_2)]
    session = new_session_with_pool_state()
    session.cluster = cluster
    session._profile_manager = cluster.profile_manager
    session.keyspace = 'ks'
    session.is_shutdown = False
    session.shutdown = Mock()
    cluster.shard_aware_options.disable_shardaware_port = True
    connections = [_PublicationConnection(host.endpoint, compression=False)
                   for host in hosts]
    for connection in connections:
        connection.features = ProtocolFeatures(shard_id=0)
        connection.keyspace = 'ks'
        connection.is_defunct = True
        connection.in_flight = 1
    candidate = _PublicationConnection(hosts[0].endpoint, compression=False)
    candidate.features = ProtocolFeatures(shard_id=1)
    cluster.connection_factory = Mock(side_effect=connections + [candidate])
    pools = [HostConnection(host, HostDistance.LOCAL, session) for host in hosts]
    session._pools.update(zip(hosts, pools))
    hosts[0].sharding_info = _ShardingInfo(0, 2, '', '', 0, None, None)
    successor = (HostConnection(hosts[0], HostDistance.IGNORED, session)
                 if supersede == 'pool' else None)
    for host in hosts:
        host.signal_connection_failure = Mock(return_value=True)

    io_waiting = Event()
    contended = Event()
    reactor_done = Event()
    response_lock = Lock()
    responses = []
    delivered = []
    delivery_futures = []
    errors = []

    def send(message, request_id, cb, **kwargs):
        with response_lock:
            responses.append(cb)
        io_waiting.set()
        return 0

    candidate.send_msg = send

    def respond():
        with response_lock:
            callbacks = list(responses)
            responses.clear()
        for callback in callbacks:
            callback(ResultMessage(RESULT_KIND_SET_KEYSPACE))

    def admit(host, is_addition, generation):
        future = cluster._lifecycle.submit_async(
            cluster.executor, delivered.append, host)
        delivery_futures.append(future)
        return future

    cluster.on_down_potentially_blocking = Mock(side_effect=admit)
    work_lock = pools[0]._lock

    class ObservedLock:
        def acquire(self, blocking=True):
            if failure.ident == get_ident() and io_waiting.is_set():
                contended.set()
            return work_lock.acquire(blocking)

        def release(self):
            work_lock.release()

        def __enter__(self):
            self.acquire()

        def __exit__(self, *args):
            self.release()

    pools[0]._lock = ObservedLock()

    def run(fn):
        try:
            fn()
        except BaseException as exc:
            errors.append(exc)

    def process_responses():
        # One reactor must finish the other host's failure callback before
        # it can deliver the USE response that releases the pool work lock.
        if supersede == 'pool':
            with session._lock, hosts[0].lock:
                session._pools[hosts[0]] = successor
        pools[1].return_connection(connections[1])
        respond()
        reactor_done.set()

    failure = Thread(target=run, args=(lambda: pools[0].return_connection(
        connections[0]),))
    worker = Thread(target=run, args=(lambda: pools[0]._open_connection_to_missing_shard(1),))
    reactor = Thread(target=run, args=(process_responses,))
    try:
        worker.start()
        assert io_waiting.wait(5)
        failure.start()
        assert contended.wait(5)
        reactor.start()
        assert reactor_done.wait(2), 'cross-host failure blocked reactor progress'
    finally:
        # Release controlled I/O even for the unfixed production negative
        # control, so assertion failures cannot leave blocked driver threads.
        respond()
        for thread in (worker, failure, reactor):
            if thread.ident is not None:
                thread.join(5)
        for pool in pools:
            pool.shutdown()
    assert all(not thread.is_alive() for thread in (worker, failure, reactor))
    assert not errors
    assert all(connection.in_flight == 0 for connection in connections)
    for future in delivery_futures:
        future.result(timeout=5)
    assert delivered == ([hosts[1]] if supersede else [hosts[1], hosts[0]])
    assert hosts[0].is_up is bool(supersede)
    assert not hosts[1].is_up
    hosts[1].signal_connection_failure.assert_called_once()
    assert hosts[0].signal_connection_failure.call_count == int(supersede != 'pool')
    if successor is not None:
        assert session._pools[hosts[0]] is successor
        assert not successor.is_shutdown
        successor.shutdown()
    assert candidate.keyspace == 'ks'


@pytest.mark.parametrize('healthy', [False, True])
def test_publication_progress_with_unsynchronized_background_shard(
        topology, healthy):
    from cassandra.protocol import ResultMessage, RESULT_KIND_SET_KEYSPACE
    from cassandra.shard_info import _ShardingInfo

    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    session = new_session_with_pool_state()
    session.cluster = cluster
    session._profile_manager = cluster.profile_manager
    session.keyspace = 'ks'
    session.is_shutdown = False
    session.shutdown = Mock()
    sharding = _ShardingInfo(0, 2, '', '', 0, None, None)
    first = _PublicationConnection(host.endpoint, compression=False)
    first.features = ProtocolFeatures(shard_id=0, sharding_info=sharding)
    second = _PublicationConnection(host.endpoint, compression=False)
    second.features = ProtocolFeatures(shard_id=1, sharding_info=sharding)
    requests = []
    pending = []
    response_lock = Lock()
    cleanup_mode = Event()
    initial_use = Event()
    current_use = Event()
    work = []
    submit = session.submit

    def schedule(fn, *args, **kwargs):
        future = submit(fn, *args, **kwargs)
        if getattr(fn, '__name__', '') == '_open_connection_to_missing_shard':
            work.append(future)
        return future

    session.submit = schedule

    def respond(message, request_id, cb, **kwargs):
        cb(ResultMessage(RESULT_KIND_SET_KEYSPACE))
        return 0

    def unanswered(message, request_id, cb, **kwargs):
        with response_lock:
            requests.append(cb)
            immediate = cleanup_mode.is_set()
            if not immediate:
                pending.append(cb)
            (initial_use if len(requests) == 1 else current_use).set()
        if immediate:
            cb(ResultMessage(RESULT_KIND_SET_KEYSPACE))
        return 0

    def respond_one():
        with response_lock:
            callback = pending.pop(0) if pending else None
            assert callback is not None or cleanup_mode.is_set()
        if callback is not None:
            callback(ResultMessage(RESULT_KIND_SET_KEYSPACE))

    first.send_msg = respond
    second.send_msg = unanswered
    cluster.connection_factory = Mock(side_effect=[first, second])
    cluster.shard_aware_options.disable_shardaware_port = True
    candidates = []

    def construct(*args, **kwargs):
        pool = HostConnection(*args, **kwargs)
        candidates.append(pool)
        assert initial_use.wait(5)
        # Force the background socket to await a corrective USE after its
        # original target succeeds. It must not qualify as usable membership.
        session.keyspace = 'newks'
        pool._set_keyspace_for_all_conns('newks', lambda pool, errors: None)
        respond_one()
        assert current_use.wait(5)
        if not healthy:
            first.close()
        return pool

    before = host._health_generation
    try:
        with patch('cassandra.cluster.HostConnection', side_effect=construct):
            publication = session.add_or_renew_pool(host, False)
            assert current_use.wait(5)
            assert publication.result(timeout=2) is True
        assert host._health_generation == before + int(healthy)
    finally:
        with response_lock:
            cleanup_mode.set()
            callbacks = list(pending)
            pending.clear()
        for callback in callbacks:
            callback(ResultMessage(RESULT_KIND_SET_KEYSPACE))
        for future in work:
            future.result(timeout=5)
        for pool in candidates:
            pool.shutdown()
    assert second.keyspace == 'newks'
    assert host._health_generation == before + int(healthy)


def test_refresh_publishes_nodes_and_tokens_before_queued_listener(topology):
    cluster, connection = topology
    cluster.refresh_nodes()
    old = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    old_tokens = cluster.metadata.token_map
    assert old in old_tokens.token_to_host_owner.values()
    entered = Event()
    release = Event()
    listener = Mock()
    cluster.register_listener(listener)

    def slow_callback():
        entered.set()
        assert release.wait(5)

    worker = Thread(target=cluster._lifecycle.submit, args=(slow_callback,))
    worker.start()
    try:
        assert entered.wait(5)
        del connection.peer_results[1][1]
        cluster.refresh_nodes()

        assert cluster.metadata.get_host_by_host_id(HOST_ID_3) is None
        assert cluster.metadata.get_host(old.endpoint) is None
        assert old._is_removed
        assert old not in cluster.metadata.token_map.token_to_host_owner.values()
        assert old in old_tokens.token_to_host_owner.values()
        listener.on_remove.assert_not_called()
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    listener.on_remove.assert_called_once_with(old)


def test_up_listener_sees_all_sessions_ready_and_can_refresh(topology):
    cluster, _ = topology
    host = cluster.metadata.get_host_by_host_id(HOST_ID_3)
    host.set_down()
    cluster.control_connection.on_up = Mock()
    sessions = [Mock(), Mock()]
    ready = [Future(), Future()]
    for session, result in zip(sessions, ready):
        session.add_or_renew_pool.return_value = result
        cluster.sessions.add(session)
    listener = Mock()
    observed = []

    def on_up(current):
        assert current is host
        assert current.is_up
        assert all(result.result(timeout=0) is True for result in ready)
        cluster.refresh_nodes()
        assert cluster.metadata.get_host_by_host_id(host.host_id) is host
        observed.append(current)

    listener.on_up.side_effect = on_up
    cluster.register_listener(listener)
    cluster.on_up(host)
    ready[0].set_result(True)
    assert not host.is_up
    listener.on_up.assert_not_called()
    ready[1].set_result(True)

    assert host.is_up
    assert observed == [host]
    listener.on_up.assert_called_once_with(host)
    for session in sessions:
        session.add_or_renew_pool.assert_called_once_with(
            host, is_host_addition=False)


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
