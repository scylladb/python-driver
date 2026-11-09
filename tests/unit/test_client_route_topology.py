# Copyright 2026 ScyllaDB, Inc.
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

"""Client-route changes must preserve host identity and topology indexes."""

from unittest.mock import Mock
from uuid import UUID

import pytest

from cassandra.client_routes import ClientRouteProxy, ClientRoutesConfig, _Route
from cassandra.cluster import (
    Cluster, ControlConnection, ExecutionProfile, EXEC_PROFILE_DEFAULT,
)
from cassandra.policies import RoundRobinPolicy
from tests.unit.test_client_routes import _routes_query_response
from tests.unit.test_control_connection import MockConnection, _node_meta_results


@pytest.fixture
def routed_topology():
    policy = RoundRobinPolicy()
    config = ClientRoutesConfig([
        ClientRouteProxy('conn-a'), ClientRouteProxy('conn-b'),
    ])
    with Cluster(contact_points=[], client_routes_config=config,
                 execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(
                     load_balancing_policy=policy)}) as cluster:
        cluster.control_connection.shutdown()
        connection = MockConnection()
        routes = [
            _Route('conn-a', UUID(int=2), '127.0.0.10', 9142),
            _Route('conn-b', UUID(int=2), '127.0.0.11', 9242),
            _Route('conn-a', UUID(int=3), '127.0.0.12', 9342),
        ]
        def query_routes(message, timeout):
            return _routes_query_response(message, routes)

        connection.wait_for_response = Mock(side_effect=query_routes)
        cluster._client_routes_handler.initialize(connection, 1)
        # Keep metadata, lifecycle and policy behavior real. Replace socket I/O
        # and execute scheduled callbacks inline so assertions need no sleeps.
        cluster._prepare_all_queries = Mock()
        cluster.scheduler.schedule_unique = (
            lambda delay, fn, *args, **kwargs: fn(*args, **kwargs))
        control = ControlConnection(cluster, 1, 2, 0, 0)
        control._connection = connection
        cluster.control_connection = control
        assert control.refresh_node_list_and_token_map()
        yield cluster, connection, routes, policy


def refresh_topology(cluster, connection):
    connection.wait_for_responses.return_value = _node_meta_results(
        connection.local_results, connection.peer_results)
    assert cluster.control_connection.refresh_node_list_and_token_map()


def change_routes(cluster, host_id, connection_id):
    cluster.control_connection._handle_client_routes_change({
        'change_type': 'UPDATE_NODES',
        'connection_ids': [connection_id],
        'host_ids': [str(host_id)],
    })


def assert_current_topology(cluster, policy):
    metadata = cluster.metadata
    hosts = metadata.all_hosts()
    plan = list(policy.make_query_plan())
    assert len(hosts) == len(plan) == 3
    assert {id(host) for host in plan} == {id(host) for host in hosts}
    for host in hosts:
        assert metadata.get_host_by_host_id(host.host_id) is host
        assert metadata.get_host(host.endpoint) is host
        assert host.is_up and not host._is_removed


@pytest.mark.parametrize('movement', ['address', 'port'])
def test_endpoint_movement_preserves_route_selection(routed_topology, movement):
    cluster, connection, routes, policy = routed_topology
    metadata = cluster.metadata
    old = metadata.get_host_by_host_id(UUID(int=2))
    other = metadata.get_host_by_host_id(UUID(int=3))
    original_hash = hash(old)
    keyed_by_host = {old: 'node'}
    assert old.endpoint.resolve() == ('127.0.0.10', 9142)

    if movement == 'address':
        connection.peer_results[1][0][0] = '192.168.1.9'
    else:
        connection.peer_results[0].append('native_port')
        connection.peer_results[1][0].append(9142)
        connection.peer_results[1][1].append(9042)
    refresh_topology(cluster, connection)

    current = metadata.get_host_by_host_id(old.host_id)
    assert current is not old and old._is_removed
    assert current == old and hash(current) == original_hash
    assert keyed_by_host[current] == 'node'
    assert current.endpoint != old.endpoint
    assert metadata.get_host(old.endpoint) is None
    assert current.endpoint.resolve() == ('127.0.0.10', 9142)

    # Updating a different connection ID must preserve A after Host replacement.
    routes[1] = _Route('conn-b', old.host_id, '127.0.0.20', 9442)
    change_routes(cluster, old.host_id, 'conn-b')
    assert current.endpoint.resolve() == ('127.0.0.10', 9142)

    # Removing A permits fallback to B without replacing the Host again.
    routes.pop(0)
    change_routes(cluster, old.host_id, 'conn-a')
    assert current.endpoint.resolve() == ('127.0.0.20', 9442)
    assert metadata.get_host_by_host_id(current.host_id) is current
    assert metadata.get_host_by_host_id(other.host_id) is other
    assert other.endpoint.resolve() == ('127.0.0.12', 9342)
    assert_current_topology(cluster, policy)


def test_same_address_rebirth_does_not_inherit_old_route(routed_topology):
    cluster, connection, routes, policy = routed_topology
    metadata = cluster.metadata
    old = metadata.get_host_by_host_id(UUID(int=2))
    new_id = UUID(int=4)
    assert old.endpoint.resolve() == ('127.0.0.10', 9142)

    connection.peer_results[1][0][-1] = new_id
    refresh_topology(cluster, connection)

    current = metadata.get_host_by_host_id(new_id)
    assert current is not None and current != old
    assert current.address == old.address
    assert old._is_removed
    assert metadata.get_host_by_host_id(old.host_id) is None
    assert metadata.get_host(old.endpoint) is None
    # Even while the route table retains the old UUID, reborn nodes use their
    # own route lookup and fall back to the advertised endpoint if absent.
    assert current.endpoint.resolve() == ('192.168.1.1', 9042)
    assert old.endpoint.resolve() == ('127.0.0.10', 9142)

    routes.append(_Route('conn-b', new_id, '127.0.0.21', 9542))
    change_routes(cluster, new_id, 'conn-b')
    assert current.endpoint.resolve() == ('127.0.0.21', 9542)
    assert old.endpoint.resolve() == ('127.0.0.10', 9142)
    assert_current_topology(cluster, policy)


def test_shared_proxy_destination_keeps_hosts_distinct(routed_topology):
    cluster, connection, routes, policy = routed_topology
    metadata = cluster.metadata
    first = metadata.get_host_by_host_id(UUID(int=2))
    second = metadata.get_host_by_host_id(UUID(int=3))
    keyed_by_host = {first: object(), second: object()}
    routes[2] = _Route('conn-a', second.host_id, '127.0.0.10', 9142)
    change_routes(cluster, second.host_id, 'conn-a')
    refresh_topology(cluster, connection)

    assert first.endpoint.resolve() == second.endpoint.resolve()
    assert first != second and first.endpoint != second.endpoint
    assert metadata.get_host_by_host_id(first.host_id) is first
    assert metadata.get_host_by_host_id(second.host_id) is second
    assert len(keyed_by_host) == 2
    assert keyed_by_host[first] is not keyed_by_host[second]
    assert_current_topology(cluster, policy)
