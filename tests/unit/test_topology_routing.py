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

"""Routing must use the Hosts published by a real topology refresh."""

from unittest.mock import Mock
from uuid import UUID

import pytest

from cassandra import ConsistencyLevel
from cassandra.cluster import (
    Cluster, ControlConnection, ExecutionProfile, EXEC_PROFILE_DEFAULT,
)
from cassandra.connection import DefaultEndPoint
from cassandra.metadata import KeyspaceMetadata
from cassandra.policies import (
    DCAwareRoundRobinPolicy, HostDistance, RackAwareRoundRobinPolicy,
    RoundRobinPolicy, TokenAwarePolicy,
)
from cassandra.query import SimpleStatement
from cassandra.tablets import Tablet
from tests.unit.test_control_connection import (
    MockConnection, _node_meta_results,
)


@pytest.fixture
def peer_location(request):
    """Select the target peer's DC, rack, and remote-host limit."""
    return getattr(request, 'param', ('dc1', 'rack1', 0))


@pytest.fixture(params=[
    RoundRobinPolicy,
    lambda: DCAwareRoundRobinPolicy('dc1'),
    lambda: RackAwareRoundRobinPolicy('dc1', 'rack1'),
], ids=['round-robin', 'dc-aware', 'rack-aware'])
def routing_cluster(request, peer_location):
    """Build a real topology with a configurable peer and child policy."""
    datacenter, rack, remote_limit = peer_location
    child = request.param()
    if isinstance(child, (DCAwareRoundRobinPolicy, RackAwareRoundRobinPolicy)):
        child.used_hosts_per_remote_dc = remote_limit
    policy = TokenAwarePolicy(child, shuffle_replicas=False)
    profile = ExecutionProfile(load_balancing_policy=policy)
    with Cluster(contact_points=[], execution_profiles={
            EXEC_PROFILE_DEFAULT: profile}) as cluster:
        cluster.control_connection.shutdown()
        connection = MockConnection()
        connection.local_results[1][0][7] = [str(-(2 ** 63))]
        peer_columns = connection.peer_results[0]
        peer = connection.peer_results[1][0]
        peer[peer_columns.index('data_center')] = datacenter
        peer[peer_columns.index('rack')] = rack
        connection.peer_results[1][0][5] = ['0']
        connection.peer_results[1][1][5] = [str(2 ** 62)]
        connection.wait_for_responses.return_value = _node_meta_results(
            connection.local_results, connection.peer_results)
        # Use real lifecycle notifications, indexes, token maps and policies.
        # Only the control socket and prepared-query I/O are replaced.
        cluster._prepare_all_queries = Mock()
        for address, host_id in [('192.168.1.0', 1), ('192.168.1.1', 2),
                                 ('192.168.1.2', 3)]:
            host, _ = cluster.add_host(
                DefaultEndPoint(address),
                datacenter if host_id == 2 else 'dc1',
                rack if host_id == 2 else 'rack1', signal=False,
                host_id=UUID(int=host_id))
            host.set_up()
        cluster.metadata.keyspaces['ks'] = KeyspaceMetadata(
            'ks', True, 'SimpleStrategy', {'replication_factor': '1'})
        policy.populate(cluster, cluster.metadata.all_hosts())
        control = ControlConnection(cluster, 1, 2, 0, 0)
        control._connection = connection
        cluster.control_connection = control
        control.refresh_node_list_and_token_map()
        yield cluster, connection, policy


def assert_current_host(cluster, host):
    """Require the current Host object in both metadata indexes."""
    assert host is cluster.metadata.get_host_by_host_id(host.host_id)
    assert host is cluster.metadata.get_host(host.endpoint)
    assert not host._is_removed


def assert_current_plan(cluster, plan):
    """Require all three current hosts exactly once in a query plan."""
    assert len(plan) == len(cluster.metadata.all_hosts()) == 3
    assert len({id(host) for host in plan}) == len(plan)
    for host in plan:
        assert_current_host(cluster, host)


@pytest.mark.parametrize('transition', ['move', 'rebirth'])
@pytest.mark.parametrize('routing', ['vnode', 'tablet', 'tablet-serial'])
def test_refresh_routes_to_current_hosts(routing_cluster, transition, routing):
    """Refresh vnode and tablet routing after peer movement or rebirth."""
    cluster, connection, policy = routing_cluster
    metadata = cluster.metadata
    old = metadata.get_host_by_host_id(UUID(int=2))
    original_hash = hash(old)
    keyed_by_host = {old: 'original'}
    # Tablet placement deliberately differs from vnode placement, so ignoring
    # tablet routing cannot accidentally satisfy the replica-order assertion.
    vnode_host = (old if routing == 'vnode'
                  else metadata.get_host_by_host_id(UUID(int=1)))
    key = next(
        str(i).encode() for i in range(1000)
        if metadata.get_replicas('ks', str(i).encode())[0] is vnode_host)
    query = SimpleStatement(
        'SELECT * FROM ks.tbl', keyspace='ks', routing_key=key,
        consistency_level=(ConsistencyLevel.SERIAL
                           if routing == 'tablet-serial'
                           else ConsistencyLevel.ONE))
    query.table = 'tbl'
    token = metadata.token_map.token_class.from_key(key).value
    if routing.startswith('tablet'):
        metadata._tablets.add_tablet(
            'ks', 'tbl', Tablet(token - 1, token, [(old.host_id, 0)]))
    assert list(policy.make_query_plan('ks', query))[0] is old
    # Warm the vnode cache too.
    assert metadata.get_replicas('ks', key)[0] is vnode_host

    peer = connection.peer_results[1][0]
    if transition == 'move':
        peer[0] = '192.168.1.9'
        current_id = old.host_id
    else:
        current_id = UUID(int=4)
        peer[-1] = current_id
    connection.wait_for_responses.return_value = _node_meta_results(
        connection.local_results, connection.peer_results)
    cluster.control_connection.refresh_node_list_and_token_map()

    current = metadata.get_host_by_host_id(current_id)
    assert current is not None and current is not old
    assert old._is_removed
    assert hash(old) == original_hash
    assert keyed_by_host[old] == 'original'
    if transition == 'move':
        assert current == old and hash(current) == original_hash
        assert keyed_by_host[current] == 'original'
        assert metadata.get_host(old.endpoint) is None
    else:
        assert current != old and current not in keyed_by_host
        assert metadata.get_host_by_host_id(old.host_id) is None
        assert not metadata._tablets.table_has_tablets('ks', 'tbl')

    assert_current_plan(cluster, list(policy.make_query_plan()))
    expected_vnode = current if routing == 'vnode' else vnode_host
    assert metadata.get_replicas('ks', key)[0] is expected_vnode
    plan = list(policy.make_query_plan('ks', query))
    assert_current_plan(cluster, plan)
    assert plan[0] is (expected_vnode if transition == 'rebirth' else current)

    if transition == 'rebirth' and routing.startswith('tablet'):
        # Use current vnodes until a fresh tablet payload arrives.
        metadata._tablets.add_tablet(
            'ks', 'tbl', Tablet(token - 1, token, [(current_id, 0)]))
        plan = list(policy.make_query_plan('ks', query))
        assert_current_plan(cluster, plan)
        assert plan[0] is current


@pytest.mark.parametrize('peer_location', [
    ('dc1', 'rack2', 0),
    ('dc2', 'rack2', 1),
    ('dc2', 'rack2', 0),
], indirect=True, ids=['other-rack', 'remote-enabled', 'remote-ignored'])
@pytest.mark.parametrize('transition', ['move', 'rebirth'])
def test_refresh_preserves_policy_locality(routing_cluster, peer_location,
                                           transition):
    """Keep current host membership and locality across peer replacement."""
    cluster, connection, policy = routing_cluster
    metadata = cluster.metadata
    child = policy._child_policy
    old = metadata.get_host_by_host_id(UUID(int=2))
    if isinstance(child, RoundRobinPolicy) or peer_location[0] == 'dc1':
        expected_distance = HostDistance.LOCAL
    elif peer_location[2]:
        expected_distance = HostDistance.REMOTE
    else:
        expected_distance = HostDistance.IGNORED
    assert child.distance(old) == expected_distance
    key = next(str(i).encode() for i in range(1000)
               if metadata.get_replicas('ks', str(i).encode())[0] is old)
    query = SimpleStatement('SELECT * FROM ks.tbl', keyspace='ks',
                            routing_key=key)
    # Warm replica and policy state before replacing the peer.
    list(policy.make_query_plan('ks', query))

    def assert_locality(current):
        """Check child ordering, replica promotion, and policy membership."""
        assert_current_host(cluster, current)
        assert child.distance(current) == expected_distance
        expected_hosts = [host for host in metadata.all_hosts()
                          if child.distance(host) != HostDistance.IGNORED]
        plans = [list(child.make_query_plan()),
                 list(policy.make_query_plan('ks', query))]
        if expected_distance != HostDistance.IGNORED:
            assert plans[1][0] is current
        for plan in plans:
            assert {id(host) for host in plan} == {
                id(host) for host in expected_hosts}
            assert len(plan) == len(expected_hosts)
            for host in plan:
                assert_current_host(cluster, host)
        # Token-aware routing imposes its own distance ordering. Check the
        # child's ordering directly so that normalization cannot hide a bug.
        priority = {HostDistance.LOCAL_RACK: 0, HostDistance.LOCAL: 1,
                    HostDistance.REMOTE: 2}
        ranks = [priority[child.distance(host)] for host in plans[0]]
        assert ranks == sorted(ranks)
        if isinstance(child, (DCAwareRoundRobinPolicy,
                              RackAwareRoundRobinPolicy)):
            dc_hosts = child._dc_live_hosts[peer_location[0]]
            assert any(host is current for host in dc_hosts)
            if current is not old:
                assert all(host is not old for host in dc_hosts)
        if isinstance(child, RackAwareRoundRobinPolicy):
            rack_hosts = child._live_hosts[peer_location[:2]]
            assert any(host is current for host in rack_hosts)
            if current is not old:
                assert all(host is not old for host in rack_hosts)

    assert_locality(old)
    peer = connection.peer_results[1][0]
    if transition == 'move':
        peer[0] = '192.168.1.9'
        current_id = old.host_id
    else:
        current_id = UUID(int=4)
        peer[-1] = current_id
    connection.wait_for_responses.return_value = _node_meta_results(
        connection.local_results, connection.peer_results)
    cluster.control_connection.refresh_node_list_and_token_map()

    current = metadata.get_host_by_host_id(current_id)
    assert current is not None and current is not old
    assert old._is_removed
    assert (current.datacenter, current.rack) == peer_location[:2]
    assert metadata.get_replicas('ks', key)[0] is current
    assert_locality(current)
