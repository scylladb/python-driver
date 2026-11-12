import logging
import os
import struct
import time
import unittest
from threading import Event
from unittest.mock import patch
from uuid import uuid4

from cassandra import OperationTimedOut
from cassandra.cluster import (
    EXEC_PROFILE_DEFAULT, ExecutionProfile, NoHostAvailable,
)
from cassandra.connection import ConnectionException, DefaultEndPoint
from cassandra.policies import (
    ConstantReconnectionPolicy, DCAwareRoundRobinPolicy, HostStateListener,
    RackAwareRoundRobinPolicy, RoundRobinPolicy,
)

from tests.integration import (
    SCYLLA_VERSION, TestCluster, get_cluster, get_node, local, remove_cluster,
    use_cluster,
)

LOGGER = logging.getLogger(__name__)
LOCAL_QUERY = "SELECT host_id FROM system.local WHERE key='local'"


class ReplacementListener(HostStateListener):
    def __init__(self, old_host, new_endpoint):
        self.old_host = old_host
        self.new_endpoint = new_endpoint
        self.removed = Event()
        self.added = Event()
        self.current = None

    def on_remove(self, host):
        if host is self.old_host:
            self.removed.set()

    def on_add(self, host):
        if host is not self.old_host and host.endpoint == self.new_endpoint:
            self.current = host
            self.added.set()

    def on_up(self, host):
        pass

    def on_down(self, host):
        pass


@local
class TestIpAddressChange(unittest.TestCase):
    def setUp(self):
        options = patch.dict(
            os.environ, {'SCYLLA_EXT_OPTS': '--memory 2048M'})
        options.start()
        self.addCleanup(options.stop)
        # Register before setup so partial startup failures are cleaned up too.
        self.addCleanup(remove_cluster)
        rebirth = self._testMethodName == 'test_same_endpoint_node_rebirth'
        nodes = 1 if rebirth else 3
        # Isolate these destructive tests from other locally running clusters.
        # A unique name also avoids reusing mutations with KEEP_TEST_CLUSTER.
        cluster_name = 'test_ip_change_{}'.format(uuid4().hex[:8])
        use_cluster(cluster_name, [nodes], ipformat='127.0.92.%d',
                    start=True, set_keyspace=False, smp=2)
        self.cluster = TestCluster(
            contact_points=[get_node(1).network_interfaces['binary'][0]],
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(
                load_balancing_policy=RoundRobinPolicy(), request_timeout=5)},
            reconnection_policy=ConstantReconnectionPolicy(0.5),
            topology_event_refresh_window=0, status_event_refresh_window=0)
        # LIFO cleanup shuts down every session before removing CCM data.
        self.addCleanup(self.cluster.shutdown)
        self.sessions = [self.cluster.connect(), self.cluster.connect()]
        host = self.cluster.metadata.all_hosts()[0]
        for name, policy in [
                ('dc', DCAwareRoundRobinPolicy(host.datacenter)),
                ('rack', RackAwareRoundRobinPolicy(
                    host.datacenter, host.rack))]:
            self.cluster.add_execution_profile(
                name, ExecutionProfile(load_balancing_policy=policy))
        self.wait_for(self.assert_all_pools_ready)

    def state(self):
        control = self.cluster.control_connection._connection
        return {
            'hosts': [(id(h), str(h.host_id), str(h.endpoint),
                       h.is_up, h._is_removed)
                      for h in self.cluster.metadata.all_hosts()],
            'pools': [[(id(h), id(p.host), str(h.endpoint), p.is_shutdown)
                       for h, p in session._pools.copy().items()]
                      for session in self.sessions],
            'control': None if control is None else (
                str(control.endpoint),
                str(getattr(control, '_control_connection_host_id', None)),
                control.is_closed, control.is_defunct),
        }

    def wait_for(self, check, timeout=90):
        deadline = time.monotonic() + timeout
        while True:
            try:
                return check()
            except (AssertionError, NoHostAvailable, OperationTimedOut,
                    ConnectionException) as exc:
                if time.monotonic() >= deadline:
                    raise AssertionError(
                        'Topology did not converge: {}; state={!r}'.format(
                            exc, self.state())) from exc
                time.sleep(0.25)

    def assert_all_pools_ready(self):
        for session in self.sessions:
            for host in self.cluster.metadata.all_hosts():
                pool = session._pools.get(host)
                assert pool is not None and pool.host is host
                assert not pool.is_shutdown

    def capture(self, node, new_endpoint):
        endpoint = DefaultEndPoint(*node.network_interfaces['binary'])
        old = self.cluster.metadata.get_host(endpoint)
        assert old is not None
        pools = [session._pools[old] for session in self.sessions]
        assert all(pool.host is old for pool in pools)
        listener = ReplacementListener(old, new_endpoint)
        self.cluster.register_listener(listener)
        self.addCleanup(self.cluster.unregister_listener, listener)
        prepared = [session.prepare(LOCAL_QUERY) for session in self.sessions]
        return old, pools, listener, prepared

    def assert_replacement(self, old, old_pools, listener, prepared,
                           same_identity):
        metadata = self.cluster.metadata
        current = metadata.get_host(listener.new_endpoint)
        assert current is not None and current is not old
        assert current.is_up and not current._is_removed
        assert old._is_removed and not old.is_up
        assert metadata.get_host_by_host_id(current.host_id) is current
        if same_identity:
            assert current.host_id == old.host_id
            assert current == old and hash(current) == hash(old)
            assert metadata.get_host(old.endpoint) is None
        else:
            assert current.host_id != old.host_id and current != old
            assert metadata.get_host_by_host_id(old.host_id) is None
        assert listener.removed.is_set() and listener.added.is_set()
        assert listener.current is current
        for profile in self.cluster.profile_manager.profiles.values():
            plan = list(profile.load_balancing_policy.make_query_plan())
            assert len(plan) == len(metadata.all_hosts())
            assert len({id(h) for h in plan}) == len(plan)
            assert any(h is current for h in plan)
            for host in plan:
                assert host is metadata.get_host_by_host_id(host.host_id)
                assert host is not old and not host._is_removed
        for session, old_pool, statement in zip(
                self.sessions, old_pools, prepared):
            assert old_pool.is_shutdown
            pool = session._pools.get(current)
            assert pool is not None and pool is not old_pool
            assert pool.host is current and not pool.is_shutdown
            assert all(h is not old and p.host is not old
                       for h, p in session._pools.copy().items())
            for query in (LOCAL_QUERY, statement):
                future = session.execute_async(query, host=current, timeout=5)
                assert future.result().one().host_id == current.host_id
                assert future.coordinator_host is current
        control = self.cluster.control_connection._connection
        assert control is not None
        assert not control.is_closed and not control.is_defunct
        control_host = metadata.get_host_by_host_id(
            control._control_connection_host_id)
        assert control_host is not None and control_host is not old
        assert control.endpoint == control_host.endpoint
        return current

    def move_node(self, move_control):
        control = self.cluster.control_connection._connection
        control_endpoint = control.endpoint
        node = next(
            node for node in get_cluster().nodelist()
            if (DefaultEndPoint(*node.network_interfaces['binary'])
                == control_endpoint) == move_control)
        old_endpoint = DefaultEndPoint(*node.network_interfaces['binary'])
        new_ip = '127.0.92.33'
        new_endpoint = DefaultEndPoint(new_ip, old_endpoint.port)
        old, pools, listener, prepared = self.capture(node, new_endpoint)
        session = self.sessions[0]
        tablets_ddl = (" AND tablets = {'enabled': false}"
                       if SCYLLA_VERSION else '')
        session.execute(
            "CREATE KEYSPACE preserved WITH replication = "
            "{'class': 'NetworkTopologyStrategy', 'replication_factor': '1'}"
            + tablets_ddl)
        session.execute(
            'CREATE TABLE preserved.rows (key int PRIMARY KEY, value text)')
        # Give the moved node the only copy, so another replica cannot hide
        # lost data. Disable tablets above to use the vnode ownership map.
        for data_key in range(10000):
            routing_key = struct.pack('>i', data_key)
            replicas = self.cluster.metadata.get_replicas(
                'preserved', routing_key)
            if len(replicas) == 1 and replicas[0] is old:
                break
        else:
            self.fail('No routing key found for moved host: {!r}'.format(
                self.state()))
        session.execute(
            'INSERT INTO preserved.rows (key, value) VALUES (%s, %s)',
            (data_key, 'before move'), host=old)
        row_query = 'SELECT value FROM preserved.rows WHERE key=%s'
        assert session.execute(
            row_query, (data_key,), host=old).one().value == 'before move'
        node.stop(wait_other_notice=True)
        LOGGER.debug('Moving %s from %s to %s',
                     old.host_id, old_endpoint, new_endpoint)
        node.set_configuration_options(values={
            'listen_address': new_ip, 'rpc_address': new_ip,
            'api_address': new_ip})
        node.network_interfaces = {
            name: (new_ip, port)
            for name, (_, port) in node.network_interfaces.items()}
        node.start(wait_for_binary_proto=True)

        def moved_and_readable():
            current = self.assert_replacement(
                old, pools, listener, prepared, True)
            replicas = self.cluster.metadata.get_replicas(
                'preserved', routing_key)
            assert len(replicas) == 1 and replicas[0] is current
            for session in self.sessions:
                future = session.execute_async(
                    row_query, (data_key,), host=current, timeout=5)
                row = future.result().one()
                assert row is not None and row.value == 'before move'
                assert future.coordinator_host is current

        self.wait_for(moved_and_readable)
        assert old.endpoint == old_endpoint
        if move_control:
            assert self.cluster.control_connection._connection is not control
            assert control.is_closed or control.is_defunct

    def test_peer_ip_movement(self):
        self.move_node(move_control=False)

    def test_control_connection_ip_movement(self):
        self.move_node(move_control=True)

    def test_same_endpoint_node_rebirth(self):
        node = get_node(1)
        endpoint = DefaultEndPoint(*node.network_interfaces['binary'])
        old, pools, listener, prepared = self.capture(node, endpoint)
        control = self.cluster.control_connection._connection
        node.stop()
        # Wipe system.local and commitlogs as well as user data, preserving the
        # configured endpoint while forcing the server to generate a new ID.
        node.clear()
        node.start(wait_for_binary_proto=True)
        self.wait_for(lambda: self.assert_replacement(
            old, pools, listener, prepared, False))
        assert self.cluster.control_connection._connection is not control
        assert control.is_closed or control.is_defunct
