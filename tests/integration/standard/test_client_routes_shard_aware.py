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

"""
Advanced shard awareness through client routes (PrivateLink).

The node listens for PROXY protocol v2 on a shard-aware port
(``native_shard_aware_transport_port_proxy_protocol``), where it assigns each
connection the shard that the client's source port, taken from the PROXY
header, maps to, and on a regular port (``native_transport_port_proxy_protocol``),
where it picks the shard itself. The emulated NLB in front of it either sends
that header, like a private endpoint configured for PROXY protocol v2, or does
not, like an endpoint that replaces the source port.

Connections that do not choose their source port must go to a port where the
node picks the shard: on a shard-aware port they would land on the shards their
OS-chosen ports map to, and Linux prefers even ports for connect(), so with an
even shard count the odd shards would never be reached.
"""

import logging
import time
import unittest
import uuid

import pytest

from cassandra.cluster import Cluster
from cassandra.client_routes import ClientRoutesConfig, ClientRouteProxy
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from tests.integration import (
    SCYLLA_VERSION,
    MockLoggingHandler,
    TestCluster,
    skip_scylla_version_lt,
    use_cluster,
)
from tests.integration.standard.client_routes_utils import (
    NLBEmulator,
    get_host_ids_from_cluster,
    node_id_from_ip,
    post_routes_for_nlb,
    wait_for_routes_visible,
)
from tests.util import wait_until, wait_until_not_raised

log = logging.getLogger(__name__)

# The cluster setup below configures ScyllaDB-only listeners and shard counts,
# so the whole module, setup included, is skipped on Cassandra.
pytestmark = pytest.mark.skipif(SCYLLA_VERSION is None,
                                reason="PROXY protocol listeners and shard awareness are ScyllaDB-only")

CLUSTER_NAME = 'test_client_routes_shard_aware'
SHARDS = 4
REGULAR_PORT = 9042
PROXY_PROTOCOL_REGULAR_PORT = 29042
PROXY_PROTOCOL_SHARD_AWARE_PORT = 39042


def setup_module():
    use_cluster(CLUSTER_NAME, [1], start=True, smp=SHARDS, configuration_options={
        'native_transport_port_proxy_protocol': PROXY_PROTOCOL_REGULAR_PORT,
        'native_shard_aware_transport_port_proxy_protocol': PROXY_PROTOCOL_SHARD_AWARE_PORT,
    })


@skip_scylla_version_lt(reason='scylladb/scylladb#27522 - native transport listeners with proxy protocol v2',
                        scylla_version="2026.1.0")
class TestClientRoutesAdvancedShardAwareness(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.direct_cluster = TestCluster()
        cls.direct_session = cls.direct_cluster.connect()
        cls.host_id_map = get_host_ids_from_cluster(cls.direct_session)
        cls.node_addrs = {node_id_from_ip(ip): ip for ip in cls.host_id_map}
        for host in cls.direct_cluster.metadata.all_hosts():
            assert host.sharding_info.shards_count == SHARDS, \
                "%s runs %d shards, expected %d" % (host, host.sharding_info.shards_count, SHARDS)

    @classmethod
    def tearDownClass(cls):
        cls.direct_cluster.shutdown()

    def _start_nlb(self, proxy_protocol, native_port):
        """Start an NLB in front of the nodes and publish routes through it."""
        nlb = NLBEmulator(native_port=native_port, proxy_protocol=proxy_protocol)
        nlb.start(self.node_addrs)
        self.addCleanup(nlb.stop)
        connection_id = str(uuid.uuid4())
        post_routes_for_nlb("127.0.0.1", connection_id, self.host_id_map, nlb)
        wait_for_routes_visible(self.direct_session, connection_id, len(self.host_id_map))
        return nlb, connection_id

    def _connect(self, nlb, connection_id, advanced_shard_awareness=None):
        """
        Connect through the NLB, recording the shard each new connection
        requests through its source port (None when it does not request one).
        ``advanced_shard_awareness=None`` leaves the option at its default.
        """
        config_options = {}
        if advanced_shard_awareness is not None:
            config_options['advanced_shard_awareness'] = advanced_shard_awareness
        cluster = Cluster(
            contact_points=[NLBEmulator.LISTEN_HOST],
            port=nlb.discovery_port,
            client_routes_config=ClientRoutesConfig(
                proxies=[ClientRouteProxy(connection_id, NLBEmulator.LISTEN_HOST)],
                **config_options
            ),
            load_balancing_policy=RoundRobinPolicy(),
        )
        self.addCleanup(cluster.shutdown)

        requested_shards = []
        connection_factory = cluster.connection_factory

        def recording_connection_factory(endpoint, *args, **kwargs):
            requested_shards.append(kwargs.get('shard_id'))
            return connection_factory(endpoint, *args, **kwargs)
        cluster.connection_factory = recording_connection_factory

        session = cluster.connect(wait_for_all_pools=True)
        return session, requested_shards

    def _wait_for_all_shards(self, session):
        """
        Wait until every pool has a connection to every shard. A pool retries
        a shard it is still missing only when a request is routed to it, so
        while waiting, send each pool's host a request whose routing key maps
        to each of its missing shards.
        """
        routing_keys = {}

        def routing_key_for(sharding_info, token_class, shard_id):
            if shard_id not in routing_keys:
                for i in range(100000):
                    key = b"shard-key-%d" % i
                    if sharding_info.shard_id_from_token(token_class.from_key(key).value) == shard_id:
                        routing_keys[shard_id] = key
                        break
            return routing_keys[shard_id]

        def all_shards_connected():
            pools = list(session._pools.values())
            if len(pools) == len(self.node_addrs) and all(
                    sorted(pool._connections) == list(range(SHARDS)) for pool in pools):
                return True
            token_map = session.cluster.metadata.token_map
            if token_map is None:
                return False
            for pool in pools:
                for shard_id in range(SHARDS):
                    if shard_id not in pool._connections:
                        key = routing_key_for(pool.host.sharding_info, token_map.token_class, shard_id)
                        session.execute(SimpleStatement("SELECT key FROM system.local", routing_key=key),
                                        host=pool.host)
            return False
        try:
            wait_until(all_shards_connected, 0.5, 60)
        except Exception:
            self.fail("Pools did not connect to all %d shards: %r" % (SHARDS, {
                host: sorted(pool._connections) for host, pool in session._pools.items()}))

    def _server_side_shards(self, session):
        """
        Map the client port of each of ``session``'s connections listed in
        system.clients to the shard serving it. Rows are picked by the
        SESSION_ID the driver reports, so connections of other clients from
        the same address cannot be mistaken for the session's.
        """
        session_id = str(session.cluster.session_id)
        rows = self.direct_session.execute("SELECT port, shard_id, client_options FROM system.clients")
        return {row.port: row.shard_id for row in rows
                if (row.client_options or {}).get('SESSION_ID') == session_id}

    def test_connections_land_on_requested_shards_with_proxy_protocol(self):
        """
        Through an endpoint that forwards the source port with PROXY protocol
        v2, every shard-aware connection lands on the shard it requested, so
        the pool never falls back.
        """
        nlb, connection_id = self._start_nlb(proxy_protocol=True,
                                             native_port=PROXY_PROTOCOL_SHARD_AWARE_PORT)
        session, requested_shards = self._connect(nlb, connection_id, advanced_shard_awareness=True)
        self._wait_for_all_shards(session)

        # Besides the control connection and each pool's first connection,
        # the pools requested every remaining shard.
        self.assertGreaterEqual(
            len([s for s in requested_shards if s is not None]),
            len(self.node_addrs) * (SHARDS - 1),
            "Expected shard-aware connection attempts, got %r" % requested_shards)

        for host, pool in session._pools.items():
            self.assertEqual(pool.advanced_shardaware_block_until, 0,
                             "Pool for %s stopped choosing source ports" % host)
            self.assertEqual(len(pool._excess_connections), 0)

        # The node took each connection's source port from the PROXY header
        # (system.clients shows the driver's port, not the NLB's) and served
        # it on the shard that port maps to. On this listener that holds for
        # every connection, including the first one of each pool, which does
        # not choose its port.
        client_ports = {}
        for pool in session._pools.values():
            for shard_id, connection in pool._connections.items():
                client_ports[connection._socket.getsockname()[1]] = shard_id
        for port, shard_id in client_ports.items():
            self.assertEqual(port % SHARDS, shard_id,
                             "Connection from port %d is on shard %d" % (port, shard_id))

        def server_sees_driver_ports():
            server_shards = self._server_side_shards(session)
            for port, shard_id in client_ports.items():
                self.assertIn(port, server_shards,
                              "system.clients does not show the driver's source port %d" % port)
                self.assertEqual(server_shards[port], shard_id)
        wait_until_not_raised(server_sees_driver_ports, 0.5, 30)

        session.execute("SELECT key FROM system.local")

    def test_falls_back_when_endpoint_does_not_carry_source_port(self):
        """
        Through an endpoint that replaces the source port and leads to the
        node's regular port, shard-aware connections land on shards the node
        picks. The driver warns, falls back, and still connects to every shard.
        """
        nlb, connection_id = self._start_nlb(proxy_protocol=False, native_port=REGULAR_PORT)

        # The node picks the shard, which can match the requested one by
        # chance, so allow several sessions before one of them sees a mismatch.
        # Each attempt counts only its own session's warnings: the previous
        # session is shut down, executor included, before the next starts.
        with MockLoggingHandler().set_module_name('cassandra.pool') as handler:
            for attempt in range(10):
                handler.reset()
                session, _ = self._connect(nlb, connection_id, advanced_shard_awareness=True)
                self._wait_for_all_shards(session)
                fell_back = [pool for pool in session._pools.values()
                             if pool.advanced_shardaware_block_until > time.time()]
                if fell_back:
                    break
                log.info("Attempt %d: every connection landed on its requested shard by chance", attempt)
                session.cluster.shutdown()
            else:
                self.fail("No session saw a connection land on an unrequested shard")

        warnings = [m for m in handler.messages['warning'] if "[client routes]" in m]
        self.assertTrue(warnings, "Expected a client routes shard mismatch warning")
        self.assertIn("PROXY protocol v2", warnings[0])
        self.assertIn("advanced_shard_awareness=False", warnings[0])
        # Warned once per pool, however many connections missed their shard.
        self.assertLessEqual(len(warnings), len(self.node_addrs))

        session.execute("SELECT key FROM system.local")

    def test_disabled_by_default(self):
        """
        Without advanced_shard_awareness no connection requests a shard, and
        through an endpoint leading to the node's regular PROXY protocol port
        the pool still reaches every shard.
        """
        nlb, connection_id = self._start_nlb(proxy_protocol=True,
                                             native_port=PROXY_PROTOCOL_REGULAR_PORT)
        session, requested_shards = self._connect(nlb, connection_id)
        self._wait_for_all_shards(session)

        self.assertTrue(session.cluster.shard_aware_options.disable_shardaware_port)
        self.assertEqual([s for s in requested_shards if s is not None], [],
                         "No connection should request a shard through its source port")
        session.execute("SELECT key FROM system.local")
