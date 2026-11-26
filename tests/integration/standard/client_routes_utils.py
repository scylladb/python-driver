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
Helpers shared by the client routes (PrivateLink) integration tests: an
emulated network load balancer and functions that publish routes for it
through ScyllaDB's REST API.
"""

import logging
import threading
import time

from tests.client_routes_rest import post_client_routes
from tests.tcp_proxy import TcpProxy

log = logging.getLogger(__name__)


class NLBEmulator:
    """
    Emulates a Network Load Balancer for a CCM cluster.

    Provides:
    - One *discovery port* (round-robin across all live nodes, used as the
      driver's ``contact_points``).
    - One *per-node port* for each node (dedicated proxy to that node's
      native transport port).

    All proxies listen on ``LISTEN_HOST`` (127.254.254.101), an address
    outside the CCM node range, simulating a real NLB endpoint.

    Port layout (all ports are OS-assigned by default):
        LISTEN_HOST:discovery_port        ->  round-robin to all live nodes
        LISTEN_HOST:<os-assigned>         ->  node1 (127.0.0.1:9042)
        LISTEN_HOST:<os-assigned>         ->  node2 (127.0.0.2:9042)
        ...

    Automatically creates/removes per-node proxies when nodes are
    added/removed so CCM cluster operations are reflected seamlessly.
    """

    LISTEN_HOST = "127.254.254.101"

    def __init__(self, discovery_port=0,
                 per_node_base=0,
                 native_port=9042,
                 node_addresses=None):
        self.discovery_port = discovery_port
        self.per_node_base = per_node_base
        self.native_port = native_port
        self._deferred_node_addresses = node_addresses

        self._node_proxies = {}
        self._discovery_proxy = None
        self._rr_index = 0
        # RLock: add_node() holds it while _add_node_proxy() re-acquires it.
        self._lock = threading.RLock()
        self._running = False

    def start(self, node_addresses):
        """
        Start the NLB with an initial set of node addresses.

        :param node_addresses: dict of node_id -> ip_address, e.g.
                               {1: "127.0.0.1", 2: "127.0.0.2"}
        """
        self._running = True
        try:
            for node_id, addr in node_addresses.items():
                self._add_node_proxy(node_id, addr)

            first_addr = list(node_addresses.values())[0]
            self._discovery_proxy = TcpProxy(
                self.LISTEN_HOST, self.discovery_port,
                first_addr, self.native_port,
            )
            self._discovery_proxy.start()
            self.discovery_port = self._discovery_proxy.listen_port
        except Exception:
            self.stop()
            raise
        original_handler = self._discovery_proxy._handle_new_connection

        def rr_handler(client_sock):
            addrs = self._live_addresses()
            if not addrs:
                client_sock.close()
                return
            idx = self._rr_index % len(addrs)
            self._rr_index += 1
            addr = addrs[idx]
            original_handler(client_sock, target_host=addr, target_port=self.native_port)

        self._discovery_proxy._handle_new_connection = rr_handler

        log.info("NLB started: discovery=%s:%d, %d node proxies",
                 self.LISTEN_HOST, self.discovery_port, len(self._node_proxies))
        return self

    def __enter__(self):
        if not self._running and self._deferred_node_addresses is not None:
            self.start(self._deferred_node_addresses)
        return self

    def __exit__(self, *args):
        self.stop()

    def stop(self):
        self._running = False
        if self._discovery_proxy:
            self._discovery_proxy.stop()
        for proxy in self._node_proxies.values():
            proxy.stop()
        self._node_proxies.clear()
        log.info("NLB stopped")

    def add_node(self, node_id, addr):
        # Serialize against remove_node(): TcpProxy.stop() blocks until
        # joined, so by the time we get the lock any freed fds are reaped.
        with self._lock:
            self._add_node_proxy(node_id, addr)

    def remove_node(self, node_id):
        with self._lock:
            proxy = self._node_proxies.pop(node_id, None)
            if proxy:
                proxy.stop()
        if proxy:
            log.info("NLB removed node %d", node_id)

    def node_port(self, node_id):
        proxy = self._node_proxies.get(node_id)
        if proxy:
            return proxy.listen_port
        return self.per_node_base + node_id

    def get_node_proxy(self, node_id):
        return self._node_proxies.get(node_id)

    def total_proxy_connections(self):
        return sum(p.total_connections for p in self._node_proxies.values())

    def active_proxy_connections(self):
        return sum(p.active_connections for p in self._node_proxies.values())

    def drop_all_connections(self):
        for proxy in self._node_proxies.values():
            proxy.drop_connections()
        if self._discovery_proxy:
            self._discovery_proxy.drop_connections()

    def _add_node_proxy(self, node_id, addr):
        port = 0
        proxy = TcpProxy(self.LISTEN_HOST, port, addr, self.native_port)
        proxy.start()
        with self._lock:
            self._node_proxies[node_id] = proxy
        log.info("NLB added node %d: %s:%d -> %s:%d",
                 node_id, self.LISTEN_HOST, port, addr, self.native_port)

    def _live_addresses(self):
        """
        IPs of nodes with active proxies.

        Snapshots under _lock: rr_handler() (the discovery port's accept
        handler) calls this from the discovery TcpProxy's own accept-loop
        thread, concurrently with add_node()/remove_node() mutating
        _node_proxies from other threads. Without the lock, a node being
        added/removed mid-iteration can raise "RuntimeError: dictionary
        changed size during iteration".
        """
        with self._lock:
            return [p.target_host for p in self._node_proxies.values()]


def get_host_ids_from_cluster(session):
    """
    Build a mapping of rpc_address -> host_id for all nodes in the cluster.

    Uses the driver's metadata rather than querying system.local / system.peers
    directly, because those queries can be routed to different coordinators
    (system.local returns the coordinator's own info while system.peers omits
    the coordinator), leading to a node being missing from the map.
    """
    host_id_map = {}
    for host in session.cluster.metadata.all_hosts():
        host_id_map[host.address] = host.host_id
    return host_id_map


def build_routes_for_nlb(connection_id, host_id_map, nlb):
    """
    Build routes that direct each host_id through the NLB per-node proxy.

    :param connection_id: Connection ID string
    :param host_id_map: dict ip -> uuid host_id (from get_host_ids_from_cluster)
    :param nlb: NLBEmulator instance
    :return: list of route dicts
    """
    routes = []
    for ip, host_id in host_id_map.items():
        node_id = int(ip.split(".")[-1])
        port = nlb.node_port(node_id)
        routes.append({
            "connection_id": connection_id,
            "host_id": host_id,
            "address": NLBEmulator.LISTEN_HOST,
            "port": port,
        })
    return routes


def post_routes_for_nlb(contact_point, connection_id, host_id_map, nlb, retries=1):
    """Build routes for the NLB and POST them via the REST API."""
    routes = build_routes_for_nlb(connection_id, host_id_map, nlb)
    post_client_routes(contact_point, routes, retries=retries)
    return routes

def wait_for_routes_visible(session, connection_id, expected_count, timeout=10, poll_interval=0.1):
    """
    Poll system.client_routes on **every** node until each one sees at
    least *expected_count* rows for *connection_id*.

    ``system.client_routes`` is a node-local table, so routes posted via
    the REST API to one node are not guaranteed to be visible on the
    others at the same time.  This helper ensures they have propagated
    everywhere before the test proceeds.

    :param session: an active driver Session (direct, not through NLB)
    :param connection_id: the connection_id string to filter on
    :param expected_count: how many rows we expect to see per node
    :param timeout: maximum seconds to wait
    :param poll_interval: seconds between polls
    """
    all_hosts = list(session.cluster.metadata.all_hosts())
    deadline = time.time() + timeout
    while True:
        pending_hosts = []
        for host in all_hosts:
            rows = list(session.execute(
                "SELECT * FROM system.client_routes WHERE connection_id = %s",
                (connection_id,),
                host=host,
            ))
            if len(rows) < expected_count:
                pending_hosts.append((host, len(rows)))
        if not pending_hosts:
            return
        if time.time() >= deadline:
            details = ", ".join(
                "%s: %d" % (h.address, count) for h, count in pending_hosts
            )
            raise RuntimeError(
                "Timed out waiting for %d routes (connection_id=%s) to appear "
                "in system.client_routes on all nodes; pending: %s"
                % (expected_count, connection_id, details)
            )
        time.sleep(poll_interval)


def node_id_from_ip(ip):
    """Extract node_id from an IP like '127.0.0.3' -> 3."""
    return int(ip.split(".")[-1])
