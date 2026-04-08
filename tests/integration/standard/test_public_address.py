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

"""
Reproducer for https://github.com/scylladb/python-driver/issues/720

A 3-node CCM cluster with broadcast_rpc_address set to 127.0.1.{1,2,3}
(different from the internal rpc_address 127.0.0.{1,2,3}).

Two sets of TCP proxies:
  - 127.0.1.{1,2,3}:9042 → 127.0.0.{1,2,3}:9042  (advertised via broadcast_rpc_address)
  - 127.0.2.{1,2,3}:9042 → 127.0.0.{1,2,3}:9042  (NOT advertised — simulates cloud NAT)
"""

import logging
import os
import select
import socket
import threading
import unittest

from cassandra.cluster import Cluster
from cassandra.policies import WhiteListRoundRobinPolicy

from tests.integration import (
    use_cluster, get_cluster, local,
    default_protocol_version, wait_for_node_socket,
)

LOGGER = logging.getLogger(__name__)

CLUSTER_NAME = 'test_public_addr'
PROXY_PORT = 9042


class TCPProxy:
    """
    A minimal TCP proxy that forwards connections from a listen address
    to a target address. Runs in a background thread.
    """

    def __init__(self, listen_host, listen_port, target_host, target_port):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port
        self._server_sock = None
        self._thread = None
        self._stop_event = threading.Event()

    def start(self):
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.settimeout(1.0)
        self._server_sock.bind((self.listen_host, self.listen_port))
        self._server_sock.listen(32)
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()
        LOGGER.debug("TCP proxy %s:%d -> %s:%d started",
                      self.listen_host, self.listen_port,
                      self.target_host, self.target_port)

    def stop(self):
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self._server_sock:
            self._server_sock.close()

    def _accept_loop(self):
        while not self._stop_event.is_set():
            try:
                client, _ = self._server_sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            t = threading.Thread(target=self._relay, args=(client,), daemon=True)
            t.start()

    def _relay(self, client):
        target = None
        try:
            target = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            target.connect((self.target_host, self.target_port))
            socks = [client, target]
            while not self._stop_event.is_set():
                readable, _, errored = select.select(socks, [], socks, 1.0)
                if errored:
                    break
                for s in readable:
                    data = s.recv(65536)
                    if not data:
                        return
                    other = target if s is client else client
                    other.sendall(data)
        except OSError:
            pass
        finally:
            client.close()
            if target:
                target.close()


_proxies = []


def setup_module():
    os.environ['SCYLLA_EXT_OPTS'] = os.environ.get('SCYLLA_EXT_OPTS', '') + ' --smp 1 --memory 512M'

    use_cluster(CLUSTER_NAME, [3], start=False, set_keyspace=False)

    ccm_cluster = get_cluster()
    for i in range(1, 4):
        node = ccm_cluster.nodes[f'node{i}']
        node.set_configuration_options(values={
            'broadcast_rpc_address': f'127.0.1.{i}',
        })

    ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
    for node in ccm_cluster.nodes.values():
        wait_for_node_socket(node, 120)

    # Advertised proxies: 127.0.1.x (matches broadcast_rpc_address)
    for i in range(1, 4):
        p = TCPProxy(f'127.0.1.{i}', PROXY_PORT, f'127.0.0.{i}', PROXY_PORT)
        p.start()
        _proxies.append(p)

    # Unadvertised proxies: 127.0.2.x (simulates cloud NAT, unknown to nodes)
    for i in range(1, 4):
        p = TCPProxy(f'127.0.2.{i}', PROXY_PORT, f'127.0.0.{i}', PROXY_PORT)
        p.start()
        _proxies.append(p)


def teardown_module():
    for p in _proxies:
        p.stop()
    _proxies.clear()


@local
class TestPublicAddress(unittest.TestCase):

    def test_connect_via_single_broadcast_address_with_whitelist(self):
        """
        Connect via advertised broadcast_rpc_address (127.0.1.1).
        system.local returns rpc_address=127.0.1.1, so the whitelist
        accepts it.
        """
        proxy_address = '127.0.1.1'
        policy = WhiteListRoundRobinPolicy([proxy_address])
        cluster = Cluster(
            contact_points=[proxy_address],
            load_balancing_policy=policy,
            protocol_version=default_protocol_version,
        )
        try:
            session = cluster.connect()
            result = session.execute("SELECT * FROM system.local WHERE key='local'")
            assert result.one() is not None
        finally:
            cluster.shutdown()

    def test_connect_via_all_broadcast_addresses_with_whitelist(self):
        """
        Connect via all advertised broadcast_rpc_addresses (127.0.1.{1,2,3}).
        """
        proxy_addresses = [f'127.0.1.{i}' for i in range(1, 4)]
        policy = WhiteListRoundRobinPolicy(proxy_addresses)
        cluster = Cluster(
            contact_points=proxy_addresses,
            load_balancing_policy=policy,
            protocol_version=default_protocol_version,
        )
        try:
            session = cluster.connect(wait_for_all_pools=True)

            host_addresses = {h.broadcast_rpc_address for h in cluster.metadata.all_hosts()}
            assert set(proxy_addresses) == host_addresses, \
                f"Expected {set(proxy_addresses)}, got {host_addresses}"

            result = session.execute("SELECT * FROM system.local WHERE key='local'")
            assert result.one() is not None
        finally:
            cluster.shutdown()

    def test_connect_via_unadvertised_nat_address_with_whitelist(self):
        """
        Reproducer for the exact scenario in issue #720.

        Connect via unadvertised NAT proxy (127.0.2.1) with
        WhiteListRoundRobinPolicy(['127.0.2.1']). The node has
        broadcast_rpc_address=127.0.1.1, so system.local returns
        rpc_address=127.0.1.1 — NOT 127.0.2.1 that we connected to.

        The driver must preserve the original contact point endpoint
        (127.0.2.1) so the whitelist accepts it. Without the fix, the
        driver replaces it with 127.0.1.1 from system.local and the
        whitelist rejects it → NoHostAvailable.
        """
        nat_address = '127.0.2.1'
        policy = WhiteListRoundRobinPolicy([nat_address])
        cluster = Cluster(
            contact_points=[nat_address],
            load_balancing_policy=policy,
            protocol_version=default_protocol_version,
        )
        try:
            session = cluster.connect()
            result = session.execute("SELECT * FROM system.local WHERE key='local'")
            assert result.one() is not None
        finally:
            cluster.shutdown()
