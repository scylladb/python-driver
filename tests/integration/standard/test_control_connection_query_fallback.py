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

import unittest

import pytest

from cassandra import InvalidRequest
from cassandra.cluster import ControlConnectionQueryFallback, NoHostAvailable

from tests.integration import TestCluster, local, remove_cluster, use_cluster


_CLUSTER_NAME = "control_connection_query_fallback"
_UNREACHABLE_BROADCAST_RPC_ADDRESS = "127.255.255.1"


def setup_module():
    remove_cluster()

    ccm_cluster = use_cluster(_CLUSTER_NAME, [1], start=False)
    ccm_cluster.nodes["node1"].set_configuration_options(values={
        "broadcast_rpc_address": _UNREACHABLE_BROADCAST_RPC_ADDRESS,
    })
    ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)


def teardown_module():
    remove_cluster()


@local
class ControlConnectionQueryFallbackIntegrationTests(unittest.TestCase):

    def setUp(self):
        self.cluster = None

    def tearDown(self):
        if self.cluster is not None:
            self.cluster.shutdown()

    def _assert_unreachable_broadcast_rpc_metadata(self):
        hosts = self.cluster.metadata.all_hosts()
        assert len(hosts) == 1

        host = hosts[0]
        assert host.broadcast_rpc_address == _UNREACHABLE_BROADCAST_RPC_ADDRESS
        assert host.endpoint.address == _UNREACHABLE_BROADCAST_RPC_ADDRESS
        return host

    def test_disabled_raises_when_broadcast_rpc_address_is_unreachable(self):
        self.cluster = TestCluster(
            allow_control_connection_query_fallback=ControlConnectionQueryFallback.Disabled,
            connect_timeout=1,
        )

        with pytest.raises(NoHostAvailable):
            self.cluster.connect()

        self._assert_unreachable_broadcast_rpc_metadata()
        assert self.cluster.control_connection._connection is not None
        assert self.cluster.get_all_pools() == []

    def test_fallback_executes_queries_when_broadcast_rpc_address_is_unreachable(self):
        self.cluster = TestCluster(
            allow_control_connection_query_fallback=ControlConnectionQueryFallback.Fallback,
            connect_timeout=1,
        )

        session = self.cluster.connect()

        self._assert_unreachable_broadcast_rpc_metadata()
        assert session._initial_connect_futures
        assert list(session.get_pools()) == []

        row = session.execute(
            "SELECT release_version, rpc_address FROM system.local WHERE key='local'").one()
        assert str(row.rpc_address) == _UNREACHABLE_BROADCAST_RPC_ADDRESS
        assert row.release_version

    def test_no_node_pool_fallback_executes_queries_without_creating_pools(self):
        self.cluster = TestCluster(
            allow_control_connection_query_fallback=ControlConnectionQueryFallback.SkipPoolCreation,
            connect_timeout=1,
        )

        session = self.cluster.connect()

        self._assert_unreachable_broadcast_rpc_metadata()
        assert session._initial_connect_futures == set()
        assert list(session.get_pools()) == []

        row = session.execute(
            "SELECT release_version, rpc_address FROM system.local WHERE key='local'").one()
        assert str(row.rpc_address) == _UNREACHABLE_BROADCAST_RPC_ADDRESS
        assert row.release_version

    def _bootstrap_keyspaces(self, *keyspaces, tables=()):
        bootstrap_cluster = TestCluster(
            allow_control_connection_query_fallback=ControlConnectionQueryFallback.SkipPoolCreation,
            connect_timeout=1,
        )
        try:
            setup_session = bootstrap_cluster.connect()
            for keyspace in keyspaces:
                setup_session.execute("DROP KEYSPACE IF EXISTS {}".format(keyspace))
                setup_session.execute(
                    "CREATE KEYSPACE {} WITH replication = "
                    "{{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}}".format(keyspace))
            for table in tables:
                setup_session.execute(table)
        finally:
            bootstrap_cluster.shutdown()

    def test_shared_control_connection_accepts_only_one_session_keyspace(self):
        self._bootstrap_keyspaces(
            'fallback_ks_one', 'fallback_ks_two',
            tables=("CREATE TABLE fallback_ks_one.items (id int PRIMARY KEY, value text)",))

        self.cluster = TestCluster(
            allow_control_connection_query_fallback=ControlConnectionQueryFallback.SkipPoolCreation,
            connect_timeout=1,
        )
        session_one = self.cluster.connect('fallback_ks_one')
        session_one_peer = self.cluster.connect('fallback_ks_one')
        control_connection = self.cluster.control_connection._connection

        assert list(session_one.get_pools()) == []
        assert list(session_one_peer.get_pools()) == []
        with pytest.raises(InvalidRequest, match='already attached'):
            self.cluster.connect('fallback_ks_two')
        with pytest.raises(InvalidRequest, match='already attached'):
            self.cluster.connect()

        insert_one = session_one.execute_async(
            "INSERT INTO items (id, value) VALUES (1, 'one')")
        insert_two = session_one_peer.execute_async(
            "INSERT INTO items (id, value) VALUES (2, 'two')")
        insert_one.result()
        insert_two.result()

        prepared_one = session_one.prepare(
            "INSERT INTO items (id, value) VALUES (?, ?)")
        prepared_insert_one = session_one.execute_async(prepared_one, (2, 'prepared-one'))
        prepared_insert_one.result()

        select_one = session_one_peer.execute_async(
            "SELECT value FROM items WHERE id IN (1, 2)")

        assert {row.value for row in select_one.result()} == {'one', 'prepared-one'}
        assert self.cluster.control_connection._connection is control_connection

    def test_shared_control_connection_keyspace_is_reclaimed_after_shutdown(self):
        self._bootstrap_keyspaces('fallback_ks_one', 'fallback_ks_two')

        self.cluster = TestCluster(
            allow_control_connection_query_fallback=ControlConnectionQueryFallback.SkipPoolCreation,
            connect_timeout=1,
        )
        session_one = self.cluster.connect('fallback_ks_one')
        control_connection = self.cluster.control_connection._connection

        assert list(session_one.get_pools()) == []
        session_one.execute("SELECT key FROM system.local WHERE key='local'")
        assert control_connection.keyspace == 'fallback_ks_one'

        # while the binding is held, another keyspace cannot use the fallback
        with pytest.raises(InvalidRequest, match='already attached'):
            self.cluster.connect('fallback_ks_two')

        session_one.shutdown()

        # the binding is released with its only holder, so it can be taken over
        session_two = self.cluster.connect('fallback_ks_two')
        session_two.execute("SELECT key FROM system.local WHERE key='local'")
        assert control_connection.keyspace == 'fallback_ks_two'

        session_two.shutdown()

        # ...but not by a session without a keyspace: the shared connection is
        # still in 'fallback_ks_two' and CQL cannot unset it
        with pytest.raises(InvalidRequest, match='cannot be reset to no keyspace'):
            self.cluster.connect()

        assert self.cluster.control_connection._connection is control_connection
