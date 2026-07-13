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
            monitor_reporting_enabled=False,
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
            monitor_reporting_enabled=False,
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
            monitor_reporting_enabled=False,
        )

        session = self.cluster.connect()

        self._assert_unreachable_broadcast_rpc_metadata()
        assert session._initial_connect_futures == set()
        assert list(session.get_pools()) == []

        row = session.execute(
            "SELECT release_version, rpc_address FROM system.local WHERE key='local'").one()
        assert str(row.rpc_address) == _UNREACHABLE_BROADCAST_RPC_ADDRESS
        assert row.release_version
