# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from types import SimpleNamespace
from unittest.mock import Mock

import pytest


@pytest.fixture
def integration_cluster(monkeypatch, tmp_path):
    monkeypatch.delenv('SCYLLA_VERSION', raising=False)
    monkeypatch.setenv('CASSANDRA_VERSION', '3.11.4')
    monkeypatch.delenv('MAPPED_CASSANDRA_VERSION', raising=False)
    import tests.integration as integration

    monkeypatch.setattr(integration, 'path', str(tmp_path))
    monkeypatch.setattr(integration, 'CCM_CLUSTER', None)
    monkeypatch.setattr(integration, 'SCYLLA_VERSION', 'release:2026.1')
    monkeypatch.setattr(integration, 'CCM_KWARGS', {'version': '3.11.4'})
    monkeypatch.setattr(integration, 'wait_for_node_socket', Mock())
    monkeypatch.setattr(integration, 'setup_keyspace', Mock())
    monkeypatch.setattr(integration, 'common', SimpleNamespace(
        switch_cluster=Mock()), raising=False)
    return integration


class ScyllaNode:
    data_center = 'dc1'
    workloads = []

    def __init__(self, smp=2):
        self._smp = smp

    def smp(self):
        return self._smp

    def set_smp(self, smp):
        self._smp = smp


def cluster_with_nodes(nodes):
    cluster = Mock(name='ccm_cluster')
    cluster.name = 'shard_test'
    cluster.nodes = dict(enumerate(nodes))
    cluster.nodelist.return_value = nodes
    return cluster


@pytest.mark.parametrize('source', ['new', 'loaded', 'current'])
def test_configure_shards_before_start(
        integration_cluster, monkeypatch, source):
    integration = integration_cluster
    cluster = cluster_with_nodes([ScyllaNode(), ScyllaNode()])
    load = Mock(return_value=cluster)
    if source == 'new':
        load.side_effect = FileNotFoundError('cluster not created yet')
    elif source == 'current':
        monkeypatch.setattr(integration, 'CCM_CLUSTER', cluster)
    monkeypatch.setattr(integration, 'CCMClusterFactory',
                        SimpleNamespace(load=load))
    monkeypatch.setattr(integration, 'CCMScyllaCluster',
                        Mock(return_value=cluster), raising=False)

    shards_at_start = []
    cluster.start.side_effect = lambda **kwargs: shards_at_start.append(
        [node.smp() for node in cluster.nodelist()])
    assert integration.use_cluster('shard_test', [2], smp=3) is cluster
    assert shards_at_start == [[3, 3]]
    if source == 'current':
        cluster.stop.assert_called_once_with()
    integration.setup_keyspace.assert_called_once()


@pytest.mark.parametrize('smp', [None, 3])
def test_compatible_shards_reuse_current_cluster(
        integration_cluster, monkeypatch, smp):
    integration = integration_cluster
    node = ScyllaNode(3)
    monkeypatch.setattr(node, 'set_smp', Mock(wraps=node.set_smp))
    cluster = cluster_with_nodes([node])
    monkeypatch.setattr(integration, 'CCM_CLUSTER', cluster)
    load = Mock()
    monkeypatch.setattr(integration, 'CCMClusterFactory',
                        SimpleNamespace(load=load))

    assert integration.use_cluster('shard_test', [1], smp=smp) is cluster
    load.assert_not_called()
    cluster.stop.assert_not_called()
    cluster.start.assert_called_once()
    if smp is None:
        node.set_smp.assert_not_called()
    else:
        node.set_smp.assert_called_once_with(smp)


def test_deferred_start_keeps_requested_shards(
        integration_cluster, monkeypatch):
    integration = integration_cluster
    cluster = cluster_with_nodes([ScyllaNode()])
    monkeypatch.setattr(integration, 'CCMClusterFactory', SimpleNamespace(
        load=Mock(return_value=cluster)))

    integration.use_cluster('shard_test', [1], start=False, smp=3)
    assert cluster.nodelist()[0].smp() == 3
    cluster.start.assert_not_called()


@pytest.mark.parametrize('smp', [None, 3])
def test_cassandra_does_not_require_shard_api(
        integration_cluster, monkeypatch, smp):
    integration = integration_cluster
    monkeypatch.setattr(integration, 'SCYLLA_VERSION', None)
    cluster = cluster_with_nodes([
        SimpleNamespace(data_center='dc1', workloads=[])])
    monkeypatch.setattr(integration, 'CCM_CLUSTER', cluster)

    assert integration.use_cluster('shard_test', [1], smp=smp) is cluster
    cluster.stop.assert_not_called()
    cluster.start.assert_called_once()


@pytest.mark.parametrize('scylla', [False, True])
def test_bootstrap_preserves_shards_only_for_scylla(
        integration_cluster, monkeypatch, scylla):
    monkeypatch.setattr(integration_cluster, 'SCYLLA_VERSION', None)
    from tests.integration.standard import test_client_routes

    monkeypatch.setattr(test_client_routes, 'SCYLLA_VERSION',
                        'release:2026.1' if scylla else None)
    wait = Mock()
    monkeypatch.setattr(test_client_routes, 'wait_for_node_socket', wait)
    shards_at_start = []

    class CassandraNode:
        def __init__(self, *args, **kwargs):
            self.start = Mock(side_effect=lambda **kwargs: shards_at_start.append(
                self.smp() if scylla else None))

    class ScyllaNode(CassandraNode):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._smp = 2

        def smp(self):
            return self._smp

        def set_smp(self, smp):
            self._smp = smp

    source = ScyllaNode() if scylla else CassandraNode()
    if scylla:
        source.set_smp(3)
    cluster = cluster_with_nodes([source])
    case = test_client_routes.TestFullNodeReplacementThroughNlb()
    case._bootstrap_node(cluster, 2, data_center='dc1', rack='rack1')

    node = cluster.add.call_args.args[0]
    assert type(node) is type(source)
    if scylla:
        assert node.smp() == 3
    cluster.add.assert_called_once_with(
        node, is_seed=False, data_center='dc1', rack='rack1')
    node.start.assert_called_once_with(
        wait_for_binary_proto=True, wait_other_notice=True)
    assert shards_at_start == [3 if scylla else None]
    wait.assert_called_once_with(node, 120)
