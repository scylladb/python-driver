import os
import logging
import unittest

from cassandra.cluster import ExecutionProfile
from cassandra.connection import DefaultEndPoint
from cassandra.policies import WhiteListRoundRobinPolicy

from tests.integration import use_cluster, get_node, get_cluster, local, TestCluster
from tests.util import wait_until_not_raised

LOGGER = logging.getLogger(__name__)

_saved_scylla_ext_opts = None


def setup_module():
    global _saved_scylla_ext_opts
    _saved_scylla_ext_opts = os.environ.get('SCYLLA_EXT_OPTS')
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 2 --memory 2048M"
    use_cluster('test_ip_change', [3], start=True)


def teardown_module():
    if _saved_scylla_ext_opts is None:
        os.environ.pop('SCYLLA_EXT_OPTS', None)
    else:
        os.environ['SCYLLA_EXT_OPTS'] = _saved_scylla_ext_opts

@local
class TestIpAddressChange(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.cluster = TestCluster()
        cls.session = cls.cluster.connect()

    @classmethod
    def teardown_class(cls):
        cls.cluster.shutdown()

    def test_change_address_during_live_session(self):
        node3 = get_node(3)
        old_endpoint = DefaultEndPoint(*node3.network_interfaces['binary'])
        old_host = self.cluster.metadata.get_host(old_endpoint)
        assert old_host is not None
        host_id = old_host.host_id

        LOGGER.debug("Stop node3")
        node3.stop()

        LOGGER.debug("Change IP address for node3")
        ip_prefix = get_cluster().get_ipprefix()
        new_ip = f'{ip_prefix}33'
        node3.set_configuration_options(values={'listen_address': new_ip, 'rpc_address': new_ip, 'api_address': new_ip})
        node3.network_interfaces = {k: (new_ip, v[1]) for k, v in node3.network_interfaces.items()}
        LOGGER.debug(f"Start node3 again with ip address {new_ip}")
        node3.start(wait_for_binary_proto=True)

        def new_address_found():
            metadata = self.cluster.metadata
            current = metadata.get_host_by_host_id(host_id)
            assert current is not None
            assert current is not old_host
            assert current.host_id == host_id
            assert current.endpoint == DefaultEndPoint(new_ip, old_endpoint.port)
            assert old_host.endpoint == old_endpoint
            assert old_host._is_removed
            assert metadata.get_host(old_endpoint) is None
            assert metadata.get_host(current.endpoint) is current

        wait_until_not_raised(new_address_found, 0.5, 100)

        new_node_only = ExecutionProfile(load_balancing_policy=WhiteListRoundRobinPolicy([new_ip]))
        self.cluster.add_execution_profile("new_node", new_node_only)

        def new_node_connectable():
            LOGGER.info(self.cluster.shard_aware_stats())
            local_info = self.session.execute("SELECT * FROM system.local WHERE key='local'", execution_profile="new_node").one()
            LOGGER.debug(local_info._asdict())
            assert local_info.broadcast_address == new_ip
            assert local_info.host_id == host_id

        wait_until_not_raised(new_node_connectable, 0.5, 100)
