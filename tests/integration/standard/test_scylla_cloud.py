import json
import logging
import os.path
from unittest import TestCase
from ccmlib.utils.ssl_utils import generate_ssl_stores
from ccmlib.utils.sni_proxy import refresh_certs, start_sni_proxy, create_cloud_config, NodeInfo

from cassandra import DependencyException
from cassandra.policies import TokenAwarePolicy, RoundRobinPolicy, ConstantReconnectionPolicy
from tests.integration import use_cluster, PROTOCOL_VERSION
from cassandra.cluster import Cluster, TwistedConnection


supported_connection_classes = [TwistedConnection]

try:
    from cassandra.io.libevreactor import LibevConnection
    supported_connection_classes += [LibevConnection]
except DependencyException:
    pass


try:
    from cassandra.io.asyncorereactor import AsyncoreConnection
    supported_connection_classes += [AsyncoreConnection]
except DependencyException:
    pass

#from cassandra.io.geventreactor import GeventConnection
#from cassandra.io.eventletreactor import EventletConnection
#from cassandra.io.asyncioreactor import AsyncioConnection

# need to run them with specific configuration like `gevent.monkey.patch_all()` or under async functions
# unsupported_connection_classes = [GeventConnection, AsyncioConnection, EventletConnection]
LOGGER = logging.getLogger(__name__)


def get_cluster_info(cluster, port=9142):
    session = Cluster(
        contact_points=list(map(lambda node: node.address(), cluster.nodelist())), protocol_version=PROTOCOL_VERSION,
        load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
        reconnection_policy=ConstantReconnectionPolicy(5)
    ).connect()

    nodes_info = []

    for row in session.execute('select host_id, broadcast_address, data_center from system.local'):
        if row[0] and row[1]:
            nodes_info.append(NodeInfo(address=row[1],
                                       port=port,
                                       host_id=row[0],
                                       data_center=row[2]))

    for row in session.execute('select host_id, broadcast_address, data_center from system.local'):
        nodes_info.append(NodeInfo(address=row[1],
                                   port=port,
                                   host_id=row[0],
                                   data_center=row[2]))

    return nodes_info


class ScyllaCloudConfigTests(TestCase):
    def start_cluster_with_proxy(self):
        ccm_cluster = self.ccm_cluster
        generate_ssl_stores(ccm_cluster.get_path())
        ssl_port = 9142
        sni_port = 443
        ccm_cluster.set_configuration_options(dict(
            client_encryption_options=
            dict(require_client_auth=True,
                 truststore=os.path.join(ccm_cluster.get_path(), 'ccm_node.cer'),
                 certificate=os.path.join(ccm_cluster.get_path(), 'ccm_node.pem'),
                 keyfile=os.path.join(ccm_cluster.get_path(), 'ccm_node.key'),
                 enabled=True),
            native_transport_port_ssl=ssl_port))

        ccm_cluster._update_config()

        ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        nodes_info = get_cluster_info(ccm_cluster, port=ssl_port)
        refresh_certs(ccm_cluster, nodes_info)

        docker_id, listen_address, listen_port = \
            start_sni_proxy(ccm_cluster.get_path(), nodes_info=nodes_info, listen_port=sni_port)
        ccm_cluster.sni_proxy_docker_ids = [docker_id]
        ccm_cluster.sni_proxy_listen_port = listen_port
        ccm_cluster._update_config()

        config_data_yaml, config_path_yaml = create_cloud_config(ccm_cluster.get_path(),
                                                                 port=listen_port, address=listen_address,
                                                                 nodes_info=nodes_info)
        return config_data_yaml, config_path_yaml

    def test_1_node_cluster(self):
        self.ccm_cluster = use_cluster("sni_proxy", [1], start=False)
        config_data_yaml, config_path_yaml = self.start_cluster_with_proxy()

        for config in [config_path_yaml, config_data_yaml]:
            for connection_class in supported_connection_classes:
                logging.warning('testing with class: %s', connection_class.__name__)
                cluster = Cluster(scylla_cloud=config, connection_class=connection_class)
                try:
                    with cluster.connect() as session:
                        res = session.execute("SELECT * FROM system.local WHERE key='local'")
                        assert res.all()

                        assert len(cluster.metadata._hosts) == 1
                        assert len(cluster.metadata._host_id_by_endpoint) == 1
                finally:
                    cluster.shutdown()

    def test_3_node_cluster(self):
        self.ccm_cluster = use_cluster("sni_proxy", [3], start=False)
        config_data_yaml, config_path_yaml = self.start_cluster_with_proxy()

        for config in [config_path_yaml, config_data_yaml]:
            for connection_class in supported_connection_classes:
                logging.warning('testing with class: %s', connection_class.__name__)
                cluster = Cluster(scylla_cloud=config, connection_class=connection_class)
                try:
                    with cluster.connect() as session:
                        res = session.execute("SELECT * FROM system.local WHERE key='local'")
                        assert res.all()
                        assert len(cluster.metadata._hosts) == 3
                        assert len(cluster.metadata._host_id_by_endpoint) == 3
                finally:
                    cluster.shutdown()
