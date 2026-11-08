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

from tests.integration import use_cluster, TestCluster


class MisconfiguredAuthenticationTests(unittest.TestCase):
    """ One node (not the contact point) has password auth. The rest of the nodes have no auth """
    @classmethod
    def setUpClass(cls):
        ccm_cluster = use_cluster(cls.__name__, [3], start=False)
        node3 = ccm_cluster.nodes['node3']
        node3.set_configuration_options(values={
            'authenticator': 'PasswordAuthenticator',
            'authorizer': 'CassandraAuthorizer',
        })
        ccm_cluster.start(wait_for_binary_proto=True, wait_other_notice=True)

        cls.ccm_cluster = ccm_cluster

    def test_connect_no_auth_provider(self):
        with TestCluster() as cluster:
            # connect() returns once the first pool succeeds. Wait for
            # the authentication attempt on node3 before checking its
            # state.
            cluster.connect(wait_for_all_pools=True)
            hosts = cluster.metadata.all_hosts()
            expected_addresses = {
                node.network_interfaces['binary'][0]
                for node in self.ccm_cluster.nodes.values()
            }
            assert {host.address for host in hosts} == expected_addresses
            down_addresses = {host.address for host in hosts if not host.is_up}
            assert down_addresses == {
                self.ccm_cluster.nodes['node3'].network_interfaces['binary'][0]
            }

    @classmethod
    def tearDownClass(cls):
        cls.ccm_cluster.stop()
