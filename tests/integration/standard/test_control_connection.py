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
# limitations under the License
#
#
#
from threading import Event

from cassandra import InvalidRequest

import unittest
import requests


from cassandra.protocol import ConfigurationException
from tests.integration import use_singledc, PROTOCOL_VERSION, TestCluster, greaterthanorequalcass40, \
    xfail_scylla_version_lt
from tests.integration.datatype_utils import update_datatypes


def setup_module():
    use_singledc()
    update_datatypes()


class ControlConnectionTests(unittest.TestCase):
    def setUp(self):
        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest(
                "Native protocol 3,0+ is required for UDTs using %r"
                % (PROTOCOL_VERSION,))
        self.cluster = TestCluster()

    def tearDown(self):
        try:
            self.session.execute("DROP KEYSPACE keyspacetodrop ")
        except (ConfigurationException, InvalidRequest):
            # we already removed the keyspace.
            pass
        self.cluster.shutdown()

    def test_drop_keyspace(self):
        """
        Test to validate that dropping a keyspace with user defined types doesn't kill the control connection.


        Creates a keyspace, and populates with a user defined type. It then records the control_connection's id. It
        will then drop the keyspace and get the id of the control_connection again. They should be the same. If they are
        not dropping the keyspace likely caused the control connection to be rebuilt.

        @since 2.7.0
        @jira_ticket PYTHON-358
        @expected_result the control connection is not killed

        @test_category connection
        """

        self.session = self.cluster.connect()
        self.session.execute("""
            CREATE KEYSPACE keyspacetodrop
            WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor': '1' }
            """)
        self.session.set_keyspace("keyspacetodrop")
        self.session.execute("CREATE TYPE user (age int, name text)")
        self.session.execute("CREATE TABLE mytable (a int PRIMARY KEY, b frozen<user>)")
        cc_id_pre_drop = id(self.cluster.control_connection._connection)
        self.session.execute("DROP KEYSPACE keyspacetodrop")
        cc_id_post_drop = id(self.cluster.control_connection._connection)
        assert cc_id_post_drop == cc_id_pre_drop

    def test_get_control_connection_host(self):
        """
        Test to validate Cluster.get_control_connection_host() metadata

        @since 3.5.0
        @jira_ticket PYTHON-583
        @expected_result the control connection metadata should accurately reflect cluster state.

        @test_category metadata
        """

        host = self.cluster.get_control_connection_host()
        assert host is None

        self.session = self.cluster.connect()
        cc_host = self.cluster.control_connection._connection.host

        host = self.cluster.get_control_connection_host()
        assert host.address == cc_host
        assert host.is_up

        # reconnect and make sure that the new host is reflected correctly
        self.cluster.control_connection._reconnect()
        new_host1 = self.cluster.get_control_connection_host()

        self.cluster.control_connection._reconnect()
        new_host2 = self.cluster.get_control_connection_host()

        assert new_host1 != new_host2

    # TODO: enable after https://github.com/scylladb/python-driver/issues/121 is fixed
    @unittest.skip('Fails on scylla due to the broadcast_rpc_port is None')
    @greaterthanorequalcass40
    def test_control_connection_port_discovery(self):
        """
        Test to validate that the correct port is discovered when peersV2 is used (C* 4.0+).

        Unit tests already validate that the port can be picked up (or not) from the query. This validates
        it picks up the correct port from a real server and is able to connect.
        """
        self.cluster = TestCluster()

        host = self.cluster.get_control_connection_host()
        assert host is None

        self.session = self.cluster.connect()
        cc_endpoint = self.cluster.control_connection._connection.endpoint

        host = self.cluster.get_control_connection_host()
        assert host.endpoint == cc_endpoint
        assert host.is_up
        hosts = self.cluster.metadata.all_hosts()
        assert len(hosts) == 3

        for host in hosts:
            assert 9042 == host.broadcast_rpc_port
            assert 7000 == host.broadcast_port

    @xfail_scylla_version_lt(reason='scylladb/scylladb#26992 - system.client_routes is not yet supported',
                             oss_scylla_version="7.0", ent_scylla_version="2025.4.0")
    def test_client_routes_change_event(self):
        cluster = TestCluster()

        # Establish control connection
        cluster.connect()

        flag = Event()

        connection_ids = ["anytext", "11510f50-f906-4844-8c74-49ddab9ac6a9"]
        host_ids = ["1a13fa42-c45b-410f-8ba5-58b42ada9c12", "aa13fa42-c45b-410f-8ba5-58b42ada9c12"]
        got_connection_ids = []
        got_host_ids = []

        def on_event(event):
            nonlocal got_connection_ids
            nonlocal got_host_ids
            try:
                assert event.get("change_type") == "UPDATE_NODES"
                got_connection_ids = event.get("connection_ids")
                got_host_ids = event.get("host_ids")
            finally:
                flag.set()

        cluster.control_connection._connection.register_watchers({"CLIENT_ROUTES_CHANGE": on_event})

        try:
            payload = [
                {
                    "connection_id": connection_ids[0],  # Should be a UUID if API requires that
                    "host_id": host_ids[0],
                    "address": "localhost",
                    "port": 9042,
                    "tls_port": 0,
                    "alternator_port": 0,
                    "alternator_https_port": 0,
                    "rack": "string",
                    "datacenter": "string"
                },
                {
                    "connection_id": connection_ids[1],
                    "host_id": host_ids[1],
                    "address": "localhost",
                    "port": 9042,
                    "tls_port": 0,
                    "alternator_port": 0,
                    "alternator_https_port": 0,
                    "rack": "string",
                    "datacenter": "string"
                }
            ]
            response = requests.post(
                "http://" + cluster.contact_points[0] + ":10000/v2/client-routes",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                })
            assert response.status_code == 200
            assert flag.wait(20), "Schema change event was not received after registering watchers"
            assert got_connection_ids == connection_ids
            assert got_host_ids == host_ids
        finally:
            cluster.shutdown()
