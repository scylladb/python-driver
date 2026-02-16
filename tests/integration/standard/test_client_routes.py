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

import os
import unittest
import uuid

from cassandra.cluster import Cluster
from cassandra.client_routes import ClientRoutesConfig, ClientRoutesEndpoint, ClientRoutesHandler
from tests.integration import TestCluster, use_cluster

def setup_module():
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 2 --memory 2048M"
    use_cluster('test_client_routes', [3], start=True)

class TestGetHostPortMapping(unittest.TestCase):
    """
    Test _query_routes method with different filtering scenarios.
    """

    @classmethod
    def setUpClass(cls):
        """Create test keyspace and table, populate with test data."""
        cls.cluster = TestCluster()
        cls.session = cls.cluster.connect()

        cls.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS gocql_test
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)

        cls.session.execute("""
            CREATE TABLE IF NOT EXISTS gocql_test.client_routes (
                connection_id uuid,
                host_id uuid,
                address text,
                port int,
                tls_port int,
                alternator_port int,
                alternator_https_port int,
                datacenter text,
                rack text,
                PRIMARY KEY (connection_id, host_id)
            )
        """)

        cls.session.execute("TRUNCATE gocql_test.client_routes")

        cls.host_ids = [uuid.uuid4() for _ in range(3)]
        cls.connection_ids = [uuid.uuid4() for _ in range(3)]
        cls.racks = ["rack1", "rack2", "rack3"]
        cls.expected = []

        for idx, host_id in enumerate(cls.host_ids):
            rack = cls.racks[idx]
            ip = f"127.0.0.{idx + 1}"

            for connection_id in cls.connection_ids:
                cls.session.execute(
                    """
                    INSERT INTO gocql_test.client_routes
                    (connection_id, host_id, address, port, tls_port,
                     alternator_port, alternator_https_port, datacenter, rack)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (connection_id, host_id, ip, 9042, 9142, 0, 0, 'dc1', rack)
                )

                cls.expected.append({
                    'connection_id': connection_id,
                    'host_id': host_id,
                    'address': ip,
                    'port': 9042,
                    'tls_port': 9142,
                    'datacenter': 'dc1',
                    'rack': rack
                })

        cls._sort_routes(cls.expected)

    @classmethod
    def tearDownClass(cls):
        """Clean up test keyspace."""
        try:
            cls.session.execute("DROP KEYSPACE IF EXISTS gocql_test")
        finally:
            cls.cluster.shutdown()

    @staticmethod
    def _sort_routes(routes):
        """Sort routes by connection_id then host_id for deterministic comparison."""
        routes.sort(key=lambda r: (str(r['connection_id']), str(r['host_id'])))

    def _query_and_compare(self, connection_ids, host_ids, expected):
        """
        Query routes using ClientRoutesHandler._query_routes and compare with expected.

        :param connection_ids: List of connection UUIDs or None
        :param host_ids: List of host UUIDs or None
        :param expected: Expected list of route dicts
        """
        config = ClientRoutesConfig(
            endpoints=[ClientRoutesEndpoint(
                connection_id=self.connection_ids[0],
                connection_addr="127.0.0.1"
            )],
            table_name="gocql_test.client_routes"
        )
        handler = ClientRoutesHandler(config)

        routes = handler._query_routes(
            self.cluster.control_connection,
            connection_ids=connection_ids,
            host_ids=host_ids
        )

        got = []
        for route in routes:
            got.append({
                'connection_id': route.connection_id,
                'host_id': route.host_id,
                'address': route.address,
                'port': route.port,
                'tls_port': route.tls_port,
                'datacenter': route.datacenter,
                'rack': route.rack
            })

        self._sort_routes(got)

        self.assertEqual(len(got), len(expected),
                        f"Expected {len(expected)} routes, got {len(got)}")

        for i, (got_route, expected_route) in enumerate(zip(got, expected)):
            self.assertEqual(got_route['connection_id'], expected_route['connection_id'],
                           f"Route {i}: connection_id mismatch")
            self.assertEqual(got_route['host_id'], expected_route['host_id'],
                           f"Route {i}: host_id mismatch")
            self.assertEqual(got_route['address'], expected_route['address'],
                           f"Route {i}: address mismatch")
            self.assertEqual(got_route['port'], expected_route['port'],
                           f"Route {i}: port mismatch")
            self.assertEqual(got_route['tls_port'], expected_route['tls_port'],
                           f"Route {i}: tls_port mismatch")

    def test_get_all(self):
        """Test querying all routes without filters."""
        self._query_and_compare(None, None, self.expected)

    def test_get_all_hosts(self):
        """Test querying with connection_ids filter only."""
        self._query_and_compare(self.connection_ids, None, self.expected)

    def test_get_all_connections(self):
        """Test querying with host_ids filter only."""
        self._query_and_compare(None, self.host_ids, self.expected)

    def test_get_concrete(self):
        """Test querying with both connection_ids and host_ids filters."""
        self._query_and_compare(self.connection_ids, self.host_ids, self.expected)

    def test_get_concrete_host(self):
        """Test querying specific connection and host combination."""
        filtered_expected = [
            r for r in self.expected
            if r['connection_id'] == self.connection_ids[0] and
               r['host_id'] == self.host_ids[0]
        ]

        self._query_and_compare(
            [self.connection_ids[0]],
            [self.host_ids[0]],
            filtered_expected
        )
