# Copyright 2025 ScyllaDB, Inc.
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
import uuid
import time
from unittest.mock import Mock, patch

from cassandra.client_routes import (
    ClientRoutesEndpoint,
    ClientRoutesConfig,
    _ResolvedRoutes,
    _ResolvedRoute,
    _ClientRoutesHandler
)
from cassandra import DriverException


class TestClientRoutesEndpoint(unittest.TestCase):

    def test_endpoint_none_connection_id(self):
        with self.assertRaises(ValueError):
            ClientRoutesEndpoint(None)


class TestClientRoutesConfig(unittest.TestCase):

    def test_config_with_endpoints(self):
        ep1 = ClientRoutesEndpoint(str(uuid.uuid4()), "10.0.0.1")
        ep2 = ClientRoutesEndpoint(str(uuid.uuid4()), "10.0.0.2")
        config = ClientRoutesConfig([ep1, ep2])
        self.assertEqual(len(config.endpoints), 2)
        self.assertEqual(config.table_name, "system.client_routes")

    def test_config_custom_table_name(self):
        ep = ClientRoutesEndpoint(str(uuid.uuid4()))
        config = ClientRoutesConfig([ep])
        config.table_name = "custom.routes"
        self.assertEqual(config.table_name, "custom.routes")

    def test_config_empty_endpoints(self):
        with self.assertRaises(ValueError):
            ClientRoutesConfig([])

    def test_config_invalid_endpoint_type(self):
        with self.assertRaises(TypeError):
            ClientRoutesConfig(["not-an-endpoint"])

    def test_config_cache_ttl_validation(self):
        ep = ClientRoutesEndpoint(str(uuid.uuid4()))
        # Must be >= 0
        with self.assertRaises(ValueError):
            ClientRoutesConfig([ep], cache_ttl_seconds=-1)
        # 0 is valid (never cache)
        config = ClientRoutesConfig([ep], cache_ttl_seconds=0)
        self.assertEqual(config.cache_ttl_seconds, 0)


class Test_ResolvedRoutes(unittest.TestCase):

    def test_get_by_host_id(self):
        routes = _ResolvedRoutes()
        host_id = uuid.uuid4()
        route = _ResolvedRoute(
            connection_id=uuid.uuid4(),
            host_id=host_id,
            address="example.com",
            port=9042,
            tls_port=9142,
            datacenter="dc1",
            rack="rack1",
            all_known_ips=["192.168.1.1", "192.168.1.2"],
            current_ip="192.168.1.1",
            update_time=12345.0,
            forced_resolve=False
        )

        routes.update([route])

        retrieved = routes.get_by_host_id(host_id)
        self.assertEqual(retrieved.host_id, host_id)
        self.assertEqual(retrieved.address, "example.com")

    def test_merge_routes(self):
        routes = _ResolvedRoutes()
        host_id1 = uuid.uuid4()
        host_id2 = uuid.uuid4()

        route1 = _ResolvedRoute(
            connection_id=uuid.uuid4(), host_id=host_id1,
            address="host1.com", port=9042, tls_port=None,
            datacenter="dc1", rack="rack1",
            all_known_ips=["192.168.1.1"], current_ip="192.168.1.1",
            update_time=12345.0, forced_resolve=False
        )

        route2 = _ResolvedRoute(
            connection_id=uuid.uuid4(), host_id=host_id2,
            address="host2.com", port=9042, tls_port=None,
            datacenter="dc1", rack="rack1",
            all_known_ips=["192.168.1.2"], current_ip="192.168.1.2",
            update_time=12345.0, forced_resolve=False
        )

        routes.update([route1])
        routes.merge([route2])

        self.assertIsNotNone(routes.get_by_host_id(host_id1))
        self.assertIsNotNone(routes.get_by_host_id(host_id2))


class TestClientRoutesHandler(unittest.TestCase):

    def setUp(self):
        self.conn_id = uuid.uuid4()
        self.endpoint = ClientRoutesEndpoint(str(self.conn_id), "10.0.0.1")
        self.config = ClientRoutesConfig([self.endpoint])

    def test_handler_initialization(self):
        handler = _ClientRoutesHandler(self.config, ssl_enabled=False)
        self.assertIsNotNone(handler)
        self.assertEqual(handler.ssl_enabled, False)

    @patch.object(_ClientRoutesHandler, '_query_routes')
    def test_initialize(self, mock_query):
        host_id = uuid.uuid4()
        mock_query.return_value = [
            _ResolvedRoute(
                connection_id=self.conn_id,
                host_id=host_id,
                address="node1.example.com",
                port=9042,
                tls_port=9142,
                datacenter="dc1",
                rack="rack1",
                all_known_ips=None,
                current_ip=None,
                update_time=None,
                forced_resolve=True
            )
        ]

        handler = _ClientRoutesHandler(self.config)
        mock_control_conn = Mock()

        handler.initialize(mock_control_conn)

        mock_query.assert_called_once()
        # Verify route was stored
        route = handler._routes.get_by_host_id(host_id)
        self.assertIsNotNone(route)
        self.assertEqual(route.address, "node1.example.com")

class TestQueryBuilding(unittest.TestCase):

    def test_query_all_routes(self):
        """Test querying all routes without filters."""
        conn_id = uuid.uuid4()
        config = ClientRoutesConfig([ClientRoutesEndpoint(str(conn_id), "10.0.0.1")])
        handler = _ClientRoutesHandler(config)

        mock_control_conn = Mock()
        mock_connection = Mock()
        mock_control_conn._connection = mock_connection
        mock_control_conn._timeout = 10

        # Mock response
        mock_result = Mock()
        mock_result.parsed_rows = []
        mock_result.column_names = []
        mock_connection.wait_for_response.return_value = mock_result

        # Query all routes
        handler._query_routes(mock_control_conn, connection_ids=None, host_ids=None)

        # Verify query structure
        call_args = mock_connection.wait_for_response.call_args
        query_msg = call_args[0][0]
        query_str = query_msg.query.lower()

        self.assertIn("select * from", query_str)
        self.assertIn("allow filtering", query_str)
        self.assertNotIn("where", query_str)

    def test_query_with_connection_ids_only(self):
        """Test querying with connection_ids filter only."""
        conn_id1 = uuid.uuid4()
        conn_id2 = uuid.uuid4()
        config = ClientRoutesConfig([ClientRoutesEndpoint(str(conn_id1), "10.0.0.1")])
        handler = _ClientRoutesHandler(config)

        mock_control_conn = Mock()
        mock_connection = Mock()
        mock_control_conn._connection = mock_connection
        mock_control_conn._timeout = 10

        mock_result = Mock()
        mock_result.parsed_rows = []
        mock_result.column_names = []
        mock_connection.wait_for_response.return_value = mock_result

        # Query with connection_ids filter
        handler._query_routes(mock_control_conn, connection_ids=[conn_id1, conn_id2], host_ids=None)

        call_args = mock_connection.wait_for_response.call_args
        query_msg = call_args[0][0]
        query_str = query_msg.query.lower()

        self.assertIn("where", query_str)
        self.assertIn("connection_id in", query_str)
        self.assertNotIn("host_id in", query_str)

    def test_query_with_host_ids_only(self):
        """Test querying with host_ids filter only."""
        conn_id = uuid.uuid4()
        host_id = uuid.uuid4()
        config = ClientRoutesConfig([ClientRoutesEndpoint(str(conn_id), "10.0.0.1")])
        handler = _ClientRoutesHandler(config)

        mock_control_conn = Mock()
        mock_connection = Mock()
        mock_control_conn._connection = mock_connection
        mock_control_conn._timeout = 10

        mock_result = Mock()
        mock_result.parsed_rows = []
        mock_result.column_names = []
        mock_connection.wait_for_response.return_value = mock_result

        # Query with host_ids filter
        handler._query_routes(mock_control_conn, connection_ids=None, host_ids=[host_id])

        call_args = mock_connection.wait_for_response.call_args
        query_msg = call_args[0][0]
        query_str = query_msg.query.lower()

        self.assertIn("where", query_str)
        self.assertIn("host_id in", query_str)
        self.assertNotIn("connection_id in", query_str)

    def test_query_with_both_filters(self):
        """Test querying with both connection_ids and host_ids filters."""
        conn_id = uuid.uuid4()
        host_id1 = uuid.uuid4()
        host_id2 = uuid.uuid4()
        config = ClientRoutesConfig([ClientRoutesEndpoint(str(conn_id), "10.0.0.1")])
        handler = _ClientRoutesHandler(config)

        mock_control_conn = Mock()
        mock_connection = Mock()
        mock_control_conn._connection = mock_connection
        mock_control_conn._timeout = 10

        mock_result = Mock()
        mock_result.parsed_rows = []
        mock_result.column_names = []
        mock_connection.wait_for_response.return_value = mock_result

        # Query with both filters
        handler._query_routes(mock_control_conn, connection_ids=[conn_id], host_ids=[host_id1, host_id2])

        call_args = mock_connection.wait_for_response.call_args
        query_msg = call_args[0][0]
        query_str = query_msg.query.lower()

        self.assertIn("where", query_str)
        self.assertIn("connection_id in", query_str)
        self.assertIn("host_id in", query_str)
        # When both filters present, should not use ALLOW FILTERING
        self.assertNotIn("allow filtering", query_str)


if __name__ == '__main__':
    unittest.main()
