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
import uuid
import time
from unittest.mock import Mock, patch
from collections import namedtuple

from cassandra.client_routes import (
    ClientRoutesEndpoint,
    ClientRoutesConfig,
    DNSResolver,
    ResolvedRoutes,
    ResolvedRoute,
    ClientRoutesHandler,
    ClientRoutesAddressTranslator,
    _parse_route_row
)
from cassandra import DriverException


class TestClientRoutesEndpoint(unittest.TestCase):

    def test_endpoint_with_uuid_string(self):
        conn_id = str(uuid.uuid4())
        endpoint = ClientRoutesEndpoint(conn_id, "10.0.0.1")
        self.assertIsInstance(endpoint.connection_id, uuid.UUID)
        self.assertEqual(endpoint.connection_addr, "10.0.0.1")

    def test_endpoint_with_uuid_object(self):
        conn_id = uuid.uuid4()
        endpoint = ClientRoutesEndpoint(conn_id)
        self.assertEqual(endpoint.connection_id, conn_id)
        self.assertIsNone(endpoint.connection_addr)

    def test_endpoint_invalid_uuid(self):
        with self.assertRaises(ValueError):
            ClientRoutesEndpoint("not-a-uuid")

    def test_endpoint_none_connection_id(self):
        with self.assertRaises(ValueError):
            ClientRoutesEndpoint(None)


class TestClientRoutesConfig(unittest.TestCase):

    def test_config_with_endpoints(self):
        ep1 = ClientRoutesEndpoint(uuid.uuid4(), "10.0.0.1")
        ep2 = ClientRoutesEndpoint(uuid.uuid4(), "10.0.0.2")
        config = ClientRoutesConfig([ep1, ep2])
        self.assertEqual(len(config.endpoints), 2)
        self.assertEqual(config.table_name, "system.client_routes")

    def test_config_custom_table_name(self):
        ep = ClientRoutesEndpoint(uuid.uuid4())
        config = ClientRoutesConfig([ep], table_name="custom.routes")
        self.assertEqual(config.table_name, "custom.routes")

    def test_config_empty_endpoints(self):
        with self.assertRaises(ValueError):
            ClientRoutesConfig([])

    def test_config_invalid_endpoint_type(self):
        with self.assertRaises(TypeError):
            ClientRoutesConfig(["not-an-endpoint"])

    def test_config_max_resolver_concurrency_validation(self):
        ep = ClientRoutesEndpoint(uuid.uuid4())
        # Must be > 0
        with self.assertRaises(ValueError):
            ClientRoutesConfig([ep], max_resolver_concurrency=0)
        with self.assertRaises(ValueError):
            ClientRoutesConfig([ep], max_resolver_concurrency=-1)

    def test_config_resolve_period_validation(self):
        ep = ClientRoutesEndpoint(uuid.uuid4())
        # Must be >= 0
        with self.assertRaises(ValueError):
            ClientRoutesConfig([ep], resolve_healthy_endpoint_period_seconds=-1)
        # 0 is valid (never re-resolve healthy)
        config = ClientRoutesConfig([ep], resolve_healthy_endpoint_period_seconds=0)
        self.assertEqual(config.resolve_healthy_endpoint_period_seconds, 0)


class TestDNSResolver(unittest.TestCase):

    @patch('cassandra.client_routes.socket.getaddrinfo')
    def test_resolve_success(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (None, None, None, None, ('192.168.1.1', 9042)),
            (None, None, None, None, ('192.168.1.2', 9042))
        ]

        resolver = DNSResolver()
        all_ips, current_ip, timestamp = resolver.resolve("example.com")

        self.assertIsNotNone(all_ips)
        self.assertEqual(len(all_ips), 2)
        self.assertIn(current_ip, all_ips)
        self.assertIsNotNone(timestamp)

    @patch('cassandra.client_routes.socket.getaddrinfo')
    def test_resolve_uses_cache(self, mock_getaddrinfo):
        mock_getaddrinfo.return_value = [
            (None, None, None, None, ('192.168.1.1', 9042))
        ]

        resolver = DNSResolver(cache_duration_ms=5000)

        # First resolution
        all_ips1, current_ip1, ts1 = resolver.resolve("example.com")

        # Second resolution with cache (should return cached result)
        all_ips2, current_ip2, ts2 = resolver.resolve("example.com", all_ips1, current_ip1, ts1)

        self.assertEqual(all_ips1, all_ips2)
        self.assertEqual(current_ip1, current_ip2)
        self.assertEqual(ts1, ts2)
        # getaddrinfo should only be called once
        self.assertEqual(mock_getaddrinfo.call_count, 1)

    @patch('cassandra.client_routes.socket.getaddrinfo')
    def test_resolve_failure_returns_cached(self, mock_getaddrinfo):
        mock_getaddrinfo.side_effect = Exception("DNS failure")

        resolver = DNSResolver()
        cached_ips = ["192.168.1.100"]
        cached_ip = "192.168.1.100"
        cached_at = 12345.0

        all_ips, current_ip, timestamp = resolver.resolve("example.com", cached_ips, cached_ip, cached_at)

        # Should return cached values on failure
        self.assertEqual(all_ips, cached_ips)
        self.assertEqual(current_ip, cached_ip)
        self.assertEqual(timestamp, cached_at)


class TestResolvedRoutes(unittest.TestCase):

    def test_get_by_host_id(self):
        routes = ResolvedRoutes()
        host_id = uuid.uuid4()
        route = ResolvedRoute(
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
        self.assertEqual(retrieved.current_ip, "192.168.1.1")

    def test_merge_routes(self):
        routes = ResolvedRoutes()
        host_id1 = uuid.uuid4()
        host_id2 = uuid.uuid4()

        route1 = ResolvedRoute(
            connection_id=uuid.uuid4(), host_id=host_id1,
            address="host1.com", port=9042, tls_port=None,
            datacenter="dc1", rack="rack1",
            all_known_ips=["192.168.1.1"], current_ip="192.168.1.1",
            update_time=12345.0, forced_resolve=False
        )

        route2 = ResolvedRoute(
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
        self.endpoint = ClientRoutesEndpoint(self.conn_id, "10.0.0.1")
        self.config = ClientRoutesConfig([self.endpoint])

    def test_handler_initialization(self):
        handler = ClientRoutesHandler(self.config, ssl_enabled=False)
        self.assertIsNotNone(handler)
        self.assertEqual(handler.ssl_enabled, False)

    @patch.object(ClientRoutesHandler, '_query_routes')
    @patch.object(ClientRoutesHandler, '_resolve_and_update_in_place')
    def test_initialize(self, mock_resolve, mock_query):
        host_id = uuid.uuid4()
        mock_query.return_value = [
            ResolvedRoute(
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

        handler = ClientRoutesHandler(self.config)
        mock_control_conn = Mock()

        handler.initialize(mock_control_conn)

        # Verify route was stored
        route = handler._routes.get_by_host_id(host_id)
        self.assertIsNotNone(route)
        mock_resolve.assert_called_once()

    def test_translate_address_no_host(self):
        handler = ClientRoutesHandler(self.config)
        addr = "192.168.1.1"

        # Should return unchanged when host is None
        result = handler.translate_address(addr, None)
        self.assertEqual(result, addr)

    def test_translate_address_not_found(self):
        handler = ClientRoutesHandler(self.config)

        class MockHost:
            def __init__(self):
                self.host_id = uuid.uuid4()

        mock_host = MockHost()

        with self.assertRaises(DriverException) as cm:
            handler.translate_address("192.168.1.1", mock_host)

        self.assertIn("No route found", str(cm.exception))

class TestClientRoutesAddressTranslator(unittest.TestCase):

    def test_translate_v1_returns_unchanged(self):
        mock_handler = Mock()
        translator = ClientRoutesAddressTranslator(mock_handler)

        addr = "192.168.1.1"
        result = translator.translate(addr)

        # V1 API should return unchanged (can't translate without host_id)
        self.assertEqual(result, addr)

    def test_translate_with_host_success(self):
        mock_handler = Mock()
        mock_handler.translate_address.return_value = "10.0.0.1"

        translator = ClientRoutesAddressTranslator(mock_handler)

        result = translator.translate_with_host_id("192.168.1.1", uuid.uuid4())

        self.assertEqual(result, "10.0.0.1")
        mock_handler.translate_address.assert_called_once()


class TestParseRouteRow(unittest.TestCase):

    def test_parse_complete_row(self):
        RowType = namedtuple('RowType', [
            'connection_id', 'host_id', 'address', 'port',
            'tls_port', 'datacenter', 'rack'
        ])

        conn_id = uuid.uuid4()
        host_id = uuid.uuid4()

        row = {
            'connection_id': conn_id,
            'host_id': host_id,
            'address': "node1.example.com",
            'port': 9042,
            'tls_port': 9142,
            'datacenter': "dc1",
            'rack': "rack1"
        }

        route = _parse_route_row(row)

        self.assertEqual(route.connection_id, conn_id)
        self.assertEqual(route.host_id, host_id)
        self.assertEqual(route.address, "node1.example.com")
        self.assertEqual(route.port, 9042)
        self.assertEqual(route.tls_port, 9142)
        self.assertEqual(route.datacenter, "dc1")
        self.assertEqual(route.rack, "rack1")
        self.assertIsNone(route.all_known_ips)
        self.assertIsNone(route.current_ip)
        self.assertIsNone(route.update_time)
        self.assertTrue(route.forced_resolve)  # Should be True initially


class TestResolvedRouteMergeLogic(unittest.TestCase):

    def test_merge_with_unresolved_unchanged_record(self):
        """Test that unchanged records don't get forcedResolve flag set."""
        routes = ResolvedRoutes()
        conn_id = uuid.uuid4()
        host_id = uuid.uuid4()

        # Initial resolved route
        route = ResolvedRoute(
            connection_id=conn_id,
            host_id=host_id,
            address="a1",
            port=9042,
            tls_port=None,
            datacenter=None,
            rack=None,
            all_known_ips=["10.0.0.1"],
            current_ip="10.0.0.1",
            update_time=time.time(),
            forced_resolve=False
        )
        routes.update([route])

        # Merge with identical unresolved route
        unresolved = [ResolvedRoute(
            connection_id=conn_id,
            host_id=host_id,
            address="a1",
            port=9042,
            tls_port=None,
            datacenter=None,
            rack=None,
            all_known_ips=None,
            current_ip=None,
            update_time=None,
            forced_resolve=True
        )]
        routes.merge_with_unresolved(unresolved)

        # Should remain unchanged (forced_resolve stays False)
        result = routes.get_by_host_id(host_id)
        self.assertFalse(result.forced_resolve)

    def test_merge_with_unresolved_changed_record(self):
        """Test that changed records get forcedResolve flag set."""
        routes = ResolvedRoutes()
        conn_id = uuid.uuid4()
        host_id = uuid.uuid4()

        # Initial resolved route
        route = ResolvedRoute(
            connection_id=conn_id,
            host_id=host_id,
            address="a1",
            port=9042,
            tls_port=None,
            datacenter=None,
            rack=None,
            all_known_ips=["10.0.0.1"],
            current_ip="10.0.0.1",
            update_time=time.time(),
            forced_resolve=False
        )
        routes.update([route])

        # Merge with changed unresolved route
        unresolved = [ResolvedRoute(
            connection_id=conn_id,
            host_id=host_id,
            address="a2",  # Changed
            port=9043,     # Changed
            tls_port=None,
            datacenter=None,
            rack=None,
            all_known_ips=None,
            current_ip=None,
            update_time=None,
            forced_resolve=True
        )]
        routes.merge_with_unresolved(unresolved)

        # Should be updated with forcedResolve=True
        result = routes.get_by_host_id(host_id)
        self.assertEqual(result.address, "a2")
        self.assertEqual(result.port, 9043)
        self.assertTrue(result.forced_resolve)

    def test_merge_with_unresolved_new_record(self):
        """Test that new records are added with forcedResolve=True."""
        routes = ResolvedRoutes()
        conn_id = uuid.uuid4()
        host_id = uuid.uuid4()

        # Merge with new unresolved route
        unresolved = [ResolvedRoute(
            connection_id=conn_id,
            host_id=host_id,
            address="a3",
            port=9044,
            tls_port=None,
            datacenter=None,
            rack=None,
            all_known_ips=None,
            current_ip=None,
            update_time=None,
            forced_resolve=True
        )]
        routes.merge_with_unresolved(unresolved)

        # Should be added with forcedResolve=True
        result = routes.get_by_host_id(host_id)
        self.assertIsNotNone(result)
        self.assertTrue(result.forced_resolve)


class TestQueryBuilding(unittest.TestCase):

    @patch.object(ClientRoutesHandler, '_resolve_and_update_in_place')
    def test_query_all_routes(self, mock_resolve):
        """Test querying all routes without filters."""
        conn_id = uuid.uuid4()
        config = ClientRoutesConfig([ClientRoutesEndpoint(conn_id, "10.0.0.1")])
        handler = ClientRoutesHandler(config)

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

    @patch.object(ClientRoutesHandler, '_resolve_and_update_in_place')
    def test_query_with_connection_ids_only(self, mock_resolve):
        """Test querying with connection_ids filter only."""
        conn_id1 = uuid.uuid4()
        conn_id2 = uuid.uuid4()
        config = ClientRoutesConfig([ClientRoutesEndpoint(conn_id1, "10.0.0.1")])
        handler = ClientRoutesHandler(config)

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

    @patch.object(ClientRoutesHandler, '_resolve_and_update_in_place')
    def test_query_with_host_ids_only(self, mock_resolve):
        """Test querying with host_ids filter only."""
        conn_id = uuid.uuid4()
        host_id = uuid.uuid4()
        config = ClientRoutesConfig([ClientRoutesEndpoint(conn_id, "10.0.0.1")])
        handler = ClientRoutesHandler(config)

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

    @patch.object(ClientRoutesHandler, '_resolve_and_update_in_place')
    def test_query_with_both_filters(self, mock_resolve):
        """Test querying with both connection_ids and host_ids filters."""
        conn_id = uuid.uuid4()
        host_id1 = uuid.uuid4()
        host_id2 = uuid.uuid4()
        config = ClientRoutesConfig([ClientRoutesEndpoint(conn_id, "10.0.0.1")])
        handler = ClientRoutesHandler(config)

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
