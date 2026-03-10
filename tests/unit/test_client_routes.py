# Copyright 2026 ScyllaDB, Inc.
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

import socket
import ssl
import unittest
import uuid
from unittest.mock import Mock, patch

from cassandra.client_routes import (
    ClientRouteProxy,
    ClientRoutesChangeType,
    ClientRoutesConfig,
    _RouteStore,
    _Route,
    _ClientRoutesHandler
)
from cassandra.connection import ClientRoutesEndPoint, ClientRoutesEndPointFactory
from cassandra.cluster import Cluster


class TestClientRouteProxy(unittest.TestCase):

    def test_endpoint_none_connection_id(self):
        with self.assertRaises(ValueError):
            ClientRouteProxy(None)


class TestClientRoutesConfig(unittest.TestCase):

    def test_config_with_proxies(self):
        ep1 = ClientRouteProxy(str(uuid.uuid4()), "10.0.0.1")
        ep2 = ClientRouteProxy(str(uuid.uuid4()), "10.0.0.2")
        config = ClientRoutesConfig([ep1, ep2])
        self.assertEqual(len(config.proxies), 2)

    def test_config_empty_proxies(self):
        with self.assertRaises(ValueError):
            ClientRoutesConfig([])

    def test_config_invalid_proxy_type(self):
        with self.assertRaises(TypeError):
            ClientRoutesConfig(["not-a-proxy"])



class TestRouteStore(unittest.TestCase):

    def test_get_by_host_id(self):
        routes = _RouteStore()
        host_id = uuid.uuid4()
        route = _Route(
            connection_id=str(uuid.uuid4()),
            host_id=host_id,
            address="example.com",
            port=9042,
        )

        routes.update([route])

        retrieved = routes.get_by_host_id(host_id)
        self.assertEqual(retrieved.host_id, host_id)
        self.assertEqual(retrieved.address, "example.com")

    def test_merge_routes(self):
        routes = _RouteStore()
        host_id1 = uuid.uuid4()
        host_id2 = uuid.uuid4()

        route1 = _Route(
            connection_id=str(uuid.uuid4()), host_id=host_id1,
            address="host1.com", port=9042,
        )

        route2 = _Route(
            connection_id=str(uuid.uuid4()), host_id=host_id2,
            address="host2.com", port=9042,
        )

        routes.update([route1])
        routes.merge([route2], affected_host_ids={host_id2})

        self.assertIsNotNone(routes.get_by_host_id(host_id1))
        self.assertIsNotNone(routes.get_by_host_id(host_id2))

    def test_merge_deletes_affected_host_with_no_new_route(self):
        """When an affected host_id has no corresponding new route, it should be removed."""
        store = _RouteStore()
        host_id1 = uuid.uuid4()
        host_id2 = uuid.uuid4()
        conn_id = str(uuid.uuid4())

        store.update([
            _Route(connection_id=conn_id, host_id=host_id1, address="a.com", port=9042),
            _Route(connection_id=conn_id, host_id=host_id2, address="b.com", port=9042),
        ])
        self.assertIsNotNone(store.get_by_host_id(host_id1))
        self.assertIsNotNone(store.get_by_host_id(host_id2))

        # Merge with host_id2 affected but no new route for it → deletion
        store.merge([], affected_host_ids={host_id2})

        self.assertIsNotNone(store.get_by_host_id(host_id1))
        self.assertIsNone(store.get_by_host_id(host_id2))

    def test_select_preferred_routes_keeps_existing_connection_id(self):
        """When multiple connection_ids provide routes for the same host_id,
        the one already in use should be preferred."""
        store = _RouteStore()
        host_id = uuid.uuid4()
        conn_a = "conn-a"
        conn_b = "conn-b"

        # Populate store with conn_a for host_id
        store.update([_Route(connection_id=conn_a, host_id=host_id, address="a.com", port=9042)])
        self.assertEqual(store.get_by_host_id(host_id).connection_id, conn_a)

        # Update with both conn_a and conn_b for the same host_id
        store.update([
            _Route(connection_id=conn_b, host_id=host_id, address="b.com", port=9042),
            _Route(connection_id=conn_a, host_id=host_id, address="a-new.com", port=9042),
        ])
        # conn_a should be preferred since it was already in use
        result = store.get_by_host_id(host_id)
        self.assertEqual(result.connection_id, conn_a)
        self.assertEqual(result.address, "a-new.com")

    def test_select_preferred_routes_falls_back_when_existing_gone(self):
        """When the existing connection_id is no longer among candidates,
        the first candidate should be selected."""
        store = _RouteStore()
        host_id = uuid.uuid4()

        store.update([_Route(connection_id="old-conn", host_id=host_id, address="old.com", port=9042)])

        # Update only has new connection_ids
        store.update([
            _Route(connection_id="new-a", host_id=host_id, address="a.com", port=9042),
            _Route(connection_id="new-b", host_id=host_id, address="b.com", port=9042),
        ])
        result = store.get_by_host_id(host_id)
        self.assertEqual(result.connection_id, "new-a")


class TestClientRoutesHandler(unittest.TestCase):

    def setUp(self):
        self.conn_id = uuid.uuid4()
        self.proxy = ClientRouteProxy(str(self.conn_id), "10.0.0.1")
        self.config = ClientRoutesConfig([self.proxy])

    def test_handler_initialization(self):
        handler = _ClientRoutesHandler(self.config, ssl_enabled=False)
        self.assertIsNotNone(handler)
        self.assertEqual(handler.ssl_enabled, False)

    @patch.object(_ClientRoutesHandler, '_query_all_routes_for_connections')
    def test_initialize(self, mock_query):
        host_id = uuid.uuid4()
        mock_query.return_value = [
            _Route(
                connection_id=self.conn_id,
                host_id=host_id,
                address="node1.example.com",
                port=9042,
            )
        ]

        handler = _ClientRoutesHandler(self.config)
        mock_conn = Mock()

        handler.initialize(mock_conn, timeout=5.0)

        mock_query.assert_called_once()
        route = handler._routes.get_by_host_id(host_id)
        self.assertIsNotNone(route)
        self.assertEqual(route.address, "node1.example.com")

    @patch.object(_ClientRoutesHandler, '_query_routes_for_change_event')
    def test_handle_change_filters_by_configured_connection_ids(self, mock_query):
        """Events with unrelated connection_ids should be ignored."""
        handler = _ClientRoutesHandler(self.config)
        mock_conn = Mock()
        host_id = str(uuid.uuid4())

        # Event with a connection_id NOT in our config → should return early
        handler.handle_client_routes_change(
            mock_conn, 5.0,
            ClientRoutesChangeType.UPDATE_NODES,
            connection_ids=["unrelated-conn-id"],
            host_ids=[host_id],
        )
        mock_query.assert_not_called()

    @patch.object(_ClientRoutesHandler, '_query_routes_for_change_event')
    def test_handle_change_merges_when_host_ids_present(self, mock_query):
        """When host_ids are provided, routes should be merged (not full replace)."""
        handler = _ClientRoutesHandler(self.config)
        mock_conn = Mock()

        existing_host = uuid.uuid4()
        new_host = uuid.uuid4()
        conn_id = str(self.conn_id)

        # Pre-populate a route
        handler._routes.update([
            _Route(connection_id=conn_id, host_id=existing_host, address="old.com", port=9042),
        ])

        mock_query.return_value = [
            _Route(connection_id=conn_id, host_id=new_host, address="new.com", port=9042),
        ]

        handler.handle_client_routes_change(
            mock_conn, 5.0,
            ClientRoutesChangeType.UPDATE_NODES,
            connection_ids=[conn_id],
            host_ids=[str(new_host)],
        )

        # Existing route should still be there (merge, not replace)
        self.assertIsNotNone(handler._routes.get_by_host_id(existing_host))
        self.assertIsNotNone(handler._routes.get_by_host_id(new_host))

    @patch.object(_ClientRoutesHandler, '_query_all_routes_for_connections')
    def test_handle_change_updates_when_no_host_ids(self, mock_query):
        """When no host_ids are provided, routes should be fully replaced."""
        handler = _ClientRoutesHandler(self.config)
        mock_conn = Mock()
        conn_id = str(self.conn_id)

        old_host = uuid.uuid4()
        handler._routes.update([
            _Route(connection_id=conn_id, host_id=old_host, address="old.com", port=9042),
        ])

        new_host = uuid.uuid4()
        mock_query.return_value = [
            _Route(connection_id=conn_id, host_id=new_host, address="new.com", port=9042),
        ]

        handler.handle_client_routes_change(
            mock_conn, 5.0,
            ClientRoutesChangeType.UPDATE_NODES,
            connection_ids=None,
            host_ids=None,
        )

        # Full replace: old_host gone, new_host present
        self.assertIsNone(handler._routes.get_by_host_id(old_host))
        self.assertIsNotNone(handler._routes.get_by_host_id(new_host))

    @patch.object(_ClientRoutesHandler, '_query_routes_for_change_event')
    def test_handle_change_propagates_query_failure(self, mock_query):
        """If _query_routes raises, handle_client_routes_change should propagate."""
        handler = _ClientRoutesHandler(self.config)
        mock_conn = Mock()
        mock_query.side_effect = Exception("network error")

        conn_id = self.proxy.connection_id
        host_id = str(uuid.uuid4())
        with self.assertRaises(Exception) as cm:
            handler.handle_client_routes_change(
                mock_conn, 5.0,
                ClientRoutesChangeType.UPDATE_NODES,
                connection_ids=[conn_id],
                host_ids=[host_id],
            )
        self.assertIn("network error", str(cm.exception))

    @patch.object(_ClientRoutesHandler, '_query_all_routes_for_connections')
    def test_initialize_propagates_exception_on_failure(self, mock_query):
        """initialize should propagate exceptions to caller."""
        handler = _ClientRoutesHandler(self.config)
        mock_conn = Mock()
        mock_query.side_effect = Exception("query failed")

        with self.assertRaises(Exception) as ctx:
            handler.initialize(mock_conn, 5.0)
        self.assertIn("query failed", str(ctx.exception))
        self.assertEqual(mock_query.call_count, 1)

    @patch.object(_ClientRoutesHandler, '_query_all_routes_for_connections')
    def test_initialize_keeps_old_routes_on_failure(self, mock_query):
        """On failure, existing routes must be preserved (critical for PL clusters)."""
        handler = _ClientRoutesHandler(self.config)
        mock_conn = Mock()
        host_id = uuid.uuid4()

        # Pre-populate a route
        handler._routes.update([
            _Route(connection_id=str(self.conn_id), host_id=host_id, address="old.com", port=9042),
        ])

        mock_query.side_effect = Exception("query failed")
        with self.assertRaises(Exception):
            handler.initialize(mock_conn, 5.0)

        # Old route must still be there
        self.assertIsNotNone(handler._routes.get_by_host_id(host_id))

    @patch.object(_ClientRoutesHandler, '_query_all_routes_for_connections')
    def test_initialize_updates_routes_on_success(self, mock_query):
        """initialize should update routes on success."""
        handler = _ClientRoutesHandler(self.config)
        mock_conn = Mock()
        host_id = uuid.uuid4()

        mock_query.return_value = [
            _Route(connection_id=str(self.conn_id), host_id=host_id, address="new.com", port=9042),
        ]

        handler.initialize(mock_conn, 5.0)

        self.assertEqual(mock_query.call_count, 1)
        route = handler._routes.get_by_host_id(host_id)
        self.assertIsNotNone(route)
        self.assertEqual(route.address, "new.com")

class TestClientRoutesEndPoint(unittest.TestCase):

    def setUp(self):
        self.conn_id = uuid.uuid4()
        self.proxy = ClientRouteProxy(str(self.conn_id), "10.0.0.1")
        self.config = ClientRoutesConfig([self.proxy])
        self.handler = _ClientRoutesHandler(self.config, ssl_enabled=False)

    def test_resolve_falls_back_when_no_mapping(self):
        """resolve() should return original address/port when no route mapping exists."""
        host_id = uuid.uuid4()
        ep = ClientRoutesEndPoint(
            host_id=host_id,
            handler=self.handler,
            original_address="10.0.0.1",
            original_port=9042,
        )
        self.assertEqual(ep.resolve(), ("10.0.0.1", 9042))

    @patch('cassandra.client_routes.socket.getaddrinfo',
           return_value=[(socket.AF_INET, socket.SOCK_STREAM, 0, '', ("192.168.1.100", 9042))])
    def test_resolve_returns_address_when_route_exists(self, _mock_getaddrinfo):
        """resolve() should return the DNS-resolved address and port when a route exists."""
        host_id = uuid.uuid4()
        self.handler._routes.update([
            _Route(connection_id=str(self.conn_id), host_id=host_id,
                   address="nlb.example.com", port=9042),
        ])
        ep = ClientRoutesEndPoint(
            host_id=host_id,
            handler=self.handler,
            original_address="10.0.0.1",
            original_port=9042,
        )
        self.assertEqual(ep.resolve(), ("192.168.1.100", 9042))
        _mock_getaddrinfo.assert_called_once_with(
            "nlb.example.com", 9042, socket.AF_UNSPEC, socket.SOCK_STREAM)

    @patch('cassandra.client_routes.socket.getaddrinfo',
           side_effect=socket.gaierror("DNS resolution failed"))
    def test_resolve_host_dns_failure_raises(self, _mock_getaddrinfo):
        """resolve_host should propagate socket.gaierror on DNS failure."""
        host_id = uuid.uuid4()
        self.handler._routes.update([
            _Route(connection_id=str(self.conn_id), host_id=host_id,
                   address="nonexistent.example.com", port=9042),
        ])
        with self.assertRaises(socket.gaierror):
            self.handler.resolve_host(host_id)

    def test_resolve_host_missing_port_raises(self):
        """resolve_host should raise ValueError when route has no port."""
        host_id = uuid.uuid4()
        self.handler._routes.update([
            _Route(connection_id=str(self.conn_id), host_id=host_id,
                   address="host.com", port=0),
        ])
        with self.assertRaises(ValueError):
            self.handler.resolve_host(host_id)


class TestClientRoutesEndPointFactory(unittest.TestCase):

    def setUp(self):
        self.conn_id = uuid.uuid4()
        proxy = ClientRouteProxy(str(self.conn_id), "10.0.0.1")
        self.config = ClientRoutesConfig([proxy])
        self.handler = _ClientRoutesHandler(self.config, ssl_enabled=False)
        self.factory = ClientRoutesEndPointFactory(self.handler, default_port=9042)

    def test_create_from_row(self):
        """Factory should create a ClientRoutesEndPoint from a peers row."""
        host_id = uuid.uuid4()
        row = {
            "host_id": host_id,
            "rpc_address": "10.0.0.5",
            "native_transport_port": 9042,
            "peer": "10.0.0.5",
        }
        ep = self.factory.create(row)
        self.assertIsInstance(ep, ClientRoutesEndPoint)
        self.assertEqual(ep.host_id, host_id)
        self.assertEqual(ep.address, "10.0.0.5")

    def test_create_missing_host_id_raises(self):
        """Factory should raise ValueError when row has no host_id."""
        row = {"rpc_address": "10.0.0.5", "native_transport_port": 9042}
        with self.assertRaises(ValueError):
            self.factory.create(row)

class TestClientRoutesSSLValidation(unittest.TestCase):

    def test_check_hostname_with_ssl_context_raises(self):
        """Cluster should reject check_hostname=True with client_routes_config."""
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self.assertTrue(ssl_ctx.check_hostname)

        config = ClientRoutesConfig(
            proxies=[ClientRouteProxy(str(uuid.uuid4()), "10.0.0.1")]
        )
        with self.assertRaises(ValueError) as cm:
            Cluster(
                contact_points=["10.0.0.1"],
                ssl_context=ssl_ctx,
                client_routes_config=config,
            )
        self.assertIn("check_hostname", str(cm.exception))

    def test_check_hostname_with_ssl_options_raises(self):
        """Cluster should reject check_hostname=True in ssl_options with client_routes_config."""
        config = ClientRoutesConfig(
            proxies=[ClientRouteProxy(str(uuid.uuid4()), "10.0.0.1")]
        )
        with self.assertRaises(ValueError) as cm:
            Cluster(
                contact_points=["10.0.0.1"],
                ssl_options={'check_hostname': True},
                client_routes_config=config,
            )
        self.assertIn("check_hostname", str(cm.exception))

    def test_disabled_check_hostname_with_client_routes_ok(self):
        """Cluster should allow check_hostname=False with client_routes_config."""
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.check_hostname = False

        config = ClientRoutesConfig(
            proxies=[ClientRouteProxy(str(uuid.uuid4()), "10.0.0.1")]
        )
        # Should not raise
        cluster = Cluster(
            contact_points=["10.0.0.1"],
            ssl_context=ssl_ctx,
            client_routes_config=config,
        )
        cluster.shutdown()

    def test_no_ssl_with_client_routes_ok(self):
        """Cluster should allow client_routes_config without SSL."""
        config = ClientRoutesConfig(
            proxies=[ClientRouteProxy(str(uuid.uuid4()), "10.0.0.1")]
        )
        # Should not raise
        cluster = Cluster(
            contact_points=["10.0.0.1"],
            client_routes_config=config,
        )
        cluster.shutdown()


if __name__ == '__main__':
    unittest.main()
