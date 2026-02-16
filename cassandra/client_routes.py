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

"""
Client Routes support for Private Link and similar network configurations.

This module implements support for dynamic address translation via the
system.client_routes table and CLIENT_ROUTES_CHANGE events.
"""

from __future__ import absolute_import

from dataclasses import dataclass, replace
import logging
import socket
import threading
import time
import uuid
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Set, Tuple

from cassandra import ConsistencyLevel
from cassandra.protocol import QueryMessage
from cassandra.query import dict_factory

if TYPE_CHECKING:
    from cassandra.cluster import ControlConnection

log = logging.getLogger(__name__)


class ClientRoutesEndpoint:

    def __init__(self, connection_id: str, connection_addr: Optional[str] = None):
        """
        :param connection_id: UUID string identifying the connection
        :param connection_addr: Optional string address for initial connection
        """
        if connection_id is None:
            raise ValueError("connection_id is required")

        self.connection_id = connection_id
        self.connection_addr = connection_addr

    def __repr__(self) -> str:
        return f"ClientRoutesEndpoint(connection_id={self.connection_id}, connection_addr={self.connection_addr})"


class ClientRoutesConfig:
    """
    Configuration for client routes (Private Link support).

    :param endpoints: List of ClientRoutesEndpoint objects (REQUIRED, at least one)
    :param cache_ttl_seconds: How long DNS resolution results are cached (default: 300 seconds = 5 minutes)
    """

    table_name: str = "system.client_routes"

    def __init__(self, endpoints: List[ClientRoutesEndpoint],
                 cache_ttl_seconds: int = 300):
        """
        :param endpoints: List of ClientRoutesEndpoint objects
        :param cache_ttl_seconds: DNS cache TTL in seconds (must be >= 0, default: 300 = 5 minutes)
        """
        if not endpoints:
            raise ValueError("At least one endpoint must be specified")

        if not isinstance(endpoints, (list, tuple)):
            raise TypeError("endpoints must be a list or tuple")

        for endpoint in endpoints:
            if not isinstance(endpoint, ClientRoutesEndpoint):
                raise TypeError("All endpoints must be ClientRoutesEndpoint instances")

        if cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds must be >= 0")

        self.endpoints = list(endpoints)
        self.cache_ttl_seconds = cache_ttl_seconds

    def __repr__(self) -> str:
        return f"ClientRoutesConfig(endpoints={self.endpoints}, table_name={self.table_name})"


# Internal data structures

@dataclass
class _ResolvedRoute:
    connection_id: str
    host_id: uuid.UUID
    address: str  # ipv4, ipv6 or DNS hostname from system.client_routes
    port: int
    tls_port: Optional[int]
    datacenter: Optional[str]
    rack: Optional[str]
    all_known_ips: Optional[List[str]]  # List of all resolved IP addresses
    current_ip: Optional[str]           # Currently selected IP address
    update_time: Optional[float]        # Timestamp of last resolution
    forced_resolve: bool                # Flag to force resolution on next cycle

    # Compatibility helper for existing namedtuple-style updates in the file
    def _replace(self, **changes) -> '_ResolvedRoute':
        return replace(self, **changes)

class _ResolvedRoutes:
    """
    Thread-safe storage for resolved routes using lock-free reads.

    This uses atomic pointer swaps for updates, allowing lock-free reads
    while serializing writes.
    """

    def __init__(self) -> None:
        self._routes_by_host_id: Dict[uuid.UUID, _ResolvedRoute] = {}  # Dict[UUID, _ResolvedRoute]
        self._lock = threading.RLock()

    def get_by_host_id(self, host_id: uuid.UUID) -> Optional[_ResolvedRoute]:
        """
        Get route for a host ID (lock-free read).

        :param host_id: UUID of the host
        :return: _ResolvedRoute or None
        """
        return self._routes_by_host_id.get(host_id)

    def get_all(self) -> List[_ResolvedRoute]:
        """
        Get all routes as a list (lock-free read).

        :return: List of _ResolvedRoute
        """
        return list(self._routes_by_host_id.values())

    def update(self, routes: List[_ResolvedRoute]) -> None:
        """
        Replace all routes atomically.

        :param routes: List of _ResolvedRoute objects
        """
        with self._lock:
            self._routes_by_host_id = {route.host_id: route for route in routes}

    def merge(self, new_routes: List[_ResolvedRoute]) -> None:
        """
        Merge new routes with existing ones atomically.

        :param new_routes: List of _ResolvedRoute objects to merge
        """
        with self._lock:
            updated = dict(self._routes_by_host_id)
            for route in new_routes:
                updated[route.host_id] = route
            self._routes_by_host_id = updated

    def merge_with_unresolved(self, new_routes: List[_ResolvedRoute]) -> None:
        """
        Merge unresolved routes, marking changed ones for forced resolution.

        :param new_routes: List of _ResolvedRoute objects from system.client_routes
        """
        with self._lock:
            updated = dict(self._routes_by_host_id)

            for new_route in new_routes:
                key = new_route.host_id
                existing = updated.get(key)

                if existing is None:
                    # New route, add with forced_resolve=True
                    updated[key] = new_route._replace(forced_resolve=True)
                else:
                    # Check if route details changed (address, port, tls_port)
                    if (existing.connection_id != new_route.connection_id or
                        existing.address != new_route.address or
                        existing.port != new_route.port or
                        existing.tls_port != new_route.tls_port):
                        # Route changed, mark for forced resolution
                        updated[key] = new_route._replace(forced_resolve=True)
                    # Otherwise keep existing route with its resolution state

            self._routes_by_host_id = updated

    def update_single(self, host_id: uuid.UUID, update_fn: Callable[[_ResolvedRoute], _ResolvedRoute]) -> Optional[_ResolvedRoute]:
        """
        Update a single route using CAS (compare-and-swap) pattern.

        :param host_id: UUID of the host to update
        :param update_fn: Function that takes existing route and returns updated route
        :return: Updated _ResolvedRoute or None if not found
        """
        with self._lock:
            existing = self._routes_by_host_id.get(host_id)
            if existing:
                updated = update_fn(existing)
                self._routes_by_host_id[host_id] = updated
                return updated
            return None



class _ClientRoutesHandler:
    """
    Handles dynamic address translation for Private Link via system.client_routes.

    Lifecycle:
    1. Construction: Create with configuration
    2. Initialization: Read system.client_routes after control connection established
    3. Steady state: Listen for CLIENT_ROUTES_CHANGE events and update routes
    4. Translation: Translate addresses using Host ID lookup
    5. Shutdown: Clean up resources
    """

    def __init__(self, config: 'ClientRoutesConfig', ssl_enabled: bool = False):
        """
        :param config: ClientRoutesConfig instance
        :param ssl_enabled: Whether TLS is enabled (determines port selection)
        """
        if not isinstance(config, ClientRoutesConfig):
            raise TypeError("config must be a ClientRoutesConfig instance")

        self.config: ClientRoutesConfig = config
        self.ssl_enabled: bool = ssl_enabled
        self._routes: _ResolvedRoutes = _ResolvedRoutes()
        self._initial_endpoints: Set[str] = {ep.connection_id for ep in config.endpoints}
        self._is_shutdown: bool = False
        self._lock = threading.RLock()

    def initialize(self, control_connection: 'ControlConnection') -> None:
        """
        Initialize handler after control connection is established.

        Reads system.client_routes for all configured connection IDs.
        DNS resolution happens at the Endpoint level on each connection attempt.

        :param control_connection: The ControlConnection instance
        """
        if self._is_shutdown:
            return

        log.info("[client routes] Initializing with %d endpoints", len(self.config.endpoints))

        try:
            connection_ids = [ep.connection_id for ep in self.config.endpoints]
            routes = self._query_routes(control_connection, connection_ids=connection_ids)

            self._routes.merge_with_unresolved(routes)

            log.info("[client routes] Initialized with %d routes", len(self._routes.get_all()))
        except Exception as e:
            log.error("[client routes] Initialization failed: %s", e, exc_info=True)
            raise

    def handle_client_routes_change(self, control_connection: 'ControlConnection', change_type: str,
                                    connection_ids: Optional[List[str]], host_ids: Optional[List[str]]) -> None:
        """
        Handle CLIENT_ROUTES_CHANGE event.

        :param control_connection: The ControlConnection instance
        :param change_type: Type of change (e.g., "UPDATED")
        :param connection_ids: List of affected connection ID strings
        :param host_ids: List of affected host ID strings
        """
        if self._is_shutdown:
            return

        log.debug("[client routes] Handling CLIENT_ROUTES_CHANGE: change_type=%s, "
                  "connection_ids=%s, host_ids=%s",
                  change_type, connection_ids, host_ids)

        try:
            filtered_conn_ids = None
            if connection_ids:
                configured_ids = {str(ep.connection_id) for ep in self.config.endpoints}
                filtered = [cid for cid in connection_ids if cid in configured_ids]
                if not filtered:
                    log.debug("[client routes] All connection IDs filtered out, ignoring event")
                    return
                filtered_conn_ids = [uuid.UUID(cid) for cid in filtered]

            host_uuids = [uuid.UUID(hid) for hid in host_ids] if host_ids else None

            routes = self._query_routes(
                control_connection,
                connection_ids=filtered_conn_ids,
                host_ids=host_uuids
            )

            self._routes.merge_with_unresolved(routes)

            log.debug("[client routes] Updated routes after CLIENT_ROUTES_CHANGE")
        except Exception as e:
            log.warning("[client routes] Failed to handle CLIENT_ROUTES_CHANGE: %s", e, exc_info=True)

    def handle_control_connection_reconnect(self, control_connection: 'ControlConnection') -> None:
        """
        Handle control connection recreation - full re-read of all connection IDs.

        :param control_connection: The new ControlConnection instance
        """
        if self._is_shutdown:
            return

        log.info("[client routes] Control connection reconnected, re-reading all routes")

        try:
            self.initialize(control_connection)
        except Exception as e:
            log.error("[client routes] Failed to re-initialize after reconnect: %s", e, exc_info=True)

    def _query_routes(self, control_connection: 'ControlConnection', connection_ids: Optional[List[uuid.UUID]] = None,
                     host_ids: Optional[List[uuid.UUID]] = None) -> List[_ResolvedRoute]:
        """
        Query system.client_routes table.

        :param control_connection: ControlConnection to execute query
        :param connection_ids: Optional list of connection UUIDs to filter by
        :param host_ids: Optional list of host UUIDs to filter by
        :return: List of _ResolvedRoute (with resolved_ip/resolved_at as None)
        """
        query_parts = [f"SELECT * FROM {self.config.table_name}"]
        where_clauses = []

        if connection_ids:
            conn_id_list = ', '.join(str(cid) for cid in connection_ids)
            where_clauses.append(f"connection_id IN ({conn_id_list})")

        if host_ids:
            host_id_list = ', '.join(str(hid) for hid in host_ids)
            where_clauses.append(f"host_id IN ({host_id_list})")

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        if (not connection_ids or len(connection_ids) == 0) and (not host_ids or len(host_ids) == 0):
            query_parts.append("ALLOW FILTERING")
        query = " ".join(query_parts)

        log.debug("[client routes] Querying: %s", query)

        query_msg = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
        result = control_connection._connection.wait_for_response(
            query_msg, timeout=control_connection._timeout
        )

        routes = []
        if hasattr(result, 'parsed_rows') and result.parsed_rows:
            rows = dict_factory(
                                result.column_names,
                                result.parsed_rows)
            for row in rows:
                try:
                    routes.append(_ResolvedRoute(
                        connection_id=row['connection_id'],
                        host_id=row['host_id'],
                        address=row['address'],
                        port=row['port'],
                        tls_port=row.get('tls_port'),
                        datacenter=row.get('datacenter'),
                        rack=row.get('rack'),
                        all_known_ips=None,
                        current_ip=None,
                        update_time=None,
                        forced_resolve=True  # Force initial resolution
                    ))
                except Exception as e:
                    log.warning("[client routes] Failed to parse route row: %s", e)

        return routes

    def resolve_host(self, host_id: uuid.UUID, default_port: Optional[int] = None) -> Tuple[str, int]:
        """
        Resolve a host_id to an (ip, port) pair.

        Looks up the current route, selects the appropriate port,
        checks the DNS cache, and resolves if needed.

        :param host_id: Host UUID to resolve
        :param default_port: Fallback port if route doesn't specify one
        :return: Tuple of (ip_address, port)
        :raises Exception: If no route is found or DNS resolution fails
        """
        route = self._routes.get_by_host_id(host_id)
        if route is None:
            raise Exception(
                "No client route found for host_id=%s" % host_id
            )

        hostname = route.address

        port = route.port
        if self.ssl_enabled and route.tls_port:
            port = route.tls_port
        elif default_port:
            port = default_port

        current_time = time.time()
        cache_ttl = self.config.cache_ttl_seconds
        cache_valid = (
            route.all_known_ips is not None and
            len(route.all_known_ips) > 0 and
            route.update_time is not None and
            (current_time - route.update_time) < cache_ttl and
            not route.forced_resolve
        )

        if cache_valid:
            resolved_ip = route.current_ip if route.current_ip else route.all_known_ips[0]
            log.debug("[client routes] Using cached IP for host_id=%s -> %s (age: %.1fs)",
                      host_id, resolved_ip, current_time - route.update_time)
            return resolved_ip, port

        try:
            result = socket.getaddrinfo(hostname, port,
                                        socket.AF_INET, socket.SOCK_STREAM)
            if not result:
                raise socket.gaierror("No addresses found for %s" % hostname)

            all_ips = [addr[4][0] for addr in result]
            resolved_ip = all_ips[0]

            def update_route(existing_route):
                return existing_route._replace(
                    all_known_ips=all_ips,
                    current_ip=resolved_ip,
                    update_time=current_time,
                    forced_resolve=False
                )

            self._routes.update_single(host_id, update_route)

            log.debug("[client routes] Resolved host_id=%s -> %s -> %s (%d IPs)",
                      host_id, hostname, resolved_ip, len(all_ips))
            return resolved_ip, port
        except socket.gaierror as e:
            log.warning('[client routes] Could not resolve hostname "%s" (host_id=%s): %s',
                        hostname, host_id, e)
            raise

    def shutdown(self) -> None:
        """
        Shutdown the handler and release resources.
        """
        with self._lock:
            if self._is_shutdown:
                return

            self._is_shutdown = True
            log.info("[client routes] Handler shutdown")
