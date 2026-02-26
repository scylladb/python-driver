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

"""
Client Routes support for Private Link and similar network configurations.

This module implements support for dynamic address translation via the
system.client_routes table and CLIENT_ROUTES_CHANGE events.
"""

from __future__ import absolute_import

import logging
import socket
import threading
import time
import uuid
from collections import namedtuple

from cassandra import DriverException
from cassandra.query import dict_factory

log = logging.getLogger(__name__)


class ClientRoutesEndpoint:

    def __init__(self, connection_id, connection_addr=None):
        """
        :param connection_id: UUID string or UUID object identifying the connection
        :param connection_addr: Optional string address for initial connection
        """
        if connection_id is None:
            raise ValueError("connection_id is required")

        if isinstance(connection_id, str):
            try:
                self.connection_id = uuid.UUID(connection_id)
            except ValueError:
                raise ValueError(f"Invalid UUID format for connection_id: {connection_id}")
        elif isinstance(connection_id, uuid.UUID):
            self.connection_id = connection_id
        else:
            raise TypeError("connection_id must be a UUID or string")

        self.connection_addr = connection_addr

    def __repr__(self):
        return f"ClientRoutesEndpoint(connection_id={self.connection_id}, connection_addr={self.connection_addr})"


class ClientRoutesConfig:
    """
    Configuration for client routes (Private Link support).

    :param endpoints: List of ClientRoutesEndpoint objects (REQUIRED, at least one)
    :param table_name: Name of the system table to query (default: "system.client_routes")
    """

    def __init__(self, endpoints, table_name="system.client_routes",
                 max_resolver_concurrency=1, resolve_healthy_endpoint_period_seconds=3600):
        """
        :param endpoints: List of ClientRoutesEndpoint objects
        :param table_name: System table name for route discovery
        :param max_resolver_concurrency: Maximum concurrent DNS resolutions (must be > 0, default: 1)
        :param resolve_healthy_endpoint_period_seconds: How often to re-resolve healthy endpoints in seconds (must be >= 0, default: 3600 = 1 hour)
        """
        if not endpoints:
            raise ValueError("At least one endpoint must be specified")

        if not isinstance(endpoints, (list, tuple)):
            raise TypeError("endpoints must be a list or tuple")

        for endpoint in endpoints:
            if not isinstance(endpoint, ClientRoutesEndpoint):
                raise TypeError("All endpoints must be ClientRoutesEndpoint instances")

        if max_resolver_concurrency <= 0:
            raise ValueError("max_resolver_concurrency must be > 0")

        if resolve_healthy_endpoint_period_seconds < 0:
            raise ValueError("resolve_healthy_endpoint_period_seconds must be >= 0")

        self.endpoints = list(endpoints)
        self.table_name = table_name
        self.max_resolver_concurrency = max_resolver_concurrency
        self.resolve_healthy_endpoint_period_seconds = resolve_healthy_endpoint_period_seconds

    def __repr__(self):
        return f"ClientRoutesConfig(endpoints={self.endpoints}, table_name={self.table_name})"


# Internal data structures

ResolvedRoute = namedtuple('ResolvedRoute', [
    'connection_id',
    'host_id',
    'address',  # DNS hostname from system.client_routes
    'port',
    'tls_port',
    'datacenter',
    'rack',
    'all_known_ips',  # List of all resolved IP addresses
    'current_ip',     # Currently selected IP address
    'update_time',    # Timestamp of last resolution
    'forced_resolve'  # Flag to force resolution on next cycle
])


class DNSResolver:
    """
    DNS resolver with caching to avoid spamming DNS servers.

    :param cache_duration_ms: How long to cache DNS resolutions (default: 500ms)
    """

    def __init__(self, cache_duration_ms=500):
        self.cache_duration_ms = cache_duration_ms

    def resolve(self, hostname, cached_ips=None, current_ip=None, cached_at=None):
        """
        Resolve a hostname to all IP addresses with caching.

        :param hostname: DNS hostname to resolve
        :param cached_ips: Previously resolved list of IPs (for cache check)
        :param current_ip: Previously selected current IP
        :param cached_at: Timestamp of cached resolution
        :return: Tuple of (all_ips, current_ip, timestamp) or (cached_ips, current_ip, cached_at) on failure
        """
        # Check if cached result is still valid
        if cached_ips is not None and cached_at is not None:
            age_ms = (time.time() - cached_at) * 1000
            if age_ms < self.cache_duration_ms:
                return (cached_ips, current_ip, cached_at)
        # Perform DNS resolution
        try:
            result = socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_STREAM)
            if result:
                # Extract all unique IPs
                all_ips = list(set([addr[4][0] for addr in result]))
                timestamp = time.time()

                # Select current IP: prefer existing if still in list, otherwise use first
                selected_ip = None
                if current_ip and current_ip in all_ips:
                    selected_ip = current_ip
                else:
                    selected_ip = all_ips[0] if all_ips else None

                log.debug("Resolved %s to %d IPs, selected: %s", hostname, len(all_ips), selected_ip)
                return (all_ips, selected_ip, timestamp)
            else:
                log.warning("No DNS results for %s", hostname)
                return (cached_ips, current_ip, cached_at) if cached_ips else (None, None, None)
        except (socket.gaierror, Exception) as e:
            log.warning("DNS resolution failed for %s: %s", hostname, e)
            # Return cached IPs as fallback (best-effort continuity)
            return (cached_ips, current_ip, cached_at) if cached_ips else (None, None, None)


class ResolvedRoutes:
    """
    Thread-safe storage for resolved routes using lock-free reads.

    This uses atomic pointer swaps for updates, allowing lock-free reads
    while serializing writes.
    """

    def __init__(self):
        self._routes_by_host_id = {}  # Dict[UUID, ResolvedRoute]
        self._lock = threading.RLock()

    def get_by_host_id(self, host_id):
        """
        Get route for a host ID (lock-free read).

        :param host_id: UUID of the host
        :return: ResolvedRoute or None
        """
        return self._routes_by_host_id.get(host_id)

    def get_all(self):
        """
        Get all routes as a list (lock-free read).

        :return: List of ResolvedRoute
        """
        return list(self._routes_by_host_id.values())

    def update(self, routes):
        """
        Replace all routes atomically.

        :param routes: List of ResolvedRoute objects
        """
        with self._lock:
            self._routes_by_host_id = {route.host_id: route for route in routes}

    def merge(self, new_routes):
        """
        Merge new routes with existing ones atomically.

        :param new_routes: List of ResolvedRoute objects to merge
        """
        with self._lock:
            updated = dict(self._routes_by_host_id)
            for route in new_routes:
                updated[route.host_id] = route
            self._routes_by_host_id = updated

    def merge_with_unresolved(self, new_routes):
        """
        Merge unresolved routes, marking changed ones for forced resolution.

        :param new_routes: List of ResolvedRoute objects from system.client_routes
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

    def update_single(self, host_id, update_fn):
        """
        Update a single route using CAS (compare-and-swap) pattern.

        :param host_id: UUID of the host to update
        :param update_fn: Function that takes existing route and returns updated route
        :return: Updated ResolvedRoute or None if not found
        """
        with self._lock:
            existing = self._routes_by_host_id.get(host_id)
            if existing:
                updated = update_fn(existing)
                self._routes_by_host_id[host_id] = updated
                return updated
            return None


def _parse_route_row(row):
    """
    Parse a row from system.client_routes into a ResolvedRoute.

    :param row: dict from system.client_routes query result
    :return: ResolvedRoute (with all_known_ips and current_ip as None initially)
    """
    return ResolvedRoute(
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
    )


class ClientRoutesHandler:
    """
    Handles dynamic address translation for Private Link via system.client_routes.

    Lifecycle:
    1. Construction: Create with configuration
    2. Initialization: Read system.client_routes after control connection established
    3. Steady state: Listen for CLIENT_ROUTES_CHANGE events and update routes
    4. Translation: Translate addresses using Host ID lookup
    5. Shutdown: Clean up resources
    """

    def __init__(self, config, ssl_enabled=False):
        """
        :param config: ClientRoutesConfig instance
        :param ssl_enabled: Whether TLS is enabled (determines port selection)
        """
        if not isinstance(config, ClientRoutesConfig):
            raise TypeError("config must be a ClientRoutesConfig instance")

        self.config = config
        self.ssl_enabled = ssl_enabled
        self._resolver = DNSResolver()
        self._routes = ResolvedRoutes()
        self._initial_endpoints = {ep.connection_id for ep in config.endpoints}
        self._is_shutdown = False
        self._lock = threading.RLock()

    def initialize(self, control_connection):
        """
        Initialize handler after control connection is established.

        Reads system.client_routes for all configured connection IDs and resolves DNS.
        This is a synchronous operation that blocks until complete.

        :param control_connection: The ControlConnection instance
        """
        if self._is_shutdown:
            return

        log.info("[client routes] Initializing with %d endpoints", len(self.config.endpoints))

        try:
            # Query all connection IDs
            connection_ids = [ep.connection_id for ep in self.config.endpoints]
            routes = self._query_routes(control_connection, connection_ids=connection_ids)

            # Merge unresolved routes and resolve
            self._routes.merge_with_unresolved(routes)
            self._resolve_and_update_in_place()

            log.info("[client routes] Initialized with %d routes", len(self._routes.get_all()))
        except Exception as e:
            log.error("[client routes] Initialization failed: %s", e, exc_info=True)
            raise

    def handle_client_routes_change(self, control_connection, change_type, connection_ids, host_ids):
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

            # Query affected routes
            routes = self._query_routes(
                control_connection,
                connection_ids=filtered_conn_ids,
                host_ids=host_uuids
            )

            # Merge and resolve
            self._routes.merge_with_unresolved(routes)
            self._resolve_and_update_in_place()

            log.debug("[client routes] Updated routes after CLIENT_ROUTES_CHANGE")
        except Exception as e:
            log.warning("[client routes] Failed to handle CLIENT_ROUTES_CHANGE: %s", e, exc_info=True)

    def handle_control_connection_reconnect(self, control_connection):
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

    def _query_routes(self, control_connection, connection_ids=None, host_ids=None):
        """
        Query system.client_routes table.

        :param control_connection: ControlConnection to execute query
        :param connection_ids: Optional list of connection UUIDs to filter by
        :param host_ids: Optional list of host UUIDs to filter by
        :return: List of ResolvedRoute (with resolved_ip/resolved_at as None)
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

        from cassandra.protocol import QueryMessage
        from cassandra import ConsistencyLevel

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
                    routes.append(_parse_route_row(row))
                except Exception as e:
                    log.warning("[client routes] Failed to parse route row: %s", e)

        return routes

    def _resolve_and_update_in_place(self):
        """
        Resolve routes that need resolution.

        Routes are resolved if:
        1. Marked with forced_resolve=True
        2. Have no IP address information
        3. Were resolved more than resolve_healthy_endpoint_period_seconds ago
        """
        all_routes = self._routes.get_all()
        if not all_routes:
            return

        # Calculate cutoff time for healthy re-resolution
        cutoff_time = None
        if self.config.resolve_healthy_endpoint_period_seconds == 0:
            cutoff_time = None  # Never re-resolve healthy endpoints
        else:
            cutoff_time = time.time() - self.config.resolve_healthy_endpoint_period_seconds

        # Identify routes that need resolution
        routes_to_resolve = []
        for route in all_routes:
            needs_resolve = False

            if route.current_ip is None or route.all_known_ips is None or route.forced_resolve:
                needs_resolve = True
            elif cutoff_time is not None and route.update_time is not None:
                if route.update_time < cutoff_time:
                    needs_resolve = True

            if needs_resolve:
                routes_to_resolve.append(route)

        if not routes_to_resolve:
            return

        # Resolve in parallel with concurrency limit
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=self.config.max_resolver_concurrency) as executor:
            futures = {}
            for route in routes_to_resolve:
                future = executor.submit(self._resolve_single_route, route)
                futures[future] = route

            # Wait for all resolutions and update
            for future in as_completed(futures):
                try:
                    resolved_route = future.result()
                    if resolved_route:
                        # Update single route atomically
                        self._routes.update_single(resolved_route.host_id, lambda _: resolved_route)
                except Exception as e:
                    route = futures[future]
                    log.warning("[client routes] Failed to resolve %s: %s", route.address, e)

    def _resolve_single_route(self, route):
        """
        Resolve a single route and return updated ResolvedRoute.
        """
        all_ips, current_ip, timestamp = self._resolver.resolve(
            route.address,
            route.all_known_ips,
            route.current_ip,
            route.update_time
        )

        if all_ips and current_ip:
            return route._replace(
                all_known_ips=all_ips,
                current_ip=current_ip,
                update_time=timestamp,
                forced_resolve=False
            )
        else:
            # Resolution failed, keep old values but update time
            return route._replace(
                update_time=time.time()
            )

    def translate_address(self, addr, host_id):
        """
        Translate an address using Host ID lookup.

        This is called per connection attempt. Key behaviors:
        - Initial endpoints (contact points) are NOT translated
        - Empty Host ID returns address unchanged (bootstrap placeholder)
        - Host ID not found returns error
        - Found route with resolved IP returns translated address
        - Found route without resolved IP performs on-demand DNS resolution with CAS retry

        :param addr: Original IP address
        :param host_id: Host ID
        :return: Translated IP address
        :raises: DriverException if translation fails
        """
        if self._is_shutdown:
            return addr

        if host_id is None:
            return addr

        if not host_id:
            return addr

        # Look up route by host ID
        route = self._routes.get_by_host_id(host_id)

        if route is None:
            raise DriverException(
                f"[client routes] No route found for host_id={host_id}, "
                f"addr={addr}. This may indicate configuration mismatch or "
                f"that CLIENT_ROUTES_CHANGE events are not being received."
            )

        # If already resolved, return it
        if route.current_ip:
            return route.current_ip

        # On-demand DNS resolution
        log.debug("[client routes] On-demand DNS resolution for %s", route.address)

        # Try to resolve
        current_ip, timestamp = self._resolver.resolve(
            route.address,
            route.current_ip,
            route.update_time
        )

        if not current_ip:
            raise DriverException(
                f"[client routes] DNS resolution failed for {route.address} "
                f"(host_id={host_id})"
            )

        # Update with resolved data
        updated_route = route._replace(
            current_ip=current_ip,
            update_time=timestamp,
            forced_resolve=False
        )

        with self._routes._lock:
            current_route = self._routes.get_by_host_id(host_id)

            if current_route is None:
                raise DriverException(
                    f"[client routes] Route for host_id={host_id} disappeared during resolution"
                )

            if (current_route.current_ip and current_route.update_time and
                updated_route.update_time and current_route.update_time >= updated_route.update_time):
                return current_route.current_ip

            self._routes._routes_by_host_id[host_id] = updated_route
            return current_ip

    def shutdown(self):
        """
        Shutdown the handler and release resources.
        """
        with self._lock:
            if self._is_shutdown:
                return

            self._is_shutdown = True
            log.info("[client routes] Handler shutdown")


class ClientRoutesAddressTranslator:
    """
    AddressTranslator implementation that uses ClientRoutesHandler.

    This bridges the AddressTranslator interface with the ClientRoutesHandler.
    """

    def __init__(self, handler):
        """
        :param handler: ClientRoutesHandler instance
        """
        self.handler = handler

    def translate(self, addr):
        """
        Legacy V1 API - not sufficient for client routes.
        """
        # Can't properly translate without host_id, return unchanged
        return addr

    def translate_with_host_id(self, addr, host_id=None):
        """
        V2 API - translate using Host metadata.
        """
        if not host_id:
            return addr

        try:
            return self.handler.translate_address(addr, host_id)
        except Exception as e:
            log.warning("[client routes] Translation failed for %s: %s", addr, e)
            return addr


