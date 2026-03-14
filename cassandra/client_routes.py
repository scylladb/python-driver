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

"""
Client Routes support for Private Link and similar network configurations.

This module implements support for dynamic address translation via the
system.client_routes table and CLIENT_ROUTES_CHANGE events.
"""

from __future__ import absolute_import

from dataclasses import dataclass
import enum
import logging
import socket
import threading
import time
import uuid
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Set, Tuple

from cassandra import ConsistencyLevel
from cassandra.protocol import QueryMessage
from cassandra.query import dict_factory

if TYPE_CHECKING:
    from cassandra.cluster import ControlConnection

log = logging.getLogger(__name__)


class ClientRoutesChangeType(enum.Enum):
    """
    Types of CLIENT_ROUTES_CHANGE events.

    Currently the protocol defines only UPDATE_NODES.
    New variants will be added here if the protocol is extended.
    """
    UPDATE_NODES = "UPDATE_NODES"


class ClientRouteProxy:

    connection_id: str
    connection_addr: Optional[str]

    def __init__(self, connection_id: str, connection_addr: Optional[str] = None):
        """
        :param connection_id: String identifying the connection
        :param connection_addr: Optional string address for initial connection
        """
        if connection_id is None:
            raise ValueError("connection_id is required")

        self.connection_id = connection_id
        self.connection_addr = connection_addr

    def __repr__(self) -> str:
        return f"ClientRouteProxy(connection_id={self.connection_id}, connection_addr={self.connection_addr})"


class ClientRoutesConfig:
    """
    Configuration for client routes (Private Link support).

    :param proxies: List of :class:`ClientRouteProxy` objects
        (REQUIRED, at least one)
    """

    proxies: List[ClientRouteProxy]

    def __init__(self, proxies: List[ClientRouteProxy]):
        """
        :param proxies: List of ClientRouteProxy objects
        """
        if not proxies:
            raise ValueError("At least one proxy must be specified")

        if not isinstance(proxies, (list, tuple)):
            raise TypeError("proxies must be a list or tuple")

        for proxy in proxies:
            if not isinstance(proxy, ClientRouteProxy):
                raise TypeError("All proxies must be ClientRouteProxy instances")

        self.proxies = list(proxies)

    def __repr__(self) -> str:
        return f"ClientRoutesConfig(proxies={self.proxies})"


@dataclass
class _Route:
    connection_id: str
    host_id: uuid.UUID
    address: str  # ipv4, ipv6 or DNS hostname from system.client_routes
    port: int

class _RouteStore:
    """
    Thread-safe storage for routes using lock-free reads.

    This uses atomic pointer swaps for updates, allowing lock-free reads
    while serializing writes.
    """

    _routes_by_host_id: Dict[uuid.UUID, _Route]
    _lock: threading.Lock

    def __init__(self) -> None:
        self._routes_by_host_id = {}
        self._lock = threading.Lock()

    def get_by_host_id(self, host_id: uuid.UUID) -> Optional[_Route]:
        """
        Get route for a host ID (lock-free read).

        :param host_id: UUID of the host
        :return: _Route or None
        """
        return self._routes_by_host_id.get(host_id)

    def get_all(self) -> List[_Route]:
        """
        Get all routes as a list (lock-free read).

        :return: List of _Route
        """
        return list(self._routes_by_host_id.values())

    def _select_preferred_routes(self, new_routes: List[_Route]) -> List[_Route]:
        """
        When multiple routes exist for the same host_id (different connection_ids),
        prefer the connection_id already in use. Only migrate to a different
        connection_id when the previously used one is no longer available.

        Must be called under self._lock.
        """
        by_host: Dict[uuid.UUID, List[_Route]] = {}
        for route in new_routes:
            by_host.setdefault(route.host_id, []).append(route)

        selected = []
        for host_id, candidates in by_host.items():
            if len(candidates) == 1:
                selected.append(candidates[0])
                continue

            existing = self._routes_by_host_id.get(host_id)
            if existing:
                preferred = [c for c in candidates if c.connection_id == existing.connection_id]
                if preferred:
                    selected.append(preferred[0])
                    continue

            selected.append(candidates[0])

        return selected

    def update(self, routes: List[_Route]) -> None:
        """
        Replace all routes atomically.

        :param routes: List of _Route objects
        """
        with self._lock:
            preferred = self._select_preferred_routes(routes)
            self._routes_by_host_id = {route.host_id: route for route in preferred}

    def merge(self, new_routes: List[_Route], affected_host_ids: Set[uuid.UUID]) -> None:
        """
        Merge new routes with existing ones atomically.

        Routes for affected_host_ids are replaced entirely: existing routes
        for those hosts are dropped and replaced with whatever is in new_routes.
        This handles deletions from system.client_routes (affected host present
        but no new route for it).

        :param new_routes: List of _Route objects to merge
        :param affected_host_ids: Set of host IDs affected by the change.
        """
        with self._lock:
            preferred = self._select_preferred_routes(new_routes)
            new_by_host = {r.host_id: r for r in preferred}

            updated = {hid: r for hid, r in self._routes_by_host_id.items()
                       if hid not in affected_host_ids}
            updated.update(new_by_host)
            self._routes_by_host_id = updated


class _ClientRoutesHandler:
    """
    Handles dynamic address translation for Private Link via system.client_routes.

    Lifecycle:
    1. Construction: Create with configuration
    2. Initialization: Read system.client_routes after control connection established
    3. Steady state: Listen for CLIENT_ROUTES_CHANGE events and update routes
    4. Translation: Translate addresses using Host ID lookup
    """

    config: 'ClientRoutesConfig'
    ssl_enabled: bool
    _routes: _RouteStore
    _connection_ids: Set[str]

    def __init__(self, config: 'ClientRoutesConfig', ssl_enabled: bool = False):
        """
        :param config: ClientRoutesConfig instance
        :param ssl_enabled: Whether TLS is enabled (determines port selection)
        """
        if not isinstance(config, ClientRoutesConfig):
            raise TypeError("config must be a ClientRoutesConfig instance")

        self.config = config
        self.ssl_enabled = ssl_enabled
        self._routes = _RouteStore()
        self._connection_ids = {dep.connection_id for dep in config.proxies}

    def initialize(self, control_connection: 'ControlConnection') -> None:
        """
        Initialize handler after control connection is established.

        Reads system.client_routes for all configured connection IDs.
        DNS resolution happens at the Endpoint level on each connection attempt.

        :param control_connection: The ControlConnection instance
        """
        log.info("[client routes] Initializing with %d proxies", len(self.config.proxies))

        try:
            connection_ids = self._connection_ids
            routes = self._query_routes(control_connection, connection_ids=connection_ids)

            self._routes.update(routes)
        except Exception as e:
            log.error("[client routes] Initialization failed: %s", e, exc_info=True)
            raise

    def handle_client_routes_change(self, control_connection: 'ControlConnection',
                                    change_type: 'ClientRoutesChangeType',
                                    connection_ids: Optional[Sequence[str]], host_ids: Optional[Sequence[str]]) -> None:
        """
        Handle CLIENT_ROUTES_CHANGE event.

        Currently the protocol defines only :attr:`ClientRoutesChangeType.UPDATE_NODES`.
        New variants will be added to the enum if the protocol is extended.

        :param control_connection: The ControlConnection instance
        :param change_type: A :class:`ClientRoutesChangeType` value
        :param connection_ids: List of affected connection ID strings
        :param host_ids: List of affected host ID strings
        """
        filtered_conn_ids = None
        if connection_ids:
            configured_ids = self._connection_ids
            filtered = [cid for cid in connection_ids if cid in configured_ids]
            if not filtered:
                return
            filtered_conn_ids = filtered

        host_uuids = [uuid.UUID(hid) for hid in host_ids] if host_ids else None

        try:
            routes = self._query_routes(
                control_connection,
                connection_ids=filtered_conn_ids,
                host_ids=host_uuids
            )
        except Exception as e:
            log.warning("[client routes] Failed to query routes for CLIENT_ROUTES_CHANGE: %s", e, exc_info=True)
            return

        if host_uuids:
            self._routes.merge(routes, affected_host_ids=set(host_uuids))
        else:
            self._routes.update(routes)

    def handle_control_connection_reconnect(self, control_connection: 'ControlConnection') -> None:
        """
        Handle control connection recreation - full reload of all connection IDs.

        Unlike initialize() which merges, this replaces all routes to remove
        any stale entries that may no longer exist in the system.client_routes table.

        :param control_connection: The new ControlConnection instance
        """
        log.info("[client routes] Control connection reconnected, reloading all routes")

        connection_ids = self._connection_ids
        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                routes = self._query_routes(control_connection, connection_ids=connection_ids)
            except Exception as e:
                if attempt < max_attempts:
                    log.warning("[client routes] Transient failure reloading routes "
                                "(attempt %d/%d): %s", attempt, max_attempts, e)
                    # Blocking sleep is acceptable here: this runs only on
                    # control-connection reconnect (rare), at most 2 sleeps
                    # totalling 2 s.  Do not copy this pattern into hot paths
                    # — use the scheduler for non-blocking delays instead.
                    time.sleep(1)
                else:
                    log.error("[client routes] Failed to reload routes after %d attempts: %s",
                              max_attempts, e, exc_info=True)
                continue
            self._routes.update(routes)
            return

    def _query_routes(self, control_connection: 'ControlConnection', connection_ids: Optional[List[str]] = None,
                     host_ids: Optional[List[uuid.UUID]] = None) -> List[_Route]:
        """
        Query system.client_routes table.

        :param control_connection: ControlConnection to execute query
        :param connection_ids: Optional list of connection ID strings to filter by
        :param host_ids: Optional list of host UUIDs to filter by
        :return: List of _Route
        """
        query = "SELECT connection_id, host_id, address, port, tls_port FROM system.client_routes"
        where_clauses = []
        params = []

        if connection_ids:
            placeholders = ', '.join('?' for _ in connection_ids)
            where_clauses.append("connection_id IN (%s)" % placeholders)
            params.extend(cid.encode('utf-8') for cid in connection_ids)

        if host_ids:
            placeholders = ', '.join('?' for _ in host_ids)
            where_clauses.append("host_id IN (%s)" % placeholders)
            params.extend(hid.bytes for hid in host_ids)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        else:
            query += " ALLOW FILTERING"

        query_msg = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE,
                                 query_params=params if params else None)
        result = control_connection._connection.wait_for_response(
            query_msg, timeout=control_connection._timeout
        )

        routes = []
        rows = dict_factory(result.column_names, result.parsed_rows)
        for row in rows:
            try:
                absent = []
                port = row['tls_port'] if self.ssl_enabled else row['port']
                connection_id = row['connection_id']
                host_id = row['host_id']
                address = row['address']

                if not port:
                    absent.append("tls_port" if self.ssl_enabled else "port")
                if not connection_id:
                    absent.append("connection_id")
                if not host_id:
                    absent.append("host_id")
                if not address:
                    absent.append("address")

                if absent:
                    log.error("[client routes] read a route %s, that has no values for the following fields: %s", row, ",".join(absent))
                    continue

                routes.append(_Route(
                    connection_id=connection_id,
                    host_id=host_id,
                    address=address,
                    port=port,
                ))
            except Exception as e:
                log.warning("[client routes] Failed to parse route row: %s", e)

        return routes

    def resolve_host(self, host_id: uuid.UUID) -> Optional[Tuple[str, int]]:
        """
        Resolve a host_id to an (address, port) pair.

        Looks up the current route and selects the appropriate port.

        :param host_id: Host UUID to resolve
        :return: Tuple of (address, port) or None if no route mapping exists
        """
        route = self._routes.get_by_host_id(host_id)
        if route is None:
            return None

        if not route.port:
            raise AttributeError("Mapping for host %s has no port" % host_id)

        try:
            result = socket.getaddrinfo(route.address, route.port,
                                        socket.AF_UNSPEC, socket.SOCK_STREAM)
            if not result:
                raise socket.gaierror("No addresses found for %s" % route.address)
            resolved_ip = result[0][4][0]
            return resolved_ip, route.port
        except socket.gaierror as e:
            log.warning('[client routes] Could not resolve hostname "%s" (host_id=%s): %s',
                        route.address, host_id, e)
            raise
