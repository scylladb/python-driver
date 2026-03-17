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
import uuid
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Set, Tuple

from cassandra import ConsistencyLevel
from cassandra.protocol import QueryMessage
from cassandra.query import dict_factory

if TYPE_CHECKING:
    from cassandra.connection import Connection

log = logging.getLogger(__name__)


class ClientRoutesChangeType(enum.Enum):
    """
    Types of CLIENT_ROUTES_CHANGE events.

    Currently the protocol defines only UPDATE_NODES.
    New variants will be added here if the protocol is extended.
    """
    UPDATE_NODES = "UPDATE_NODES"


@dataclass
class ClientRouteProxy:
    """
    :param connection_id: String identifying the connection (required)
    :param connection_addr_override:: Optional string address for initial connection
    """

    connection_id: str
    connection_addr_override: Optional[str] = None

    def __post_init__(self):
        if self.connection_id is None:
            raise ValueError("connection_id is required")

class ClientRoutesConfig:
    """
    Configuration for client routes (Private Link support).

    :param proxies: List of :class:`ClientRouteProxy` objects
        (REQUIRED, at least one)
    :param advanced_shard_awareness: Whether to enable advanced shard awareness
        (default: ``False``)
    """

    proxies: List[ClientRouteProxy]
    advanced_shard_awareness: bool

    def __init__(self, proxies: List[ClientRouteProxy], advanced_shard_awareness: bool = False):
        """
        :param proxies: List of ClientRouteProxy objects
        :param advanced_shard_awareness: Enable advanced shard awareness (default False)
        """
        if not proxies:
            raise ValueError("At least one proxy must be specified")

        if not isinstance(proxies, (list, tuple)):
            raise TypeError("proxies must be a list or tuple")

        for proxy in proxies:
            if not isinstance(proxy, ClientRouteProxy):
                raise TypeError("All proxies must be ClientRouteProxy instances")

        self.proxies = proxies
        self.advanced_shard_awareness = advanced_shard_awareness

    def __repr__(self) -> str:
        return (f"ClientRoutesConfig(proxies={self.proxies}, "
                f"advanced_shard_awareness={self.advanced_shard_awareness})")


@dataclass(frozen=True)
class _Route:
    connection_id: str
    host_id: uuid.UUID
    address: str  # ipv4, ipv6 or DNS hostname from system.client_routes
    port: int

class _RouteStore:
    """
    Thread-safe storage for routes. Reads are safe under CPython's GIL;
    writes are serialized with a lock.

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
    _proxy_addresses_override: Dict[str, str]

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
        # Precalculate proxy address mappings for efficient lookup
        self._proxy_addresses_override = {
            proxy.connection_id: proxy.connection_addr_override
            for proxy in config.proxies
            if proxy.connection_addr_override
        }

    def initialize(self, connection: 'Connection', timeout: float) -> None:
        """
        Load all routes from system.client_routes.

        Called once at startup and again whenever the control connection
        is re-established.  Reads all configured connection IDs and
        replaces the in-memory route store atomically.

        Raises on failure so the caller can decide how to react (e.g.
        abort startup or schedule a reconnect).

        :param connection: The Connection instance to execute queries on
        :param timeout: Query timeout in seconds
        """
        log.info("[client routes] Loading routes for %d proxies", len(self.config.proxies))

        routes = self._query_all_routes_for_connections(connection, timeout, self._connection_ids)
        self._routes.update(routes)

    def handle_client_routes_change(self, connection: 'Connection', timeout: float,
                                    change_type: 'ClientRoutesChangeType',
                                    connection_ids: Sequence[str], host_ids: Sequence[str]) -> None:
        """
        Handle CLIENT_ROUTES_CHANGE event.

        Currently the protocol defines only :attr:`ClientRoutesChangeType.UPDATE_NODES`.
        New variants will be added to the enum if the protocol is extended.

        :param connection: The Connection instance to execute queries on
        :param timeout: Query timeout in seconds
        :param change_type: A :class:`ClientRoutesChangeType` value
        :param connection_ids: Affected connection ID strings; empty means all.
        :param host_ids: Affected host ID strings; empty means all.
        """

        full_refresh = False
        if not connection_ids or not host_ids:
            log.warning(
                "[client routes] CLIENT_ROUTES_CHANGE has no connection_ids or host_ids, doing full refresh")
            full_refresh = True
        elif len(connection_ids) != len(host_ids):
            log.warning("[client routes] CLIENT_ROUTES_CHANGE has mismatched lengths (conn: %d, host: %d), doing full refresh",
                     len(connection_ids), len(host_ids))
            full_refresh = True

        if full_refresh:
            routes = self._query_all_routes_for_connections(connection, timeout, self._connection_ids)
            self._routes.update(routes)
            return

        host_uuids = [uuid.UUID(hid) for hid in host_ids]
        pairs = [(cid, hid) for cid, hid in zip(connection_ids, host_uuids)
                 if cid in self._connection_ids]

        if not pairs:
            return

        routes = self._query_routes_for_change_event(connection, timeout, pairs)
        self._routes.merge(routes, affected_host_ids=set(host_uuids))

    def _query_all_routes_for_connections(self, connection: 'Connection', timeout: float,
                                          connection_ids: Set[str]) -> List[_Route]:
        """
        Query all routes for the given connection IDs (complete refresh).

        Used when control connection reconnects or as a fallback when
        CLIENT_ROUTES_CHANGE event has malformed data.

        :param connection: Connection to execute query on
        :param timeout: Query timeout in seconds
        :param connection_ids: Set of connection ID strings
        :return: List of _Route
        """
        if not connection_ids:
            return []

        placeholders = ', '.join('?' for _ in connection_ids)
        query = f"SELECT connection_id, host_id, address, port, tls_port FROM system.client_routes WHERE connection_id IN ({placeholders})"
        params = [cid.encode('utf-8') for cid in connection_ids]

        log.debug("[client routes] Querying all routes for connection_ids=%s", connection_ids)
        return self._execute_routes_query(connection, timeout, query, params)

    def _query_routes_for_change_event(self, connection: 'Connection', timeout: float,
                                       route_pairs: List[Tuple[str, uuid.UUID]]) -> List[_Route]:
        """
        Query specific routes affected by a CLIENT_ROUTES_CHANGE event.

        Takes a list of (connection_id, host_id) pairs that represent the exact
        routes affected by an operation. This provides precise updates without
        fetching unrelated routes.

        If the pairs list is empty or None, falls back to a complete refresh
        of all routes for safety.

        :param connection: Connection to execute query on
        :param timeout: Query timeout in seconds
        :param route_pairs: List of (connection_id, host_id) tuples
        :return: List of _Route
        """
        unique_pairs = list(dict.fromkeys(route_pairs))

        conn_ids = list(dict.fromkeys(cid for cid, _ in unique_pairs))
        host_ids = list(dict.fromkeys(hid for _, hid in unique_pairs))

        log.debug("[client routes] Querying route pairs from CLIENT_ROUTES_CHANGE "
                  "(first 5 of %d): %s", len(unique_pairs), unique_pairs[:5])

        conn_ph = ', '.join('?' for _ in conn_ids)
        host_ph = ', '.join('?' for _ in host_ids)
        query = (
            "SELECT connection_id, host_id, address, port, tls_port "
            "FROM system.client_routes "
            f"WHERE connection_id IN ({conn_ph}) AND host_id IN ({host_ph})"
        )
        params: List = [cid.encode('utf-8') for cid in conn_ids]
        params.extend(hid.bytes for hid in host_ids)

        return self._execute_routes_query(connection, timeout, query, params)

    def _execute_routes_query(self, connection: 'Connection', timeout: float,
                             query: str, params: List) -> List[_Route]:
        """
        Execute a routes query and parse results.

        Common helper for both complete refresh and change event queries.

        :param connection: Connection to execute query on
        :param timeout: Query timeout in seconds
        :param query: CQL query string
        :param params: Query parameters
        :return: List of _Route
        """
        log.debug("[client routes] Executing query: %s with %d parameters", query, len(params))

        query_msg = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE,
                                 query_params=params if params else None)
        result = connection.wait_for_response(
            query_msg, timeout=timeout
        )

        routes = []
        broken = 0
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
                    broken += 1
                    continue

                final_address = self._proxy_addresses_override.get(connection_id, address)

                routes.append(_Route(
                    connection_id=connection_id,
                    host_id=host_id,
                    address=final_address,
                    port=port,
                ))
            except Exception as e:
                log.warning("[client routes] Failed to parse route row: %s", e)
                broken += 1

        if broken and not routes:
            raise RuntimeError(
                "[client routes] All %d route rows failed validation; "
                "refusing to return empty result that would wipe the route store" % broken
            )

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
            raise ValueError("Mapping for host %s has no port" % host_id)

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
