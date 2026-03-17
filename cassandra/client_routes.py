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
import threading
import uuid
from typing import Dict, List, Optional, Set

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
