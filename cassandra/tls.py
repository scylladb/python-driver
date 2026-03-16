# Copyright ScyllaDB, Inc.
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
TLS session caching implementation for faster reconnections.
"""

from collections import OrderedDict, namedtuple
from threading import RLock
from typing import Any, Optional, Tuple
import time


# Named tuple for TLS session cache entries
_SessionCacheEntry = namedtuple('_SessionCacheEntry', ['session', 'timestamp'])


class TLSSessionCache:
    """
    Base class for TLS session caching.

    Implementations should provide thread-safe caching of TLS sessions
    to enable session resumption for faster reconnections.
    """

    def get_session(self, endpoint: 'EndPoint') -> Optional[Any]:
        """
        Get a cached TLS session for the given endpoint.
        """
        raise NotImplementedError

    def set_session(self, endpoint: 'EndPoint', session: Any) -> None:
        """
        Store a TLS session for the given endpoint.
        """
        raise NotImplementedError

    def clear_expired(self) -> None:
        """Remove all expired sessions from the cache."""
        raise NotImplementedError

    def clear(self) -> None:
        """Clear all sessions from the cache."""
        raise NotImplementedError

    def size(self) -> int:
        """Return the current number of cached sessions."""
        raise NotImplementedError


class DefaultTLSSessionCache(TLSSessionCache):
    """
    Default implementation of TLS session caching.

    This cache stores TLS sessions per endpoint to allow quick TLS
    renegotiation when reconnecting to the same server. Sessions are
    automatically expired after a TTL and the cache has a maximum
    size with LRU eviction using OrderedDict.

    TLS session resumption works with both TLS 1.2 and TLS 1.3:
    - TLS 1.2: Session IDs (RFC 5246) and optionally Session Tickets (RFC 5077)
    - TLS 1.3: Session Tickets (RFC 8446)

    Python's ssl.SSLSession API handles both versions transparently, so no
    version-specific checks are needed.
    """

    # Cleanup expired sessions every N set_session calls
    _EXPIRY_CLEANUP_INTERVAL = 100

    def __init__(self, max_size: int = 100, ttl: int = 3600, cache_by_host_only: bool = False):
        """
        Initialize the TLS session cache.

        Args:
            max_size: Maximum number of sessions to cache (default: 100)
            ttl: Time-to-live for cached sessions in seconds (default: 3600)
            cache_by_host_only: If True, cache sessions by host only (ignoring port).
                               If False, cache by host and port (default: False)
        """
        self._sessions = OrderedDict()  # OrderedDict for O(1) LRU eviction
        self._lock = RLock()
        self._max_size = max_size
        self._ttl = ttl
        self._cache_by_host_only = cache_by_host_only
        self._operation_count = 0  # Counter for opportunistic cleanup

    def _make_key(self, endpoint: 'EndPoint') -> Tuple:
        """
        Create a cache key from endpoint.

        Uses the endpoint's tls_session_cache_key property which returns
        appropriate components for each endpoint type (e.g., includes
        server_name for SNI endpoints to prevent cache collisions).
        """
        key = endpoint.tls_session_cache_key
        if self._cache_by_host_only:
            return (key[0],)
        else:
            return key

    def get_session(self, endpoint: 'EndPoint') -> Optional[Any]:
        """Get a cached TLS session for the given endpoint."""
        key = self._make_key(endpoint)
        with self._lock:
            if key not in self._sessions:
                return None

            entry = self._sessions[key]

            # Check if session has expired
            if time.time() - entry.timestamp > self._ttl:
                del self._sessions[key]
                return None

            # Move to end to mark as recently used (LRU)
            self._sessions.move_to_end(key)
            return entry.session

    def set_session(self, endpoint: 'EndPoint', session: Any) -> None:
        """Store a TLS session for the given endpoint."""
        if session is None:
            return

        key = self._make_key(endpoint)
        current_time = time.time()

        with self._lock:
            # Opportunistically clean up expired sessions periodically
            self._operation_count += 1
            if self._operation_count >= self._EXPIRY_CLEANUP_INTERVAL:
                self._operation_count = 0
                self._clear_expired_unlocked(current_time)

            # If key already exists, just update it
            if key in self._sessions:
                self._sessions[key] = _SessionCacheEntry(session, current_time)
                self._sessions.move_to_end(key)
                return

            # If cache is at max size, remove least recently used entry (first item)
            if len(self._sessions) >= self._max_size:
                self._sessions.popitem(last=False)

            # Store session with creation time
            self._sessions[key] = _SessionCacheEntry(session, current_time)

    def _clear_expired_unlocked(self, current_time: Optional[float] = None) -> None:
        """Remove all expired sessions (must be called with lock held)."""
        if current_time is None:
            current_time = time.time()
        expired_keys = [
            key for key, entry in self._sessions.items()
            if current_time - entry.timestamp > self._ttl
        ]
        for key in expired_keys:
            del self._sessions[key]

    def clear_expired(self) -> None:
        """Remove all expired sessions from the cache."""
        with self._lock:
            self._clear_expired_unlocked()

    def clear(self) -> None:
        """Clear all sessions from the cache."""
        with self._lock:
            self._sessions.clear()

    def size(self) -> int:
        """Return the current number of cached sessions."""
        with self._lock:
            return len(self._sessions)


class TLSSessionCacheOptions:
    """
    Configuration options for the default TLS session cache.
    """

    def __init__(self, max_size: int = 100, ttl: int = 3600, cache_by_host_only: bool = False):
        """
        Initialize TLS session cache options.

        Args:
            max_size: Maximum number of sessions to cache (default: 100)
            ttl: Time-to-live for cached sessions in seconds (default: 3600)
            cache_by_host_only: If True, cache sessions by host only (ignoring port).
                               If False, cache by host and port (default: False)
        """
        self.max_size = max_size
        self.ttl = ttl
        self.cache_by_host_only = cache_by_host_only

    def create_cache(self) -> DefaultTLSSessionCache:
        """Build and return a DefaultTLSSessionCache instance."""
        return DefaultTLSSessionCache(
            max_size=self.max_size,
            ttl=self.ttl,
            cache_by_host_only=self.cache_by_host_only
        )
