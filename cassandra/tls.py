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
TLS session caching implementation for faster reconnections.
"""

from abc import ABC, abstractmethod
from collections import OrderedDict, namedtuple
from threading import RLock
import time
from typing import Optional


# Named tuple for TLS session cache entries
_SessionCacheEntry = namedtuple('_SessionCacheEntry', ['session', 'timestamp'])


class TLSSessionCache(ABC):
    """
    Abstract base class for TLS session caching.
    
    Implementations should provide thread-safe caching of TLS sessions
    to enable session resumption for faster reconnections.
    """
    
    @abstractmethod
    def get_session(self, endpoint):
        """
        Get a cached TLS session for the given endpoint.
        
        Args:
            endpoint: The EndPoint object representing the connection target
            
        Returns:
            ssl.SSLSession object if a valid cached session exists, None otherwise
        """
        pass
    
    @abstractmethod
    def set_session(self, endpoint, session):
        """
        Store a TLS session for the given endpoint.
        
        Args:
            endpoint: The EndPoint object representing the connection target
            session: The ssl.SSLSession object to cache
        """
        pass
    
    @abstractmethod
    def clear_expired(self):
        """Remove all expired sessions from the cache."""
        pass
    
    @abstractmethod
    def clear(self):
        """Clear all sessions from the cache."""
        pass
    
    @abstractmethod
    def size(self):
        """Return the current number of cached sessions."""
        pass


class TLSSessionCacheOptions(ABC):
    """
    Abstract base class for TLS session cache configuration options.
    """
    
    @abstractmethod
    def create_cache(self):
        """
        Build and return a TLSSessionCache implementation.
        
        Returns:
            TLSSessionCache: A configured session cache instance
        """
        pass


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
    
    def __init__(self, max_size=100, ttl=3600, cache_by_host_only=False):
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
    
    def _make_key(self, endpoint):
        """Create a cache key from endpoint."""
        if self._cache_by_host_only:
            return (endpoint.address,)
        else:
            return (endpoint.address, endpoint.port)
    
    def get_session(self, endpoint):
        """
        Get a cached TLS session for the given endpoint.
        
        Args:
            endpoint: The EndPoint object representing the connection target
            
        Returns:
            ssl.SSLSession object if a valid cached session exists, None otherwise
        """
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
    
    def set_session(self, endpoint, session):
        """
        Store a TLS session for the given endpoint.
        
        Args:
            endpoint: The EndPoint object representing the connection target
            session: The ssl.SSLSession object to cache
        """
        if session is None:
            return
        
        key = self._make_key(endpoint)
        current_time = time.time()
        
        with self._lock:
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
    
    def clear_expired(self):
        """Remove all expired sessions from the cache."""
        current_time = time.time()
        with self._lock:
            expired_keys = [
                key for key, entry in self._sessions.items()
                if current_time - entry.timestamp > self._ttl
            ]
            for key in expired_keys:
                del self._sessions[key]
    
    def clear(self):
        """Clear all sessions from the cache."""
        with self._lock:
            self._sessions.clear()
    
    def size(self):
        """Return the current number of cached sessions."""
        with self._lock:
            return len(self._sessions)


class DefaultTLSSessionCacheOptions(TLSSessionCacheOptions):
    """
    Default implementation of TLS session cache configuration options.
    """
    
    def __init__(self, max_size=100, ttl=3600, cache_by_host_only=False):
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
    
    def create_cache(self):
        """
        Build and return a DefaultTLSSessionCache implementation.
        
        Returns:
            DefaultTLSSessionCache: A configured session cache instance
        """
        return DefaultTLSSessionCache(
            max_size=self.max_size,
            ttl=self.ttl,
            cache_by_host_only=self.cache_by_host_only
        )
