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

import time
import unittest
from unittest.mock import Mock
from threading import Thread

from cassandra.tls import DefaultTLSSessionCache


class MockEndPoint:
    """Mock EndPoint for testing."""
    def __init__(self, address, port):
        self.address = address
        self.port = port

    @property
    def tls_session_cache_key(self):
        return (self.address, self.port)


class TLSSessionCacheTest(unittest.TestCase):
    """Test the TLSSessionCache implementation."""

    def test_cache_basic_operations(self):
        """Test basic get and set operations."""
        cache = DefaultTLSSessionCache(max_size=10, ttl=60)
        
        # Create a mock session and endpoint
        mock_session = Mock()
        endpoint = MockEndPoint('host1', 9042)
        
        # Initially empty
        self.assertIsNone(cache.get_session(endpoint))
        self.assertEqual(cache.size(), 0)
        
        # Set a session
        cache.set_session(endpoint, mock_session)
        self.assertEqual(cache.size(), 1)
        
        # Retrieve the session
        retrieved = cache.get_session(endpoint)
        self.assertEqual(retrieved, mock_session)
        
    def test_cache_different_endpoints(self):
        """Test that different endpoints have separate cache entries."""
        cache = DefaultTLSSessionCache(max_size=10, ttl=60)
        
        session1 = Mock(name='session1')
        session2 = Mock(name='session2')
        session3 = Mock(name='session3')
        
        endpoint1 = MockEndPoint('host1', 9042)
        endpoint2 = MockEndPoint('host2', 9042)
        endpoint3 = MockEndPoint('host1', 9043)
        
        cache.set_session(endpoint1, session1)
        cache.set_session(endpoint2, session2)
        cache.set_session(endpoint3, session3)
        
        self.assertEqual(cache.size(), 3)
        self.assertEqual(cache.get_session(endpoint1), session1)
        self.assertEqual(cache.get_session(endpoint2), session2)
        self.assertEqual(cache.get_session(endpoint3), session3)
        
    def test_cache_ttl_expiration(self):
        """Test that sessions expire after TTL."""
        cache = DefaultTLSSessionCache(max_size=10, ttl=1)  # 1 second TTL
        
        mock_session = Mock()
        endpoint = MockEndPoint('host1', 9042)
        cache.set_session(endpoint, mock_session)
        
        # Should be retrievable immediately
        self.assertIsNotNone(cache.get_session(endpoint))
        
        # Wait for expiration
        time.sleep(1.1)
        
        # Should be expired
        self.assertIsNone(cache.get_session(endpoint))
        self.assertEqual(cache.size(), 0)
        
    def test_cache_max_size_eviction(self):
        """Test that LRU eviction works when cache is full."""
        cache = DefaultTLSSessionCache(max_size=3, ttl=60)
        
        session1 = Mock(name='session1')
        session2 = Mock(name='session2')
        session3 = Mock(name='session3')
        session4 = Mock(name='session4')
        
        endpoint1 = MockEndPoint('host1', 9042)
        endpoint2 = MockEndPoint('host2', 9042)
        endpoint3 = MockEndPoint('host3', 9042)
        endpoint4 = MockEndPoint('host4', 9042)
        
        # Fill cache to capacity
        cache.set_session(endpoint1, session1)
        cache.set_session(endpoint2, session2)
        cache.set_session(endpoint3, session3)
        
        self.assertEqual(cache.size(), 3)
        
        # Access session2 to mark it as recently used
        cache.get_session(endpoint2)
        
        # Add a fourth session - should evict session1 (least recently used)
        cache.set_session(endpoint4, session4)
        
        self.assertEqual(cache.size(), 3)
        self.assertIsNone(cache.get_session(endpoint1))
        self.assertIsNotNone(cache.get_session(endpoint2))
        self.assertIsNotNone(cache.get_session(endpoint3))
        self.assertIsNotNone(cache.get_session(endpoint4))
        
    def test_cache_clear_expired(self):
        """Test manual clearing of expired sessions."""
        cache = DefaultTLSSessionCache(max_size=10, ttl=1)
        
        session1 = Mock(name='session1')
        session2 = Mock(name='session2')
        
        endpoint1 = MockEndPoint('host1', 9042)
        endpoint2 = MockEndPoint('host2', 9042)
        
        cache.set_session(endpoint1, session1)
        time.sleep(1.1)  # Let session1 expire
        cache.set_session(endpoint2, session2)
        
        # Before clearing, both are in cache
        self.assertEqual(cache.size(), 2)
        
        # Clear expired sessions
        cache.clear_expired()
        
        # Only session2 should remain
        self.assertEqual(cache.size(), 1)
        self.assertIsNone(cache.get_session(endpoint1))
        self.assertIsNotNone(cache.get_session(endpoint2))
        
    def test_cache_clear_all(self):
        """Test clearing all sessions from cache."""
        cache = DefaultTLSSessionCache(max_size=10, ttl=60)
        
        endpoint1 = MockEndPoint('host1', 9042)
        endpoint2 = MockEndPoint('host2', 9042)
        endpoint3 = MockEndPoint('host3', 9042)
        
        cache.set_session(endpoint1, Mock())
        cache.set_session(endpoint2, Mock())
        cache.set_session(endpoint3, Mock())
        
        self.assertEqual(cache.size(), 3)
        
        cache.clear()
        
        self.assertEqual(cache.size(), 0)
        
    def test_cache_none_session(self):
        """Test that None sessions are not cached."""
        cache = DefaultTLSSessionCache(max_size=10, ttl=60)
        
        endpoint = MockEndPoint('host1', 9042)
        cache.set_session(endpoint, None)
        
        self.assertEqual(cache.size(), 0)
        self.assertIsNone(cache.get_session(endpoint))
        
    def test_cache_update_existing_session(self):
        """Test that updating an existing session works correctly."""
        cache = DefaultTLSSessionCache(max_size=10, ttl=60)
        
        session1 = Mock(name='session1')
        session2 = Mock(name='session2')
        
        endpoint = MockEndPoint('host1', 9042)
        
        cache.set_session(endpoint, session1)
        self.assertEqual(cache.get_session(endpoint), session1)
        
        # Update with new session
        cache.set_session(endpoint, session2)
        self.assertEqual(cache.get_session(endpoint), session2)
        
        # Size should still be 1
        self.assertEqual(cache.size(), 1)
        
    def test_cache_thread_safety(self):
        """Test that cache operations are thread-safe."""
        cache = DefaultTLSSessionCache(max_size=100, ttl=60)
        errors = []
        
        def set_sessions(thread_id):
            try:
                for i in range(50):
                    session = Mock(name=f'session_{thread_id}_{i}')
                    endpoint = MockEndPoint(f'host{thread_id}', 9042 + i)
                    cache.set_session(endpoint, session)
            except Exception as e:
                errors.append(e)
        
        def get_sessions(thread_id):
            try:
                for i in range(50):
                    endpoint = MockEndPoint(f'host{thread_id}', 9042 + i)
                    cache.get_session(endpoint)
            except Exception as e:
                errors.append(e)
        
        # Create multiple threads doing concurrent operations
        threads = []
        for i in range(5):
            t1 = Thread(target=set_sessions, args=(i,))
            t2 = Thread(target=get_sessions, args=(i,))
            threads.extend([t1, t2])
        
        for t in threads:
            t.start()
        
        for t in threads:
            t.join()
        
        # Check that no errors occurred
        self.assertEqual(len(errors), 0, f"Thread safety test failed with errors: {errors}")
        
        # Check that cache is not empty and within max size
        self.assertGreater(cache.size(), 0)
        self.assertLessEqual(cache.size(), 100)
    
    def test_cache_by_host_only(self):
        """Test caching by host only (ignoring port)."""
        cache = DefaultTLSSessionCache(max_size=10, ttl=60, cache_by_host_only=True)
        
        session = Mock(name='session')
        
        endpoint1 = MockEndPoint('host1', 9042)
        endpoint2 = MockEndPoint('host1', 9043)  # Same host, different port
        
        # Set session for first endpoint
        cache.set_session(endpoint1, session)
        self.assertEqual(cache.size(), 1)
        
        # Get session using second endpoint (same host, different port)
        # Should return the same session because we're caching by host only
        retrieved = cache.get_session(endpoint2)
        self.assertEqual(retrieved, session)
        
        # Cache should still have size 1
        self.assertEqual(cache.size(), 1)

    def test_automatic_expired_cleanup(self):
        """Test that expired sessions are cleaned up automatically during set_session."""
        cache = DefaultTLSSessionCache(max_size=10, ttl=1)
        # Override cleanup interval for testing
        cache._EXPIRY_CLEANUP_INTERVAL = 5

        # Add some sessions that will expire
        for i in range(3):
            endpoint = MockEndPoint(f'host{i}', 9042)
            cache.set_session(endpoint, Mock(name=f'session{i}'))

        self.assertEqual(cache.size(), 3)

        # Wait for sessions to expire
        time.sleep(1.1)

        # Add sessions until cleanup is triggered (5 operations)
        for i in range(5):
            endpoint = MockEndPoint(f'newhost{i}', 9042)
            cache.set_session(endpoint, Mock(name=f'newsession{i}'))

        # Expired sessions should have been cleaned up
        # The 3 expired sessions should be removed
        # Only the 5 new sessions should remain
        self.assertEqual(cache.size(), 5)
