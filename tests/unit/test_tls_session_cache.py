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

import time
import unittest
from unittest.mock import Mock
from threading import Thread

from cassandra.connection import TLSSessionCache


class TLSSessionCacheTest(unittest.TestCase):
    """Test the TLSSessionCache implementation."""

    def test_cache_basic_operations(self):
        """Test basic get and set operations."""
        cache = TLSSessionCache(max_size=10, ttl=60)
        
        # Create a mock session
        mock_session = Mock()
        
        # Initially empty
        self.assertIsNone(cache.get_session('host1', 9042))
        self.assertEqual(cache.size(), 0)
        
        # Set a session
        cache.set_session('host1', 9042, mock_session)
        self.assertEqual(cache.size(), 1)
        
        # Retrieve the session
        retrieved = cache.get_session('host1', 9042)
        self.assertEqual(retrieved, mock_session)
        
    def test_cache_different_endpoints(self):
        """Test that different endpoints have separate cache entries."""
        cache = TLSSessionCache(max_size=10, ttl=60)
        
        session1 = Mock(name='session1')
        session2 = Mock(name='session2')
        session3 = Mock(name='session3')
        
        cache.set_session('host1', 9042, session1)
        cache.set_session('host2', 9042, session2)
        cache.set_session('host1', 9043, session3)
        
        self.assertEqual(cache.size(), 3)
        self.assertEqual(cache.get_session('host1', 9042), session1)
        self.assertEqual(cache.get_session('host2', 9042), session2)
        self.assertEqual(cache.get_session('host1', 9043), session3)
        
    def test_cache_ttl_expiration(self):
        """Test that sessions expire after TTL."""
        cache = TLSSessionCache(max_size=10, ttl=1)  # 1 second TTL
        
        mock_session = Mock()
        cache.set_session('host1', 9042, mock_session)
        
        # Should be retrievable immediately
        self.assertIsNotNone(cache.get_session('host1', 9042))
        
        # Wait for expiration
        time.sleep(1.1)
        
        # Should be expired
        self.assertIsNone(cache.get_session('host1', 9042))
        self.assertEqual(cache.size(), 0)
        
    def test_cache_max_size_eviction(self):
        """Test that LRU eviction works when cache is full."""
        cache = TLSSessionCache(max_size=3, ttl=60)
        
        session1 = Mock(name='session1')
        session2 = Mock(name='session2')
        session3 = Mock(name='session3')
        session4 = Mock(name='session4')
        
        # Fill cache to capacity
        cache.set_session('host1', 9042, session1)
        time.sleep(0.01)  # Ensure different access times
        cache.set_session('host2', 9042, session2)
        time.sleep(0.01)
        cache.set_session('host3', 9042, session3)
        
        self.assertEqual(cache.size(), 3)
        
        # Access session2 to update its access time
        time.sleep(0.01)
        cache.get_session('host2', 9042)
        
        # Add a fourth session - should evict session1 (oldest access)
        time.sleep(0.01)
        cache.set_session('host4', 9042, session4)
        
        self.assertEqual(cache.size(), 3)
        self.assertIsNone(cache.get_session('host1', 9042))
        self.assertIsNotNone(cache.get_session('host2', 9042))
        self.assertIsNotNone(cache.get_session('host3', 9042))
        self.assertIsNotNone(cache.get_session('host4', 9042))
        
    def test_cache_clear_expired(self):
        """Test manual clearing of expired sessions."""
        cache = TLSSessionCache(max_size=10, ttl=1)
        
        session1 = Mock(name='session1')
        session2 = Mock(name='session2')
        
        cache.set_session('host1', 9042, session1)
        time.sleep(1.1)  # Let session1 expire
        cache.set_session('host2', 9042, session2)
        
        # Before clearing, both are in cache
        self.assertEqual(cache.size(), 2)
        
        # Clear expired sessions
        cache.clear_expired()
        
        # Only session2 should remain
        self.assertEqual(cache.size(), 1)
        self.assertIsNone(cache.get_session('host1', 9042))
        self.assertIsNotNone(cache.get_session('host2', 9042))
        
    def test_cache_clear_all(self):
        """Test clearing all sessions from cache."""
        cache = TLSSessionCache(max_size=10, ttl=60)
        
        cache.set_session('host1', 9042, Mock())
        cache.set_session('host2', 9042, Mock())
        cache.set_session('host3', 9042, Mock())
        
        self.assertEqual(cache.size(), 3)
        
        cache.clear()
        
        self.assertEqual(cache.size(), 0)
        
    def test_cache_none_session(self):
        """Test that None sessions are not cached."""
        cache = TLSSessionCache(max_size=10, ttl=60)
        
        cache.set_session('host1', 9042, None)
        
        self.assertEqual(cache.size(), 0)
        self.assertIsNone(cache.get_session('host1', 9042))
        
    def test_cache_update_existing_session(self):
        """Test that updating an existing session works correctly."""
        cache = TLSSessionCache(max_size=10, ttl=60)
        
        session1 = Mock(name='session1')
        session2 = Mock(name='session2')
        
        cache.set_session('host1', 9042, session1)
        self.assertEqual(cache.get_session('host1', 9042), session1)
        
        # Update with new session
        cache.set_session('host1', 9042, session2)
        self.assertEqual(cache.get_session('host1', 9042), session2)
        
        # Size should still be 1
        self.assertEqual(cache.size(), 1)
        
    def test_cache_thread_safety(self):
        """Test that cache operations are thread-safe."""
        cache = TLSSessionCache(max_size=100, ttl=60)
        errors = []
        
        def set_sessions(thread_id):
            try:
                for i in range(50):
                    session = Mock(name=f'session_{thread_id}_{i}')
                    cache.set_session(f'host{thread_id}', 9042 + i, session)
            except Exception as e:
                errors.append(e)
        
        def get_sessions(thread_id):
            try:
                for i in range(50):
                    cache.get_session(f'host{thread_id}', 9042 + i)
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


if __name__ == '__main__':
    unittest.main()
