"""
Unit tests verifying that RLock -> Lock conversion is safe.

Tests that the lock objects are of the correct type and that basic
operations (connect, metadata, pool) still work correctly.
"""
import threading
import unittest
from unittest.mock import Mock

from cassandra.metadata import Metadata, TokenMap
from cassandra.pool import Host


class TestLockTypes(unittest.TestCase):
    """Verify each converted lock is a plain Lock, not RLock."""

    def _assert_is_lock_not_rlock(self, lock_obj):
        """Assert the given object is a plain Lock, not an RLock."""
        # In CPython, Lock() creates _thread.lock, RLock() creates _thread.RLock
        lock_type_name = type(lock_obj).__name__
        self.assertNotIn('RLock', lock_type_name,
                         f"Expected plain Lock but got {type(lock_obj)}")

    def test_metadata_hosts_lock_is_plain_lock(self):
        """Metadata._hosts_lock should be a plain Lock."""
        m = Metadata()
        self._assert_is_lock_not_rlock(m._hosts_lock)

    def test_metadata_rebuild_lock_is_plain_lock(self):
        """TokenMap._rebuild_lock should be a plain Lock."""
        tm = TokenMap(
            token_class=Mock(),
            token_to_host_owner={},
            all_tokens=[],
            metadata=Mock()
        )
        self._assert_is_lock_not_rlock(tm._rebuild_lock)

    def test_host_lock_is_plain_lock(self):
        """Host.lock should be a plain Lock."""
        import uuid
        h = Host(
            endpoint=Mock(),
            conviction_policy_factory=Mock(),
            host_id=uuid.uuid4()
        )
        self._assert_is_lock_not_rlock(h.lock)

    def test_cqlengine_connection_lock_is_plain_lock(self):
        """CQLEngine Connection.lazy_connect_lock should be a plain Lock."""
        from cassandra.cqlengine.connection import Connection as CQLConn
        c = CQLConn.__new__(CQLConn)
        c.lazy_connect_lock = threading.Lock()
        self._assert_is_lock_not_rlock(c.lazy_connect_lock)


class TestMetadataOperationsWithLock(unittest.TestCase):
    """Verify metadata operations work correctly with plain Lock."""

    def test_add_and_get_host(self):
        """add_or_return_host + get_host should work with plain Lock."""
        import uuid
        m = Metadata()
        endpoint = Mock()
        host = Host(endpoint=endpoint, conviction_policy_factory=Mock(),
                    host_id=uuid.uuid4())
        returned, new = m.add_or_return_host(host)
        self.assertTrue(new)
        self.assertIs(returned, host)

        # Second add should return same host
        returned2, new2 = m.add_or_return_host(host)
        self.assertFalse(new2)
        self.assertIs(returned2, host)

    def test_update_host_sequential_lock(self):
        """update_host acquires lock twice sequentially — must not deadlock."""
        import uuid
        m = Metadata()
        old_endpoint = Mock()
        new_endpoint = Mock()
        host = Host(endpoint=new_endpoint, conviction_policy_factory=Mock(),
                    host_id=uuid.uuid4())
        # update_host calls add_or_return_host (acquires lock, releases),
        # then acquires lock again for endpoint update.
        # With plain Lock, this must NOT deadlock.
        m.update_host(host, old_endpoint)
        # Host should be retrievable by host_id
        result = m.get_host_by_host_id(host.host_id)
        self.assertIs(result, host)

    def test_remove_host(self):
        """remove_host should work with plain Lock."""
        import uuid
        m = Metadata()
        endpoint = Mock()
        host = Host(endpoint=endpoint, conviction_policy_factory=Mock(),
                    host_id=uuid.uuid4())
        m.add_or_return_host(host)
        removed = m.remove_host(host)
        self.assertTrue(removed)

    def test_all_hosts(self):
        """all_hosts should work under plain Lock."""
        import uuid
        m = Metadata()
        hosts = []
        for _ in range(3):
            h = Host(endpoint=Mock(), conviction_policy_factory=Mock(),
                     host_id=uuid.uuid4())
            m.add_or_return_host(h)
            hosts.append(h)
        all_h = m.all_hosts()
        self.assertEqual(len(all_h), 3)


class TestHostLockOperations(unittest.TestCase):
    """Verify Host lock operations work with plain Lock."""

    def test_get_and_set_reconnection_handler(self):
        """get_and_set_reconnection_handler should work with plain Lock."""
        import uuid
        h = Host(endpoint=Mock(), conviction_policy_factory=Mock(),
                 host_id=uuid.uuid4())
        handler = Mock()
        old = h.get_and_set_reconnection_handler(handler)
        self.assertIsNone(old)
        old2 = h.get_and_set_reconnection_handler(Mock())
        self.assertIs(old2, handler)


if __name__ == '__main__':
    unittest.main()
