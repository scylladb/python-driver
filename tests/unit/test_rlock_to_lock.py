"""
Unit tests verifying that RLock -> Lock conversion is safe.

Tests that the lock objects are of the correct type and that basic
operations (connect, metadata, pool) still work correctly.
"""
import threading
import traceback
import unittest
from unittest.mock import Mock, patch

from cassandra.cluster import Cluster
from cassandra.metadata import Metadata, TokenMap
from cassandra.pool import Host


class TestLockTypes(unittest.TestCase):
    """Verify each converted lock is a plain Lock, not RLock."""

    def _assert_is_lock_not_rlock(self, lock_obj):
        """Assert the given object is a plain Lock, not an RLock.

        Compare against the concrete runtime types produced by
        threading.Lock()/threading.RLock() in this interpreter, rather than
        matching on type(...).__name__: CPython's Lock/RLock are C-level
        factory functions (not classes), so relying on the string name of
        the produced type is fragile across implementations/versions.
        """
        self.assertIs(type(lock_obj), type(threading.Lock()),
                      f"Expected plain Lock but got {type(lock_obj)}")
        self.assertIsNot(type(lock_obj), type(threading.RLock()),
                         f"Expected plain Lock but got an RLock: {type(lock_obj)}")

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
        """CQLEngine Connection.lazy_connect_lock should be a plain Lock.

        Construct the Connection through its real __init__ (rather than
        bypassing it via __new__ and assigning lazy_connect_lock by hand)
        so this actually verifies what the production constructor creates,
        not merely that a Lock instance CAN be assigned to that attribute.
        """
        from cassandra.cqlengine.connection import Connection as CQLConn
        c = CQLConn(name='test-connection', hosts=['127.0.0.1'])
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


class TestClusterConnectFailureNoDeadlock(unittest.TestCase):
    """Verify Cluster.connect() failure path doesn't deadlock with plain Lock.

    Cluster._lock is a plain Lock. connect() acquires it, and on failure
    calls shutdown() which also acquires it. The shutdown() call must happen
    after releasing the lock to avoid deadlock.
    """

    def test_connect_failure_calls_shutdown_without_deadlock(self):
        """connect() should call shutdown() and re-raise on control connection failure."""
        cluster = Cluster(contact_points=[])
        # Ensure Cluster._lock is a plain Lock (not RLock). Compare concrete
        # runtime types rather than matching type(...).__name__ as a string.
        self.assertIs(type(cluster._lock), type(threading.Lock()))
        self.assertIsNot(type(cluster._lock), type(threading.RLock()))

        with patch.object(cluster.connection_class, 'initialize_reactor'):
            with patch.object(cluster.control_connection, 'connect',
                              side_effect=Exception("test connection failure")):
                with patch.object(cluster, 'shutdown') as mock_shutdown:
                    with self.assertRaises(Exception) as ctx:
                        cluster.connect()
                    self.assertIn("test connection failure", str(ctx.exception))
                    mock_shutdown.assert_called_once()

    def test_connect_failure_preserves_original_traceback(self):
        """connect() must re-raise the control-connection failure with its
        original traceback intact, not just a frame pointing at the
        `raise connect_exc` call site.

        `connect_exc` is captured in `except Exception as connect_exc:`
        inside the `with self._lock:` block, and re-raised via
        `raise connect_exc` *outside* that block (after releasing the lock
        and calling shutdown()). In Python 3, an exception object carries
        its own accumulating `__traceback__`; re-raising the same object
        via `raise connect_exc` extends that traceback with the new frame
        rather than resetting it. This test pins down that behavior with a
        failure raised several frames deep, so a future change that
        reconstructs/replaces the exception (e.g. `raise
        SomeException(str(connect_exc))`) instead of re-raising the same
        object would be caught.
        """
        cluster = Cluster(contact_points=[])

        def _simulate_deep_control_connection_failure():
            def _innermost_socket_failure():
                raise RuntimeError("simulated deep control connection failure")
            _innermost_socket_failure()

        caught = None
        with patch.object(cluster.connection_class, 'initialize_reactor'):
            with patch.object(cluster.control_connection, 'connect',
                              side_effect=_simulate_deep_control_connection_failure):
                with patch.object(cluster, 'shutdown'):
                    # Deliberately not using assertRaises: TestCase's
                    # assertRaises context manager clears the traceback
                    # (exc_value.with_traceback(None)) before storing the
                    # exception, specifically to avoid reference cycles --
                    # which would defeat the purpose of this test.
                    try:
                        cluster.connect()
                    except RuntimeError as exc:
                        caught = exc

        self.assertIsNotNone(caught, "Cluster.connect() did not raise")
        frame_names = [frame.name for frame in traceback.extract_tb(caught.__traceback__)]
        self.assertIn('_innermost_socket_failure', frame_names,
                      "Original failure frame was lost from the traceback; "
                      f"got frames: {frame_names}")
        self.assertIn('_simulate_deep_control_connection_failure', frame_names,
                      "Original failure frame was lost from the traceback; "
                      f"got frames: {frame_names}")
        self.assertIn('connect', frame_names,
                      "Cluster.connect's own frame (the re-raise site) should "
                      "also still be present in the traceback")


if __name__ == '__main__':
    unittest.main()
