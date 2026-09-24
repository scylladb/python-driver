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
import threading
import unittest

from unittest.mock import patch, Mock
import socket

from cassandra import DependencyException

try:
    from cassandra.io.libevreactor import _cleanup as libev__cleanup
    from cassandra.io.libevreactor import LibevConnection, LibevLoop
except (ImportError, DependencyException):
    LibevConnection = None  # noqa
    LibevLoop = None  # noqa

from tests.unit.io.utils import ReactorTestMixin, TimerTestMixin


class LibevConnectionTest(ReactorTestMixin, unittest.TestCase):

    connection_class = LibevConnection
    socket_attr_name = '_socket'
    null_handle_function_args = None, 0

    def setUp(self):
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        LibevConnection.initialize_reactor()

        # we patch here rather than as a decorator so that the Mixin can avoid
        # specifying patch args to test methods
        patchers = [patch(obj) for obj in
                    ('socket.socket',
                     'cassandra.io.libevwrapper.IO',
                     'cassandra.io.libevreactor.LibevLoop.maybe_start'
                     )]
        for p in patchers:
            self.addCleanup(p.stop)
        for p in patchers:
            p.start()

    def test_watchers_are_finished(self):
        """
        Test for asserting that watchers are closed in LibevConnection

        This test simulates a process termination without calling cluster.shutdown(), which would trigger
        _global_loop._cleanup. It will check the watchers have been closed
        Finally it will restore the LibevConnection reactor so it doesn't affect
        the rest of the tests

        @since 3.10
        @jira_ticket PYTHON-747
        @expected_result the watchers are closed

        @test_category connection
        """
        from cassandra.io.libevreactor import _global_loop
        reactor_needs_restore = False
        try:
            with patch.object(_global_loop, "_thread"),\
                 patch.object(_global_loop, "notify"):

                self.make_connection()

                # We have to make a copy because the connections shouldn't
                # be alive when we verify them
                live_connections = set(_global_loop._live_conns)

                # This simulates the process ending without cluster.shutdown()
                # being called, then with atexit _cleanup for libevreactor would
                # be called
                reactor_needs_restore = True
                libev__cleanup(_global_loop)
                for conn in live_connections:
                    assert conn._write_watcher.stop.mock_calls
                    assert conn._read_watcher.stop.mock_calls

        finally:
            if reactor_needs_restore:
                _global_loop._shutdown = False
                # _cleanup stopped the prepare watcher; restart it so the shared
                # singleton loop is left in a working state for subsequent tests
                # (otherwise timers would never be scheduled and tests would hang).
                _global_loop._preparer.start()


class _InstrumentedLock(object):
    """
    Wraps a real threading.Lock and calls a hook the first time it is
    acquired. Used to pause a thread *while it holds the lock* so a second
    thread's attempt to acquire the same lock can be observed as blocking
    (or not).
    """

    def __init__(self, on_first_acquire):
        self._real_lock = threading.Lock()
        self._on_first_acquire = on_first_acquire
        self._acquire_count = 0
        self._count_lock = threading.Lock()

    def acquire(self, *args, **kwargs):
        got = self._real_lock.acquire(*args, **kwargs)
        if got:
            with self._count_lock:
                self._acquire_count += 1
                first = self._acquire_count == 1
            if first:
                self._on_first_acquire()
        return got

    def release(self):
        self._real_lock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class LibevLoopRaceTest(unittest.TestCase):
    """
    Regression tests for GH-980: LibevLoop._run_loop()'s decision to exit
    the reactor thread (based on _live_conns being empty) must be atomic
    with connection_created() registering a new connection. If it isn't,
    a connection can be added in the instant after the exit check reads an
    empty _live_conns but before the reactor commits to exiting -- and
    since maybe_start() (called right after connection_created()) also
    sees the stale "already started" state, nobody ever starts a new
    reactor thread for that connection. It is silently orphaned forever.
    """

    def setUp(self):
        if LibevLoop is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')

    def test_connection_created_cannot_race_the_exit_check(self):
        """
        Force the exact interleaving that produces the hang: pause the
        reactor thread right after it enters the critical section that
        decides whether to exit (i.e. right after it acquires the lock
        that must guard both _live_conns and the started/shutdown state),
        then try to register a new connection from another thread. With
        the fix, connection_created() must block until the reactor
        finishes its decision, so the two operations can never interleave.

        @jira_ticket GH-980
        """
        loop = LibevLoop()

        # No real watchers are involved in this test; make each pass of
        # the reactor loop return immediately.
        loop._loop = Mock()
        loop._shutdown = False
        loop._live_conns = set()  # nothing live -> the reactor wants to exit
        loop._started = True  # simulate an already-running reactor thread

        reactor_in_critical_section = threading.Event()
        release_reactor = threading.Event()

        def pause_reactor():
            reactor_in_critical_section.set()
            # Hold the lock open long enough to give connection_created()
            # a real chance to race in while we're "deciding".
            release_reactor.wait(timeout=5)

        loop._lock = _InstrumentedLock(pause_reactor)

        reactor_thread = threading.Thread(target=loop._run_loop, name="test_reactor", daemon=True)
        reactor_thread.start()
        self.addCleanup(reactor_thread.join, 5)

        self.assertTrue(
            reactor_in_critical_section.wait(timeout=5),
            "reactor thread never entered its exit-check critical section")

        conn = Mock()
        connection_created_done = threading.Event()

        def create_connection():
            loop.connection_created(conn)
            connection_created_done.set()

        creator_thread = threading.Thread(target=create_connection, name="test_creator", daemon=True)
        creator_thread.start()
        self.addCleanup(creator_thread.join, 5)

        # While the reactor is still deciding, connection_created() must
        # NOT be able to complete -- if it does, the exit decision and the
        # connection registration were not atomic (the bug from GH-980:
        # a two-lock split where a writer could slip a connection in
        # between the reactor's read of _live_conns and its commit to
        # exit/started=False).
        raced_in = connection_created_done.wait(timeout=0.5)
        self.assertFalse(
            raced_in,
            "connection_created() completed while the reactor thread was "
            "still deciding whether to exit -- the exit check and "
            "connection registration are not atomic, reproducing GH-980")

        # Let the reactor finish its decision (it will see the pre-race
        # empty _live_conns, and exit).
        release_reactor.set()
        creator_thread.join(timeout=5)
        reactor_thread.join(timeout=5)

        self.assertFalse(reactor_thread.is_alive())
        self.assertTrue(connection_created_done.is_set())
        # The connection was registered (never lost)...
        self.assertIn(conn, loop._live_conns)
        # ...and because it landed strictly after the reactor committed to
        # exiting, _started correctly reflects "not running": a
        # subsequent maybe_start() (as LibevConnection.__init__ always
        # calls right after connection_created()) will see this and spin
        # up a fresh thread instead of stranding the connection.
        self.assertFalse(loop._started)

        with patch('cassandra.io.libevreactor.Thread') as mock_thread_cls:
            loop.maybe_start()
        mock_thread_cls.assert_called_once()
        self.assertTrue(loop._started)

    def test_live_connection_prevents_exit(self):
        """
        Sanity check for the other side of the same critical section: if
        connection_created() completes (and is visible) before the
        reactor's exit check runs, the reactor must see the connection and
        keep the loop running rather than exit.
        """
        loop = LibevLoop()

        conn = Mock()
        loop.connection_created(conn)
        loop._shutdown = False

        calls = {'n': 0}

        def fake_start():
            calls['n'] += 1
            if calls['n'] == 1:
                return
            # Second pass: simulate the connection being closed so the
            # loop can actually terminate instead of spinning forever.
            loop.connection_destroyed(conn)

        loop._loop = Mock()
        loop._loop.start = fake_start

        reactor_thread = threading.Thread(target=loop._run_loop, name="test_reactor", daemon=True)
        reactor_thread.start()
        reactor_thread.join(timeout=5)

        self.assertFalse(reactor_thread.is_alive())
        self.assertEqual(calls['n'], 2)
        self.assertFalse(loop._started)


class LibevTimerPatcher(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        cls.patchers = [
            patch('socket.socket', spec=socket.socket),
            patch('cassandra.io.libevwrapper.IO')
        ]
        for p in cls.patchers:
            p.start()

    @classmethod
    def tearDownClass(cls):
        for p in cls.patchers:
            try:
                p.stop()
            except:
                pass


class LibevTimerTest(TimerTestMixin, LibevTimerPatcher):
    connection_class = LibevConnection

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        from cassandra.io.libevreactor import _global_loop
        return _global_loop._timers

    def make_connection(self):
        c = LibevConnection('1.2.3.4', cql_version='3.0.1')
        c._socket_impl = Mock()
        c._socket.return_value.send.side_effect = lambda x: len(x)
        return c

    def setUp(self):
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')

        LibevConnection.initialize_reactor()
        super(LibevTimerTest, self).setUp()
