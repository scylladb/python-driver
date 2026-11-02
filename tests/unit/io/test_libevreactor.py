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
import errno
import sys
import threading
import time
import weakref
import unittest

from unittest.mock import patch, Mock
import socket

from cassandra import DependencyException
from cassandra.connection import DefaultEndPoint

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

    def test_watcher_failure_releases_first_watcher_and_socket(self):
        """
        If the write watcher cannot be created (e.g. no free CRT fd on
        Windows), the read watcher and the socket must not be leaked.
        """
        class Watcher(object):
            pass
        first = Watcher()
        first_ref = weakref.ref(first)
        sock = Mock()
        from cassandra.io import libevwrapper
        libevwrapper.IO.side_effect = [first, OSError(errno.EMFILE, 'Too many open files')]
        del first

        def connect(conn):
            conn._socket = sock

        with patch.object(LibevConnection, '_connect_socket', connect):
            with self.assertRaises(OSError):
                LibevConnection(DefaultEndPoint('1.2.3.4'), cql_version='3.0.1', connect_timeout=5)
        sock.close.assert_called_once_with()
        libevwrapper.IO.side_effect = None  # the mock's iterator holds `first` too
        self.assertIsNone(first_ref())

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
                # _cleanup took the preparer's loop reference back; drop it
                # again as in LibevLoop.__init__, or the loop never returns
                _global_loop._loop.unref()


class _InstrumentedLock(object):
    """
    Wraps a real threading.RLock (like LibevLoop._lock) and calls a hook the first time it is
    acquired. Used to pause a thread *while it holds the lock* so a second
    thread's attempt to acquire the same lock can be observed as blocking
    (or not).
    """

    def __init__(self, on_first_acquire):
        self._real_lock = threading.RLock()
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

    def _is_owned(self):
        return self._real_lock._is_owned()

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

    def test_connection_destroyed_before_exit_check_is_closed(self):
        """
        A connection may be closed after the reactor's last
        _loop_will_run() pass - which is where the sockets of closed
        connections are closed - and just before its exit check. The
        reactor then exits because _live_conns is empty, so it must close
        that connection's socket itself, or nobody ever will.

        @jira_ticket GH-1131
        """
        loop = LibevLoop()

        conn = Mock()
        loop.connection_created(conn)
        loop._shutdown = False
        loop._started = True  # simulate a running reactor thread

        def fake_start():
            # The connection is closed by another thread while the libev
            # loop returns, because it has no more active watchers.
            loop.connection_destroyed(conn)

        loop._loop = Mock()
        loop._loop.start = fake_start

        reactor_thread = threading.Thread(target=loop._run_loop, name="test_reactor", daemon=True)
        reactor_thread.start()
        reactor_thread.join(timeout=5)

        self.assertFalse(reactor_thread.is_alive())
        self.assertFalse(loop._started)
        conn._socket.close.assert_called_once_with()
        self.assertEqual(loop._closed_conns, set())

    def test_connection_destroyed_after_exit_is_closed(self):
        """
        A connection may be closed after the reactor thread has already
        exited. No reactor thread is going to start for an already-closed
        connection, so connection_destroyed() must close its socket
        itself, or nobody ever will.

        @jira_ticket GH-1131
        """
        loop = LibevLoop()

        conn = Mock()
        loop.connection_created(conn)
        loop._shutdown = False
        loop._started = False  # the reactor thread has exited

        with patch('cassandra.io.libevreactor.Thread') as mock_thread_cls:
            loop.connection_destroyed(conn)
        mock_thread_cls.assert_not_called()

        conn._socket.close.assert_called_once_with()
        self.assertEqual(loop._closed_conns, set())
        self.assertEqual(loop._live_conns, set())
        self.assertEqual(loop._new_conns, set())


    def test_connection_destroyed_reentered_under_lock(self):
        """
        A GC finalizer (Session.__del__ -> shutdown() -> conn.close()) can
        fire inside any block holding _lock and call connection_destroyed()
        on the same thread. That must not deadlock, and the connection must
        still be closed by the next loop pass.

        @jira_ticket GH-1133
        """
        loop = LibevLoop()

        conn = Mock()
        loop.connection_created(conn)
        loop._started = True  # simulate a running reactor thread

        def finalizer_under_lock():
            with loop._lock:
                loop.connection_destroyed(conn)

        t = threading.Thread(target=finalizer_under_lock, daemon=True)
        t.start()
        t.join(timeout=5)
        self.assertFalse(t.is_alive(), "connection_destroyed() deadlocked")
        self.assertIn(conn, loop._live_conns)

        loop._loop_will_run(None)

        conn._socket.close.assert_called_once_with()
        self.assertEqual(loop._live_conns, set())
        self.assertEqual(loop._new_conns, set())
        self.assertEqual(loop._closed_conns, set())
        self.assertEqual(loop._deferred_destroyed, [])


    def _reenter_on_close(self, loop, victim):
        # Simulate a GC finalizer closing `victim` while _close_conns() runs
        # under _lock, which is where the reactor exit paths close sockets.
        real_close_conns = loop._close_conns

        def close_conns(conns):
            loop._close_conns = real_close_conns  # close() runs only once
            loop.connection_destroyed(victim)
            real_close_conns(conns)
        loop._close_conns = close_conns

    def test_connection_destroyed_reentered_during_reactor_exit(self):
        """
        No loop pass follows the reactor's exit block, so a close deferred
        there must be finished before the thread ends.

        @jira_ticket GH-1133
        """
        loop = LibevLoop()
        loop._started = True
        loop._loop = Mock()  # start() returns at once with no live conns
        victim = Mock()
        self._reenter_on_close(loop, victim)

        t = threading.Thread(target=loop._run_loop, daemon=True)
        t.start()
        t.join(timeout=5)

        self.assertFalse(t.is_alive())
        victim._socket.close.assert_called_once_with()
        self.assertEqual(loop._deferred_destroyed, [])

    def test_connection_destroyed_reentered_with_reactor_stopped(self):
        """
        With no reactor running, connection_destroyed() closes the socket
        itself under _lock; a close deferred there must still be finished.

        @jira_ticket GH-1133
        """
        loop = LibevLoop()
        conn, victim = Mock(), Mock()
        loop.connection_created(conn)
        loop.connection_created(victim)
        loop._started = False
        self._reenter_on_close(loop, victim)

        t = threading.Thread(target=loop.connection_destroyed, args=(conn,), daemon=True)
        t.start()
        t.join(timeout=5)

        self.assertFalse(t.is_alive(), "connection_destroyed() deadlocked")
        conn._socket.close.assert_called_once_with()
        victim._socket.close.assert_called_once_with()
        self.assertEqual(loop._live_conns, set())
        self.assertEqual(loop._deferred_destroyed, [])


    def test_close_drains_many_deferred_closes(self):
        """
        One GC pass can defer closing every shard connection at once. The
        close() that drains them must not recurse per connection, or it
        dies with RecursionError before failing its own pending requests.

        @jira_ticket GH-1133
        """
        loop = LibevLoop()
        loop._started = True
        deferred = [Mock() for _ in range(sys.getrecursionlimit() + 100)]
        for c in deferred:
            loop.connection_created(c)
        loop._deferred_destroyed.extend(deferred)

        conn = Mock(lock=threading.Lock(), is_closed=False, is_defunct=False, last_error=None)
        loop.connection_created(conn)
        with patch('cassandra.io.libevreactor._global_loop', loop):
            LibevConnection.close(conn)

        conn.error_all_requests.assert_called_once()
        conn.connected_event.set.assert_called_once_with()
        self.assertEqual(loop._deferred_destroyed, [])
        self.assertEqual(loop._live_conns, set())

        loop._loop_will_run(None)
        for c in deferred + [conn]:
            c._socket.close.assert_called_once_with()


class LibevLoopCleanupTest(unittest.TestCase):

    def setUp(self):
        if LibevLoop is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')

    def _start_loop(self):
        """A started LibevLoop with 4 open connections, waiting in ev_run"""
        from cassandra.io import libevwrapper as libev

        class FakeConnection(object):
            deque = ()
            _write_watcher_is_active = False
            _read_watcher = None
            _write_watcher = None
            endpoint = 'fake'

            def __init__(self, sock):
                self._socket = sock
                self._read_watcher = libev.IO(sock.fileno(), libev.EV_READ, loop._loop, Mock())
                self._write_watcher = libev.IO(sock.fileno(), libev.EV_WRITE, loop._loop, Mock())

            def close(self):
                pass

        loop = LibevLoop()
        socks = []

        def teardown():
            # also when an assertion failed: end the loop thread and release
            # the sockets, as nothing else owns this local loop
            loop._cleanup()
            for sock in socks:
                sock.close()
        self.addCleanup(teardown)

        conns = []
        for _ in range(4):
            sock, peer = socket.socketpair()
            socks += [sock, peer]
            conn = FakeConnection(sock)
            loop.connection_created(conn)
            loop.maybe_start()
            conns.append(conn)

        # with open connections and their read watchers active, the loop
        # thread can't leave ev_run until _cleanup() stops the watchers
        deadline = time.time() + 2
        while not all(c._read_watcher.is_active() for c in conns):
            self.assertLess(time.time(), deadline, "read watchers never started")
            time.sleep(0.001)
        self.assertTrue(loop._thread.is_alive())
        return loop, conns

    def test_cleanup_ends_the_loop_thread(self):
        """
        At exit the atexit _cleanup() runs while the loop thread is in ev_run:
        after Cluster.shutdown() it's closing the sockets of the closed
        connections, without it the connections are still open. _cleanup()
        must end that thread, instead of waiting out the join timeout, which
        made every process using the driver exit 5s late.
        """
        loop, conns = self._start_loop()

        thread = loop._thread
        start = time.time()
        loop._cleanup()

        self.assertFalse(thread.is_alive())
        self.assertLess(time.time() - start, 2)

    def test_cleanup_while_loop_thread_arms_the_timer(self):
        """
        _update_timer() checks _shutdown, then runs the timer callbacks and
        arms the loop timer. If _cleanup() stops the timer in between, the
        timer armed after it keeps ev_run going until it fires, so the join
        in _cleanup() waits out its timeout.
        """
        loop, conns = self._start_loop()

        class TimerSpy(object):
            def __init__(self, timer):
                self.timer = timer
                self.stopped = threading.Event()

            def start(self, timeout):
                self.timer.start(timeout)

            def stop(self):
                self.timer.stop()
                self.stopped.set()

        timer = loop._loop_timer = TimerSpy(loop._loop_timer)
        cleanup = threading.Thread(target=loop._cleanup)
        cleanup_started = threading.Event()

        def service_timeouts():
            # runs on the loop thread, after _update_timer() checked _shutdown
            if not cleanup.is_alive() and not loop._shutdown:
                cleanup.start()
                cleanup_started.set()
                # let _cleanup() stop the timer before it's armed below
                timer.stopped.wait(2)
            return time.time() + 100

        thread = loop._thread
        start = time.time()
        with patch.object(loop._timers, 'service_timeouts', service_timeouts):
            loop.notify()  # wake the loop thread to run _update_timer()
            self.assertTrue(cleanup_started.wait(2))
            cleanup.join(10)

        self.assertFalse(thread.is_alive())
        self.assertLess(time.time() - start, 2)


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


class LibevRealSocketTest(unittest.TestCase):

    def test_io_watcher_on_real_socket(self):
        """
        An IO watcher on a real socket must fire. On Windows this used to kill
        the process with 0xC0000409 (scylladb/python-driver#501).
        """
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        from cassandra.io import libevwrapper

        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        fired = []
        loop = libevwrapper.Loop()

        def callback(watcher, revents):
            fired.append(revents)
            watcher.stop()
            timer.stop()

        def timeout():
            watcher.stop()
            timer.stop()

        watcher = libevwrapper.IO(b, libevwrapper.EV_READ, loop, callback)
        # Run the loop on this thread: it returns once both watchers are
        # stopped, so nothing native is left running when the test ends.
        timer = libevwrapper.Timer(loop, timeout)
        try:
            watcher.start()
            timer.start(5)
            a.send(b'x')
            loop.start()
        finally:
            # The callbacks and the native watchers reference each other, and
            # IO/Timer cannot be collected by the GC. Clearing the closure
            # cells frees them (and on Windows the socket duplicate) here.
            del watcher, timer
        self.assertEqual(fired, [libevwrapper.EV_READ])

    def test_io_init_failures_keep_reference_counts(self):
        """
        IO.__init__ failures must release the loop and callback references
        exactly once, and re-initializing a watcher is refused instead of
        leaking what the first initialization acquired.
        """
        if LibevConnection is None:
            raise unittest.SkipTest('libev does not appear to be installed correctly')
        from cassandra.io import libevwrapper

        a, b = socket.socketpair()
        self.addCleanup(a.close)
        self.addCleanup(b.close)
        loop = libevwrapper.Loop()

        def callback(watcher, revents):
            pass

        not_callable = object()
        refs = sys.getrefcount(loop), sys.getrefcount(callback), sys.getrefcount(not_callable)
        for _ in range(10):
            with self.assertRaises(TypeError):
                libevwrapper.IO(b, libevwrapper.EV_READ, loop, not_callable)
            with self.assertRaises((TypeError, ValueError)):
                libevwrapper.IO(-1, libevwrapper.EV_READ, loop, callback)
            with self.assertRaises(TypeError):
                libevwrapper.IO(object(), libevwrapper.EV_READ, loop, callback)
            watcher = libevwrapper.IO(b, libevwrapper.EV_READ, loop, callback)
            with self.assertRaises(RuntimeError):
                watcher.__init__(b, libevwrapper.EV_READ, loop, callback)
            del watcher
        self.assertEqual(
            (sys.getrefcount(loop), sys.getrefcount(callback), sys.getrefcount(not_callable)), refs)
