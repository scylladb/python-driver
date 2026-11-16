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
import atexit
from collections import deque
import logging
import os
import socket
import ssl
from threading import Event, Lock, RLock, Thread, current_thread
import time

from cassandra import DependencyException
try:
    import cassandra.io.libevwrapper as libev
except ImportError:
    raise DependencyException(
        "The C extension needed to use libev was not found.  This "
        "probably means that you didn't have the required build dependencies "
        "when installing the driver.  See "
        "https://python-driver.docs.scylladb.com/stable/installation.html#libev-support "
        "for instructions on installing build dependencies and building "
        "the C extension.")

from cassandra.connection import (Connection, ConnectionShutdown,
                                  NONBLOCKING, Timer, TimerManager)


log = logging.getLogger(__name__)


def _cleanup(loop):
    if loop:
        loop._cleanup()


class LibevLoop(object):

    def __init__(self):
        self._pid = os.getpid()
        self._loop = libev.Loop()
        self._notifier = libev.Async(self._loop)
        self._notifier.start()

        # prevent _notifier from keeping the loop from returning
        self._loop.unref()

        self._started = False
        self._shutdown = False
        # Single lock for _started/_shutdown *and* _live_conns/_new_conns/
        # _closed_conns. The exit check in _run_loop() must see the started/
        # shutdown flags and the live connection set as one atomic snapshot,
        # otherwise connection_created() can register a connection in the
        # instant after the exit check reads an empty _live_conns but before
        # _started is flipped to False -- a lost wakeup that hangs the
        # connection forever (see issue #980). Two separate locks can't give
        # that atomicity no matter which one each side takes, so there is
        # only one lock here, not a hold-both-locks protocol.
        # It is an RLock only so connection_destroyed() can detect being
        # re-entered by a thread that already holds it, via _is_owned():
        # private, but threading.Condition relies on it too.
        self._lock = RLock()
        self._lock_thread = RLock()

        self._thread = None

        # set of all connections; only replaced with a new copy
        # while holding _lock, never modified in place
        self._live_conns = set()
        # newly created connections that need their write/read watcher started
        self._new_conns = set()
        # recently closed connections that need their write/read watcher stopped
        self._closed_conns = set()
        # connections closed while their thread held _lock; drained by the loop
        self._deferred_destroyed = []
        self._management_callbacks = deque()
        self._management_users = set()
        self._cleanup_started = False
        self._dispose_on_exit = False
        self._runner_exited = True

        self._preparer = libev.Prepare(self._loop, self._loop_will_run)
        # prevent _preparer from keeping the loop from returning
        self._loop.unref()
        self._preparer.start()

        self._timers = TimerManager()
        self._loop_timer = libev.Timer(self._loop, self._on_loop_timer)
        self._management_keeper = libev.Timer(self._loop, self._keep_management_alive)

    def maybe_start(self):
        # Serialize all admissions with startup, including its failure path.
        with self._lock_thread:
            with self._lock:
                if self._shutdown:
                    raise RuntimeError("Libev reactor is shutting down")
                if not self._started:
                    log.debug("Starting libev event loop")
                    self._started = True
                    self._runner_exited = False
                    self._thread = Thread(target=self._run_loop_if_current, name="event_loop")
                    self._thread.daemon = True
                    try:
                        self._thread.start()
                    except BaseException:
                        if not self._thread.is_alive():
                            self._started = False
                            self._thread = None
                        raise
            self._notifier.send()

    def _run_loop_if_current(self):
        # start() can be interrupted after native launch but before bootstrap.
        # Rollback and replacement use these locks too; an invalidated target
        # must retire without entering native code or publishing owner state.
        with self._lock_thread:
            with self._lock:
                if current_thread() is not self._thread:
                    return
        self._run_loop()

    def _run_loop(self):
        while True:
            self._loop.start()
            # there are still active watchers, no deadlock
            with self._lock:
                # Reading _live_conns and deciding/committing the exit here
                # happen atomically under the same lock that guards
                # connection_created()/connection_destroyed(). So any
                # concurrent connection_created() either finishes-before this
                # read (its connection is seen in _live_conns, loop
                # restarts) or finishes-after this block sets _started =
                # False (maybe_start(), called right after
                # connection_created(), then observes _started == False and
                # starts a fresh thread). There is no interleaving in which
                # the new connection is invisible to both.
                if not self._shutdown and (self._live_conns or self._management_callbacks):
                    log.debug("Restarting event loop")
                    continue
                else:
                    # all Connections have been closed, no active watchers
                    log.debug("All Connections currently closed, event loop ended")
                    shutdown = self._shutdown
                    self._started = False
                    # A connection may have been closed after the last
                    # _loop_will_run() pass, which is where closed
                    # connections' sockets are closed. No reactor thread
                    # will run it again for them, so close them here.
                    # This happens under the lock, so a new reactor thread
                    # can't be touching the watchers concurrently. On
                    # shutdown, _cleanup() closes all connections itself.
                    if not self._shutdown:
                        to_stop = self._closed_conns
                        self._closed_conns = set()
                        self._close_conns(to_stop)
                    break
        self._drain_deferred()
        # Only the shutdown owner may drain after publishing stopped state.
        # Ordinary idle exit allows a replacement runner to claim new work.
        if shutdown:
            while self._management_callbacks:
                self._dispatch_management()
        with self._lock:
            # A retired runner cannot certify the replacement's native exit.
            if current_thread() is self._thread:
                self._runner_exited = True
                if self._dispose_on_exit:
                    self._release_loop()

    def _cleanup_unpublished(self):
        # An unpublished candidate has no future owner. If bounded cleanup
        # cannot join its runner, that runner releases the native callbacks
        # after returning from libev and draining accepted work.
        self._cleanup()
        with self._lock:
            self._dispose_on_exit = True
            if (self._thread is None or not self._thread.is_alive()
                    or self._runner_exited):
                self._release_loop()

    def _release_loop(self):
        # Native watchers retain bound callbacks without GC traversal support.
        # Stop/join must precede this outside the single surviving fork thread.
        self._preparer = self._loop_timer = self._management_keeper = None
        self._notifier = None
        self._loop = None

    def _cleanup(self):
        with self._lock_thread:
            with self._lock:
                if self._cleanup_started:
                    return
                self._cleanup_started = True
                self._shutdown = True
        # Stop the prepare watcher first to prevent race conditions.
        # It was unref'd when started, so ref the loop back first: stopping it
        # unbalanced leaves libev's active count at -1, and a loop thread still
        # in ev_run then never returns, so the join below waits its full timeout.
        # The timer is stopped by _update_timer() once _shutdown is set, but that
        # runs from the prepare watcher, so stop it here too
        if self._preparer:
            self._loop.ref()
            self._preparer.stop()
        self._loop_timer.stop()
        self._management_keeper.stop()

        # TODO: unguarded read; sets are copy-on-write, so at most a stale
        # snapshot (redundant idempotent close), never a torn read.
        for conn in self._live_conns | self._new_conns | self._closed_conns:
            conn.close()
            for watcher in (conn._write_watcher, conn._read_watcher):
                if watcher:
                    watcher.stop()
            conn._socket.close()

        self.notify()  # wake the timer watcher

        # PYTHON-752 Thread might have just been created and not started
        # Use longer timeout to allow proper cleanup
        with self._lock_thread:
            thread = self._thread
        if thread is not None and thread is not current_thread():
            thread.join(timeout=5.0)

        if self._thread is not None and self._thread.is_alive():
            log.warning(
                "Event loop thread could not be joined, so shutdown may not be clean. "
                "Please call Cluster.shutdown() to avoid this.")

        log.debug("Event loop thread was joined")

    def _cleanup_after_fork(self):
        # Only this thread survived. Do not enter inherited Python locks,
        # invoke connection callbacks, or join vanished parent threads.
        self._shutdown = True
        for conn in self._live_conns | self._new_conns | self._closed_conns:
            try:
                conn._socket.close()
            except OSError:
                pass
            conn._read_watcher = conn._write_watcher = None
        self._live_conns = self._new_conns = self._closed_conns = set()
        self._deferred_destroyed = []
        self._management_callbacks.clear()
        self._management_users.clear()
        # Break watcher/callback cycles and release the native loop, whose
        # destructor closes its internal wakeup descriptors in the child.
        self._release_loop()

    def add_timer(self, timer):
        self._timers.add_timer(timer)
        self._notifier.send()  # wake up in case this timer is earlier

    def _update_timer(self):
        if not self._shutdown:
            next_end = self._timers.service_timeouts()
            if next_end:
                self._loop_timer.start(next_end - time.time())  # timer handles negative values
        # _cleanup() may have run meanwhile (service_timeouts() runs callbacks)
        # and stopped the timer before it was armed above. Check again, or the
        # armed timer keeps ev_run going and the join in _cleanup() times out
        if self._shutdown:
            self._loop_timer.stop()

    def _on_loop_timer(self):
        self._timers.service_timeouts()

    def call_soon(self, callback):
        with self._lock_thread:
            with self._lock:
                if self._shutdown:
                    raise RuntimeError("Libev reactor is shutting down")
                self._management_callbacks.append(callback)
            try:
                self.maybe_start()
            except BaseException:
                with self._lock:
                    try:
                        self._management_callbacks.remove(callback)
                    except ValueError:
                        return  # Already claimed: the dispatch was accepted.
                raise

    def _keep_management_alive(self):
        if self._management_users and not self._shutdown:
            self._management_keeper.start(3600)
        # Cleanup or release can stop the timer while start() is in flight.
        if self._shutdown or not self._management_users:
            self._management_keeper.stop()

    def borrow_management(self):
        token = object()
        ready, abandoned = Event(), Event()

        def acquire():
            if abandoned.is_set():
                return
            self._management_users.add(token)
            self._keep_management_alive()
            if abandoned.is_set():
                release(lambda: None)
            else:
                ready.set()

        def release(completed):
            def detach():
                self._management_users.discard(token)
                if not self._management_users:
                    self._management_keeper.stop()
                completed()
            if current_thread() is self._thread:
                detach()
            else:
                # Cleanup closes admission under this same lock. Either queue
                # the release before shutdown or detach after it has begun.
                with self._lock_thread:
                    if self._shutdown or self._thread is None or not self._thread.is_alive():
                        with self._lock:
                            detach()
                    else:
                        self.call_soon(detach)

        if current_thread() is self._thread:
            with self._lock:
                if self._shutdown:
                    raise RuntimeError("Libev reactor is shutting down")
                acquire()
        else:
            self.call_soon(acquire)
            if not ready.wait(5):
                abandoned.set()
                release(lambda: None)
                raise RuntimeError('Libev management borrow did not complete')
        return release

    def _dispatch_management(self):
        # Bounded batches keep management completions fair to socket I/O.
        for _ in range(64):
            with self._lock:
                if not self._management_callbacks:
                    return
                callback = self._management_callbacks.popleft()
            try:
                callback()
            except BaseException:
                log.exception("Error in management reactor dispatch")
        self._notifier.send()

    def notify(self):
        self._notifier.send()

    def connection_created(self, conn):
        with self._lock:
            new_live_conns = self._live_conns.copy()
            new_live_conns.add(conn)
            self._live_conns = new_live_conns

            new_new_conns = self._new_conns.copy()
            new_new_conns.add(conn)
            self._new_conns = new_new_conns

    def connection_destroyed(self, conn):
        self._connection_destroyed(conn)
        self._drain_deferred()

    def _connection_destroyed(self, conn):
        if self._lock._is_owned():
            # Re-entered on a thread that already holds _lock: any allocation
            # inside a locked block can run a GC finalizer (Session.__del__ ->
            # shutdown() -> conn.close()). Updating the sets here would be
            # overwritten by the enclosing copy-on-write update, and with a
            # plain Lock it deadlocked (GH-1133). Finish it once the lock
            # is released, see _drain_deferred().
            self._deferred_destroyed.append(conn)
            self._notifier.send()
            return

        with self._lock:
            new_conns = self._new_conns.copy()
            new_conns.discard(conn)
            self._new_conns = new_conns

            new_live_conns = self._live_conns.copy()
            new_live_conns.discard(conn)
            self._live_conns = new_live_conns

            if not self._started and not self._shutdown:
                # No reactor thread is running to close the socket in
                # _loop_will_run(), and none is going to start for an
                # already-closed connection, so close it here. Holding
                # the lock ensures no reactor thread starts meanwhile.
                # On shutdown, _cleanup() closes all connections itself.
                self._close_conns((conn,))
            else:
                new_closed_conns = self._closed_conns.copy()
                new_closed_conns.add(conn)
                self._closed_conns = new_closed_conns

        self._notifier.send()

    def _drain_deferred(self):
        # Called after every release of _lock that may have deferred a
        # connection_destroyed(): end of connection_destroyed(), each loop
        # pass, and reactor exit, which nothing else would run after.
        # Can run on several threads at once, so pop() can lose the race.
        # Calls the non-draining variant: closes deferred meanwhile are
        # picked up by this loop, so the stack doesn't grow per connection.
        if self._lock._is_owned():
            return  # still inside the locked block that deferred them
        while True:
            try:
                conn = self._deferred_destroyed.pop()
            except IndexError:
                return
            self._connection_destroyed(conn)

    @staticmethod
    def _close_conns(conns):
        for conn in conns:
            if conn._write_watcher:
                conn._write_watcher.stop()
                # clear reference cycles from IO callback
                del conn._write_watcher
            if conn._read_watcher:
                conn._read_watcher.stop()
                # clear reference cycles from IO callback
                del conn._read_watcher
            conn._socket.close()
            log.debug("Closed socket to %s", conn.endpoint)

    def _loop_will_run(self, prepare):
        changed = False
        self._drain_deferred()
        self._dispatch_management()

        # TODO: unguarded read; copy-on-write set, self-heals next tick.
        for conn in self._live_conns:
            if not conn.deque and conn._write_watcher_is_active:
                if conn._write_watcher:
                    conn._write_watcher.stop()
                conn._write_watcher_is_active = False
                changed = True
            elif conn.deque and not conn._write_watcher_is_active:
                conn._write_watcher.start()
                conn._write_watcher_is_active = True
                changed = True

        if self._new_conns:
            with self._lock:
                to_start = self._new_conns
                self._new_conns = set()

            for conn in to_start:
                if conn._read_watcher:
                    conn._read_watcher.start()

            changed = True

        if self._closed_conns:
            with self._lock:
                to_stop = self._closed_conns
                self._closed_conns = set()

            self._close_conns(to_stop)

            changed = True

        # TODO: update to do connection management, timer updates through dedicated async 'notifier' callbacks
        self._update_timer()

        if changed:
            self._notifier.send()


def _atexit_cleanup():
    """Cleanup function called by atexit that uses the current _global_loop value.

    This wrapper ensures that cleanup receives the actual LibevLoop instance
    instead of None, which was the value of _global_loop when the module was
    imported.
    """
    global _global_loop
    if _global_loop is not None:
        _cleanup(_global_loop)


_global_loop = None
_initialization_lock = Lock()
atexit.register(_atexit_cleanup)


class LibevConnection(Connection):
    """
    An implementation of :class:`.Connection` that uses libev for its event loop.
    """
    _write_watcher_is_active = False
    _read_watcher = None
    _write_watcher = None
    _socket = None

    @classmethod
    def initialize_reactor(cls):
        global _global_loop
        with _initialization_lock:
            if _global_loop is not None and _global_loop._pid != os.getpid():
                _global_loop._cleanup_after_fork()
                _global_loop = None
            if _global_loop is None:
                candidate = LibevLoop()
                ready = Event()
                try:
                    candidate.call_soon(ready.set)
                    if not ready.wait(5):
                        raise RuntimeError("Libev reactor startup did not complete")
                except BaseException:
                    candidate._cleanup_unpublished()
                    raise
                _global_loop = candidate

    @classmethod
    def _management_dispatch(cls, callback):
        if _global_loop is None:
            raise RuntimeError("Libev reactor is unavailable")
        _global_loop.call_soon(callback)

    @classmethod
    def _management_borrow(cls):
        return _global_loop.borrow_management()

    @classmethod
    def _management_in_context(cls):
        return _global_loop is not None and current_thread() is _global_loop._thread

    @classmethod
    def handle_fork(cls):
        global _global_loop
        if _global_loop:
            _global_loop._cleanup()
            _global_loop = None

    @classmethod
    def create_timer(cls, timeout, callback):
        timer = Timer(timeout, callback)
        _global_loop.add_timer(timer)
        return timer

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)

        self.deque = deque()
        self._deque_lock = Lock()
        self._connect_socket()
        self._socket.setblocking(0)

        try:
            with _global_loop._lock:
                self._read_watcher = libev.IO(self._socket.fileno(), libev.EV_READ, _global_loop._loop, self.handle_read)
                self._write_watcher = libev.IO(self._socket.fileno(), libev.EV_WRITE, _global_loop._loop, self.handle_write)
        except Exception:
            # Not registered with the loop yet, so nothing else will clean up.
            # Dropping the watcher breaks its cycle with this connection
            # through the bound callback, which IO cannot collect.
            self._read_watcher = self._write_watcher = None
            self._socket.close()
            raise

        self._send_options_message()

        _global_loop.connection_created(self)

        # start the global event loop if needed
        _global_loop.maybe_start()

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.endpoint)

        _global_loop.connection_destroyed(self)

        # don't leave in-progress operations hanging
        if not self.is_defunct:
            msg = "Connection to %s was closed" % self.endpoint
            if self.last_error:
                msg += ": %s" % (self.last_error,)
            shutdown_exc = ConnectionShutdown(msg)
            self.error_all_requests(shutdown_exc)
            if not self.connected_event.is_set():
                self.last_error = shutdown_exc
            self.connected_event.set()

    def handle_write(self, watcher, revents, errno=None):
        if self.is_closed:
            return
        if revents & libev.EV_ERROR:
            if errno:
                exc = IOError(errno, os.strerror(errno))
            else:
                exc = Exception("libev reported an error")

            self.defunct(exc)
            return

        while True:
            try:
                with self._deque_lock:
                    next_msg = self.deque.popleft()
            except IndexError:
                if not self._socket_writable:
                    self._socket_writable = True
                return

            try:
                sent = self._socket.send(next_msg)
            except socket.error as err:
                if (err.args[0] in NONBLOCKING or
                        err.args[0] in (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE)):
                    if err.args[0] in NONBLOCKING:
                        self._socket_writable = False
                    with self._deque_lock:
                        self.deque.appendleft(next_msg)
                else:
                    self.defunct(err)
                return
            else:
                if sent < len(next_msg):
                    with self._deque_lock:
                        self.deque.appendleft(next_msg[sent:])
                    # we've seen some cases that 0 is returned instead of NONBLOCKING. But usually,
                    # we don't expect this to happen. https://bugs.python.org/issue20951
                    if sent == 0:
                        self._socket_writable = False
                        return

    def handle_read(self, watcher, revents, errno=None):
        if self.is_closed:
            return
        if revents & libev.EV_ERROR:
            if errno:
                exc = IOError(errno, os.strerror(errno))
            else:
                exc = Exception("libev reported an error")

            self.defunct(exc)
            return
        try:
            while True:
                buf = self._socket.recv(self.in_buffer_size)
                self._iobuf.write(buf)
                if len(buf) < self.in_buffer_size:
                    break
        except socket.error as err:
            if isinstance(err, ssl.SSLError):
                if err.args[0] in (ssl.SSL_ERROR_WANT_READ, ssl.SSL_ERROR_WANT_WRITE):
                    if not self._iobuf.tell():
                        return
                else:
                    self.defunct(err)
                    return
            elif err.args[0] in NONBLOCKING:
                if not self._iobuf.tell():
                    return
            else:
                self.defunct(err)
                return

        if self._iobuf.tell():
            self.process_io_buffer()
        else:
            log.debug("Connection %s closed by server", self)
            self.last_error = ConnectionShutdown(
                "Connection to %s was closed by server" % self.endpoint)
            self.close()

    def push(self, data):
        sabs = self.out_buffer_size
        if len(data) > sabs:
            chunks = []
            for i in range(0, len(data), sabs):
                chunks.append(data[i:i + sabs])
        else:
            chunks = [data]

        with self._deque_lock:
            self.deque.extend(chunks)
            _global_loop.notify()


def _reset_management_startup_lock_after_fork():
    global _initialization_lock
    _initialization_lock = Lock()


if hasattr(os, 'register_at_fork'):
    os.register_at_fork(after_in_child=_reset_management_startup_lock_after_fork)
