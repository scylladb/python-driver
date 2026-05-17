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
Reproduction tests for issue #614:
  "[Errno 9] Bad file descriptor" race conditions in AsyncioConnection.

These tests use real socket pairs to demonstrate race conditions where:
  - close() schedules _close() asynchronously, creating a window where
    is_closed=True but the socket fd is still open
  - factory() can return a dead connection when the server closes during setup
  - push() accepts data after close() has been called
  - handle_write() gets EBADF/BrokenPipe when the server closes mid-write

Each test asserts the CORRECT expected behavior. Tests FAIL on the current
codebase because the bugs exist, proving the race conditions are real.
"""

import logging
import socket
import time
import unittest

from cassandra.connection import DefaultEndPoint, ConnectionShutdown

AsyncioConnection, ASYNCIO_AVAILABLE = None, False
try:
    from cassandra.io.asyncioreactor import AsyncioConnection
    ASYNCIO_AVAILABLE = True
except (ImportError, SyntaxError, AttributeError):
    AsyncioConnection = None
    ASYNCIO_AVAILABLE = False

from tests import is_monkey_patched

log = logging.getLogger(__name__)

skip_me = is_monkey_patched() or not ASYNCIO_AVAILABLE


if ASYNCIO_AVAILABLE and AsyncioConnection is not None:
    class SocketPairAsyncioConnection(AsyncioConnection):
        """
        A subclass of AsyncioConnection that uses a pre-provided socket from
        a socketpair instead of connecting to a real CQL server.

        This bypasses _connect_socket() and _send_options_message() so we can
        test I/O race conditions with a real fd registered on the event loop.
        """

        _test_socket = None  # Set before __init__

        def _connect_socket(self):
            """Use the pre-provided test socket instead of connecting."""
            self._socket = self._test_socket

        def _send_options_message(self):
            """Skip the CQL handshake -- we don't have a real server."""
            pass


def _make_connection_with_socketpair():
    """
    Create a SocketPairAsyncioConnection backed by a real socketpair.

    Returns (connection, server_socket) where server_socket is the
    other end of the pair that can be used to send data to / close
    the connection.
    """
    client_sock, server_sock = socket.socketpair()
    client_sock.setblocking(False)
    server_sock.setblocking(False)

    SocketPairAsyncioConnection._test_socket = client_sock
    conn = SocketPairAsyncioConnection(
        DefaultEndPoint("127.0.0.1"),
        connect_timeout=5,
    )
    return conn, server_sock


@unittest.skipIf(is_monkey_patched(), 'runtime is monkey patched for another reactor')
@unittest.skipUnless(ASYNCIO_AVAILABLE, "asyncio is not available for this runtime")
class TestAsyncioRaceConditions614(unittest.TestCase):
    """
    Reproduction tests for race conditions in AsyncioConnection that
    cause "[Errno 9] Bad file descriptor" errors (issue #614).

    These tests FAIL on the current codebase, proving the bugs exist.
    """

    @classmethod
    def setUpClass(cls):
        if skip_me:
            return
        AsyncioConnection.initialize_reactor()

    def test_close_leaves_socket_open_window(self):
        """
        Verify that after close() the socket fd becomes unusable within a
        reasonable time window.

        The asynchronous _close() is scheduled by close(). We wait up to
        0.5 s for the socket to be closed, which is more than enough for the
        event-loop coroutine to run.

        Expected correct behavior: after close() returns and _close() has had
        time to execute, the socket should no longer be usable for I/O.
        """
        conn, server_sock = _make_connection_with_socketpair()
        try:
            # Give handle_read time to start awaiting sock_recv
            time.sleep(0.1)

            # close() sets is_closed=True and schedules _close() asynchronously
            conn.close()
            self.assertTrue(conn.is_closed)

            # Wait for _close() to finish on the event loop thread
            time.sleep(0.5)

            # The socket fd should be closed by now.
            fd = -1
            try:
                fd = conn._socket.fileno()
                socket_still_open = (fd != -1)
            except OSError:
                socket_still_open = False

            self.assertFalse(
                socket_still_open,
                "close() + _close() completed but socket fd={} "
                "is still open. The socket should be closed after "
                "_close() runs.".format(fd)
            )

        finally:
            server_sock.close()

    def test_factory_returns_closed_connection_on_server_eof(self):
        """
        BUG: When the server closes the connection during handshake,
        factory() returns a closed (dead) connection instead of raising.

        The sequence:
          1. Server closes its end -> handle_read gets empty buffer (EOF)
          2. handle_read calls close() -> _close() sets connected_event
          3. factory() wakes up: connected_event is set, last_error is None
          4. factory() returns the connection -- but it's already closed!

        Expected correct behavior: factory() should detect that the connection
        is closed and raise an exception instead of returning a dead connection.
        """
        conn, server_sock = _make_connection_with_socketpair()
        try:
            # Give handle_read time to start awaiting on sock_recv
            time.sleep(0.1)

            # Server closes its end -- handle_read sees EOF (empty recv)
            server_sock.close()

            # Wait for handle_read to process EOF and call close() -> _close()
            # _close() sets connected_event when not defunct
            conn.connected_event.wait(timeout=2.0)

            # Simulate what factory() does after connected_event.wait():
            #   if conn.last_error:
            #       raise conn.last_error
            #   elif not conn.connected_event.is_set():
            #       raise OperationTimedOut(...)
            #   else:
            #       return conn  <-- BUG: returns closed connection
            factory_would_raise = False
            if conn.last_error:
                factory_would_raise = True
            elif not conn.connected_event.is_set():
                factory_would_raise = True

            # factory() should raise when the connection is dead
            self.assertTrue(
                factory_would_raise,
                "BUG: factory() would return a dead connection! "
                "connected_event.is_set()={}, last_error={}, is_closed={}, "
                "is_defunct={}. factory() has no check for is_closed before "
                "returning, so it returns a connection that is already "
                "closed.".format(
                    conn.connected_event.is_set(), conn.last_error,
                    conn.is_closed, conn.is_defunct
                )
            )

        finally:
            if not server_sock._closed:
                server_sock.close()

    def test_push_accepts_data_after_close(self):
        """
        BUG: push() does not check is_closed, so data can be enqueued
        to the write queue after close() has been called.

        This is part of the TOCTOU race in send_msg(): send_msg checks
        is_closed, then calls push(). But even without the TOCTOU in
        send_msg, push() itself never validates the connection state.

        Expected correct behavior: push() should raise ConnectionShutdown
        (or similar) when called on a closed connection.
        """
        conn, server_sock = _make_connection_with_socketpair()
        try:
            # Give handle_read time to start
            time.sleep(0.1)

            # close() sets is_closed=True and schedules _close()
            conn.close()
            self.assertTrue(conn.is_closed)

            # BUG: push() succeeds even though is_closed=True.
            # The data gets enqueued to _write_queue and may cause
            # EBADF when handle_write tries to send it after _close()
            # closes the socket.
            push_succeeded = False
            try:
                conn.push(b'\x00' * 100)
                push_succeeded = True
            except (ConnectionShutdown, OSError):
                push_succeeded = False

            self.assertFalse(
                push_succeeded,
                "BUG: push() accepted data after close() was called "
                "(is_closed=True). This data will be enqueued to the "
                "write queue and handle_write() will attempt to send it, "
                "potentially getting EBADF when _close() closes the socket."
            )

        finally:
            time.sleep(0.5)
            server_sock.close()

    def test_handle_write_defuncts_on_server_close(self):
        """
        When the server closes while data is queued for writing,
        handle_write() and/or handle_read() should defunct the connection
        so that all pending requests are properly errored out.

        The sequence:
          1. Push data into the write queue
          2. Close server end -> handle_read sees EOF, handle_write gets error
          3. The finally block in handle_read/handle_write calls defunct()
          4. All pending requests get ConnectionShutdown errors
        """
        conn, server_sock = _make_connection_with_socketpair()
        try:
            # Give handle_read time to start
            time.sleep(0.1)

            # Push data so handle_write has work to do
            for _ in range(10):
                conn.push(b'\x00' * 65536)

            # Close server end -- triggers EOF in handle_read and
            # BrokenPipe in handle_write
            server_sock.close()

            # Wait for the chain reaction
            time.sleep(1.0)

            self.assertTrue(conn.is_closed,
                            "Connection should be closed after server EOF")

            # Connection should be defunct so pending requests get errored out
            self.assertTrue(
                conn.is_defunct,
                "Connection should be defunct after server close so that "
                "pending requests are properly errored out. "
                "is_defunct={}, last_error={}".format(
                    conn.is_defunct, conn.last_error)
            )

        finally:
            if not server_sock._closed:
                server_sock.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
