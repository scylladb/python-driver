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
Tests for the libev close() race condition fix (scylladb/python-driver#614).

The race: close() sets is_closed=True and calls _socket.close() immediately,
but libev watchers are stopped asynchronously in _loop_will_run().  Between
socket close and watcher stop, handle_read()/handle_write() can fire on the
closed fd.

The fix adds early-return guards at the top of handle_read() and handle_write()
that check is_closed/is_defunct before touching the socket, and preserves
last_error in close() when connected_event is unset.
"""

import unittest
from unittest.mock import patch, MagicMock

from cassandra import DependencyException

try:
    from cassandra.io.libevreactor import LibevConnection
except (ImportError, DependencyException):
    LibevConnection = None

from tests import is_monkey_patched


def _skip_if_unavailable(test_case):
    if is_monkey_patched():
        raise unittest.SkipTest("Can't test libev with monkey patching")
    if LibevConnection is None:
        raise unittest.SkipTest('libev does not appear to be installed correctly')


class LibevCloseRaceTest(unittest.TestCase):
    """
    Tests for the close-race fix in LibevConnection (issue #614).

    Each test simulates the scenario where a watcher fires after close()
    has already been called, verifying that the handler exits gracefully
    without calling defunct() or raising.
    """

    def setUp(self):
        _skip_if_unavailable(self)
        LibevConnection.initialize_reactor()

        self.patchers = [
            patch('socket.socket'),
            patch('cassandra.io.libevwrapper.IO'),
            patch('cassandra.io.libevreactor.LibevLoop.maybe_start'),
        ]
        for p in self.patchers:
            p.start()
        self._connections = []

    def tearDown(self):
        for c in self._connections:
            c.close()
        for p in self.patchers:
            p.stop()

    def _make_connection(self):
        from cassandra.connection import DefaultEndPoint
        c = LibevConnection(DefaultEndPoint('1.2.3.4'), cql_version='3.0.1', connect_timeout=5)
        mock_socket = MagicMock()
        mock_socket.send.side_effect = lambda x: len(x)
        mock_socket.recv.return_value = b''
        c._socket = mock_socket
        self._connections.append(c)
        return c

    # ------------------------------------------------------------------
    # handle_write guards
    # ------------------------------------------------------------------

    def test_handle_write_returns_immediately_when_closed(self):
        """
        handle_write() must be a no-op if is_closed is already True.
        This prevents EBADF when the watcher fires after close().
        """
        c = self._make_connection()
        c.is_closed = True
        c.deque.append(b"data")

        # Should not raise and should not call send()
        c.handle_write(None, 0)
        c._socket.send.assert_not_called()

    def test_handle_write_returns_immediately_when_defunct(self):
        """
        handle_write() must be a no-op if is_defunct is already True.
        """
        c = self._make_connection()
        c.is_defunct = True
        c.deque.append(b"data")

        c.handle_write(None, 0)
        c._socket.send.assert_not_called()

    # ------------------------------------------------------------------
    # handle_read guards
    # ------------------------------------------------------------------

    def test_handle_read_returns_immediately_when_closed(self):
        """
        handle_read() must be a no-op if is_closed is already True.
        This prevents EBADF when the watcher fires after close().
        """
        c = self._make_connection()
        c.is_closed = True

        # Should not raise and should not call recv()
        c.handle_read(None, 0)
        c._socket.recv.assert_not_called()

    def test_handle_read_returns_immediately_when_defunct(self):
        """
        handle_read() must be a no-op if is_defunct is already True.
        """
        c = self._make_connection()
        c.is_defunct = True

        c.handle_read(None, 0)
        c._socket.recv.assert_not_called()

    # ------------------------------------------------------------------
    # handle_read EOF sets last_error
    # ------------------------------------------------------------------

    def test_handle_read_eof_sets_last_error_before_close(self):
        """
        When recv() returns empty bytes (EOF / server closed connection),
        last_error must be set before close() is called so that
        factory() can detect the dead connection.
        """
        c = self._make_connection()
        c._socket.recv.return_value = b''

        with patch.object(c, 'close') as mock_close:
            c.handle_read(None, 0)
            mock_close.assert_called_once()
            self.assertIsNotNone(c.last_error)
            self.assertIn("closed by server", str(c.last_error))

    # ------------------------------------------------------------------
    # close() preserves last_error for factory()
    # ------------------------------------------------------------------

    def test_close_sets_last_error_when_connected_event_not_set(self):
        """
        When close() is called before connected_event is set (i.e. the
        connection was never fully established), last_error must be
        populated so factory() doesn't return a dead connection.
        """
        c = self._make_connection()
        # connected_event is not set by default in a fresh connection
        c.connected_event.clear()

        c.close()

        self.assertIsNotNone(c.last_error)
        self.assertIn("was closed", str(c.last_error))
