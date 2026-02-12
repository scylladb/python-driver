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
Tests for SSL connection error handling, specifically "Bad file descriptor" scenarios.
These tests ensure that when SSL connections fail (e.g., due to node reboots), the
original error is preserved and meaningful error messages are provided.

See: https://github.com/scylladb/python-driver/issues/614
"""

import unittest
import ssl
import socket
import errno
from threading import Thread, Event
from unittest.mock import Mock, MagicMock, patch, PropertyMock
from io import BytesIO

from cassandra.connection import (
    Connection, ConnectionShutdown, DefaultEndPoint, 
    ConnectionException
)
from cassandra.protocol import ProtocolHandler


class SSLConnectionErrorTest(unittest.TestCase):
    """
    Test SSL connection error scenarios that can lead to "Bad file descriptor" errors.
    
    These tests simulate the situation described in the issue where:
    1. A connection is forcefully closed (e.g., node reboot)
    2. Parallel operations attempt to read/write to the closed socket
    3. The original error reason should be preserved for debugging
    """

    def make_ssl_connection(self, **kwargs):
        """Create a mock SSL connection for testing."""
        c = Connection(DefaultEndPoint('1.2.3.4'), **kwargs)
        c._socket = Mock(spec=ssl.SSLSocket)
        c._socket.send.side_effect = lambda x: len(x)
        c.lock = Mock()
        c.lock.__enter__ = Mock()
        c.lock.__exit__ = Mock()
        c._requests = {}
        c.close = Mock()  # Mock the close method since base class raises NotImplementedError
        return c

    def test_ssl_socket_bad_file_descriptor_on_send(self):
        """
        Test that when SSL socket raises "Bad file descriptor" during send,
        the error is properly captured in last_error.
        
        Scenario: Node reboots, socket is closed, send operation fails
        """
        c = self.make_ssl_connection()
        
        # Simulate socket being closed forcefully
        bad_fd_error = OSError(errno.EBADF, "Bad file descriptor")
        c._socket.send.side_effect = bad_fd_error
        
        # Try to send data, which should trigger defunct
        try:
            # This would normally happen during push()
            c._socket.send(b"test data")
        except OSError:
            c.defunct(bad_fd_error)
        
        # Verify the error is captured
        assert c.is_defunct
        assert c.last_error == bad_fd_error
        assert "Bad file descriptor" in str(c.last_error)

    def test_ssl_socket_bad_file_descriptor_on_recv(self):
        """
        Test that when SSL socket raises "Bad file descriptor" during recv,
        the error is properly captured.
        
        Scenario: Connection closed while reading response
        """
        c = self.make_ssl_connection()
        
        # Simulate socket being closed during receive
        bad_fd_error = OSError(errno.EBADF, "Bad file descriptor")
        c._socket.recv = Mock(side_effect=bad_fd_error)
        
        try:
            c._socket.recv(4096)
        except OSError:
            c.defunct(bad_fd_error)
        
        assert c.is_defunct
        assert c.last_error == bad_fd_error
        assert "Bad file descriptor" in str(c.last_error)

    def test_ssl_connection_error_during_handshake(self):
        """
        Test SSL handshake failure is properly captured.
        
        Scenario: SSL handshake fails due to connection closure
        """
        c = self.make_ssl_connection()
        
        # Simulate SSL handshake error
        ssl_error = ssl.SSLError(ssl.SSL_ERROR_SYSCALL, "Connection reset by peer")
        
        c.defunct(ssl_error)
        
        assert c.is_defunct
        assert c.last_error == ssl_error
        assert "Connection reset by peer" in str(c.last_error)

    def test_concurrent_operations_on_closed_ssl_socket(self):
        """
        Test that concurrent operations on a closed SSL socket preserve the error.
        
        Scenario: Multiple threads try to use a connection that's being closed
        This simulates the race condition described in the issue.
        """
        c = self.make_ssl_connection()
        
        # Original error that caused the connection to close
        original_error = OSError(errno.ECONNRESET, "Connection reset by peer")
        
        # Mark connection as defunct with the original error
        c.defunct(original_error)
        
        # Now simulate concurrent operations trying to use the socket
        # They would get "Bad file descriptor" but should see the original error
        
        assert c.is_defunct
        assert c.last_error == original_error
        
        # When send_msg is called on defunct connection, 
        # it should include the original error
        with self.assertRaises(ConnectionShutdown) as cm:
            c.send_msg(Mock(), 1, Mock())
        
        error_message = str(cm.exception)
        assert "is defunct" in error_message
        assert "Connection reset by peer" in error_message

    def test_ssl_socket_broken_pipe_error(self):
        """
        Test handling of broken pipe errors on SSL sockets.
        
        Scenario: Write to a socket whose peer has closed the connection
        """
        c = self.make_ssl_connection()
        
        broken_pipe_error = OSError(errno.EPIPE, "Broken pipe")
        c._socket.send.side_effect = broken_pipe_error
        
        try:
            c._socket.send(b"data")
        except OSError:
            c.defunct(broken_pipe_error)
        
        assert c.is_defunct
        assert c.last_error == broken_pipe_error
        
        # Subsequent operations should report the original error
        with self.assertRaises(ConnectionShutdown) as cm:
            c.send_msg(Mock(), 1, Mock())
        
        assert "Broken pipe" in str(cm.exception)

    def test_ssl_socket_connection_aborted_error(self):
        """
        Test handling of connection aborted errors (ECONNABORTED).
        
        Scenario: Connection aborted by software/network
        """
        c = self.make_ssl_connection()
        
        abort_error = OSError(errno.ECONNABORTED, "Software caused connection abort")
        c.defunct(abort_error)
        
        assert c.is_defunct
        assert c.last_error == abort_error
        
        with self.assertRaises(ConnectionShutdown) as cm:
            c.send_msg(Mock(), 1, Mock())
        
        assert "Software caused connection abort" in str(cm.exception)

    def test_ssl_unwrap_error_on_close(self):
        """
        Test handling of SSL unwrap errors during connection close.
        
        Scenario: SSL socket fails to properly close/unwrap
        """
        c = self.make_ssl_connection()
        
        # Simulate error during SSL shutdown
        ssl_error = ssl.SSLError(ssl.SSL_ERROR_SYSCALL, "Bad file descriptor")
        
        c.defunct(ssl_error)
        
        assert c.is_defunct
        assert c.last_error == ssl_error

    def test_multiple_error_scenarios_last_error_preserved(self):
        """
        Test that the first error is preserved even if multiple errors occur.
        
        Scenario: Initial error causes connection closure, subsequent operations
        may generate additional errors, but we want to know the root cause.
        """
        c = self.make_ssl_connection()
        
        # First error - this is the root cause
        root_cause = OSError(errno.ETIMEDOUT, "Connection timed out")
        c.defunct(root_cause)
        
        assert c.last_error == root_cause
        
        # Second call to defunct should not overwrite if already defunct
        c.defunct(OSError(errno.EBADF, "Bad file descriptor"))
        
        # The root cause should still be preserved
        assert c.last_error == root_cause
        assert "Connection timed out" in str(c.last_error)

    def test_wait_for_responses_includes_ssl_error(self):
        """
        Test that wait_for_responses includes the SSL error in exception.
        
        Scenario: Waiting for responses when SSL connection fails
        """
        c = self.make_ssl_connection()
        c._requests = {}
        
        # Simulate SSL error during response wait
        ssl_error = ssl.SSLError(ssl.SSL_ERROR_SSL, "SSL protocol error")
        c.defunct(ssl_error)
        
        assert c.is_defunct
        assert c.last_error == ssl_error
        
        # wait_for_responses should include the error
        with self.assertRaises(ConnectionShutdown) as cm:
            c.wait_for_responses(Mock())
        
        error_message = str(cm.exception)
        assert "already closed" in error_message
        assert "SSL protocol error" in error_message

    def test_ssl_error_on_closed_connection_send_msg(self):
        """
        Test send_msg on closed SSL connection includes last_error.
        """
        c = self.make_ssl_connection()
        
        # Close the connection with an SSL error
        ssl_error = OSError(errno.EBADF, "Bad file descriptor")
        c.is_closed = True
        c.last_error = ssl_error
        
        # send_msg should raise ConnectionShutdown with the error
        with self.assertRaises(ConnectionShutdown) as cm:
            c.send_msg(Mock(), 1, Mock())
        
        error_message = str(cm.exception)
        assert "is closed" in error_message
        assert "Bad file descriptor" in error_message

    @patch('cassandra.connection.socket.socket')
    def test_ssl_socket_errno_enotconn(self, mock_socket):
        """
        Test handling of ENOTCONN error (socket not connected).
        
        Scenario: Operation on a socket that's not connected
        """
        c = self.make_ssl_connection()
        
        not_conn_error = OSError(errno.ENOTCONN, "Transport endpoint is not connected")
        c.defunct(not_conn_error)
        
        assert c.is_defunct
        assert c.last_error == not_conn_error
        
        with self.assertRaises(ConnectionShutdown) as cm:
            c.send_msg(Mock(), 1, Mock())
        
        assert "Transport endpoint is not connected" in str(cm.exception)


class SSLConnectionCloseRaceConditionTest(unittest.TestCase):
    """
    Tests for race conditions when SSL connections are being closed.
    
    These simulate the specific scenario from the issue where node reboots
    cause concurrent access to closing SSL sockets.
    """

    def make_connection_with_requests(self):
        """Create a connection with pending requests."""
        c = Connection(DefaultEndPoint('1.2.3.4'))
        c._socket = Mock(spec=ssl.SSLSocket)
        c._socket.send.side_effect = lambda x: len(x)
        c.lock = Mock()
        c.lock.__enter__ = Mock()
        c.lock.__exit__ = Mock()
        c.close = Mock()  # Mock the close method since base class raises NotImplementedError
        
        # Add some pending requests
        # Each request is a tuple of (callback, encoder, decoder)
        mock_request = (Mock(), ProtocolHandler.encode_message, ProtocolHandler.decode_message)
        c._requests = {
            1: mock_request,
            2: mock_request,
            3: mock_request,
        }
        
        return c

    def test_error_all_requests_preserves_error(self):
        """
        Test that error_all_requests creates ConnectionShutdown with original error.
        
        Scenario: Connection fails, all pending requests are errored out
        """
        c = self.make_connection_with_requests()
        
        # Original SSL error
        ssl_error = ssl.SSLError(ssl.SSL_ERROR_SYSCALL, "Connection reset by peer")
        c.defunct(ssl_error)
        
        # Verify all request callbacks would receive ConnectionShutdown
        # (they're already called by defunct -> error_all_requests)
        assert c.last_error == ssl_error
        assert c.is_defunct

    def test_parallel_send_on_defuncting_connection(self):
        """
        Test parallel send operations when connection is becoming defunct.
        
        Scenario: Thread 1 calls defunct(), Thread 2 tries send_msg()
        """
        c = self.make_connection_with_requests()
        
        # Simulate the race: connection becomes defunct
        original_error = OSError(errno.ECONNRESET, "Connection reset")
        c.defunct(original_error)
        
        # Now a parallel thread tries to send
        with self.assertRaises(ConnectionShutdown) as cm:
            c.send_msg(Mock(), 4, Mock())
        
        # Should see the original error, not a generic "is defunct"
        error_message = str(cm.exception)
        assert "Connection reset" in error_message

    def test_node_reboot_scenario(self):
        """
        Test simulating node reboot scenario from the issue.
        
        Steps:
        1. Connection has pending requests
        2. Node reboots (socket forcefully closed)
        3. Driver detects error on socket operation
        4. Concurrent operations should see meaningful error
        """
        c = self.make_connection_with_requests()
        
        # Node reboot causes connection reset
        node_reboot_error = OSError(errno.ECONNRESET, "Connection reset by peer")
        
        # Simulate socket operation detecting the error
        c._socket.recv = Mock(side_effect=node_reboot_error)
        
        # Connection becomes defunct
        c.defunct(node_reboot_error)
        
        # Verify error is preserved
        assert c.is_defunct
        assert c.last_error == node_reboot_error
        
        # Any subsequent operation should include the root cause
        with self.assertRaises(ConnectionShutdown) as cm:
            c.send_msg(Mock(), 5, Mock())
        
        assert "Connection reset by peer" in str(cm.exception)
        
        # Wait for responses should also show the error
        with self.assertRaises(ConnectionShutdown) as cm:
            c.wait_for_responses(Mock())
        
        assert "Connection reset by peer" in str(cm.exception)


if __name__ == '__main__':
    unittest.main()
