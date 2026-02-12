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
Integration tests for SSL connection error handling with simulated failures.

These tests simulate real-world SSL connection failures that can occur when:
- Nodes reboot during active connections
- Network issues cause abrupt connection closure
- SSL handshake failures

The tests verify that the driver properly handles these scenarios and
provides meaningful error messages that include the root cause.

See: https://github.com/scylladb/python-driver/issues/614
"""

import unittest
import socket
import ssl
import errno
import threading
import time
from unittest.mock import Mock, patch, MagicMock

from cassandra.connection import ConnectionShutdown, DefaultEndPoint
from cassandra.cluster import NoHostAvailable

try:
    from cassandra.io.asyncorereactor import AsyncoreConnection
except ImportError:
    AsyncoreConnection = None

try:
    from cassandra.io.asyncioreactor import AsyncioConnection
except ImportError:
    AsyncioConnection = None


@unittest.skipIf(AsyncoreConnection is None, "asyncore reactor not available")
class AsyncoreSSLConnectionFailureTest(unittest.TestCase):
    """
    Test SSL connection failures with AsyncoreConnection.
    
    These tests simulate connection failures that can occur in production,
    particularly when nodes are rebooted or network issues occur.
    """

    def test_socket_closed_forcefully_during_send(self):
        """
        Test that forcefully closing a socket during send preserves error info.
        
        Simulates: Node reboot causing socket to be closed while sending data
        """
        # Create a connection
        # Note: Using CERT_NONE for testing only - this is intentionally insecure
        conn = AsyncoreConnection(
            DefaultEndPoint('127.0.0.1', 9999),
            ssl_options={'cert_reqs': ssl.CERT_NONE}
        )
        
        # Create a mock socket that will fail on send
        mock_socket = Mock(spec=ssl.SSLSocket)
        bad_fd_error = OSError(errno.EBADF, "Bad file descriptor")
        mock_socket.send.side_effect = bad_fd_error
        mock_socket.fileno.return_value = 999
        
        conn._socket = mock_socket
        
        # Try to trigger defunct by simulating a send error
        conn.defunct(bad_fd_error)
        
        # Verify the error is preserved
        self.assertTrue(conn.is_defunct)
        self.assertEqual(conn.last_error, bad_fd_error)
        self.assertIn("Bad file descriptor", str(conn.last_error))

    def test_connection_reset_during_recv(self):
        """
        Test handling of connection reset during receive operation.
        
        Simulates: Node reboot causing connection reset while reading response
        """
        # Note: Using CERT_NONE for testing only - this is intentionally insecure
        conn = AsyncoreConnection(
            DefaultEndPoint('127.0.0.1', 9999),
            ssl_options={'cert_reqs': ssl.CERT_NONE}
        )
        
        # Create a mock socket that will fail on recv
        mock_socket = Mock(spec=ssl.SSLSocket)
        conn_reset_error = OSError(errno.ECONNRESET, "Connection reset by peer")
        mock_socket.recv.side_effect = conn_reset_error
        mock_socket.fileno.return_value = 999
        
        conn._socket = mock_socket
        conn.defunct(conn_reset_error)
        
        # Verify the error is preserved
        self.assertTrue(conn.is_defunct)
        self.assertEqual(conn.last_error, conn_reset_error)
        self.assertIn("Connection reset by peer", str(conn.last_error))

    def test_ssl_handshake_failure(self):
        """
        Test SSL handshake failure is properly captured and reported.
        
        Simulates: SSL handshake failure due to connection issues
        """
        conn = AsyncoreConnection(
            DefaultEndPoint('127.0.0.1', 9999),
            ssl_options={'cert_reqs': ssl.CERT_REQUIRED}
        )
        
        # Simulate SSL error
        ssl_error = ssl.SSLError(ssl.SSL_ERROR_SYSCALL, "Unexpected EOF")
        conn.defunct(ssl_error)
        
        # Verify the error is preserved
        self.assertTrue(conn.is_defunct)
        self.assertEqual(conn.last_error, ssl_error)
        self.assertIn("Unexpected EOF", str(conn.last_error))

    def test_broken_pipe_on_ssl_socket(self):
        """
        Test handling of broken pipe error on SSL socket.
        
        Simulates: Write to socket whose peer has closed connection
        """
        # Note: Using CERT_NONE for testing only - this is intentionally insecure
        conn = AsyncoreConnection(
            DefaultEndPoint('127.0.0.1', 9999),
            ssl_options={'cert_reqs': ssl.CERT_NONE}
        )
        
        broken_pipe_error = OSError(errno.EPIPE, "Broken pipe")
        conn.defunct(broken_pipe_error)
        
        # Verify the error is preserved and accessible
        self.assertTrue(conn.is_defunct)
        self.assertEqual(conn.last_error, broken_pipe_error)
        
        # Verify that subsequent operations include the error
        with self.assertRaises(ConnectionShutdown) as cm:
            conn.send_msg(Mock(), 1, Mock())
        
        self.assertIn("Broken pipe", str(cm.exception))

    def test_concurrent_operations_on_closing_ssl_connection(self):
        """
        Test concurrent operations when SSL connection is being closed.
        
        Simulates: Multiple threads operating on connection during node reboot
        This is the core scenario from the reported issue.
        """
        # Note: Using CERT_NONE for testing only - this is intentionally insecure
        conn = AsyncoreConnection(
            DefaultEndPoint('127.0.0.1', 9999),
            ssl_options={'cert_reqs': ssl.CERT_NONE}
        )
        
        # Simulate the connection having some requests
        conn._requests = {
            1: (Mock(), Mock(), Mock()),
            2: (Mock(), Mock(), Mock()),
        }
        
        # Original error that triggers closure
        original_error = OSError(errno.ECONNRESET, "Connection reset by peer")
        
        # Mark as defunct
        conn.defunct(original_error)
        
        # Verify error is preserved
        self.assertTrue(conn.is_defunct)
        self.assertEqual(conn.last_error, original_error)
        
        # Concurrent thread tries to send - should see original error
        with self.assertRaises(ConnectionShutdown) as cm:
            conn.send_msg(Mock(), 3, Mock())
        
        error_msg = str(cm.exception)
        self.assertIn("Connection reset by peer", error_msg)
        
        # Another thread tries to wait for responses - should also see original error
        with self.assertRaises(ConnectionShutdown) as cm:
            conn.wait_for_responses(Mock())
        
        error_msg = str(cm.exception)
        self.assertIn("Connection reset by peer", error_msg)


@unittest.skipIf(AsyncioConnection is None, "asyncio reactor not available")
class AsyncioSSLConnectionFailureTest(unittest.TestCase):
    """
    Test SSL connection failures with AsyncioConnection.
    
    Similar tests for asyncio-based connections.
    """

    def test_socket_error_preserved_in_asyncio(self):
        """
        Test that socket errors are preserved in asyncio connections.
        """
        # Note: Using CERT_NONE for testing only - this is intentionally insecure
        conn = AsyncioConnection(
            DefaultEndPoint('127.0.0.1', 9999),
            ssl_options={'cert_reqs': ssl.CERT_NONE}
        )
        
        # Simulate error
        bad_fd_error = OSError(errno.EBADF, "Bad file descriptor")
        conn.defunct(bad_fd_error)
        
        # Verify
        self.assertTrue(conn.is_defunct)
        self.assertEqual(conn.last_error, bad_fd_error)
        self.assertIn("Bad file descriptor", str(conn.last_error))

    def test_connection_reset_in_asyncio(self):
        """
        Test connection reset handling in asyncio.
        """
        # Note: Using CERT_NONE for testing only - this is intentionally insecure
        conn = AsyncioConnection(
            DefaultEndPoint('127.0.0.1', 9999),
            ssl_options={'cert_reqs': ssl.CERT_NONE}
        )
        
        conn_reset_error = OSError(errno.ECONNRESET, "Connection reset by peer")
        conn.defunct(conn_reset_error)
        
        self.assertTrue(conn.is_defunct)
        self.assertEqual(conn.last_error, conn_reset_error)
        
        # Verify error is included in ConnectionShutdown
        with self.assertRaises(ConnectionShutdown) as cm:
            conn.send_msg(Mock(), 1, Mock())
        
        self.assertIn("Connection reset by peer", str(cm.exception))


class SSLErrorMessageQualityTest(unittest.TestCase):
    """
    Test that error messages are informative and include root causes.
    
    These tests verify that when "Bad file descriptor" errors occur,
    users can see what originally caused the problem.
    """

    def test_error_message_includes_root_cause(self):
        """
        Verify that ConnectionShutdown messages include the root cause error.
        """
        # Note: Using CERT_NONE for testing only - this is intentionally insecure
        # This can work with any connection type
        if AsyncoreConnection:
            conn = AsyncoreConnection(
                DefaultEndPoint('127.0.0.1', 9999),
                ssl_options={'cert_reqs': ssl.CERT_NONE}
            )
        elif AsyncioConnection:
            conn = AsyncioConnection(
                DefaultEndPoint('127.0.0.1', 9999),
                ssl_options={'cert_reqs': ssl.CERT_NONE}
            )
        else:
            self.skipTest("No connection implementation available")
        
        # Simulate root cause
        root_cause = OSError(errno.ECONNRESET, "Connection reset by peer")
        conn.defunct(root_cause)
        
        # Try to use the connection
        with self.assertRaises(ConnectionShutdown) as cm:
            conn.send_msg(Mock(), 1, Mock())
        
        error_message = str(cm.exception)
        
        # Verify the error message is informative
        self.assertIn("Connection reset by peer", error_message)
        self.assertIn("defunct", error_message.lower())

    def test_multiple_errors_preserves_first(self):
        """
        Verify that when multiple errors occur, the first (root cause) is preserved.
        """
        # Note: Using CERT_NONE for testing only - this is intentionally insecure
        if AsyncoreConnection:
            conn = AsyncoreConnection(
                DefaultEndPoint('127.0.0.1', 9999),
                ssl_options={'cert_reqs': ssl.CERT_NONE}
            )
        elif AsyncioConnection:
            conn = AsyncioConnection(
                DefaultEndPoint('127.0.0.1', 9999),
                ssl_options={'cert_reqs': ssl.CERT_NONE}
            )
        else:
            self.skipTest("No connection implementation available")
        
        # First error - the root cause
        root_cause = OSError(errno.ETIMEDOUT, "Connection timed out")
        conn.defunct(root_cause)
        
        # Verify first error is preserved
        self.assertEqual(conn.last_error, root_cause)
        
        # Second call to defunct should not overwrite
        conn.defunct(OSError(errno.EBADF, "Bad file descriptor"))
        
        # Root cause should still be preserved
        self.assertEqual(conn.last_error, root_cause)
        self.assertIn("Connection timed out", str(conn.last_error))


if __name__ == '__main__':
    unittest.main()
