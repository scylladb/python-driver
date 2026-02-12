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
    from cassandra.io.asyncioreactor import AsyncioConnection
except ImportError:
    AsyncioConnection = None


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
        if AsyncioConnection is None:
            self.skipTest("No connection implementation available")
        
        conn = AsyncioConnection(
            DefaultEndPoint('127.0.0.1', 9999),
            ssl_options={'cert_reqs': ssl.CERT_NONE}
        )
        
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
        if AsyncioConnection is None:
            self.skipTest("No connection implementation available")
        
        conn = AsyncioConnection(
            DefaultEndPoint('127.0.0.1', 9999),
            ssl_options={'cert_reqs': ssl.CERT_NONE}
        )
        
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
