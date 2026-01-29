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


import unittest

from unittest.mock import patch

from tests.unit.io.utils import TimerTestMixin
from tests import notpypy, EVENT_LOOP_MANAGER


try:
    from eventlet import monkey_patch
    from cassandra.io.eventletreactor import EventletConnection
except (ImportError, AttributeError):
    EventletConnection = None  # noqa

skip_condition = EventletConnection is None or EVENT_LOOP_MANAGER != "eventlet"
# There are some issues with some versions of pypy and eventlet
@notpypy
@unittest.skipIf(skip_condition, "Skipping the eventlet tests because it's not installed")
class EventletTimerTest(TimerTestMixin, unittest.TestCase):

    connection_class = EventletConnection

    @classmethod
    def setUpClass(cls):
        # This is run even though the class is skipped, so we need
        # to make sure no monkey patching is happening
        if skip_condition:
            return

        # This is being added temporarily due to a bug in eventlet:
        # https://github.com/eventlet/eventlet/issues/401
        import eventlet
        eventlet.sleep()
        monkey_patch()
        # cls.connection_class = EventletConnection

        EventletConnection.initialize_reactor()
        assert EventletConnection._timers is not None

    def setUp(self):
        socket_patcher = patch('eventlet.green.socket.socket')
        self.addCleanup(socket_patcher.stop)
        socket_patcher.start()

        super(EventletTimerTest, self).setUp()

        recv_patcher = patch.object(self.connection._socket,
                                    'recv',
                                    return_value=b'')
        self.addCleanup(recv_patcher.stop)
        recv_patcher.start()

    @property
    def create_timer(self):
        return self.connection.create_timer

    @property
    def _timers(self):
        return self.connection._timers

    # There is no unpatching because there is not a clear way
    # of doing it reliably


try:
    from eventlet.green.OpenSSL import SSL as EventletSSL
    _HAS_EVENTLET_PYOPENSSL = True
except ImportError:
    _HAS_EVENTLET_PYOPENSSL = False


@notpypy
@unittest.skipIf(skip_condition, "Skipping the eventlet tests because it's not installed")
@unittest.skipIf(not _HAS_EVENTLET_PYOPENSSL, "PyOpenSSL not available for eventlet")
class EventletTLSSessionCacheTest(unittest.TestCase):
    """Test TLS session caching for EventletConnection with PyOpenSSL."""

    @classmethod
    def setUpClass(cls):
        if skip_condition:
            return
        import eventlet
        eventlet.sleep()
        monkey_patch()
        EventletConnection.initialize_reactor()

    def test_wrap_socket_applies_cached_session(self):
        """Test that _wrap_socket_from_context applies cached TLS session."""
        from unittest.mock import Mock, MagicMock
        from cassandra.connection import DefaultEndPoint

        # Create mock objects
        mock_cache = Mock()
        mock_session = Mock()
        mock_cache.get_session.return_value = mock_session

        mock_ssl_context = MagicMock()
        mock_ssl_connection = MagicMock()

        endpoint = DefaultEndPoint('127.0.0.1', 9042)

        with patch('eventlet.green.socket.socket'):
            with patch.object(EventletConnection, '_connect_socket'):
                with patch.object(EventletConnection, '_send_options_message'):
                    conn = EventletConnection(
                        endpoint,
                        cql_version='3.0.1',
                        connect_timeout=5
                    )
                    conn.ssl_context = mock_ssl_context
                    conn.ssl_options = {}
                    conn.tls_session_cache = mock_cache
                    conn._socket = Mock()

                    # Patch SSL.Connection to return our mock
                    with patch('cassandra.io.eventletreactor.SSL.Connection', return_value=mock_ssl_connection):
                        conn._wrap_socket_from_context()

                    # Verify get_session was called with endpoint
                    mock_cache.get_session.assert_called_once_with(endpoint)

                    # Verify set_session was called on the SSL connection
                    mock_ssl_connection.set_session.assert_called_once_with(mock_session)

    def test_wrap_socket_no_session_when_cache_empty(self):
        """Test that _wrap_socket_from_context handles empty cache."""
        from unittest.mock import Mock, MagicMock
        from cassandra.connection import DefaultEndPoint

        mock_cache = Mock()
        mock_cache.get_session.return_value = None  # No cached session

        mock_ssl_context = MagicMock()
        mock_ssl_connection = MagicMock()

        endpoint = DefaultEndPoint('127.0.0.1', 9042)

        with patch('eventlet.green.socket.socket'):
            with patch.object(EventletConnection, '_connect_socket'):
                with patch.object(EventletConnection, '_send_options_message'):
                    conn = EventletConnection(
                        endpoint,
                        cql_version='3.0.1',
                        connect_timeout=5
                    )
                    conn.ssl_context = mock_ssl_context
                    conn.ssl_options = {}
                    conn.tls_session_cache = mock_cache
                    conn._socket = Mock()

                    with patch('cassandra.io.eventletreactor.SSL.Connection', return_value=mock_ssl_connection):
                        conn._wrap_socket_from_context()

                    # Verify get_session was called
                    mock_cache.get_session.assert_called_once_with(endpoint)

                    # Verify set_session was NOT called on SSL connection (no cached session)
                    mock_ssl_connection.set_session.assert_not_called()

    def test_initiate_connection_stores_session_after_handshake(self):
        """Test that _initiate_connection stores session after successful handshake."""
        from unittest.mock import Mock, MagicMock
        from cassandra.connection import DefaultEndPoint

        mock_cache = Mock()
        mock_session = Mock()

        mock_ssl_socket = MagicMock()
        mock_ssl_socket.get_session.return_value = mock_session
        mock_ssl_socket.session_reused.return_value = False

        endpoint = DefaultEndPoint('127.0.0.1', 9042)

        with patch('eventlet.green.socket.socket'):
            with patch.object(EventletConnection, '_connect_socket'):
                with patch.object(EventletConnection, '_send_options_message'):
                    conn = EventletConnection(
                        endpoint,
                        cql_version='3.0.1',
                        connect_timeout=5
                    )
                    conn.ssl_context = Mock()
                    conn.ssl_options = {}
                    conn.tls_session_cache = mock_cache
                    conn._socket = mock_ssl_socket
                    conn.uses_legacy_ssl_options = False

                    sockaddr = ('127.0.0.1', 9042)
                    conn._initiate_connection(sockaddr)

                    # Verify handshake was called
                    mock_ssl_socket.do_handshake.assert_called_once()

                    # Verify session was retrieved and stored
                    mock_ssl_socket.get_session.assert_called_once()
                    mock_cache.set_session.assert_called_once_with(endpoint, mock_session)

    def test_initiate_connection_logs_session_reuse(self):
        """Test that _initiate_connection logs when session is reused."""
        from unittest.mock import Mock, MagicMock
        from cassandra.connection import DefaultEndPoint

        mock_cache = Mock()
        mock_session = Mock()

        mock_ssl_socket = MagicMock()
        mock_ssl_socket.get_session.return_value = mock_session
        mock_ssl_socket.session_reused.return_value = True  # Session was reused

        endpoint = DefaultEndPoint('127.0.0.1', 9042)

        with patch('eventlet.green.socket.socket'):
            with patch.object(EventletConnection, '_connect_socket'):
                with patch.object(EventletConnection, '_send_options_message'):
                    conn = EventletConnection(
                        endpoint,
                        cql_version='3.0.1',
                        connect_timeout=5
                    )
                    conn.ssl_context = Mock()
                    conn.ssl_options = {}
                    conn.tls_session_cache = mock_cache
                    conn._socket = mock_ssl_socket
                    conn.uses_legacy_ssl_options = False

                    with patch('cassandra.io.eventletreactor.log') as mock_log:
                        sockaddr = ('127.0.0.1', 9042)
                        conn._initiate_connection(sockaddr)

                        # Verify session_reused was checked
                        mock_ssl_socket.session_reused.assert_called_once()

                        # Verify debug log was called for session reuse
                        mock_log.debug.assert_called()
