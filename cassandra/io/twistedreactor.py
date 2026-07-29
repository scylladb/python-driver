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
Module that implements an event loop based on twisted
( https://twistedmatrix.com ).
"""
import atexit
import logging
import time
from functools import partial
from threading import Thread, Lock
import weakref

from twisted.internet import reactor, protocol
from twisted.internet.endpoints import connectProtocol, TCP4ClientEndpoint, SSL4ClientEndpoint
from twisted.internet.interfaces import IOpenSSLClientConnectionCreator
from twisted.python.failure import Failure
from zope.interface import implementer

from cassandra.connection import (Connection, ConnectionException,
                                  ConnectionShutdown, SniEndPoint, Timer,
                                  TimerManager)
from cassandra.tls import (
    _build_pyopenssl_context_from_options as _build_pyopenssl_context,
    _encode_server_hostname,
    _ensure_pyopenssl_context_requires_verification,
    _resolve_pyopenssl_server_names,
    _validate_pyopenssl_hostname,
)

try:
    from OpenSSL import SSL
    _HAS_SSL = True
except ImportError as e:
    _HAS_SSL = False
    import_exception = e
log = logging.getLogger(__name__)


_TLS_APP_DATA_ATTR = '_cassandra_tls_app_data'


class _TLSAppData(object):
    def __init__(self, endpoint, expected_name, check_hostname):
        self.endpoint = endpoint
        self.expected_name = expected_name
        self.check_hostname = check_hostname


def _cleanup(cleanup_weakref):
    try:
        cleanup_weakref()._cleanup()
    except ReferenceError:
        return


class TwistedConnectionProtocol(protocol.Protocol):
    """
    Twisted Protocol class for handling data received and connection
    made events.
    """

    def __init__(self, connection):
        self.connection = connection

    def dataReceived(self, data):
        """
        Callback function that is called when data has been received
        on the connection.

        Reaches back to the Connection object and queues the data for
        processing.
        """
        self.connection._iobuf.write(data)
        self.connection.handle_read()

    def connectionMade(self):
        """
        Callback function that is called when a connection has succeeded.

        Reaches back to the Connection object and confirms that the connection
        is ready.
        """
        self.connection.client_connection_made(self.transport)

    def connectionLost(self, reason):
        # reason is a Failure instance
        log.debug("Connect lost: %s", reason)
        self.connection.defunct(reason.value)


class TwistedLoop(object):

    _lock = None
    _thread = None
    _timeout_task = None
    _timeout = None

    def __init__(self):
        self._lock = Lock()
        self._timers = TimerManager()

    def maybe_start(self):
        with self._lock:
            if not reactor.running:
                self._thread = Thread(target=reactor.run,
                                      name="cassandra_driver_twisted_event_loop",
                                      kwargs={'installSignalHandlers': False})
                self._thread.daemon = True
                self._thread.start()
                atexit.register(partial(_cleanup, weakref.ref(self)))

    def _reactor_stopped(self):
        return reactor._stopped

    def _cleanup(self):
        if self._thread:
            reactor.callFromThread(reactor.stop)
            self._thread.join(timeout=1.0)
            if self._thread.is_alive():
                log.warning("Event loop thread could not be joined, so "
                            "shutdown may not be clean. Please call "
                            "Cluster.shutdown() to avoid this.")
            log.debug("Event loop thread was joined")

    def add_timer(self, timer):
        self._timers.add_timer(timer)
        # callFromThread to schedule from the loop thread, where
        # the timeout task can safely be modified
        reactor.callFromThread(self._schedule_timeout, timer.end)

    def _schedule_timeout(self, next_timeout):
        if next_timeout:
            delay = max(next_timeout - time.time(), 0)
            if self._timeout_task and self._timeout_task.active():
                if next_timeout < self._timeout:
                    self._timeout_task.reset(delay)
                    self._timeout = next_timeout
            else:
                self._timeout_task = reactor.callLater(delay, self._on_loop_timer)
                self._timeout = next_timeout

    def _on_loop_timer(self):
        self._timers.service_timeouts()
        self._schedule_timeout(self._timers.next_timeout)


@implementer(IOpenSSLClientConnectionCreator)
class _SSLCreator(object):
    def __init__(self, endpoint, ssl_context, ssl_options, check_hostname):
        self.endpoint = endpoint
        self.ssl_options = ssl_options or {}
        self.check_hostname = check_hostname
        if ssl_context is None:
            raise ValueError(
                '_SSLCreator requires a prepared pyOpenSSL context')
        self.context = ssl_context
        _ensure_pyopenssl_context_requires_verification(SSL, self.context, self.check_hostname)
        if self.check_hostname and not hasattr(SSL.Connection, 'set_info_callback'):
            log.warning(
                'Installed pyOpenSSL does not support per-connection TLS info '
                'callbacks; replacing any callback configured on the supplied '
                'context')
            self.context.set_info_callback(_SSLCreator.info_callback)

    @staticmethod
    def info_callback(connection, where, ret):
        if not where & SSL.SSL_CB_HANDSHAKE_DONE:
            return
        tls_protocol = connection.get_app_data()
        app_data = getattr(tls_protocol, _TLS_APP_DATA_ATTR, None)
        if not (app_data and app_data.check_hostname):
            return
        try:
            _validate_pyopenssl_hostname(
                connection.get_peer_certificate(), app_data.expected_name)
        except Exception as exc:
            tls_protocol.failVerification(
                Failure(ConnectionException(
                    "Hostname verification failed: %s" % (exc,), app_data.endpoint)))

    def clientConnectionForTLS(self, tlsProtocol):
        connection = SSL.Connection(self.context, None)
        if self.check_hostname and hasattr(connection, 'set_info_callback'):
            connection.set_info_callback(_SSLCreator.info_callback)
        server_hostname, expected_name = _resolve_pyopenssl_server_names(
            self.endpoint.address,
            self.ssl_options.get('server_hostname'),
            self.check_hostname,
            verify_endpoint_address=isinstance(self.endpoint, SniEndPoint))
        setattr(tlsProtocol, _TLS_APP_DATA_ATTR,
                _TLSAppData(self.endpoint, expected_name, self.check_hostname))
        connection.set_app_data(tlsProtocol)
        if server_hostname is not None:
            connection.set_tlsext_host_name(
                _encode_server_hostname(server_hostname))
        return connection


class TwistedConnection(Connection):
    """
    An implementation of :class:`.Connection` that utilizes the
    Twisted event loop.
    """

    _loop = None

    @classmethod
    def initialize_reactor(cls):
        if not cls._loop:
            cls._loop = TwistedLoop()

    @classmethod
    def create_timer(cls, timeout, callback):
        timer = Timer(timeout, callback)
        cls._loop.add_timer(timer)
        return timer

    def __init__(self, *args, **kwargs):
        """
        Initialization method.

        Note that we can't call reactor methods directly here because
        it's not thread-safe, so we schedule the reactor/connection
        stuff to be run from the event loop thread when it gets the
        chance.
        """
        Connection.__init__(self, *args, **kwargs)

        self.is_closed = True
        self.connector = None
        self.transport = None

        reactor.callFromThread(self.add_connection)
        self._loop.maybe_start()

    def _check_pyopenssl(self):
        if self._ssl_enabled and not _HAS_SSL:
            raise ImportError(
                str(import_exception) +
                ', pyOpenSSL must be installed to enable SSL support with the Twisted event loop'
            )

    def _build_ssl_context_from_options(self):
        self._check_pyopenssl()
        return _build_pyopenssl_context(
            SSL,
            self.ssl_options,
            verify_by_default=self._ssl_options_verify_by_default)

    def add_connection(self):
        """
        Convenience function to connect and store the resulting
        connector.
        """
        host, port = self.endpoint.resolve()
        if self._ssl_enabled:
            # Can't use optionsForClientTLS here because it *forces* hostname verification.
            # Cool they enforce strong security, but we have to be able to turn it off
            self._check_pyopenssl()

            ssl_connection_creator = _SSLCreator(
                self.endpoint,
                self.ssl_context,
                self.ssl_options,
                self._check_hostname,
            )

            endpoint = SSL4ClientEndpoint(
                reactor,
                host,
                port,
                sslContextFactory=ssl_connection_creator,
                timeout=self.connect_timeout,
            )
        else:
            endpoint = TCP4ClientEndpoint(
                reactor,
                host,
                port,
                timeout=self.connect_timeout
            )
        connectProtocol(endpoint, TwistedConnectionProtocol(self))

    def client_connection_made(self, transport):
        """
        Called by twisted protocol when a connection attempt has
        succeeded.
        """
        with self.lock:
            self.is_closed = False
        self.transport = transport
        self._send_options_message()

    def close(self):
        """
        Disconnect and error-out all requests.
        """
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s", id(self), self.endpoint)
        reactor.callFromThread(self.transport.connector.disconnect)
        log.debug("Closed socket to %s", self.endpoint)

        if not self.is_defunct:
            msg = "Connection to %s was closed" % self.endpoint
            if self.last_error:
                msg += ": %s" % (self.last_error,)
            self.error_all_requests(ConnectionShutdown(msg))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_read(self):
        """
        Process the incoming data buffer.
        """
        self.process_io_buffer()

    def push(self, data):
        """
        This function is called when outgoing data should be queued
        for sending.

        Note that we can't call transport.write() directly because
        it is not thread-safe, so we schedule it to run from within
        the event loop when it gets the chance.
        """
        reactor.callFromThread(self.transport.write, data)
