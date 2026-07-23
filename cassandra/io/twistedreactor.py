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

from cassandra.connection import Connection, ConnectionShutdown, Timer, TimerManager, ConnectionException

try:
    from OpenSSL import SSL
    _HAS_SSL = True
except ImportError as e:
    _HAS_SSL = False
    import_exception = e
log = logging.getLogger(__name__)


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
    def __init__(self, endpoint, ssl_context, ssl_options, check_hostname, timeout, ssl_session_cache=None):
        self.endpoint = endpoint
        self.ssl_options = ssl_options
        self.check_hostname = check_hostname
        self.timeout = timeout
        self.ssl_session_cache = ssl_session_cache
        # Set in clientConnectionForTLS() to the single SSL.Connection this creator
        # drives; read by TwistedConnection._cache_tls_session_if_needed() after the
        # first CQL exchange.
        self._ssl_connection = None

        if ssl_context:
            self.context = ssl_context
        else:
            self.context = SSL.Context(SSL.TLSv1_METHOD)
            if "certfile" in self.ssl_options:
                self.context.use_certificate_file(self.ssl_options["certfile"])
            if "keyfile" in self.ssl_options:
                self.context.use_privatekey_file(self.ssl_options["keyfile"])
            if "ca_certs" in self.ssl_options:
                self.context.load_verify_locations(self.ssl_options["ca_certs"])
            if "cert_reqs" in self.ssl_options:
                self.context.set_verify(
                    self.ssl_options["cert_reqs"],
                    callback=self.verify_callback
                )
        self.context.set_info_callback(self.info_callback)

    def verify_callback(self, connection, x509, errnum, errdepth, ok):
        return ok

    def info_callback(self, connection, where, ret):
        if where & SSL.SSL_CB_HANDSHAKE_DONE:
            transport = connection.get_app_data()
            endpoint = connection._cassandra_endpoint
            cert = connection.get_peer_certificate()
            # Read check_hostname from the connection (not this creator) because a
            # shared ssl_context means set_info_callback() only keeps the last
            # creator's bound callback, so self.check_hostname could belong to a
            # different creator and bypass or misapply hostname verification.
            if connection._cassandra_check_hostname and (
                    cert is None or
                    endpoint.address != cert.get_subject().commonName):
                transport.failVerification(Failure(ConnectionException("Hostname verification failed", endpoint)))
                return
            # For TLS 1.2 (and earlier) the session is fully established at
            # handshake completion, so cache it immediately.  For TLS 1.3 the
            # server sends the NewSessionTicket *after* the handshake, so
            # get_session() here may return None or a ticketless, non-resumable
            # session; caching that and setting the guard flag would suppress
            # the deferred path and leave a ticketless session cached
            # indefinitely.  TLS 1.3 is therefore left entirely to the deferred
            # path in TwistedConnection._cache_tls_session_if_needed(), which
            # runs after the first CQL exchange once the ticket has arrived.
            # Note: pyOpenSSL's SSL.Connection exposes no session_reused(), so we cannot
            # detect resumed handshakes here; the per-connection guard flag stored on the
            # SSL.Connection prevents the deferred path from storing the same session again.
            # Both the guard flag and the cache reference live on the connection
            # (not this creator) because a shared ssl_context means
            # set_info_callback() only keeps the last creator's bound callback,
            # so creator-scoped state would belong to the wrong instance -- e.g.
            # storing one cluster's session into another cluster's cache.
            ssl_session_cache = connection._cassandra_ssl_session_cache
            if ssl_session_cache is not None and \
                    connection.get_protocol_version_name() != 'TLSv1.3':
                session = connection.get_session()
                if session is not None:
                    ssl_session_cache.set(endpoint.tls_session_cache_key, session)
                    connection._cassandra_session_cached = True

    def clientConnectionForTLS(self, tlsProtocol):
        connection = SSL.Connection(self.context, None)
        connection.set_app_data(tlsProtocol)
        # Twisted may overwrite app_data with its own state, so stash the
        # per-connection endpoint as a dedicated attribute on this specific
        # SSL.Connection.  info_callback recovers it from here because it runs off
        # the shared SSL.Context and cannot rely on this creator's attributes.
        connection._cassandra_endpoint = self.endpoint
        # Likewise stash this creator's cache on the connection so info_callback
        # writes to the correct cache even when a shared ssl_context binds it to
        # a different creator instance.
        connection._cassandra_ssl_session_cache = self.ssl_session_cache
        # Same reasoning for the hostname check policy: keep it on the connection
        # so info_callback applies this creator's check_hostname rather than that
        # of whichever creator last bound the shared context's info_callback.
        connection._cassandra_check_hostname = self.check_hostname
        # Per-connection "session already stored" guard.  Kept on the connection
        # (not this creator) so it stays correct even when a shared ssl_context
        # means info_callback fires bound to a different creator instance.
        connection._cassandra_session_cached = False
        # This creator drives exactly one connection, so this reference unambiguously
        # identifies it for TwistedConnection._cache_tls_session_if_needed().
        self._ssl_connection = connection
        if self.ssl_options and "server_hostname" in self.ssl_options:
            connection.set_tlsext_host_name(self.ssl_options['server_hostname'].encode('ascii'))

        # Apply cached TLS session for resumption (PyOpenSSL)
        if self.ssl_session_cache is not None:
            cached_session = self.ssl_session_cache.get(
                self.endpoint.tls_session_cache_key)
            if cached_session:
                try:
                    connection.set_session(cached_session)
                    log.debug("Using cached TLS session for %s", self.endpoint)
                except Exception as e:
                    log.debug("Could not restore TLS session for %s: %s", self.endpoint, e)

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
        self._ssl_creator = None  # set in add_connection() when SSL is used

        reactor.callFromThread(self.add_connection)
        self._loop.maybe_start()

    def _check_pyopenssl(self):
        if self.ssl_context or self.ssl_options:
            if not _HAS_SSL:
                raise ImportError(
                    str(import_exception) +
                    ', pyOpenSSL must be installed to enable SSL support with the Twisted event loop'
                )

    def add_connection(self):
        """
        Convenience function to connect and store the resulting
        connector.
        """
        host, port = self.endpoint.resolve()
        if self.ssl_context or self.ssl_options:
            # Can't use optionsForClientTLS here because it *forces* hostname verification.
            # Cool they enforce strong security, but we have to be able to turn it off
            self._check_pyopenssl()

            ssl_connection_creator = _SSLCreator(
                self.endpoint,
                self.ssl_context if self.ssl_context else None,
                self.ssl_options,
                self._check_hostname,
                self.connect_timeout,
                ssl_session_cache=self._ssl_session_cache,
            )
            self._ssl_creator = ssl_connection_creator

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

    def _cache_tls_session_if_needed(self, initial=False):
        """
        PyOpenSSL override of :meth:`.Connection._cache_tls_session_if_needed`.

        Twisted drives connection setup through its own machinery rather than
        the base ``_connect_socket()``, so this is only ever called from the
        ReadyMessage / AuthSuccessMessage handlers (``initial`` is always
        ``False``) — after the first application-data exchange.  For TLS 1.3
        this is the earliest point at which the resumable session ticket is
        available; for TLS 1.2 the session is also valid here.  The premature
        initial attempt is skipped defensively should it ever be invoked.
        """
        if initial:
            return
        if self._ssl_session_cache is None or self._ssl_creator is None:
            return
        if self._tls_session_cached:
            return
        ssl_conn = self._ssl_creator._ssl_connection
        if ssl_conn is None:
            return
        if getattr(ssl_conn, '_cassandra_session_cached', False):
            # info_callback already stored this connection's session eagerly
            # (TLS 1.2 path).  Mirror the flag so the driver-level invariant
            # ("True once the session has been stored") holds for this path too.
            self._tls_session_cached = True
            return
        # Note: pyOpenSSL's SSL.Connection exposes no session_reused(), so we cannot
        # skip resumed handshakes; the per-connection guards above still prevent
        # storing the same session more than once for this connection.
        try:
            session = ssl_conn.get_session()
            if session is not None:
                self._ssl_session_cache.set(
                    self.endpoint.tls_session_cache_key, session)
                self._tls_session_cached = True
                ssl_conn._cassandra_session_cached = True
        except Exception as e:
            log.debug("Could not cache TLS session for %s: %s", self.endpoint, e)

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
