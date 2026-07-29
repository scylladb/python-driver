# Copyright 2014 Symantec Corporation
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

# Originally derived from MagnetoDB source:
#   https://github.com/stackforge/magnetodb/blob/2015.1.0b1/magnetodb/common/cassandra/io/eventletreactor.py
import eventlet
from eventlet.green import socket
from eventlet.queue import Queue
from greenlet import GreenletExit
import logging
import ssl
from threading import Event
import time

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
_PYOPENSSL_ERROR = ()
_PYOPENSSL_ZERO_RETURN_ERROR = ()
try:
    from eventlet.green.OpenSSL import SSL
    _PYOPENSSL = True
    _PYOPENSSL_ERROR = (SSL.Error,)
    _PYOPENSSL_ZERO_RETURN_ERROR = (SSL.ZeroReturnError,)
except ImportError as e:
    _PYOPENSSL = False
    no_pyopenssl_error = e


log = logging.getLogger(__name__)


def _check_pyopenssl():
    if not _PYOPENSSL:
        raise ImportError(
            "{}, pyOpenSSL must be installed to enable "
            "SSL support with the Eventlet event loop".format(str(no_pyopenssl_error))
        )


class EventletConnection(Connection):
    """
    An implementation of :class:`.Connection` that utilizes ``eventlet``.

    This implementation assumes all eventlet monkey patching is active. It is not tested with partial patching.
    """

    _read_watcher = None
    _write_watcher = None

    _socket_impl = eventlet.green.socket
    _ssl_impl = eventlet.green.ssl
    _connect_socket_error_types = (
        (socket.error, ConnectionException) + _PYOPENSSL_ERROR
    )

    _timers = None
    _timeout_watcher = None
    _new_timer = None

    @classmethod
    def initialize_reactor(cls):
        eventlet.monkey_patch()
        if not cls._timers:
            cls._timers = TimerManager()
            cls._timeout_watcher = eventlet.spawn(cls.service_timeouts)
            cls._new_timer = Event()

    @classmethod
    def create_timer(cls, timeout, callback):
        timer = Timer(timeout, callback)
        cls._timers.add_timer(timer)
        cls._new_timer.set()
        return timer

    @classmethod
    def service_timeouts(cls):
        """
        cls._timeout_watcher runs in this loop forever.
        It is usually waiting for the next timeout on the cls._new_timer Event.
        When new timers are added, that event is set so that the watcher can
        wake up and possibly set an earlier timeout.
        """
        timer_manager = cls._timers
        while True:
            next_end = timer_manager.service_timeouts()
            sleep_time = max(next_end - time.time(), 0) if next_end else 10000
            cls._new_timer.wait(sleep_time)
            cls._new_timer.clear()

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)
        self._write_queue = Queue()

        self._connect_socket()

        self._read_watcher = eventlet.spawn(lambda: self.handle_read())
        self._write_watcher = eventlet.spawn(lambda: self.handle_write())
        self._send_options_message()

    def _wrap_socket_from_context(self):
        _check_pyopenssl()
        _ensure_pyopenssl_context_requires_verification(SSL, self.ssl_context, self._check_hostname)
        self._socket = SSL.Connection(self.ssl_context, self._socket)
        self._socket.set_connect_state()
        server_hostname, _expected_name = self._tls_server_names()
        if server_hostname is not None:
            # This is necessary for SNI
            self._socket.set_tlsext_host_name(_encode_server_hostname(server_hostname))
        return self._socket

    def _tls_server_names(self):
        return _resolve_pyopenssl_server_names(
            self.endpoint.address,
            (self.ssl_options or {}).get('server_hostname'),
            self._check_hostname,
            verify_endpoint_address=isinstance(self.endpoint, SniEndPoint))

    def _initiate_connection(self, sockaddr):
        super(EventletConnection, self)._initiate_connection(sockaddr)
        if self._ssl_enabled:
            self._socket.do_handshake()

    def _validate_hostname(self):
        _server_hostname, expected_name = self._tls_server_names()
        try:
            _validate_pyopenssl_hostname(self._socket.get_peer_certificate(), expected_name)
        except ssl.CertificateError as exc:
            raise ConnectionException(
                "Hostname verification failed: %s" % (exc,), self.endpoint) from exc

    def _build_ssl_context_from_options(self):
        _check_pyopenssl()
        return _build_pyopenssl_context(
            SSL,
            self.ssl_options,
            verify_by_default=self._ssl_options_verify_by_default)

    def close(self):
        with self.lock:
            if self.is_closed:
                return
            self.is_closed = True

        log.debug("Closing connection (%s) to %s" % (id(self), self.endpoint))

        cur_gthread = eventlet.getcurrent()

        if self._read_watcher and self._read_watcher != cur_gthread:
            self._read_watcher.kill()
        if self._write_watcher and self._write_watcher != cur_gthread:
            self._write_watcher.kill()
        if self._socket:
            self._socket.close()
        log.debug("Closed socket to %s" % (self.endpoint,))

        if not self.is_defunct:
            msg = "Connection to %s was closed" % self.endpoint
            if self.last_error:
                msg += ": %s" % (self.last_error,)
            self.error_all_requests(ConnectionShutdown(msg))
            # don't leave in-progress operations hanging
            self.connected_event.set()

    def handle_close(self):
        log.debug("connection closed by server")
        self.close()

    def handle_write(self):
        while True:
            try:
                next_msg = self._write_queue.get()
                self._socket.sendall(next_msg)
            except _PYOPENSSL_ZERO_RETURN_ERROR:
                log.debug("Connection %s closed by server during socket send", self)
                self.close()
                return
            except _PYOPENSSL_ERROR as err:
                log.debug("Exception during TLS socket send for %s: %s", self, err)
                self.defunct(err)
                return
            except socket.error as err:
                log.debug("Exception during socket send for %s: %s", self, err)
                self.defunct(err)
                return  # Leave the write loop
            except GreenletExit:  # graceful greenthread exit
                return

    def handle_read(self):
        while True:
            try:
                buf = self._socket.recv(self.in_buffer_size)
                if not buf:
                    log.debug("Connection %s closed by server", self)
                    self.close()
                    return
                self._iobuf.write(buf)
            except _PYOPENSSL_ZERO_RETURN_ERROR:
                log.debug("Connection %s closed by server during socket recv", self)
                self.close()
                return
            except _PYOPENSSL_ERROR as err:
                log.debug("Exception during TLS socket recv for %s: %s", self, err)
                self.defunct(err)
                return
            except socket.error as err:
                log.debug("Exception during socket recv for %s: %s",
                          self, err)
                self.defunct(err)
                return  # leave the read loop
            except GreenletExit:  # graceful greenthread exit
                return

            if self._iobuf.tell():
                self.process_io_buffer()

    def push(self, data):
        chunk_size = self.out_buffer_size
        for i in range(0, len(data), chunk_size):
            self._write_queue.put(data[i:i + chunk_size])
