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
Socket-level ``TcpProxy`` test helper.

It backs the Client Routes / NLB integration tests
(``tests/integration/standard/test_client_routes.py``), but has no
dependency on CCM or a running Cassandra/Scylla cluster, so it lives here
rather than in ``tests.integration`` -- that lets the unit test suite
(``tests/unit/test_tcp_proxy.py``) exercise it without importing
``tests.integration``, whose module-level code requires ``ccmlib`` and
CASSANDRA_VERSION/SCYLLA_VERSION to be set.
"""

import logging
import select
import socket
import threading

log = logging.getLogger(__name__)


class TcpProxy:
    """
    A simple TCP proxy that forwards connections from a local listen port
    to a target (host, port).  Tracks active connections so tests can
    verify that traffic flows through the proxy.
    """

    BUF_SIZE = 65536

    def __init__(self, listen_host, listen_port, target_host, target_port):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.target_host = target_host
        self.target_port = target_port

        self._server_sock = None
        self._running = False
        self._thread = None
        self._lock = threading.Lock()
        # Serializes manager-side shutdown with forwarder-owned close. Socket
        # methods release the GIL around their syscalls, so the registry lock
        # alone cannot prevent a descriptor from being closed and reused
        # between shutdown()'s descriptor lookup and the syscall.
        self._lifecycle_lock = threading.Lock()
        self._connections = {}  # (client_sock, target_sock) -> forwarder thread
        self.total_connections = 0

    def start(self):
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind((self.listen_host, self.listen_port))
        self.listen_port = self._server_sock.getsockname()[1]
        self._server_sock.listen(128)
        self._server_sock.setblocking(False)
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="proxy-%s:%d" % (self.listen_host, self.listen_port))
        self._thread.start()
        log.info("TcpProxy started %s:%d -> %s:%d",
                 self.listen_host, self.listen_port,
                 self.target_host, self.target_port)

    def stop(self):
        if self._server_sock:
            try:
                self._server_sock.close()
            except Exception:
                pass
        self._shutdown_and_join_connections(stopping=True)
        if self._thread:
            self._thread.join(timeout=5)
        log.info("TcpProxy stopped %s:%d", self.listen_host, self.listen_port)

    @property
    def active_connections(self):
        with self._lock:
            return len(self._connections)

    def retarget(self, new_host, new_port):
        """Change the backend target for new connections (existing ones keep the old target)."""
        self.target_host = new_host
        self.target_port = new_port
        log.info("TcpProxy %s:%d retargeted to %s:%d",
                 self.listen_host, self.listen_port, new_host, new_port)

    def drop_connections(self):
        """Forcibly close all active connections."""
        self._shutdown_and_join_connections()
        log.info("TcpProxy %s:%d dropped all connections", self.listen_host, self.listen_port)

    def _shutdown_and_join_connections(self, stopping=False):
        """
        Shut down (not close) each connection's sockets to unblock its
        forwarder thread, then join it. Only the forwarder thread itself
        closes its sockets, avoiding a close-vs-still-in-use fd-reuse race.

        stopping=True (stop() only) flips _running to False under the same
        lock as the connections snapshot, so no connection registered by
        _handle_new_connection can be missed.
        """
        with self._lock:
            if stopping:
                self._running = False
            connections = list(self._connections.items())
        for (csock, tsock), _thread in connections:
            with self._lifecycle_lock:
                self._shutdown_pair(csock, tsock)
        finished_keys = []
        for (csock, tsock), thread in connections:
            thread.join(timeout=5)
            if thread.is_alive():
                # Do NOT drop this entry from self._connections: it is
                # still a live thread owning open fds. Leaving it tracked
                # lets active_connections reflect reality and lets a
                # subsequent stop()/drop_connections() retry the shutdown
                # and join. _forward_loop() removes its own entry (under
                # _lock) once it actually exits, so there's no leak here.
                log.warning(
                    "TcpProxy %s:%d: forwarder thread %s did not exit "
                    "within timeout; leaked fds are possible",
                    self.listen_host, self.listen_port, thread.name)
            else:
                finished_keys.append((csock, tsock))
        with self._lock:
            for key in finished_keys:
                self._connections.pop(key, None)

    def _run(self):
        while self._running:
            try:
                readable, _, _ = select.select([self._server_sock], [], [], 0.2)
            except (ValueError, OSError):
                break
            for sock in readable:
                if sock is self._server_sock:
                    try:
                        client_sock, _ = self._server_sock.accept()
                    except OSError:
                        continue
                    self._handle_new_connection(client_sock)

    def _handle_new_connection(self, client_sock, target_host=None, target_port=None):
        target_host = target_host or self.target_host
        target_port = target_port or self.target_port
        try:
            target_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            target_sock.connect((target_host, target_port))
        except Exception as e:
            log.warning("TcpProxy %s:%d failed to connect to target %s:%d: %s",
                        self.listen_host, self.listen_port,
                        target_host, target_port, e)
            client_sock.close()
            return

        t = threading.Thread(target=self._forward_loop,
                             args=(client_sock, target_sock),
                             daemon=True)
        # Register then start() atomically under _lock, in that order:
        # otherwise a short-lived thread could finish (and clean up)
        # before being registered, leaking the entry, or run unseen by
        # a concurrent stop()/drop_connections(). Also re-check
        # _running, to reject connections after shutdown has begun.
        with self._lock:
            if not self._running:
                target_sock.close()
                client_sock.close()
                return
            self._connections[(client_sock, target_sock)] = t
            self.total_connections += 1
            try:
                t.start()
            except Exception as e:
                # Undo registration: join()-ing an unstarted thread later
                # would raise RuntimeError.
                self._connections.pop((client_sock, target_sock), None)
                self.total_connections -= 1
                log.warning("TcpProxy %s:%d failed to start forwarder thread: %s",
                            self.listen_host, self.listen_port, e)
                client_sock.close()
                target_sock.close()

    def _forward_loop(self, client_sock, target_sock):
        try:
            while self._running:
                readable, _, _ = select.select([client_sock, target_sock], [], [], 0.5)
                for sock in readable:
                    data = sock.recv(self.BUF_SIZE)
                    if not data:
                        return
                    if sock is client_sock:
                        target_sock.sendall(data)
                    else:
                        client_sock.sendall(data)
        except (OSError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            # Keep the connection registered until both sockets are closed,
            # and serialize close with manager-side shutdown so neither can
            # issue a syscall using a descriptor recycled by the other.
            with self._lifecycle_lock:
                self._close_pair(client_sock, target_sock)
            with self._lock:
                self._connections.pop((client_sock, target_sock), None)

    @staticmethod
    def _close_pair(csock, tsock):
        for s in (csock, tsock):
            try:
                s.close()
            except Exception:
                pass

    @staticmethod
    def _shutdown_pair(csock, tsock):
        """Best-effort shutdown (not close) to interrupt a thread blocked in select()/recv()."""
        for s in (csock, tsock):
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
