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
Regression tests for the ``TcpProxy`` test helper's connection
shutdown/join synchronization path (GitHub issues #948 and #962).

``TcpProxy`` lives in ``tests/tcp_proxy.py`` because it backs the Client
Routes / NLB integration tests, but it is a plain socket-based helper with
no dependency on a running Cassandra/Scylla cluster or CCM.  These tests
exercise it directly against a local dummy TCP echo backend, so they run as
fast, deterministic, checked-in unit tests instead of only being covered
incidentally (and non-deterministically) by the integration suite.
"""

import socket
import struct
import threading
import time
import types
import unittest
from unittest.mock import patch

from tests import tcp_proxy
from tests.tcp_proxy import TcpProxy, build_proxy_protocol_v2_header, PROXY_PROTOCOL_V2_SIGNATURE


class _EchoServer:
    """Minimal threaded TCP echo server used as TcpProxy's backend target."""

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self.port = self._sock.getsockname()[1]
        self._sock.listen(128)
        self._sock.settimeout(0.2)
        self._running = True
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def _accept_loop(self):
        while self._running:
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._echo, args=(conn,), daemon=True).start()

    @staticmethod
    def _echo(conn):
        try:
            while True:
                data = conn.recv(4096)
                if not data:
                    return
                conn.sendall(data)
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def stop(self):
        self._running = False
        try:
            self._sock.close()
        except OSError:
            pass
        self._accept_thread.join(timeout=2)


def _open_client(host, port, timeout=5):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    s.connect((host, port))
    return s


class TestTcpProxyShutdownJoin(unittest.TestCase):
    """
    Regression coverage for the forwarder-thread bookkeeping bugs described
    in issues #948 and #962. Connections must remain tracked both when a join
    times out and while their sockets are being closed, so active connection
    counts stay accurate and later shutdown calls can find live forwarders.
    """

    def setUp(self):
        self.echo = _EchoServer()
        self.addCleanup(self.echo.stop)
        self.proxy = TcpProxy("127.0.0.1", 0, "127.0.0.1", self.echo.port)
        self.proxy.start()
        self.addCleanup(self._safe_stop_proxy)

    def _safe_stop_proxy(self):
        try:
            self.proxy.stop()
        except Exception:
            pass

    def test_timed_out_forwarder_thread_is_retained_until_it_exits(self):
        """
        Exact regression test for the fix: if a forwarder thread does not
        exit within the join timeout, its entry must NOT be dropped from
        ``_connections`` -- it must stay tracked (so ``active_connections``
        reflects reality and a later shutdown call can retry) until the
        thread actually finishes.
        """
        client = _open_client(self.proxy.listen_host, self.proxy.listen_port)
        self.addCleanup(client.close)
        client.sendall(b"ping")
        self.assertEqual(client.recv(16), b"ping")

        self.assertEqual(self.proxy.active_connections, 1)
        (csock, tsock), thread = list(self.proxy._connections.items())[0]

        # Shrink this thread's effective join timeout so the test doesn't
        # have to block for the real 5s timeout, while neutering
        # _shutdown_pair so the forwarder genuinely cannot be unblocked --
        # deterministically reproducing "still alive after the timeout".
        real_join = thread.join
        thread.join = lambda timeout=None: real_join(timeout=0.05)
        try:
            with patch.object(TcpProxy, "_shutdown_pair",
                               new=staticmethod(lambda a, b: None)):
                self.proxy.drop_connections()
        finally:
            thread.join = real_join

        # The forwarder thread is still alive: the fixed code must keep
        # tracking it instead of discarding the entry.
        self.assertTrue(thread.is_alive(),
                         "test setup issue: forwarder thread should still "
                         "be running at this point")
        self.assertEqual(
            self.proxy.active_connections, 1,
            "a still-alive forwarder thread's connection entry must not be "
            "dropped after its join times out")
        self.assertIn((csock, tsock), self.proxy._connections)

        # Retry for real: this time _shutdown_pair actually runs and
        # unblocks the thread, so the retry can finish reaping it.
        self.proxy.drop_connections()

        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.proxy.active_connections, 0)
        self.assertNotIn((csock, tsock), self.proxy._connections)

    def test_forwarder_is_tracked_until_socket_cleanup_finishes(self):
        """Regression for #962: keep connections tracked through socket cleanup."""
        client = _open_client(self.proxy.listen_host, self.proxy.listen_port)
        self.addCleanup(client.close)
        client.sendall(b"ping")
        self.assertEqual(client.recv(16), b"ping")

        self.assertEqual(self.proxy.active_connections, 1)
        connection, thread = next(iter(self.proxy._connections.items()))
        cleanup_started = threading.Event()
        allow_cleanup = threading.Event()
        real_close_pair = TcpProxy._close_pair

        def blocking_close_pair(csock, tsock):
            cleanup_started.set()
            allow_cleanup.wait()
            real_close_pair(csock, tsock)

        try:
            with patch.object(TcpProxy, "_close_pair",
                              new=staticmethod(blocking_close_pair)):
                client.shutdown(socket.SHUT_RDWR)
                self.assertTrue(
                    cleanup_started.wait(timeout=5),
                    "forwarder did not begin socket cleanup")

                self.assertTrue(thread.is_alive())
                self.assertEqual(self.proxy.active_connections, 1)
                self.assertIn(connection, self.proxy._connections)
        finally:
            allow_cleanup.set()

        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(self.proxy.active_connections, 0)
        self.assertNotIn(connection, self.proxy._connections)

    def test_shutdown_is_serialized_with_socket_cleanup(self):
        """Do not let shutdown race with the forwarder's final close."""
        client = _open_client(self.proxy.listen_host, self.proxy.listen_port)
        self.addCleanup(client.close)
        client.sendall(b"ping")
        self.assertEqual(client.recv(16), b"ping")

        self.assertEqual(self.proxy.active_connections, 1)
        _, forwarder = next(iter(self.proxy._connections.items()))
        shutdown_started = threading.Event()
        allow_shutdown = threading.Event()
        close_lock_requested = threading.Event()
        close_started = threading.Event()
        dropper_errors = []
        real_shutdown_pair = TcpProxy._shutdown_pair
        real_close_pair = TcpProxy._close_pair

        class ObservedLock:
            def __init__(self):
                self._lock = threading.Lock()

            def __enter__(self):
                if threading.current_thread() is forwarder:
                    close_lock_requested.set()
                self._lock.acquire()
                return self

            def __exit__(self, *args):
                self._lock.release()

        def blocking_shutdown_pair(csock, tsock):
            shutdown_started.set()
            allow_shutdown.wait()
            real_shutdown_pair(csock, tsock)

        def recording_close_pair(csock, tsock):
            close_started.set()
            real_close_pair(csock, tsock)

        def drop_connections():
            try:
                self.proxy.drop_connections()
            except Exception as exc:
                dropper_errors.append(exc)

        self.proxy._lifecycle_lock = ObservedLock()
        dropper = threading.Thread(target=drop_connections)
        try:
            with patch.object(TcpProxy, "_shutdown_pair",
                              new=staticmethod(blocking_shutdown_pair)), \
                    patch.object(TcpProxy, "_close_pair",
                                 new=staticmethod(recording_close_pair)):
                dropper.start()
                self.assertTrue(
                    shutdown_started.wait(timeout=5),
                    "dropper did not begin socket shutdown")

                client.shutdown(socket.SHUT_RDWR)
                self.assertTrue(
                    close_lock_requested.wait(timeout=5),
                    "forwarder did not attempt socket cleanup")
                self.assertFalse(
                    close_started.is_set(),
                    "socket close overlapped an in-progress shutdown")
        finally:
            allow_shutdown.set()
            dropper.join(timeout=5)

        self.assertFalse(dropper.is_alive())
        self.assertEqual(dropper_errors, [])
        forwarder.join(timeout=5)
        self.assertFalse(forwarder.is_alive())
        self.assertEqual(self.proxy.active_connections, 0)

    def test_concurrent_stop_and_drop_leaves_no_live_forwarders(self):
        """
        Deterministic stress regression test: concurrently open/close real
        connections through the proxy while other threads hammer
        drop_connections(), then stop(); assert that (a) no unhandled
        exception escaped any thread and (b) no forwarder thread is left
        alive or tracked once stop() returns.
        """
        errors = []
        stop_event = threading.Event()
        forwarder_threads = set()
        threads_lock = threading.Lock()

        def client_worker():
            while not stop_event.is_set():
                try:
                    s = _open_client(self.proxy.listen_host,
                                      self.proxy.listen_port, timeout=1)
                except OSError:
                    continue
                try:
                    with self.proxy._lock:
                        with threads_lock:
                            forwarder_threads.update(self.proxy._connections.values())
                    s.sendall(b"x")
                    s.recv(16)
                except OSError:
                    pass
                finally:
                    try:
                        s.close()
                    except OSError:
                        pass
                time.sleep(0.005)

        def dropper_worker():
            while not stop_event.is_set():
                try:
                    self.proxy.drop_connections()
                except Exception as e:
                    errors.append(e)
                time.sleep(0.01)

        def thread_excepthook(args):
            errors.append(args.exc_value)

        old_hook = threading.excepthook
        threading.excepthook = thread_excepthook
        try:
            client_threads = [threading.Thread(target=client_worker)
                              for _ in range(4)]
            dropper_threads = [threading.Thread(target=dropper_worker)
                               for _ in range(2)]
            for t in client_threads + dropper_threads:
                t.start()

            time.sleep(1.0)

            stop_event.set()
            for t in client_threads + dropper_threads:
                t.join(timeout=5)
                self.assertFalse(t.is_alive())

            self.proxy.stop()
        finally:
            threading.excepthook = old_hook

        self.assertEqual(errors, [],
                          "unhandled exceptions during concurrent "
                          "stop/drop: %r" % (errors,))
        self.assertEqual(self.proxy.active_connections, 0)

        with threads_lock:
            collected = list(forwarder_threads)
        for t in collected:
            self.assertFalse(t.is_alive(),
                              "%s left alive after stop()" % t.name)


class TestProxyProtocolV2Header(unittest.TestCase):

    def test_ipv4(self):
        header = build_proxy_protocol_v2_header(("10.0.0.1", 50001), ("10.0.0.2", 9042))
        self.assertEqual(
            header,
            PROXY_PROTOCOL_V2_SIGNATURE
            + bytes([0x21, 0x11, 0x00, 12])
            + bytes([10, 0, 0, 1, 10, 0, 0, 2])
            + struct.pack('!HH', 50001, 9042))

    def test_ipv6(self):
        header = build_proxy_protocol_v2_header(("::1", 50001), ("fe80::2", 9042))
        self.assertEqual(header[:16], PROXY_PROTOCOL_V2_SIGNATURE + bytes([0x21, 0x21, 0x00, 36]))
        self.assertEqual(len(header), 16 + 36)
        self.assertEqual(header[16:32], socket.inet_pton(socket.AF_INET6, "::1"))
        self.assertEqual(header[-4:], struct.pack('!HH', 50001, 9042))

    def test_mixed_address_families_rejected(self):
        with self.assertRaises(ValueError):
            build_proxy_protocol_v2_header(("10.0.0.1", 1), ("::1", 2))


class _RecordingServer:
    """Accepts one connection and records the first ``size`` bytes it receives."""

    def __init__(self, size):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(1)
        self._sock.settimeout(5)
        self.port = self._sock.getsockname()[1]
        self.received = b""
        self._thread = threading.Thread(target=self._run, args=(size,), daemon=True)
        self._thread.start()

    def _run(self, size):
        conn, _ = self._sock.accept()
        conn.settimeout(5)
        with conn:
            while len(self.received) < size:
                data = conn.recv(size - len(self.received))
                if not data:
                    break
                self.received += data

    def join(self):
        self._thread.join(5)
        self._sock.close()


class TestTcpProxyProxyProtocol(unittest.TestCase):

    def _received_through_proxy(self, proxy_protocol, payload, expected_size):
        server = _RecordingServer(expected_size)
        proxy = TcpProxy("127.0.0.1", 0, "127.0.0.1", server.port, proxy_protocol=proxy_protocol)
        proxy.start()
        try:
            with socket.create_connection(("127.0.0.1", proxy.listen_port), timeout=5) as client:
                client.sendall(payload)
                server.join()
                return server.received, client.getsockname(), ("127.0.0.1", proxy.listen_port)
        finally:
            proxy.stop()

    def test_sends_header_with_client_address_before_data(self):
        header_size = 16 + 12
        received, client_addr, proxy_addr = self._received_through_proxy(
            True, b"payload", header_size + len(b"payload"))
        self.assertEqual(received[:header_size],
                         build_proxy_protocol_v2_header(client_addr, proxy_addr))
        self.assertEqual(received[header_size:], b"payload")

    def test_no_header_by_default(self):
        received, _, _ = self._received_through_proxy(False, b"payload", len(b"payload"))
        self.assertEqual(received, b"payload")

    def _handle_tracking_sockets(self, proxy, client_sock):
        """
        Run the proxy's connection handler for ``client_sock`` and return the
        target sockets it created, which must all be closed when it gives up.
        """
        created = []

        class TrackingSocket(socket.socket):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                created.append(self)

        # Only tests.tcp_proxy sees the tracking class; other threads keep
        # using the real socket module.
        tracking_socket_module = types.SimpleNamespace(
            socket=TrackingSocket, AF_INET=socket.AF_INET, SOCK_STREAM=socket.SOCK_STREAM)
        with patch.object(tcp_proxy, 'socket', tracking_socket_module):
            proxy._handle_new_connection(client_sock)
        return created

    def test_connect_failure_closes_both_sockets(self):
        """
        When the target refuses the connection, the proxy closes the client
        and its own target socket rather than leaving the latter to the
        garbage collector, which the logged exception would delay.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as unused:
            unused.bind(("127.0.0.1", 0))
            refused_port = unused.getsockname()[1]
        proxy = TcpProxy("127.0.0.1", 0, "127.0.0.1", refused_port, proxy_protocol=True)
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        with self.assertLogs('tests.tcp_proxy', level='WARNING') as logs:
            created = self._handle_tracking_sockets(proxy, client_sock)

        self.assertIn("failed to connect to target", logs.output[0])
        self.assertEqual(client_sock.fileno(), -1)
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0].fileno(), -1, "the target socket was left open")
        self.assertEqual(proxy.total_connections, 0)

    def test_header_failure_closes_both_sockets(self):
        """
        When the header cannot be sent, the proxy reports that step and closes
        both sockets, including the target one it already connected.
        """
        backend = _EchoServer()
        self.addCleanup(backend.stop)
        proxy = TcpProxy("127.0.0.1", 0, "127.0.0.1", backend.port, proxy_protocol=True)
        # Never connected, so reading its peer address for the header fails.
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        with self.assertLogs('tests.tcp_proxy', level='WARNING') as logs:
            created = self._handle_tracking_sockets(proxy, client_sock)

        self.assertIn("failed to send the PROXY protocol header to target", logs.output[0])
        self.assertEqual(client_sock.fileno(), -1)
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0].fileno(), -1, "the target socket was left open")
        self.assertEqual(proxy.total_connections, 0)

if __name__ == "__main__":
    unittest.main()
