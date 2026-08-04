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
shutdown/join synchronization path (GitHub issue #948).

``TcpProxy`` lives in ``tests/tcp_proxy.py`` because it backs the Client
Routes / NLB integration tests, but it is a plain socket-based helper with
no dependency on a running Cassandra/Scylla cluster or CCM.  These tests
exercise it directly against a local dummy TCP echo backend, so they run as
fast, deterministic, checked-in unit tests instead of only being covered
incidentally (and non-deterministically) by the integration suite.
"""

import socket
import threading
import time
import unittest
from unittest.mock import patch

from tests.tcp_proxy import TcpProxy


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
    Regression coverage for the forwarder-thread bookkeeping bug described
    in issue #948: ``_shutdown_and_join_connections`` used to unconditionally
    discard every tracked connection from ``_connections``, even ones whose
    forwarder thread was still alive after ``thread.join(timeout=5)`` timed
    out. That made ``active_connections`` under-report live connections and
    made it impossible for a later ``stop()``/``drop_connections()`` call to
    retry reaping an orphaned thread, permanently leaking the thread and its
    file descriptors.
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


if __name__ == "__main__":
    unittest.main()
