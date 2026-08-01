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

import socket
import unittest
from threading import Event, Thread

import pytest

from cassandra.cluster import NoHostAvailable
from cassandra.connection import ConnectionShutdown, DefaultEndPoint
from tests.integration import TestCluster


class MaintenanceModeCqlServer(object):
    """
    Minimal CQL listener that accepts a startup attempt and then closes the
    socket without replying, matching Scylla's maintenance-mode failure shape.
    """

    def __init__(self, max_connections=1):
        self._closed = False
        self._max_connections = max_connections
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(('127.0.0.1', 0))
        self._sock.listen(max_connections)
        self._sock.settimeout(0.2)
        self.port = self._sock.getsockname()[1]
        self.frames = []
        self.ready = Event()
        self.received_frame = Event()
        self.error = None
        self.thread = Thread(target=self._run)
        self.thread.daemon = True
        self.thread.start()

    def _run(self):
        self.ready.set()
        try:
            while len(self.frames) < self._max_connections and not self._closed:
                try:
                    client, _ = self._sock.accept()
                except socket.timeout:
                    continue

                with client:
                    client.settimeout(2)
                    frame = b''
                    while len(frame) < 9:
                        chunk = client.recv(9 - len(frame))
                        if not chunk:
                            break
                        frame += chunk
                    self.frames.append(frame)
                    self.received_frame.set()
        except Exception as exc:
            if not self._closed:
                self.error = exc
        finally:
            self.received_frame.set()

    def close(self):
        self._closed = True
        try:
            self._sock.close()
        except socket.error:
            # Best-effort test cleanup; close can race with the listener thread.
            pass
        self.thread.join(2)


class MaintenanceModeConnectionTest(unittest.TestCase):

    def setUp(self):
        self.cluster = TestCluster(contact_points=[], connect_timeout=2)
        self.cluster.connection_class.initialize_reactor()

    def tearDown(self):
        self.cluster.shutdown()

    def test_startup_close_is_observable_through_connection_factories(self):
        """
        Exercise the real reactor/factory path against a socket that behaves
        like a node rejecting regular CQL traffic while in maintenance mode.
        """
        endpoint, server = self._new_endpoint_and_server()
        try:
            conn = self.cluster.connection_class.factory(
                endpoint,
                self.cluster.connect_timeout,
                **self.cluster._make_connection_kwargs(endpoint, {}))

            assert conn.is_closed
            self._assert_server_saw_options_frame(server)
        finally:
            server.close()

        endpoint, server = self._new_endpoint_and_server()
        try:
            conn = self.cluster.connection_factory(endpoint)

            assert conn.is_closed
            self._assert_server_saw_options_frame(server)
        finally:
            server.close()

    def test_cluster_connect_reports_startup_close_as_unavailable_host(self):
        endpoint, server = self._new_endpoint_and_server(max_connections=4)
        cluster = TestCluster(
            contact_points=[endpoint],
            connect_timeout=2,
            control_connection_timeout=2)
        cluster.connection_class.initialize_reactor()

        try:
            with pytest.raises(NoHostAvailable) as exc_info:
                cluster.connect()

            errors = exc_info.value.errors
            assert errors
            assert any(isinstance(exc, ConnectionShutdown) for exc in errors.values())
            assert any("closed during the startup handshake" in str(exc)
                       for exc in errors.values())
            self._assert_server_saw_options_frame(server)
        finally:
            cluster.shutdown()
            server.close()

    def _new_endpoint_and_server(self, max_connections=1):
        server = MaintenanceModeCqlServer(max_connections=max_connections)
        assert server.ready.wait(2)
        return DefaultEndPoint('127.0.0.1', server.port), server

    def _assert_server_saw_options_frame(self, server):
        assert server.received_frame.wait(2)
        assert server.error is None
        assert server.frames
        assert len(server.frames[0]) >= 5
        assert server.frames[0][4] == 0x05  # OPTIONS
