# Copyright ScyllaDB, Inc.
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
Regression test for https://github.com/scylladb/python-driver/issues/614

Reproduces the "[Errno 9] Bad file descriptor" error that occurs in the
driver's ControlConnection._try_connect() when TLS/SSL is enabled and
topology changes happen (node stop/start, decommission).

ROOT CAUSE:
    CPython bug https://github.com/python/cpython/issues/148594:
    Python's ssl module does NOT call ERR_clear_error() before
    SSL_read_ex()/SSL_write_ex(). This allows stale entries on the
    per-thread OpenSSL error queue to corrupt the result of SSL_get_error(),
    causing spurious OSError (including EBADF errno 9) on healthy SSL
    connections.

    The driver's event loop (libev on Python 3.12+, since asyncore was
    removed in 3.12) runs multiple SSL connections on a single thread.
    When a node goes down during topology changes:
    1. SSL write/read to the dead node fails -> error pushed to OpenSSL queue
    2. Event loop processes the next connection on the same thread
    3. SSL_get_error() picks up the stale error -> reports SSL_ERROR_SYSCALL
    4. Python translates this to OSError with the stale errno (9 = EBADF)
    5. Driver sees "[Errno 9] Bad file descriptor" on a healthy connection

ENVIRONMENT:
    This test requires Python 3.12+ with OpenSSL 3.x to trigger the bug.
    Best reproduced on Python 3.14 + OpenSSL 3.5.x (Debian Trixie).
    Run with EVENT_LOOP_MANAGER=libev for the primary reproduction path.
"""

import logging
import os
import ssl
import sys
import threading
import time
import unittest

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import NoHostAvailable
from cassandra.connection import ConnectionShutdown, DefaultEndPoint
from cassandra.policies import (
    ExponentialReconnectionPolicy,
    RoundRobinPolicy,
)
from cassandra.query import SimpleStatement

from tests.integration import (
    get_cluster,
    get_node,
    local,
    remove_cluster,
    start_cluster_wait_for_up,
    TestCluster,
    use_cluster,
    EVENT_LOOP_MANAGER,
)
from tests.integration.long.utils import (
    create_schema,
    decommission,
    force_stop,
    wait_for_down,
    wait_for_up,
)

log = logging.getLogger(__name__)

# SSL certificate paths (reuse existing test certs)
SERVER_KEYSTORE_PATH = os.path.abspath("tests/integration/long/ssl/127.0.0.1.keystore")
SERVER_TRUSTSTORE_PATH = os.path.abspath("tests/integration/long/ssl/cassandra.truststore")
CLIENT_CA_CERTS = os.path.abspath("tests/integration/long/ssl/rootCa.crt")
DRIVER_KEYFILE = os.path.abspath("tests/integration/long/ssl/client.key")
DRIVER_CERTFILE = os.path.abspath("tests/integration/long/ssl/client.crt_signed")

DEFAULT_PASSWORD = "cassandra"

# Minimum Python version where the CPython bug manifests
# (asyncore removed in 3.12, forcing libev which uses single-threaded event loop)
MIN_PYTHON_FOR_BUG = (3, 12)


def setup_ssl_cluster(nodes=3):
    """
    Set up a multi-node cluster with client SSL encryption enabled.
    """
    use_cluster("ssl_topology_test", [nodes], start=False)
    ccm_cluster = get_cluster()
    ccm_cluster.stop()

    config_options = {
        'client_encryption_options': {
            'enabled': True,
            'keystore': SERVER_KEYSTORE_PATH,
            'keystore_password': DEFAULT_PASSWORD,
        }
    }
    ccm_cluster.set_configuration_options(config_options)
    start_cluster_wait_for_up(ccm_cluster)
    return ccm_cluster


def create_ssl_context():
    """Create an SSL context for connecting to the SSL-enabled cluster."""
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.load_verify_locations(CLIENT_CA_CERTS)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE  # Self-signed certs in test
    return ssl_context


def create_ssl_session(ssl_context):
    """Create a Cluster+Session with SSL and topology-resilient policies."""
    cluster = TestCluster(
        ssl_context=ssl_context,
        load_balancing_policy=RoundRobinPolicy(),
        reconnection_policy=ExponentialReconnectionPolicy(
            base_delay=0.5, max_delay=30.0
        ),
        # Short timeouts to trigger faster failover
        connect_timeout=10,
        control_connection_timeout=10,
    )
    session = cluster.connect(wait_for_all_pools=True)
    return cluster, session


class ContinuousWorkload(threading.Thread):
    """Background thread that continuously writes/reads to stress SSL connections."""

    def __init__(self, session, keyspace="ks_ssl_topo"):
        super().__init__(daemon=True)
        self.session = session
        self.keyspace = keyspace
        self._stop_event = threading.Event()
        self.writes_ok = 0
        self.reads_ok = 0
        self.write_errors = []
        self.read_errors = []

    def run(self):
        key = 0
        while not self._stop_event.is_set():
            # Write
            try:
                stmt = SimpleStatement(
                    f"INSERT INTO {self.keyspace}.cf (k, i) VALUES ({key}, {key})",
                    consistency_level=ConsistencyLevel.ONE,
                )
                self.session.execute(stmt, timeout=10)
                self.writes_ok += 1
            except Exception as e:  # noqa: BLE001 - intentionally broad for error collection
                self.write_errors.append(
                    (time.time(), type(e).__name__, str(e))
                )

            # Read
            try:
                stmt = SimpleStatement(
                    f"SELECT * FROM {self.keyspace}.cf WHERE k = {key % max(1, self.writes_ok)}",
                    consistency_level=ConsistencyLevel.ONE,
                )
                self.session.execute(stmt, timeout=10)
                self.reads_ok += 1
            except Exception as e:  # noqa: BLE001 - intentionally broad for error collection
                self.read_errors.append(
                    (time.time(), type(e).__name__, str(e))
                )

            key += 1
            time.sleep(0.1)

    def stop(self, timeout=30):
        self._stop_event.set()
        self.join(timeout)


class DriverErrorCollector(threading.Thread):
    """Monitors driver logs for 'Bad file descriptor' errors."""

    def __init__(self):
        super().__init__(daemon=True)
        self._stop_event = threading.Event()
        self.bad_fd_errors = []
        self._handler = None

    def start(self):
        # Install a log handler to capture driver error messages
        self._handler = _BadFdLogHandler(self.bad_fd_errors)
        logging.getLogger('cassandra').addHandler(self._handler)
        super().start()

    def run(self):
        self._stop_event.wait()

    def stop(self):
        self._stop_event.set()
        if self._handler:
            logging.getLogger('cassandra').removeHandler(self._handler)
        self.join(5)


class _BadFdLogHandler(logging.Handler):
    """Log handler that captures 'Bad file descriptor' messages."""

    def __init__(self, collection):
        super().__init__(level=logging.ERROR)
        self.collection = collection

    def emit(self, record):
        msg = self.format(record)
        if "Bad file descriptor" in msg or "Errno 9" in msg:
            self.collection.append(msg)


@pytest.mark.skipif(
    sys.version_info < MIN_PYTHON_FOR_BUG,
    reason=f"CPython bug #148594 requires Python {MIN_PYTHON_FOR_BUG[0]}.{MIN_PYTHON_FOR_BUG[1]}+ to manifest"
)
class TestSSLTopologyChangeBadFd(unittest.TestCase):
    """
    Reproducer for issue #614: Bad file descriptor during SSL topology changes.

    This test suite exercises various topology change scenarios with active
    SSL sessions to trigger the CPython ssl module bug where stale OpenSSL
    error queue entries cause spurious EBADF errors on healthy connections.
    """

    @classmethod
    def setUpClass(cls):
        setup_ssl_cluster(nodes=3)

    @classmethod
    def tearDownClass(cls):
        remove_cluster()

    @local
    def test_ssl_node_stop_with_active_session(self):
        """
        Stop the control connection node while maintaining an active SSL session.

        This is the primary reproduction scenario: when the control connection
        node goes down, the driver attempts to reconnect via other nodes.
        The stale OpenSSL error queue from the failed connection corrupts
        the reconnection attempt.
        """
        ssl_context = create_ssl_context()
        cluster, session = create_ssl_session(ssl_context)
        self.addCleanup(cluster.shutdown)

        create_schema(cluster, session, "ks_ssl_topo")

        # Insert initial data
        for i in range(50):
            session.execute(f"INSERT INTO ks_ssl_topo.cf (k, i) VALUES ({i}, {i})")

        collector = DriverErrorCollector()
        collector.start()
        workload = ContinuousWorkload(session)
        workload.start()

        try:
            time.sleep(2)  # Let workload stabilize

            # Find and stop the control connection host
            control_host = cluster.control_connection_host
            control_ip = control_host.address if control_host else "127.0.0.1"
            log.info(f"Stopping control connection node at {control_ip}")

            # Find CCM node matching control connection
            for node_id in range(1, 4):
                node = get_node(node_id)
                if node.network_interfaces['binary'][0] == control_ip:
                    force_stop(node_id)
                    break
            else:
                # Default: stop node 1
                force_stop(1)

            # Wait for driver to detect and failover
            time.sleep(15)

            # Check for bad fd errors
            bad_fd_errors = collector.bad_fd_errors

            if bad_fd_errors:
                log.warning(
                    f"REPRODUCED issue #614: {len(bad_fd_errors)} 'Bad file descriptor' "
                    f"errors during SSL control connection failover"
                )

            # Verify driver can still operate (recovery)
            recovered = False
            for attempt in range(10):
                try:
                    session.execute(
                        SimpleStatement(
                            "SELECT * FROM ks_ssl_topo.cf LIMIT 1",
                            consistency_level=ConsistencyLevel.ONE,
                        ),
                        timeout=5,
                    )
                    recovered = True
                    log.info(f"Driver recovered after {attempt + 1} attempts")
                    break
                except Exception:  # noqa: BLE001
                    time.sleep(2)

            # The test PASSES if we DON'T get bad_fd_errors.
            # If we DO get them, that confirms the bug reproduction.
            assert not bad_fd_errors, (
                f"python-driver issue #614 reproduced: Got 'Bad file descriptor' errors "
                f"during SSL control connection failover:\n" + "\n".join(bad_fd_errors)
            )
            assert recovered, "Driver did not recover after control connection node was stopped"

        finally:
            workload.stop()
            collector.stop()

    @local
    def test_ssl_rolling_restart_with_active_session(self):
        """
        Rolling restart of nodes while maintaining an active SSL session.

        Mimics SCT nemesis: disrupt_rolling_restart_cluster.
        Rapid reconnection attempts on the shared event loop thread make
        the stale error queue bug more likely to manifest.
        """
        ssl_context = create_ssl_context()
        cluster, session = create_ssl_session(ssl_context)
        self.addCleanup(cluster.shutdown)

        create_schema(cluster, session, "ks_ssl_topo")
        for i in range(100):
            session.execute(f"INSERT INTO ks_ssl_topo.cf (k, i) VALUES ({i}, {i})")

        collector = DriverErrorCollector()
        collector.start()
        workload = ContinuousWorkload(session)
        workload.start()

        try:
            time.sleep(2)

            # Rolling restart: stop + start each node sequentially
            for cycle in range(2):
                for node_id in range(1, 4):
                    ip = f"127.0.0.{node_id}"
                    log.info(f"Cycle {cycle + 1}: Restarting node {ip}")
                    force_stop(node_id)
                    time.sleep(5)
                    get_node(node_id).start(wait_for_binary_proto=True)
                    time.sleep(5)

            # Final check
            time.sleep(5)
            bad_fd_errors = collector.bad_fd_errors

            if bad_fd_errors:
                log.warning(
                    f"REPRODUCED issue #614 during rolling restart: "
                    f"{len(bad_fd_errors)} 'Bad file descriptor' errors"
                )

            assert not bad_fd_errors, (
                f"python-driver issue #614 reproduced during rolling restart:\n"
                + "\n".join(bad_fd_errors)
            )

        finally:
            workload.stop()
            collector.stop()

    @local
    def test_ssl_decommission_with_active_session(self):
        """
        Decommission a node while maintaining an active SSL session.

        Decommission causes the driver to close connections to the removed
        node, potentially leaving stale errors on the OpenSSL queue.
        """
        # Need a fresh cluster for decommission (can't re-add nodes easily)
        remove_cluster()
        setup_ssl_cluster(nodes=3)

        ssl_context = create_ssl_context()
        cluster, session = create_ssl_session(ssl_context)
        self.addCleanup(cluster.shutdown)

        create_schema(cluster, session, "ks_ssl_topo", replication_factor=2)
        for i in range(100):
            session.execute(f"INSERT INTO ks_ssl_topo.cf (k, i) VALUES ({i}, {i})")

        collector = DriverErrorCollector()
        collector.start()
        workload = ContinuousWorkload(session)
        workload.start()

        try:
            time.sleep(2)

            log.info("Decommissioning node 3")
            decommission(3)
            log.info("Node 3 decommissioned")

            # Wait for driver to detect topology change
            time.sleep(15)

            bad_fd_errors = collector.bad_fd_errors

            if bad_fd_errors:
                log.warning(
                    f"REPRODUCED issue #614 during decommission: "
                    f"{len(bad_fd_errors)} 'Bad file descriptor' errors"
                )

            # Verify recovery
            recovered = False
            for attempt in range(10):
                try:
                    session.execute(
                        SimpleStatement(
                            "SELECT * FROM ks_ssl_topo.cf LIMIT 1",
                            consistency_level=ConsistencyLevel.ONE,
                        ),
                        timeout=5,
                    )
                    recovered = True
                    break
                except Exception:  # noqa: BLE001
                    time.sleep(2)

            assert not bad_fd_errors, (
                f"python-driver issue #614 reproduced during decommission:\n"
                + "\n".join(bad_fd_errors)
            )
            assert recovered, "Driver did not recover after node decommission"

        finally:
            workload.stop()
            collector.stop()

    @local
    def test_ssl_rapid_reconnection_stress(self):
        """
        Rapidly cycle a node to stress SSL reconnection.

        Rapidly toggling a node creates many SSL connection/disconnection
        events on the same thread, maximizing the chance of stale errors.
        """
        ssl_context = create_ssl_context()
        cluster, session = create_ssl_session(ssl_context)
        self.addCleanup(cluster.shutdown)

        create_schema(cluster, session, "ks_ssl_topo")

        collector = DriverErrorCollector()
        collector.start()

        try:
            # Rapidly cycle node 2
            for cycle in range(5):
                log.info(f"Rapid cycle {cycle + 1}/5: stopping node 2")
                force_stop(2)
                time.sleep(3)

                log.info(f"Rapid cycle {cycle + 1}/5: starting node 2")
                get_node(2).start(wait_for_binary_proto=True)
                time.sleep(3)

                # Attempt a query each cycle to exercise reconnection
                for attempt in range(5):
                    try:
                        session.execute(
                            SimpleStatement(
                                f"INSERT INTO ks_ssl_topo.cf (k, i) VALUES ({cycle * 100 + attempt}, 1)",
                                consistency_level=ConsistencyLevel.ONE,
                            ),
                            timeout=10,
                        )
                        break
                    except Exception:  # noqa: BLE001
                        time.sleep(1)

            bad_fd_errors = collector.bad_fd_errors
            if bad_fd_errors:
                log.warning(
                    f"REPRODUCED issue #614: {len(bad_fd_errors)} 'Bad file descriptor' "
                    f"errors during rapid reconnection stress"
                )

            assert not bad_fd_errors, (
                f"python-driver issue #614 reproduced during rapid reconnection:\n"
                + "\n".join(bad_fd_errors)
            )

        finally:
            collector.stop()


class TestSSLTopologyEnvironmentInfo(unittest.TestCase):
    """
    Diagnostic test that logs environment versions relevant to issue #614.
    Run this to verify you have the right environment to reproduce the bug.
    """

    def test_environment_versions(self):
        """Log Python, OpenSSL, driver, and cryptography versions."""
        log.info(f"Python version: {sys.version}")
        log.info(f"Python version info: {sys.version_info}")
        log.info(f"OpenSSL version: {ssl.OPENSSL_VERSION}")
        log.info(f"OpenSSL version info: {ssl.OPENSSL_VERSION_INFO}")
        log.info(f"ssl module HAS_TLSv1_3: {ssl.HAS_TLSv1_3}")
        log.info(f"EVENT_LOOP_MANAGER: {EVENT_LOOP_MANAGER}")

        import cassandra  # noqa: PLC0415
        log.info(f"scylla-driver version: {cassandra.__version__}")

        from cassandra.cluster import Cluster  # noqa: PLC0415
        log.info(f"Default connection class: {Cluster.connection_class}")

        try:
            import cryptography  # noqa: PLC0415
            log.info(f"cryptography version: {cryptography.__version__}")
        except ImportError:
            log.info("cryptography: not installed")

        # Check if ERR_clear_error is accessible
        try:
            import ctypes  # noqa: PLC0415
            libssl = ctypes.CDLL("libssl.so.3")
            has_err_clear = hasattr(libssl, "ERR_clear_error")
            log.info(f"libssl.so.3 ERR_clear_error accessible: {has_err_clear}")
        except (OSError, AttributeError) as e:
            log.info(f"libssl.so.3 not loadable: {e}")

        # Summary of whether this environment can reproduce the bug
        can_reproduce = (
            sys.version_info >= MIN_PYTHON_FOR_BUG
            and ssl.OPENSSL_VERSION_INFO[0] >= 3
            and "libev" in EVENT_LOOP_MANAGER
        )
        log.info(f"Can reproduce issue #614: {can_reproduce}")
        if not can_reproduce:
            reasons = []
            if sys.version_info < MIN_PYTHON_FOR_BUG:
                reasons.append(f"Python {sys.version_info[:2]} < {MIN_PYTHON_FOR_BUG}")
            if ssl.OPENSSL_VERSION_INFO[0] < 3:
                reasons.append(f"OpenSSL {ssl.OPENSSL_VERSION_INFO[0]}.x < 3.x")
            if "libev" not in EVENT_LOOP_MANAGER:
                reasons.append(f"EVENT_LOOP_MANAGER={EVENT_LOOP_MANAGER} (need libev)")
            log.info(f"Reasons: {', '.join(reasons)}")
