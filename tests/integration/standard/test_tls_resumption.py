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
Integration test for TLS session ticket resumption.

Verifies that after the initial TLS handshake learns session tickets, the
driver reuses them on reconnection instead of performing full handshakes.
The test wraps the ``SSLSessionCache`` with a tracking layer that counts
``set()`` calls (each call means a *new* ticket was negotiated).  After
closing all connections and waiting for the driver to re-establish them,
the ticket count must remain unchanged — proving that sessions were
resumed, not renegotiated.

Requires a live Scylla / Cassandra cluster with TLS enabled.
"""

import logging
import os
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest

from cassandra.connection import SSLSessionCache

from OpenSSL import SSL

from tests import EVENT_LOOP_MANAGER
from tests.integration import (
    get_cluster, use_single_node, start_cluster_wait_for_up, TestCluster,
    CASSANDRA_IP
)

log = logging.getLogger(__name__)


def _generate_ssl_certs(cert_dir):
    """
    Generate a minimal self-signed CA and a server cert/key signed by that CA.

    Writes into *cert_dir*:
      - ca.key / ca.crt  : self-signed CA
      - cassandra.key / cassandra.csr / cassandra.crt : server cert

    :param cert_dir: directory to write files into (must already exist)
    :raises unittest.SkipTest: if ``openssl`` is not on PATH
    :raises RuntimeError: if any openssl command fails
    """
    if shutil.which("openssl") is None:
        raise unittest.SkipTest("openssl not found on PATH; skipping TLS resumption test")

    san_cnf = os.path.join(cert_dir, "san.cnf")
    with open(san_cnf, "w") as f:
        f.write("subjectAltName=IP:127.0.0.1\n")

    def _run(cmd):
        result = subprocess.run(cmd, cwd=cert_dir, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                "openssl command failed: %s\n%s" % (" ".join(cmd), result.stderr)
            )

    _run(["openssl", "req", "-x509", "-newkey", "rsa:2048",
          "-keyout", "ca.key", "-out", "ca.crt",
          "-days", "1", "-nodes", "-subj", "/CN=Test CA"])

    _run(["openssl", "req", "-newkey", "rsa:2048",
          "-keyout", "cassandra.key", "-out", "cassandra.csr",
          "-nodes", "-subj", "/CN=127.0.0.1"])

    _run(["openssl", "x509", "-req",
          "-in", "cassandra.csr", "-CA", "ca.crt", "-CAkey", "ca.key",
          "-CAcreateserial", "-out", "cassandra.crt",
          "-days", "1", "-extfile", "san.cnf"])

    log.info("Generated SSL certs in %s", cert_dir)

USES_PYOPENSSL = "twisted" in EVENT_LOOP_MANAGER or "eventlet" in EVENT_LOOP_MANAGER
if "twisted" in EVENT_LOOP_MANAGER:
    import OpenSSL
    verify_certs = {'cert_reqs': SSL.VERIFY_PEER,
                    'check_hostname': True}
else:
    verify_certs = {'cert_reqs': ssl.CERT_REQUIRED,
                    'check_hostname': True}

# ---------------------------------------------------------------------------
# Tracking wrapper around SSLSessionCache
# ---------------------------------------------------------------------------

class _TrackingSSLSessionCache(SSLSessionCache):
    """
    A thin wrapper around :class:`SSLSessionCache` that counts the number of
    full (non-resumed) TLS handshakes by inspecting the ``session_reused``
    flag passed to :meth:`set`.

    Every ``set()`` call with ``session_reused=False`` corresponds to a fresh
    TLS handshake.  Calls with ``session_reused=True`` are session updates
    after an abbreviated (resumed) handshake and are not counted.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ticket_count_lock = threading.Lock()
        self._ticket_count = 0

    def set(self, key, session):
        if session is not None:
            with self._ticket_count_lock:
                self._ticket_count += 1
        super().set(key, session)

    @property
    def ticket_count(self):
        """Total number of *unique* TLS session tickets stored so far."""
        with self._ticket_count_lock:
            return self._ticket_count

def _make_ssl_context(ca_cert_path):
    """Return a client ``ssl.SSLContext`` that trusts the test CA at *ca_cert_path*.

    TLS 1.2 is explicitly required: Python's ``ssl.SSLSocket.session_reused``
    maps to OpenSSL's ``SSL_session_reused()``, which always returns *False*
    for TLS 1.3 (PSK resumption is not reflected by that API).  Forcing
    TLS 1.2 ensures ``session_reused`` is set correctly after a resumed
    handshake, which is required by the test's assertion logic.
    """
    if USES_PYOPENSSL:
        ssl_context = SSL.Context(SSL.TLS_CLIENT_METHOD)
        ssl_context.load_verify_locations(ca_cert_path)
    else:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(ca_cert_path)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = False
        # Restrict to TLS 1.2 so that session_reused is reliable.
        ssl_context.maximum_version = ssl.TLSVersion.TLSv1_2
    return ssl_context


def _server_supports_tls_resumption(ca_cert_path, host, port=9042):
    """
    Return ``True`` if the server at *host*:*port* supports TLS 1.2 session
    resumption (session-ID or session-ticket based).

    Makes two raw TLS 1.2 connections.  The second reuses the session from
    the first by setting ``ssl.SSLSocket.session`` before calling
    ``do_handshake()``.  Returns ``True`` only when ``session_reused`` is
    ``True`` on the second connection.
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(ca_cert_path)
    ctx.check_hostname = False
    ctx.maximum_version = ssl.TLSVersion.TLSv1_2
    try:
        s1 = socket.create_connection((host, port), timeout=5)
        ssl1 = ctx.wrap_socket(s1, do_handshake_on_connect=False)
        ssl1.do_handshake()
        session = ssl1.session
        ssl1.close()
        if session is None:
            return False
        time.sleep(0.1)
        s2 = socket.create_connection((host, port), timeout=5)
        ssl2 = ctx.wrap_socket(s2, do_handshake_on_connect=False)
        ssl2.session = session
        ssl2.do_handshake()
        reused = ssl2.session_reused
        ssl2.close()
        return bool(reused)
    except Exception:
        return False


def _wait_for_all_connections(session, timeout=30):
    """
    Wait until every host's connection pool is fully connected — i.e., has
    at least one live connection per shard (or one connection if the host
    has no sharding info), or raise on timeout.

    Checking ``len(pool._connections) > 0`` is insufficient for Scylla
    clusters: the pool connects the first shard via the standard port, then
    immediately fires off shard-aware connections to the remaining shards
    asynchronously.  Those connections complete in the background and their
    TLS sessions are cached only after they finish.  Waiting for all shards
    ensures the cache is fully populated before we snapshot the ticket count.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pools = session.get_pools()
        if pools and all(
            not p.is_shutdown and (
                len(p._connections) >= (
                    p.host.sharding_info.shards_count
                    if p.host.sharding_info else 1
                )
            )
            for p in pools
        ):
            return
        time.sleep(0.1)
    raise RuntimeError(
        "Timed out after %ds waiting for all connection pools to open" % timeout
    )


def _close_all_connections(session):
    """
    Shut down every connection pool in the session and trigger
    re-creation so the driver reconnects to all hosts.
    """
    for pool in list(session._pools.values()):
        pool.shutdown()
    session.update_created_pools()


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

class TestTLSTicketResumption(unittest.TestCase):
    """
    Verify that TLS session tickets are reused across reconnections.

    Follows the pattern of the Go driver's ``TestTLSTicketResumption``:

    1. Connect to the cluster with a tracking session cache.
    2. Record the number of tickets learned.
    3. Close all connections, wait for the driver to re-establish them.
    4. Assert that **no new tickets** were learned (sessions were resumed).
    5. Repeat the close/reconnect cycle once more for confidence.

    @test_category connection:ssl:tls_resumption
    """

    _cert_dir = None

    @classmethod
    def setUpClass(cls):
        cls._cert_dir = tempfile.mkdtemp(prefix="tls_resumption_certs_")
        _generate_ssl_certs(cls._cert_dir)

        cls._server_cert_path = os.path.join(cls._cert_dir, "cassandra.crt")
        cls._server_key_path = os.path.join(cls._cert_dir, "cassandra.key")
        cls._ca_cert_path = os.path.join(cls._cert_dir, "ca.crt")

        use_single_node()
        ccm_cluster = get_cluster()
        ccm_cluster.stop()

        config_options = {
            'client_encryption_options': {
                'enabled': True,
                'certificate': cls._server_cert_path,
                'keyfile': cls._server_key_path,
                'truststore': cls._ca_cert_path,
            }
        }
        ccm_cluster.set_configuration_options(config_options)
        start_cluster_wait_for_up(ccm_cluster)

    @classmethod
    def tearDownClass(cls):
        if cls._cert_dir:
            shutil.rmtree(cls._cert_dir, ignore_errors=True)
            cls._cert_dir = None

    def test_tls_ticket_resumption(self):
        if not USES_PYOPENSSL and not _server_supports_tls_resumption(
                self.__class__._ca_cert_path, CASSANDRA_IP):
            self.skipTest(
                "Server at %s:9042 does not support TLS session resumption "
                "(every connection shows session_reused=False); "
                "skipping test" % CASSANDRA_IP
            )

        cache = _TrackingSSLSessionCache(max_size=1024, ttl=3600)
        ssl_context = _make_ssl_context(self.__class__._ca_cert_path)

        cluster = TestCluster(
            ssl_context=ssl_context,
            ssl_session_cache=cache,
        )
        session = cluster.connect(wait_for_all_pools=True)
        try:
            _wait_for_all_connections(session)

            tickets_after_initial = cache.ticket_count
            self.assertGreater(
                tickets_after_initial, 0,
                "No TLS tickets were learned during initial connection — "
                "the server may not support TLS session tickets",
            )
            log.info("Initial connection: %d ticket(s) learned",
                     tickets_after_initial)

            # ── first reconnection cycle ───────────────────────────────
            _close_all_connections(session)
            _wait_for_all_connections(session)

            tickets_after_reconnect1 = cache.ticket_count
            log.info("After 1st reconnect: %d ticket(s) total",
                     tickets_after_reconnect1)

            self.assertEqual(
                tickets_after_reconnect1, tickets_after_initial,
                "New tickets were learned after 1st reconnect — TLS sessions "
                "were NOT reused (got %d, expected %d)"
                % (tickets_after_reconnect1, tickets_after_initial),
            )

            # ── second reconnection cycle ──────────────────────────────
            _close_all_connections(session)
            _wait_for_all_connections(session)

            tickets_after_reconnect2 = cache.ticket_count
            log.info("After 2nd reconnect: %d ticket(s) total",
                     tickets_after_reconnect2)

            self.assertEqual(
                tickets_after_reconnect2, tickets_after_initial,
                "New tickets were learned after 2nd reconnect — TLS sessions "
                "were NOT reused (got %d, expected %d)"
                % (tickets_after_reconnect2, tickets_after_initial),
            )
        finally:
            cluster.shutdown()
