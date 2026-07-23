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
``set()`` calls as a sanity check on ticket activity (``set()`` runs for
both new and resumed handshakes, so the count is not asserted to stay
fixed).  Actual proof of resumption comes from the handshake itself, via
``ssl.SSLSocket.session_reused`` (stdlib) or ``SSL_session_reused()``
(PyOpenSSL), which report whether the last handshake reused a prior session.

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

try:
    from OpenSSL import SSL
except ImportError:
    SSL = None

from tests import EVENT_LOOP_MANAGER
from tests.integration import (
    get_cluster, remove_cluster, use_single_node, start_cluster_wait_for_up,
    TestCluster, CASSANDRA_IP, SCYLLA_VERSION
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
# The asyncio reactor performs TLS via asyncio's native SSL transport rather
# than wrapping the socket, so ``conn._socket`` stays a plain TCP socket (no
# ``version()``/``session``) and ``ssl_session_cache`` is not supported.
USES_ASYNCIO = "asyncio" in EVENT_LOOP_MANAGER

# ---------------------------------------------------------------------------
# Tracking wrapper around SSLSessionCache
# ---------------------------------------------------------------------------

class _TrackingSSLSessionCache(SSLSessionCache):
    """
    A thin wrapper around :class:`SSLSessionCache` that counts how many TLS
    sessions are stored via :meth:`set`.

    :meth:`Connection._cache_tls_session_if_needed` stores the current session
    on *every* successful handshake — including resumed ones, since a resumed
    TLS 1.2 handshake may issue a replacement ticket — so this count grows on
    each (re)connection and cannot by itself distinguish resumption from
    renegotiation.  The test therefore uses it only to confirm that tickets
    were learned at all, and verifies actual reuse via
    ``ssl.SSLSocket.session_reused`` (see ``_count_resumed_connections``).
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ticket_count_lock = threading.Lock()
        self._ticket_count = 0
        # Every session ever handed to set().  The live cache drains as
        # connections check out (consume) single-use tickets via get(), so a
        # snapshot of it may be empty; this preserves the full record for
        # assertions about what was actually stored.
        self._stored_sessions = []

    def set(self, key, session):
        if session is not None:
            with self._ticket_count_lock:
                self._ticket_count += 1
                self._stored_sessions.append(session)
        super().set(key, session)

    @property
    def ticket_count(self):
        """Total number of TLS sessions stored so far."""
        with self._ticket_count_lock:
            return self._ticket_count

    def stored_sessions(self):
        """Snapshot of every session ever passed to :meth:`set`."""
        with self._ticket_count_lock:
            return list(self._stored_sessions)

def _make_ssl_context(ca_cert_path, force_tls13=False):
    """Return a client ``ssl.SSLContext`` that trusts the test CA at *ca_cert_path*.

    By default TLS 1.2 is required: Python's ``ssl.SSLSocket.session_reused``
    is only reliable in the raw ``_server_supports_tls_resumption`` probe on
    TLS 1.2, because a TLS 1.3 ``SSLSession`` captured immediately after the
    handshake has no ticket yet and cannot be resumed.  The driver itself only
    caches *ticketed* sessions, so its own TLS 1.3 reconnections do resume and
    report ``session_reused`` correctly.

    Pass ``force_tls13=True`` (stdlib path only) to pin TLS 1.3 and exercise
    the driver's delayed-ticket caching path.
    """
    if USES_PYOPENSSL:
        ssl_context = SSL.Context(SSL.TLS_CLIENT_METHOD)
        ssl_context.load_verify_locations(ca_cert_path)
        # Actually verify the server certificate chain against the test CA so
        # the Twisted/Eventlet paths exercise verification (matching the stdlib
        # branch's CERT_REQUIRED intent).
        ssl_context.set_verify(SSL.VERIFY_PEER,
                               lambda conn, cert, errno, depth, ok: bool(ok))
    else:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(ca_cert_path)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = False
        if force_tls13:
            # Pin TLS 1.3 to exercise the asynchronous NewSessionTicket path.
            ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
            ssl_context.maximum_version = ssl.TLSVersion.TLSv1_3
        else:
            # Restrict to TLS 1.2 so that session_reused is reliable.
            ssl_context.maximum_version = ssl.TLSVersion.TLSv1_2
    return ssl_context


def _tls_negotiates_v13(ca_cert_path, host, port=9042):
    """
    Return ``True`` if a raw TLS 1.3 handshake with the server at
    *host*:*port* succeeds (i.e. the server supports TLS 1.3).
    """
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.load_verify_locations(ca_cert_path)
    ctx.check_hostname = False
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    try:
        with socket.create_connection((host, port), timeout=5) as raw:
            with ctx.wrap_socket(raw) as tls:
                return tls.version() == 'TLSv1.3'
    except (OSError, ssl.SSLError):
        return False


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
        with socket.create_connection((host, port), timeout=5) as s1:
            with ctx.wrap_socket(s1, do_handshake_on_connect=False) as ssl1:
                ssl1.do_handshake()
                session = ssl1.session
        if session is None:
            return False
        time.sleep(0.1)
        with socket.create_connection((host, port), timeout=5) as s2:
            with ctx.wrap_socket(s2, do_handshake_on_connect=False) as ssl2:
                ssl2.session = session
                ssl2.do_handshake()
                return bool(ssl2.session_reused)
    except (OSError, ssl.SSLError):
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


def _count_resumed_connections(session):
    """
    Inspect every live pool connection and return ``(resumed, total)`` where
    *resumed* counts connections whose last TLS handshake was an abbreviated
    (resumed) handshake, per ``ssl.SSLSocket.session_reused``.

    Only meaningful on the stdlib ``ssl`` reactors; PyOpenSSL's ``SSL.Connection``
    exposes no ``session_reused``, so those sockets are skipped (not counted).
    """
    resumed = 0
    total = 0
    for pool in list(session._pools.values()):
        for conn in list(pool._connections.values()):
            reused = getattr(getattr(conn, '_socket', None), 'session_reused', None)
            if not isinstance(reused, bool):
                continue
            total += 1
            if reused:
                resumed += 1
    return resumed, total


def _iter_live_ssl_sockets(session):
    """Yield the live TLS socket of every connection in every pool."""
    for pool in list(session._pools.values()):
        for conn in list(pool._connections.values()):
            sock = getattr(conn, '_socket', None)
            if sock is not None:
                yield sock


def _pyopenssl_connection(conn):
    """
    Return the pyOpenSSL ``SSL.Connection`` backing a driver connection, or
    ``None`` if it cannot be located.

    Eventlet stores the ``SSL.Connection`` directly as ``conn._socket``;
    Twisted keeps it on the SSL creator as ``conn._ssl_creator._ssl_connection``.
    """
    sock = getattr(conn, '_socket', None)
    if sock is not None and hasattr(sock, 'get_session'):
        return sock
    creator = getattr(conn, '_ssl_creator', None)
    if creator is not None:
        return getattr(creator, '_ssl_connection', None)
    return None


def _count_pyopenssl_resumed(session):
    """
    Return ``(resumed, total)`` for the PyOpenSSL reactors (Twisted, Eventlet).

    pyOpenSSL exposes no ``session_reused()`` Python method, but OpenSSL's
    underlying ``SSL_session_reused()`` is reachable through the low-level cffi
    binding (``OpenSSL._util.lib``) using the ``SSL *`` pointer that pyOpenSSL
    keeps on ``SSL.Connection._ssl``.  It returns ``1`` when the last handshake
    resumed a prior session.

    This is a best-effort check.  Both ``OpenSSL._util`` and the ``_ssl`` cffi
    pointer are private pyOpenSSL internals that may change across versions.
    If the binding is unavailable the function returns ``(0, 0)`` and the caller
    is expected to call ``skipTest`` rather than fail the assertion.

    Returns ``(0, 0)`` when the binding or pointer is unavailable so callers
    can skip rather than fail on unexpected pyOpenSSL internals.
    """
    try:
        from OpenSSL._util import lib as _ossl_lib
    except Exception:
        return 0, 0
    # SSL_session_reused may be absent from the imported cffi binding; accessing
    # it directly would raise AttributeError and fail the test, so honor the
    # best-effort contract by returning (0, 0) for the caller to skip.
    _session_reused = getattr(_ossl_lib, 'SSL_session_reused', None)
    if _session_reused is None:
        return 0, 0
    resumed = 0
    total = 0
    for pool in list(session._pools.values()):
        for conn in list(pool._connections.values()):
            ssl_conn = _pyopenssl_connection(conn)
            ssl_ptr = getattr(ssl_conn, '_ssl', None)
            if ssl_ptr is None:
                continue
            total += 1
            if _session_reused(ssl_ptr):
                resumed += 1
    return resumed, total


def _count_resumed(session):
    """
    Return ``(resumed, total)`` counting connections whose last TLS handshake
    was resumed, using the appropriate mechanism for the active reactor
    (``ssl.SSLSocket.session_reused`` for stdlib, ``SSL_session_reused()`` for
    PyOpenSSL).
    """
    if USES_PYOPENSSL:
        return _count_pyopenssl_resumed(session)
    return _count_resumed_connections(session)


def _teardown_ccm_cluster():
    """
    Stop and remove the shared CCM cluster, restoring it to a clean state.

    Registered via ``addClassCleanup`` so it runs even when ``setUpClass``
    fails partway through reconfiguring the cluster.
    """
    ccm_cluster = get_cluster()
    if ccm_cluster is not None:
        ccm_cluster.stop()
    remove_cluster()


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

class TestTLSTicketResumption(unittest.TestCase):
    """
    Verify that TLS session tickets are reused across reconnections.

    Follows the pattern of the Go driver's ``TestTLSTicketResumption``:

    1. Connect to the cluster with a tracking session cache.
    2. Record the number of tickets learned during the initial handshakes.
    3. Close all connections, wait for the driver to re-establish them.
    4. Verify resumption directly for each reconnected socket via
       ``session_reused`` / ``SSL_session_reused()`` (the driver deliberately
       re-stores sessions after resumed handshakes, so new ``set()`` calls are
       expected and not asserted against).
    5. Repeat the close/reconnect cycle once more for confidence.

    @test_category connection:ssl:tls_resumption
    """

    _cert_dir = None

    @classmethod
    def setUpClass(cls):
        if SCYLLA_VERSION is None:
            raise unittest.SkipTest(
                "TLS resumption test requires a local Scylla CCM cluster "
                "(SCYLLA_VERSION is not set)")

        if USES_PYOPENSSL and SSL is None:
            raise unittest.SkipTest(
                "pyOpenSSL is not installed; cannot run the TLS resumption "
                "test under the Twisted/Eventlet event loop")

        cls._cert_dir = tempfile.mkdtemp(prefix="tls_resumption_certs_")
        cls.addClassCleanup(shutil.rmtree, cls._cert_dir, ignore_errors=True)
        _generate_ssl_certs(cls._cert_dir)

        cls._server_cert_path = os.path.join(cls._cert_dir, "cassandra.crt")
        cls._server_key_path = os.path.join(cls._cert_dir, "cassandra.key")
        cls._ca_cert_path = os.path.join(cls._cert_dir, "ca.crt")

        use_single_node()
        ccm_cluster = get_cluster()
        # Register cluster teardown before mutating the shared CCM cluster so
        # that a failure during stop/configure/start still restores cluster
        # state (setUpClass failures skip tearDownClass, but registered class
        # cleanups always run).
        cls.addClassCleanup(_teardown_ccm_cluster)
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

    def test_tls_ticket_resumption(self):
        if USES_ASYNCIO:
            self.skipTest(
                "The asyncio reactor uses asyncio's native SSL transport and "
                "does not support ssl_session_cache; TLS resumption is not "
                "applicable")
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

            # The driver re-stores the current session on every handshake,
            # including resumed ones (a resumed TLS 1.2 handshake may issue a
            # replacement ticket), so a growing ticket_count no longer proves
            # renegotiation.  Verify resumption directly instead: every
            # reconnected connection must report a resumed handshake.  On the
            # stdlib ssl reactors this reads ssl.SSLSocket.session_reused; on
            # the PyOpenSSL reactors it uses OpenSSL's SSL_session_reused() via
            # the low-level binding (see _count_pyopenssl_resumed).
            self._assert_all_resumed(session, "1st reconnect")

            # ── second reconnection cycle ──────────────────────────────
            _close_all_connections(session)
            _wait_for_all_connections(session)

            tickets_after_reconnect2 = cache.ticket_count
            log.info("After 2nd reconnect: %d ticket(s) total",
                     tickets_after_reconnect2)

            self._assert_all_resumed(session, "2nd reconnect")
        finally:
            cluster.shutdown()

    def _assert_all_resumed(self, session, label):
        """Assert every inspectable live connection resumed its TLS session."""
        resumed, total = _count_resumed(session)
        if total == 0 and USES_PYOPENSSL:
            self.skipTest(
                "Cannot observe TLS session reuse on this pyOpenSSL build "
                "(SSL_session_reused binding unavailable); skipping the "
                "%s resumption assertion" % label)
        self.assertGreater(
            total, 0, "No inspectable TLS connections after %s" % label)
        self.assertEqual(
            resumed, total,
            "TLS sessions were NOT reused after %s "
            "(%d of %d connections resumed)" % (label, resumed, total),
        )

    def test_tls13_ticket_resumption(self):
        # The main test pins TLS 1.2; this variant exercises the TLS 1.3
        # delayed-ticket path, where the NewSessionTicket arrives *after* the
        # handshake.  The fix defers caching until the session actually carries
        # a ticket, so here we assert (1) every cached session has a ticket and
        # (2) a subsequent reconnection resumes.  This is a stdlib-ssl concern
        # (``ssl.SSLSession.has_ticket``); the PyOpenSSL path has no equivalent
        # introspection and is covered by test_tls_ticket_resumption.
        if USES_PYOPENSSL:
            self.skipTest(
                "TLS 1.3 delayed-ticket caching is verified on the stdlib ssl "
                "reactors; PyOpenSSL is covered by test_tls_ticket_resumption")

        if USES_ASYNCIO:
            self.skipTest(
                "The asyncio reactor uses asyncio's native SSL transport and "
                "does not support ssl_session_cache; TLS resumption is not "
                "applicable")

        ca_cert_path = self.__class__._ca_cert_path
        if not _tls_negotiates_v13(ca_cert_path, CASSANDRA_IP):
            self.skipTest(
                "Server at %s:9042 does not negotiate TLS 1.3; skipping "
                "TLS 1.3 resumption test" % CASSANDRA_IP)

        cache = _TrackingSSLSessionCache(max_size=1024, ttl=3600)
        ssl_context = _make_ssl_context(ca_cert_path, force_tls13=True)

        cluster = TestCluster(
            ssl_context=ssl_context,
            ssl_session_cache=cache,
        )
        session = cluster.connect(wait_for_all_pools=True)
        try:
            _wait_for_all_connections(session)

            # Sanity-check that every connection really negotiated TLS 1.3.
            versions = {s.version() for s in _iter_live_ssl_sockets(session)}
            self.assertEqual(
                versions, {'TLSv1.3'},
                "Expected all connections on TLS 1.3, got %s" % (versions,))

            # Core of the fix: only *ticketed* (resumable) sessions may be
            # cached.  A session cached before its NewSessionTicket arrived
            # would report has_ticket=False and could never resume.  The live
            # cache drains as connections check out single-use tickets, so
            # inspect every session the driver *stored* instead.
            cached_sessions = cache.stored_sessions()
            self.assertTrue(
                cached_sessions,
                "No TLS 1.3 sessions were cached — the delayed-ticket path "
                "never stored a session")
            for sess in cached_sessions:
                self.assertTrue(
                    getattr(sess, 'has_ticket', False),
                    "A cached TLS 1.3 session has no ticket — it was cached "
                    "before the NewSessionTicket arrived and cannot resume")

            # End-to-end: reconnect through the driver and confirm the ticketed
            # sessions actually resume.  The driver sets the cached session
            # before the handshake, so a genuine TLS 1.3 resumption reports
            # session_reused=True.
            _close_all_connections(session)
            _wait_for_all_connections(session)

            resumed, total = _count_resumed_connections(session)
            self.assertGreater(
                total, 0, "No inspectable TLS connections after reconnect")
            self.assertEqual(
                resumed, total,
                "TLS 1.3 sessions were NOT reused after reconnect "
                "(%d of %d connections resumed)" % (resumed, total),
            )
        finally:
            cluster.shutdown()
