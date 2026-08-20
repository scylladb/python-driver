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
TLS session resumption against a real, TLS-enabled Scylla cluster.

The mechanics of resumption are covered by
``tests/unit/test_tls_resumption.py`` against a local TLS server.  What needs
a real cluster is whether the *server* accepts one session offered by several
connections at once, which is the case DRIVER-113 is about: a pool opens one
connection per shard and they all offer the same cached session.
"""

import datetime
import ipaddress
import logging
import os
import ssl
import tempfile
import unittest

from cassandra.connection import SSLSessionCache
from tests import EVENT_LOOP_MANAGER
from tests.integration import (use_singledc, get_cluster, remove_cluster,
                               start_cluster_wait_for_up, CASSANDRA_IP,
                               SCYLLA_VERSION, TestCluster)
from tests.util import wait_until

log = logging.getLogger(__name__)

_cert_dir = None
_cert_path = None
_key_path = None


def _write_self_signed_cert(directory, addresses):
    """
    Write a certificate valid for every address in *addresses*, and its key,
    into *directory*.  Returns ``(cert_path, key_path)``.

    Every node of the cluster has to be covered: the client verifies hostnames,
    so a certificate naming only the contact point would leave the driver
    unable to build pools to the rest of the cluster.
    """
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, addresses[0])])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address(address)) for address in addresses]),
            critical=False)
        .sign(key, hashes.SHA256())
    )

    cert_path = os.path.join(directory, 'server.crt')
    key_path = os.path.join(directory, 'server.key')
    with open(cert_path, 'wb') as f:
        f.write(certificate.public_bytes(serialization.Encoding.PEM))
    with open(key_path, 'wb') as f:
        f.write(key.private_bytes(serialization.Encoding.PEM,
                                  serialization.PrivateFormat.TraditionalOpenSSL,
                                  serialization.NoEncryption()))
    return cert_path, key_path


def setup_module():
    """
    Restart the shared cluster with client encryption enabled, the way other
    modules in this directory reconfigure it (see test_custom_cluster).
    teardown_module drops it so the next module gets a clean one.
    """
    if SCYLLA_VERSION is None:
        raise unittest.SkipTest(
            'client_encryption_options are configured the Scylla way here; '
            'set SCYLLA_VERSION to run this')
    # This reads the environment rather than asking the connection class
    # whether it can resume, so it only holds while the reactor is selected
    # explicitly, as CI does.  Issue #984 tracks keying the skip off
    # connection_class.supports_tls_session_resumption instead.
    if 'asyncio' in EVENT_LOOP_MANAGER:
        raise unittest.SkipTest(
            'the asyncio reactor performs the TLS handshake inside '
            'loop.create_connection() and cannot resume sessions')
    try:
        import cryptography  # noqa: F401
    except ImportError:
        raise unittest.SkipTest(
            'cryptography is required to generate a server certificate') from None

    global _cert_dir, _cert_path, _key_path
    _cert_dir = tempfile.TemporaryDirectory(prefix='tls_resumption_')
    try:
        use_singledc(start=False)
        ccm_cluster = get_cluster()
        ccm_cluster.stop()
        # The certificate has to name every node, so it can only be issued once
        # the cluster exists.
        _cert_path, _key_path = _write_self_signed_cert(
            _cert_dir.name, [node.address() for node in ccm_cluster.nodelist()])
        ccm_cluster.set_configuration_options({
            # Per-shard connections go to this port, which is where resumption
            # has to pay off; Scylla leaves it unset by default.
            'native_shard_aware_transport_port_ssl': 19142,
            'client_encryption_options': {
                'enabled': True,
                'certificate': _cert_path,
                'keyfile': _key_path,
                # Off by default in Scylla; without it the server issues no
                # NewSessionTicket and nothing can be resumed.
                'enable_session_tickets': True,
            }
        })
        start_cluster_wait_for_up(ccm_cluster)
    except Exception:
        # pytest skips teardown_module when setup_module raises, so undo both
        # halves here: the cluster would otherwise be left stopped and still
        # configured for TLS for every module that runs after this one, and the
        # key and certificate would be left behind on disk.
        try:
            remove_cluster()
        finally:
            _cert_dir.cleanup()
            _cert_dir = None
        raise


def teardown_module():
    try:
        remove_cluster()
    finally:
        if _cert_dir is not None:
            _cert_dir.cleanup()


def make_ssl_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(_cert_path)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    return context


def resumption_of_every_connection(cluster):
    """
    What OpenSSL reports for each of the cluster's live connections: a list of
    ``session_reused`` flags, one per connection.
    """
    return [bool(connection._socket.session_reused)
            for holder in cluster.get_connection_holders()
            for connection in holder.get_connections()]


def expected_connection_count(cluster):
    """
    One control connection, plus one pool connection per shard of every host
    the driver considers up.
    """
    return 1 + sum(host.sharding_info.shards_count if host.sharding_info else 1
                   for host in cluster.metadata.all_hosts() if host.is_up)


def collect_resumption(cluster):
    """
    Wait for the pools to fill, then report whether each connection resumed a
    TLS session.  The wait and the count assertion matter: per-shard
    connections are opened in the background, so an assertion made too early
    would run against a fraction of them -- or against the control connection
    alone -- and pass without testing anything.
    """
    expected = expected_connection_count(cluster)
    wait_until(lambda: len(resumption_of_every_connection(cluster)) >= expected, 0.5, 40)

    resumed = resumption_of_every_connection(cluster)
    log.info('%d of %d connections resumed a TLS session (expected at least %d)',
             sum(resumed), len(resumed), expected)
    assert len(resumed) >= expected, \
        'inspected %d connections, expected at least %d' % (len(resumed), expected)
    return resumed


class TLSSessionResumptionTests(unittest.TestCase):

    def setUp(self):
        # Cluster.sessions is a WeakSet and HostConnection keeps only a
        # weakref.proxy to its session, so a Session nobody holds is collected
        # and takes the pools -- everything worth inspecting -- with it.
        self._sessions = []

    def connect(self, **kwargs):
        cluster = TestCluster(**kwargs)
        self.addCleanup(cluster.shutdown)
        self._sessions.append(cluster.connect(wait_for_all_pools=True))
        return cluster

    def test_resumption_is_on_by_default_with_an_ssl_context(self):
        cluster = self.connect(ssl_context=make_ssl_context())

        assert isinstance(cluster.ssl_session_cache, SSLSessionCache)
        assert len(cluster.ssl_session_cache) > 0

    def test_every_connection_resumes_from_a_warmed_cache(self):
        # Warm a cache, then hand it to a second cluster using the same
        # SSLContext.  Every connection that cluster opens -- including the
        # whole batch of per-shard connections opened at once, which reach the
        # node on its shard-aware port -- then has a session to offer, so the
        # server has to accept the same one from all of them concurrently.
        context = make_ssl_context()
        cache = SSLSessionCache()
        self.connect(ssl_context=context, ssl_session_cache=cache)

        cluster = self.connect(ssl_context=context, ssl_session_cache=cache)

        assert all(collect_resumption(cluster))

    def test_nothing_resumes_when_the_cache_is_disabled(self):
        context = make_ssl_context()
        self.connect(ssl_context=context, ssl_session_cache=SSLSessionCache())

        cluster = self.connect(ssl_context=context, ssl_session_cache=None)

        assert cluster.ssl_session_cache is None
        assert not any(collect_resumption(cluster))
