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
TLS session resumption exercised against a real TLS server on loopback.

These tests drive the actual code paths a connection uses -- restoring a
cached session onto the socket before the handshake, and storing the
negotiated session afterwards -- and check the outcome the way OpenSSL
reports it, through ``SSLSocket.session_reused``.  No Cassandra or Scylla
server is involved: the peer speaks TLS and echoes bytes, which is all the
socket-level code under test needs.
"""

import datetime
import ipaddress
import os
import socket
import ssl
import tempfile
import threading
import unittest

from cassandra.connection import Connection, DefaultEndPoint, SSLSessionCache

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
except ImportError:  # pragma: no cover - depends on the environment
    x509 = None


def _write_self_signed_cert(directory):
    """
    Write a self-signed certificate valid for 127.0.0.1, and its key, into
    *directory*.  Returns ``(cert_path, key_path)``.
    """
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, '127.0.0.1')])
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
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]),
            critical=False)
        .sign(key, hashes.SHA256())
    )

    cert_path = os.path.join(directory, 'cert.pem')
    key_path = os.path.join(directory, 'key.pem')
    with open(cert_path, 'wb') as f:
        f.write(certificate.public_bytes(serialization.Encoding.PEM))
    with open(key_path, 'wb') as f:
        f.write(key.private_bytes(serialization.Encoding.PEM,
                                  serialization.PrivateFormat.TraditionalOpenSSL,
                                  serialization.NoEncryption()))
    return cert_path, key_path


class _TLSEchoServer(object):
    """
    A TLS server on loopback that echoes back whatever a client sends.  Each
    accepted connection is served on its own thread, so a batch of clients can
    handshake concurrently.
    """

    def __init__(self, cert_path, key_path, tls_version):
        self.context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self.context.load_cert_chain(cert_path, key_path)
        self.context.minimum_version = tls_version
        self.context.maximum_version = tls_version

        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(('127.0.0.1', 0))
        self._listener.listen(16)
        self._listener.settimeout(0.1)
        self.port = self._listener.getsockname()[1]

        self._stop = threading.Event()
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._accept_thread.start()

    def _accept_loop(self):
        while not self._stop.is_set():
            try:
                client, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._serve, args=(client,), daemon=True).start()

    def _serve(self, client):
        try:
            tls_client = self.context.wrap_socket(client, server_side=True)
            while True:
                data = tls_client.recv(64)
                if not data:
                    return
                tls_client.sendall(data)
        except OSError:
            pass
        finally:
            try:
                client.close()
            except OSError:
                pass

    def close(self):
        self._stop.set()
        self._accept_thread.join(timeout=5)
        self._listener.close()


class _SocketOnlyConnection(Connection):
    """
    A connection that performs only the socket and TLS part of setup.  The CQL
    handshake is stood in for by an echo exchange, which is enough to have a
    TLS 1.3 server's NewSessionTicket read off the socket, exactly as the
    OPTIONS/STARTUP exchange does in a real connection.
    """

    def __init__(self, *args, **kwargs):
        Connection.__init__(self, *args, **kwargs)
        self._connect_socket()

    def exchange(self):
        self._socket.sendall(b'ping')
        assert self._socket.recv(4) == b'ping'

    def close(self):
        if self._socket is not None:
            try:
                self._socket.close()
            except OSError:
                pass

    @property
    def session_reused(self):
        return self._socket.session_reused


@unittest.skipIf(x509 is None, 'cryptography is required to generate a test certificate')
class TlsResumptionTest(unittest.TestCase):

    tls_version = ssl.TLSVersion.TLSv1_2

    @classmethod
    def setUpClass(cls):
        cls._cert_dir = tempfile.TemporaryDirectory(prefix='tls_resumption_')
        cls.addClassCleanup(cls._cert_dir.cleanup)
        cls._cert_path, cls._key_path = _write_self_signed_cert(cls._cert_dir.name)

    def setUp(self):
        self.server = _TLSEchoServer(self._cert_path, self._key_path, self.tls_version)
        self.addCleanup(self.server.close)
        self.cache = SSLSessionCache()
        self.connections = []

    def make_ssl_context(self):
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(self._cert_path)
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        return context

    def connect(self, ssl_context, cache=None, exchange=True, ssl_options=None):
        connection = _SocketOnlyConnection(
            DefaultEndPoint('127.0.0.1', self.server.port),
            ssl_context=ssl_context,
            ssl_options=ssl_options,
            ssl_session_cache=self.cache if cache is None else cache,
            connect_timeout=10)
        self.connections.append(connection)
        self.addCleanup(connection.close)
        if exchange:
            connection.exchange()
        return connection

    def test_a_second_connection_resumes_the_first_session(self):
        context = self.make_ssl_context()

        first = self.connect(context)
        assert not first.session_reused
        first._store_tls_session()
        assert len(self.cache) == 1

        second = self.connect(context)

        assert second.session_reused

    def test_concurrent_connections_all_resume_one_cached_session(self):
        # This is the case DRIVER-113 is about: a pool opens one connection per
        # shard at once, and they all have to be able to offer the session
        # cached by an earlier connection to the same node.
        context = self.make_ssl_context()
        self.connect(context)._store_tls_session()

        resumed = []
        barrier = threading.Barrier(4)

        def connect_and_record():
            barrier.wait()
            resumed.append(self.connect(context, exchange=False).session_reused)

        threads = [threading.Thread(target=connect_and_record) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert resumed == [True] * 4

    def test_no_resumption_without_a_cache(self):
        context = self.make_ssl_context()
        self.connect(context)._store_tls_session()

        without_cache = _SocketOnlyConnection(
            DefaultEndPoint('127.0.0.1', self.server.port),
            ssl_context=context, ssl_session_cache=None, connect_timeout=10)
        self.addCleanup(without_cache.close)

        assert not without_cache.session_reused

    def test_a_session_is_not_offered_to_a_different_server_name(self):
        # A resumed handshake carries no Certificate, so the name the peer was
        # verified against is never checked again.  A session established for
        # one name must therefore never be offered to a connection expecting
        # another, even though both reach the same address and port.
        context = self.make_ssl_context()
        context.check_hostname = False
        self.connect(context, ssl_options={'server_hostname': 'one.example'})._store_tls_session()

        same_name = self.connect(context, ssl_options={'server_hostname': 'one.example'})
        other_name = self.connect(context, ssl_options={'server_hostname': 'other.example'})

        assert same_name.session_reused
        assert not other_name.session_reused

    def test_a_session_is_not_offered_to_a_different_context(self):
        # A session can only be replayed onto the context it was established
        # with -- the stdlib ssl module rejects anything else -- so the context
        # is part of the cache key.
        self.connect(self.make_ssl_context())._store_tls_session()

        second = self.connect(self.make_ssl_context())

        assert not second.session_reused

    def test_resumed_connections_keep_refreshing_the_cache(self):
        context = self.make_ssl_context()
        first = self.connect(context)
        first._store_tls_session()
        # Ask the connection for its key rather than rebuilding it here, so this
        # test does not depend on the key's shape.
        key = first._tls_session_cache_key()
        first_session = self.cache.get(key)

        resumed = self.connect(context)
        assert resumed.session_reused
        resumed._store_tls_session()

        assert self.cache.get(key) is not first_session


class Tls13ResumptionTest(TlsResumptionTest):
    """
    The same coverage over TLS 1.3, plus what is specific to it.

    This assumes the local OpenSSL offers TLS 1.3; it is not guarded on
    ``ssl.HAS_TLSv1_3``, so a build without it fails here rather than skipping.
    Issue #984 tracks adding that guard.
    """

    tls_version = ssl.TLSVersion.TLSv1_3

    def test_the_session_is_only_stored_once_the_ticket_has_arrived(self):
        # A TLS 1.3 server sends its NewSessionTicket after the handshake, so a
        # session read before the first application-data exchange carries no
        # ticket and must not be cached.
        connection = self.connect(self.make_ssl_context(), exchange=False)

        assert connection._socket.version() == 'TLSv1.3'
        assert connection._get_resumable_tls_session() is None
        connection._store_tls_session()
        assert len(self.cache) == 0

        connection.exchange()

        assert connection._get_resumable_tls_session() is not None
        connection._store_tls_session()
        assert len(self.cache) == 1
