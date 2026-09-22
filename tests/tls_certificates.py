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
Certificates for tests that need a TLS server, generated rather than checked in
so that nothing expires in the repository.

``cryptography`` is optional, so this imports it defensively and reports what it
found in :data:`HAVE_CRYPTOGRAPHY`; a suite that needs a certificate skips on
that rather than failing to import.
"""

import datetime
import ipaddress
import os

try:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    HAVE_CRYPTOGRAPHY = True
except ImportError:  # pragma: no cover - depends on the environment
    HAVE_CRYPTOGRAPHY = False


def write_self_signed_cert(directory, addresses=('127.0.0.1',)):
    """
    Write a self-signed certificate naming every address in *addresses*, and
    its key, into *directory*.  Returns ``(cert_path, key_path)``.

    Every address a client will connect to has to be named: the client verifies
    hostnames, so a certificate covering only the first of them would leave it
    unable to reach the rest.  Callers against a cluster pass every node's
    address for that reason; one against a loopback server takes the default.
    """
    if not HAVE_CRYPTOGRAPHY:
        raise RuntimeError('cryptography is required to generate a certificate')

    addresses = list(addresses)
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
                [x509.IPAddress(ipaddress.ip_address(address))
                 for address in addresses]),
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
