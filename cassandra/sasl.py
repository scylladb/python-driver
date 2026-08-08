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
Internal SASL client implementation.

This module provides SASL authentication support for the driver without
requiring the external pure-sasl dependency. It implements the PLAIN and
GSSAPI mechanisms which are used by the driver.

This implementation is based on pure-sasl (https://github.com/thobbs/pure-sasl)
which is licensed under the MIT License.
"""

import base64
import platform
import struct

try:
    import kerberos
    _have_kerberos = True
except ImportError:
    _have_kerberos = False

if platform.system() == 'Windows':
    try:
        import winkerberos as kerberos
        # Fix for different capitalisation in winkerberos method name
        kerberos.authGSSClientUserName = kerberos.authGSSClientUsername
        _have_kerberos = True
    except ImportError:
        # winkerberos is an optional dependency on Windows; fall back to non-kerberos auth
        pass


class SASLError(Exception):
    """
    Represents an error in configuration or usage of the SASL client.
    """
    pass


class SASLProtocolException(Exception):
    """
    Raised when an error occurs during SASL negotiation.
    """
    pass


class QOP:
    """Quality of Protection constants."""
    AUTH = b'auth'
    AUTH_INT = b'auth-int'
    AUTH_CONF = b'auth-conf'

    all = (AUTH, AUTH_INT, AUTH_CONF)

    bit_map = {1: AUTH, 2: AUTH_INT, 4: AUTH_CONF}
    name_map = {AUTH: 1, AUTH_INT: 2, AUTH_CONF: 4}

    @classmethod
    def names_from_bitmask(cls, byt):
        return set(name for bit, name in cls.bit_map.items() if bit & byt)

    @classmethod
    def flag_from_name(cls, name):
        return cls.name_map[name]


def _b(s):
    """Convert string to bytes if necessary."""
    if isinstance(s, bytes):
        return s
    return s.encode("utf-8")


class BaseSASLMechanism:
    """Base class for SASL mechanisms."""

    name = None
    complete = False
    qop = QOP.AUTH

    def __init__(self, sasl_client, **props):
        self.sasl = sasl_client

    def process(self, challenge=None):
        """Process a challenge and return the response."""
        raise NotImplementedError()

    def dispose(self):
        """Clear sensitive data."""
        pass


class PlainMechanism(BaseSASLMechanism):
    """PLAIN SASL mechanism for username/password authentication."""

    name = 'PLAIN'

    def __init__(self, sasl_client, username=None, password=None, identity='', **props):
        super().__init__(sasl_client)
        self.identity = identity
        self.username = username
        self.password = password

    def process(self, challenge=None):
        self.complete = True
        auth_id = self.sasl.authorization_id or self.identity
        return b''.join((_b(auth_id), b'\x00', _b(self.username), b'\x00', _b(self.password)))

    def dispose(self):
        self.password = None


class GSSAPIMechanism(BaseSASLMechanism):
    """GSSAPI (Kerberos) SASL mechanism."""

    name = 'GSSAPI'

    def __init__(self, sasl_client, principal=None, **props):
        super().__init__(sasl_client)
        if not _have_kerberos:
            raise SASLError('kerberos module not installed, GSSAPI unavailable')

        self.user = None
        self._have_negotiated_details = False
        self.host = self.sasl.host
        self.service = self.sasl.service
        self.principal = principal
        self.max_buffer = sasl_client.max_buffer

        krb_service = '@'.join((self.service, self.host))
        try:
            _, self.context = kerberos.authGSSClientInit(service=krb_service,
                                                         principal=self.principal)
        except TypeError:
            if self.principal is not None:
                raise SASLError("kerberos library does not support principal parameter")
            _, self.context = kerberos.authGSSClientInit(service=krb_service)

    def _pick_qop(self, server_qop_set):
        """Choose QOP based on user requirements and server offerings."""
        user_qops = set(_b(qop) if isinstance(qop, str) else qop for qop in self.sasl.qops)
        available_qops = user_qops & server_qop_set
        if not available_qops:
            raise SASLProtocolException(
                f"No common QOP available. User requested: {user_qops}, server offered: {server_qop_set}")

        # Pick strongest available QOP
        for qop in (QOP.AUTH_CONF, QOP.AUTH_INT, QOP.AUTH):
            if qop in available_qops:
                self.qop = qop
                break

    def process(self, challenge=None):
        if not self._have_negotiated_details:
            kerberos.authGSSClientStep(self.context, '')
            _negotiated_details = kerberos.authGSSClientResponse(self.context)
            self._have_negotiated_details = True
            return base64.b64decode(_negotiated_details)

        challenge_b64 = base64.b64encode(challenge).decode('ascii')

        if self.user is None:
            ret = kerberos.authGSSClientStep(self.context, challenge_b64)
            if ret == kerberos.AUTH_GSS_COMPLETE:
                self.user = kerberos.authGSSClientUserName(self.context)
                return b''
            else:
                response = kerberos.authGSSClientResponse(self.context)
                if response:
                    response = base64.b64decode(response)
                else:
                    response = b''
            return response

        # Final step: negotiate QOP
        kerberos.authGSSClientUnwrap(self.context, challenge_b64)
        data = kerberos.authGSSClientResponse(self.context)
        plaintext_data = base64.b64decode(data)
        if len(plaintext_data) != 4:
            raise SASLProtocolException("Bad response from server")

        word, = struct.unpack('!I', plaintext_data)
        qop_bits = word >> 24
        max_length = word & 0xffffff
        server_offered_qops = QOP.names_from_bitmask(qop_bits)
        self._pick_qop(server_offered_qops)

        self.max_buffer = min(self.max_buffer, max_length)

        # Build response:
        # byte 0: the selected qop (1=auth, 2=auth-int, 4=auth-conf)
        # byte 1-3: max buffer size (big endian)
        # rest: authorization user name in UTF-8
        auth_id = self.sasl.authorization_id or self.user
        fmt = '!I' + str(len(auth_id)) + 's'
        word = QOP.flag_from_name(self.qop) << 24 | self.max_buffer
        out = struct.pack(fmt, word, _b(auth_id))

        encoded = base64.b64encode(out).decode('ascii')
        kerberos.authGSSClientWrap(self.context, encoded)
        response = kerberos.authGSSClientResponse(self.context)
        self.complete = True
        return base64.b64decode(response)

    def dispose(self):
        if hasattr(self, 'context'):
            kerberos.authGSSClientClean(self.context)


# Registry of available mechanisms
_mechanisms = {
    'PLAIN': PlainMechanism,
}

if _have_kerberos:
    _mechanisms['GSSAPI'] = GSSAPIMechanism


class SASLClient:
    """
    A SASL client for authentication with Cassandra/ScyllaDB.

    This class provides a simplified interface for SASL authentication,
    supporting PLAIN and GSSAPI mechanisms.
    """

    def __init__(self, host, service=None, mechanism=None, authorization_id=None,
                 callback=None, qops=QOP.all, mutual_auth=False, max_buffer=65536,
                 **mechanism_props):
        """
        Initialize a SASL client.

        :param host: Name of the SASL server (typically FQDN)
        :param service: Service name (e.g., 'cassandra', 'dse')
        :param mechanism: SASL mechanism to use ('PLAIN', 'GSSAPI')
        :param authorization_id: Optional authorization ID
        :param qops: Allowed quality of protection options
        :param max_buffer: Maximum buffer size
        :param mechanism_props: Additional mechanism-specific properties
        """
        self.host = host
        self.service = service
        self.authorization_id = authorization_id
        self.mechanism = mechanism
        self.callback = callback
        self.qops = set(qops)
        self.mutual_auth = mutual_auth
        self.max_buffer = max_buffer
        self._mech_props = mechanism_props
        self._chosen_mech = None

        if self.mechanism is not None:
            if mechanism not in _mechanisms:
                if mechanism == 'GSSAPI' and not _have_kerberos:
                    raise SASLError('kerberos module not installed, GSSAPI unavailable')
                raise SASLError(f'Unknown mechanism {mechanism}')
            mech_class = _mechanisms[mechanism]
            self._chosen_mech = mech_class(self, **self._mech_props)

    def process(self, challenge=None):
        """
        Process a challenge from the server during SASL negotiation.

        :param challenge: Challenge bytes from the server, or None for initial response
        :return: Response bytes to send to the server
        """
        if not self._chosen_mech:
            raise SASLError("A mechanism has not been chosen yet")
        return self._chosen_mech.process(challenge)

    @property
    def complete(self):
        """Check if SASL negotiation has completed successfully."""
        if not self._chosen_mech:
            raise SASLError("A mechanism has not been chosen yet")
        return self._chosen_mech.complete

    @property
    def qop(self):
        """Return the negotiated quality of protection."""
        if not self._chosen_mech:
            raise SASLError("A mechanism has not been chosen yet")
        return self._chosen_mech.qop

    def dispose(self):
        """Clear sensitive data."""
        if self._chosen_mech:
            self._chosen_mech.dispose()
