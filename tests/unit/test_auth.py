# -*- coding: utf-8 -*-
# # Copyright DataStax, Inc.
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

from cassandra.auth import PlainTextAuthenticator, SaslAuthProvider
from cassandra.sasl import SASLClient, SASLError, QOP

import unittest
import pytest


class TestPlainTextAuthenticator(unittest.TestCase):

    def test_evaluate_challenge_with_unicode_data(self):
        authenticator = PlainTextAuthenticator("johnӁ", "doeӁ")
        assert authenticator.evaluate_challenge(b'PLAIN-START') == "\x00johnӁ\x00doeӁ".encode('utf-8')


class TestSASLClient(unittest.TestCase):

    def test_plain_mechanism_basic(self):
        client = SASLClient('localhost', service='cassandra', mechanism='PLAIN',
                           username='testuser', password='testpass')
        response = client.process()
        assert response == b'\x00testuser\x00testpass'
        assert client.complete

    def test_plain_mechanism_with_authorization_id(self):
        client = SASLClient('localhost', service='cassandra', mechanism='PLAIN',
                           username='testuser', password='testpass',
                           authorization_id='admin')
        response = client.process()
        assert response == b'admin\x00testuser\x00testpass'
        assert client.complete

    def test_plain_mechanism_unicode(self):
        client = SASLClient('localhost', service='cassandra', mechanism='PLAIN',
                           username='johnӁ', password='doeӁ')
        response = client.process()
        expected = b'\x00' + 'johnӁ'.encode('utf-8') + b'\x00' + 'doeӁ'.encode('utf-8')
        assert response == expected
        assert client.complete

    def test_unknown_mechanism(self):
        with pytest.raises(SASLError, match='Unknown mechanism'):
            SASLClient('localhost', service='cassandra', mechanism='UNKNOWN')

    def test_gssapi_without_kerberos(self):
        # This test verifies proper error handling when kerberos is not installed
        try:
            import kerberos
            pytest.skip('kerberos is installed, skipping this test')
        except ImportError:
            with pytest.raises(SASLError, match='kerberos module not installed'):
                SASLClient('localhost', service='cassandra', mechanism='GSSAPI')

    def test_qop_constants(self):
        assert QOP.AUTH == b'auth'
        assert QOP.AUTH_INT == b'auth-int'
        assert QOP.AUTH_CONF == b'auth-conf'
        assert QOP.all == (b'auth', b'auth-int', b'auth-conf')

    def test_qop_bitmask_conversion(self):
        # Test bitmask to names
        assert QOP.names_from_bitmask(1) == {b'auth'}
        assert QOP.names_from_bitmask(2) == {b'auth-int'}
        assert QOP.names_from_bitmask(4) == {b'auth-conf'}
        assert QOP.names_from_bitmask(7) == {b'auth', b'auth-int', b'auth-conf'}

    def test_dispose_clears_password(self):
        client = SASLClient('localhost', service='cassandra', mechanism='PLAIN',
                           username='testuser', password='secret')
        client.process()
        client.dispose()
        assert client._chosen_mech.password is None


class TestSaslAuthProvider(unittest.TestCase):

    def test_host_passthrough(self):
        sasl_kwargs = {'service': 'cassandra', 'mechanism': 'PLAIN',
                       'username': 'user', 'password': 'pass'}
        provider = SaslAuthProvider(**sasl_kwargs)
        host = 'thehostname'
        authenticator = provider.new_authenticator(host)
        assert authenticator.sasl.host == host

    def test_host_rejected(self):
        sasl_kwargs = {'host': 'something'}
        with pytest.raises(ValueError):
            SaslAuthProvider(**sasl_kwargs)

    def test_initial_response(self):
        sasl_kwargs = {'service': 'cassandra', 'mechanism': 'PLAIN',
                       'username': 'testuser', 'password': 'testpass'}
        provider = SaslAuthProvider(**sasl_kwargs)
        authenticator = provider.new_authenticator('localhost')
        response = authenticator.initial_response()
        assert response == b'\x00testuser\x00testpass'
