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

from cassandra.auth import (Authenticator, PlainTextAuthProvider, PlainTextAuthenticator,
                            SaslAuthProvider, SaslAuthenticator,
                            TransitionalModePlainTextAuthProvider,
                            TransitionalModePlainTextAuthenticator)

import unittest
from unittest.mock import patch

import pytest


class TestAuthenticator(unittest.TestCase):

    def test_initial_response_defaults_to_none(self):
        assert Authenticator().initial_response() is None


class TestPlainTextAuthProvider(unittest.TestCase):

    def test_new_authenticator(self):
        provider = PlainTextAuthProvider("user", "pass")
        authenticator = provider.new_authenticator("127.0.0.1")
        assert isinstance(authenticator, PlainTextAuthenticator)
        assert authenticator.username == "user"
        assert authenticator.password == "pass"


class TestPlainTextAuthenticator(unittest.TestCase):

    def test_initial_response(self):
        authenticator = PlainTextAuthenticator("user", "pass")
        assert authenticator.initial_response() == b"\x00user\x00pass"

    def test_evaluate_challenge_with_invalid_challenge(self):
        authenticator = PlainTextAuthenticator("user", "pass")
        with pytest.raises(Exception, match="Did not receive a valid challenge"):
            authenticator.evaluate_challenge(b"UNEXPECTED")

    def test_evaluate_challenge_with_unicode_data(self):
        authenticator = PlainTextAuthenticator("johnӁ", "doeӁ")
        assert authenticator.evaluate_challenge(b'PLAIN-START') == "\x00johnӁ\x00doeӁ".encode('utf-8')


class TestTransitionalModePlainTextAuthProvider(unittest.TestCase):

    def test_logs_deprecation_warning(self):
        with self.assertLogs("cassandra.auth", level="WARNING") as logs:
            TransitionalModePlainTextAuthProvider()
        assert len(logs.records) == 1
        assert "will be removed in scylla-driver 4.0" in logs.records[0].getMessage()

    def test_new_authenticator_sends_empty_credentials(self):
        with self.assertLogs("cassandra.auth", level="WARNING"):
            provider = TransitionalModePlainTextAuthProvider()
        authenticator = provider.new_authenticator("127.0.0.1")
        assert isinstance(authenticator, TransitionalModePlainTextAuthenticator)
        assert authenticator.initial_response() == b"\x00\x00"


class TestSaslAuthProvider(unittest.TestCase):

    @patch("cassandra.auth.SASLClient")
    def test_rejects_host_kwarg(self, sasl_client):
        with pytest.raises(ValueError, match="host"):
            SaslAuthProvider(service="cassandra", mechanism="PLAIN", host="127.0.0.1")

    @patch("cassandra.auth.SASLClient")
    def test_new_authenticator_passes_host_and_kwargs(self, sasl_client):
        provider = SaslAuthProvider(service="cassandra", mechanism="PLAIN",
                                    username="user", password="pass")
        authenticator = provider.new_authenticator("127.0.0.1")
        assert isinstance(authenticator, SaslAuthenticator)
        sasl_client.assert_called_once_with("127.0.0.1", "cassandra", "PLAIN",
                                            username="user", password="pass")
        assert authenticator.sasl is sasl_client.return_value

    @patch("cassandra.auth.SASLClient", None)
    def test_requires_puresasl(self):
        with pytest.raises(ImportError, match="puresasl"):
            SaslAuthProvider(service="cassandra", mechanism="PLAIN")


class TestSaslAuthenticator(unittest.TestCase):

    @patch("cassandra.auth.SASLClient")
    def test_defaults_to_gssapi_mechanism(self, sasl_client):
        SaslAuthenticator("127.0.0.1", "cassandra")
        sasl_client.assert_called_once_with("127.0.0.1", "cassandra", "GSSAPI")

    @patch("cassandra.auth.SASLClient")
    def test_delegates_to_sasl_client(self, sasl_client):
        sasl = sasl_client.return_value
        sasl.process.side_effect = [b"initial", b"response"]
        authenticator = SaslAuthenticator("127.0.0.1", "cassandra", mechanism="PLAIN")

        assert authenticator.initial_response() == b"initial"
        sasl.process.assert_called_once_with()

        assert authenticator.evaluate_challenge(b"challenge") == b"response"
        sasl.process.assert_called_with(b"challenge")
        assert sasl.process.call_count == 2

    @patch("cassandra.auth.SASLClient", None)
    def test_requires_puresasl(self):
        with pytest.raises(ImportError, match="puresasl"):
            SaslAuthenticator("127.0.0.1", "cassandra", mechanism="PLAIN")
