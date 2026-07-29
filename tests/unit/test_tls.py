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

import ssl
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import pytest

from cassandra.tls import (
    _build_ssl_context_from_options,
    _build_pyopenssl_context_from_options,
    _default_pyopenssl_ssl_method,
    _dnsname_match,
    _encode_server_hostname,
    _ensure_pyopenssl_context_requires_verification,
    _pyopenssl_ssl_method_from_stdlib,
    _resolve_pyopenssl_server_names,
    _validate_pyopenssl_hostname,
)
from tests.unit.io.utils import make_pyopenssl_x509_certificate


try:
    from OpenSSL import crypto as openssl_crypto
except (ImportError, AttributeError):
    openssl_crypto = None


class FakePyOpenSSLContext(object):
    def __init__(self, method):
        self.method = method
        self.verify_mode = None
        self.default_verify_paths_loaded = False
        self.verify_locations = []
        self.set_verify_calls = []
        self.certificate_chain_files = []
        self.certificate_files = []
        self.privatekey_files = []
        self.cipher_lists = []

    def set_verify(self, verify_mode, callback):
        self.verify_mode = verify_mode
        self.verify_callback = callback
        self.set_verify_calls.append((verify_mode, callback))

    def get_verify_mode(self):
        return self.verify_mode

    def set_default_verify_paths(self):
        self.default_verify_paths_loaded = True

    def load_verify_locations(self, path):
        self.verify_locations.append(path)

    def use_certificate_chain_file(self, path):
        self.certificate_chain_files.append(path)

    def use_certificate_file(self, path):
        self.certificate_files.append(path)

    def use_privatekey_file(self, path):
        self.privatekey_files.append(path)

    def set_cipher_list(self, ciphers):
        self.cipher_lists.append(ciphers)


class FakePyOpenSSLContextWithoutChainFile(FakePyOpenSSLContext):
    use_certificate_chain_file = None


class FakePyOpenSSLContextWithoutGetVerifyMode(object):
    def __init__(self, method):
        self.method = method
        self.verify_mode = None

    def set_verify(self, verify_mode, callback):
        self.verify_mode = verify_mode
        self.verify_callback = callback


class FakePyOpenSSLModule(object):
    TLS_CLIENT_METHOD = object()
    VERIFY_NONE = 0
    VERIFY_PEER = 1
    Context = FakePyOpenSSLContext


class PyOpenSSLMethodTest(unittest.TestCase):

    @staticmethod
    def numeric_ssl_module():
        return SimpleNamespace(
            SSLv23_METHOD=3,
            TLSv1_METHOD=4,
            TLSv1_1_METHOD=5,
            TLSv1_2_METHOD=6,
            TLS_METHOD=7,
            TLS_CLIENT_METHOD=9,
        )

    def test_prefers_tls_client_method(self):
        tls_client_method = object()
        ssl_module = SimpleNamespace(
            TLS_CLIENT_METHOD=tls_client_method,
            TLS_METHOD=object(),
            TLSv1_2_METHOD=object())

        assert _default_pyopenssl_ssl_method(ssl_module) is tls_client_method

    def test_falls_back_to_tls_method(self):
        tls_method = object()
        ssl_module = SimpleNamespace(
            TLS_METHOD=tls_method,
            TLSv1_2_METHOD=object())

        assert _default_pyopenssl_ssl_method(ssl_module) is tls_method

    def test_falls_back_to_tlsv1_2_method(self):
        tlsv1_2_method = object()
        ssl_module = SimpleNamespace(TLSv1_2_METHOD=tlsv1_2_method)

        assert _default_pyopenssl_ssl_method(ssl_module) is tlsv1_2_method

    def test_requires_secure_method(self):
        with pytest.raises(ImportError, match="secure TLS client method"):
            _default_pyopenssl_ssl_method(SimpleNamespace())

    def test_maps_stdlib_protocol_enum(self):
        ssl_module = self.numeric_ssl_module()

        method = _pyopenssl_ssl_method_from_stdlib(
            ssl_module, ssl.PROTOCOL_TLSv1_2)

        assert method == ssl_module.TLSv1_2_METHOD

    def test_maps_unambiguous_serialized_stdlib_protocol(self):
        ssl_module = self.numeric_ssl_module()

        method = _pyopenssl_ssl_method_from_stdlib(
            ssl_module, int(ssl.PROTOCOL_TLS))

        assert method == ssl_module.TLS_METHOD

    def test_rejects_ambiguous_serialized_protocol(self):
        ssl_module = self.numeric_ssl_module()

        with pytest.raises(
                ValueError,
                match=r"ambiguous.*PROTOCOL_TLSv1_2.*TLSv1_1_METHOD"):
            _pyopenssl_ssl_method_from_stdlib(
                ssl_module, int(ssl.PROTOCOL_TLSv1_2))

    def test_preserves_unambiguous_pyopenssl_method_integer(self):
        ssl_module = self.numeric_ssl_module()

        method = _pyopenssl_ssl_method_from_stdlib(
            ssl_module, ssl_module.TLSv1_2_METHOD)

        assert method == ssl_module.TLSv1_2_METHOD

    def test_maps_symbolic_stdlib_protocol(self):
        ssl_module = self.numeric_ssl_module()

        method = _pyopenssl_ssl_method_from_stdlib(
            ssl_module, 'PROTOCOL_TLSv1_2')

        assert method == ssl_module.TLSv1_2_METHOD

    def test_rejects_unknown_symbolic_protocol(self):
        with pytest.raises(
                ValueError, match="unknown symbolic stdlib SSL protocol"):
            _pyopenssl_ssl_method_from_stdlib(
                self.numeric_ssl_module(), 'PROTOCOL_TLSv9')

    def test_rejects_server_protocol(self):
        with pytest.raises(
                ValueError, match="not supported for client connections"):
            _pyopenssl_ssl_method_from_stdlib(
                self.numeric_ssl_module(), 'PROTOCOL_TLS_SERVER')

    def test_rejects_unknown_protocol_integer(self):
        with pytest.raises(
                ValueError, match="does not identify a protocol"):
            _pyopenssl_ssl_method_from_stdlib(
                self.numeric_ssl_module(), 999)


class PyOpenSSLServerNamesTest(unittest.TestCase):

    def test_explicit_server_name_is_used_for_sni_and_verification(self):
        assert _resolve_pyopenssl_server_names(
            '10.0.0.1', 'node.example.com', True) == (
                'node.example.com', 'node.example.com')

    def test_sni_proxy_authenticates_endpoint_address(self):
        assert _resolve_pyopenssl_server_names(
            'proxy.example.com',
            'host-id',
            True,
            verify_endpoint_address=True,
        ) == ('host-id', 'proxy.example.com')

    def test_dns_endpoint_uses_implicit_sni_when_verifying(self):
        assert _resolve_pyopenssl_server_names(
            'node.example.com', None, True) == (
                'node.example.com', 'node.example.com')

    def test_ip_endpoint_omits_implicit_sni(self):
        assert _resolve_pyopenssl_server_names(
            '1.2.3.4', None, True) == (None, '1.2.3.4')


@unittest.skipIf(openssl_crypto is None, "pyOpenSSL is not available")
class PyOpenSSLHostnameValidationTest(unittest.TestCase):

    def test_dnsname_match_normalizes_unicode_hostname_to_idna(self):
        assert _dnsname_match(
            'xn--tst-qla.example', 'täst.example')

    def test_dnsname_match_does_not_apply_idna2003_aliases(self):
        assert not _dnsname_match('fass.de', 'faß.de')

    def test_server_hostname_encoding_rejects_idna2003_aliases(self):
        with self.assertRaises(ValueError):
            _encode_server_hostname('faß.de')

    def test_server_hostname_encoding_preserves_safe_idna(self):
        assert (_encode_server_hostname('täst.example') ==
                b'xn--tst-qla.example')

    def test_dnsname_match_rejects_malformed_certificate_trailing_dots(self):
        assert not _dnsname_match(
            'node.example.com..', 'node.example.com')

    def test_rejects_sole_wildcard_subject_alt_name(self):
        cert = make_pyopenssl_x509_certificate(
            'unused', san_dns_names=['*'])

        with self.assertRaises(ssl.CertificateError):
            _validate_pyopenssl_hostname(cert, 'node1')

    def test_ip_hostname_requires_ip_subject_alt_name(self):
        cert = make_pyopenssl_x509_certificate(
            'unused', san_dns_names=['*.2.3.4'])

        with self.assertRaises(ssl.CertificateError):
            _validate_pyopenssl_hostname(cert, '1.2.3.4')

    def test_ip_hostname_accepts_exact_ip_subject_alt_name(self):
        cert = make_pyopenssl_x509_certificate(
            'unused', san_ip_addresses=['1.2.3.4'])

        _validate_pyopenssl_hostname(cert, '1.2.3.4')

    def test_ip_hostname_does_not_fall_back_to_common_name(self):
        cert = make_pyopenssl_x509_certificate('1.2.3.4')

        with self.assertRaises(ssl.CertificateError):
            _validate_pyopenssl_hostname(cert, '1.2.3.4')

    def test_dns_hostname_falls_back_to_common_name_with_only_ip_san(self):
        cert = make_pyopenssl_x509_certificate(
            'node.example.com', san_ip_addresses=['1.2.3.4'])

        _validate_pyopenssl_hostname(cert, 'node.example.com')

    def test_dns_hostname_rejects_malformed_san_trailing_dots(self):
        cert = make_pyopenssl_x509_certificate(
            'unused', san_dns_names=['node.example.com..'])

        with self.assertRaises(ssl.CertificateError):
            _validate_pyopenssl_hostname(cert, 'node.example.com')

    def test_dns_hostname_accepts_subject_alt_name_on_real_x509(self):
        cert = make_pyopenssl_x509_certificate(
            'wrong.example.com', san_dns_names=['node.example.com'])

        assert isinstance(cert, openssl_crypto.X509)
        _validate_pyopenssl_hostname(cert, 'node.example.com')

    def test_rejects_missing_certificate(self):
        with self.assertRaises(ssl.CertificateError):
            _validate_pyopenssl_hostname(None, 'node.example.com')


class PyOpenSSLContextTest(unittest.TestCase):

    def test_empty_ssl_options_default_to_verify_none(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule, {})

        assert context.method is FakePyOpenSSLModule.TLS_CLIENT_METHOD
        assert context.verify_mode == FakePyOpenSSLModule.VERIFY_NONE
        assert not context.default_verify_paths_loaded

    def test_stdlib_ssl_version_is_translated(self):
        ssl_module = PyOpenSSLMethodTest.numeric_ssl_module()
        ssl_module.VERIFY_NONE = 0
        ssl_module.VERIFY_PEER = 1
        ssl_module.Context = FakePyOpenSSLContext

        context = _build_pyopenssl_context_from_options(
            ssl_module, {'ssl_version': ssl.PROTOCOL_TLS})

        assert context.method == ssl_module.TLS_METHOD

    def test_ciphers_option_is_applied(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule, {'ciphers': 'ECDHE+AESGCM'})

        assert context.cipher_lists == [b'ECDHE+AESGCM']

    def test_non_empty_ssl_options_default_to_verify_peer(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {'server_hostname': 'node.example.com'})

        assert context.verify_mode == FakePyOpenSSLModule.VERIFY_PEER
        assert context.default_verify_paths_loaded

    def test_default_verification_can_ignore_merged_endpoint_options(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {'server_hostname': 'node.example.com'},
            verify_by_default=False)

        assert context.verify_mode == FakePyOpenSSLModule.VERIFY_NONE
        assert not context.default_verify_paths_loaded

    def test_check_hostname_overrides_disabled_default_verification(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {'check_hostname': True},
            verify_by_default=False)

        assert context.verify_mode == FakePyOpenSSLModule.VERIFY_PEER
        assert context.default_verify_paths_loaded

    def test_explicit_ca_certs_do_not_load_default_verify_paths(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {'ca_certs': 'ca.pem'})

        assert context.verify_locations == ['ca.pem']
        assert not context.default_verify_paths_loaded

    def test_verify_peer_without_default_paths_api_is_supported(self):
        ssl_module = SimpleNamespace(
            TLS_CLIENT_METHOD=object(),
            VERIFY_NONE=0,
            VERIFY_PEER=1,
            Context=FakePyOpenSSLContextWithoutGetVerifyMode)

        context = _build_pyopenssl_context_from_options(
            ssl_module, {'server_hostname': 'node.example.com'})

        assert context.verify_mode == ssl_module.VERIFY_PEER

    def test_verify_peer_bitmask_loads_default_verify_paths(self):
        verify_peer_with_flags = FakePyOpenSSLModule.VERIFY_PEER | 4
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {'cert_reqs': verify_peer_with_flags})

        assert context.verify_mode == verify_peer_with_flags
        assert context.default_verify_paths_loaded

    def test_check_hostname_promotes_verify_none_to_verify_peer(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {'cert_reqs': ssl.CERT_NONE, 'check_hostname': True})

        assert context.verify_mode == FakePyOpenSSLModule.VERIFY_PEER

    def test_check_hostname_preserves_flags_while_promoting_verify_peer(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {'cert_reqs': 4, 'check_hostname': True})

        assert context.verify_mode == (4 | FakePyOpenSSLModule.VERIFY_PEER)

    def test_plain_int_cert_reqs_option_is_translated(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {'cert_reqs': int(ssl.CERT_REQUIRED)})

        assert context.verify_mode == FakePyOpenSSLModule.VERIFY_PEER

    def test_loads_client_certificate_chain_and_key(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {
                'certfile': 'client-chain.pem',
                'keyfile': 'client-key.pem',
                'cert_reqs': ssl.CERT_NONE,
            })

        assert context.certificate_chain_files == ['client-chain.pem']
        assert context.certificate_files == []
        assert context.privatekey_files == ['client-key.pem']

    def test_combined_client_certificate_and_key_file(self):
        context = _build_pyopenssl_context_from_options(
            FakePyOpenSSLModule,
            {
                'certfile': 'combined.pem',
                'cert_reqs': ssl.CERT_NONE,
            })

        assert context.certificate_chain_files == ['combined.pem']
        assert context.privatekey_files == ['combined.pem']

    def test_falls_back_when_certificate_chain_api_is_unavailable(self):
        ssl_module = SimpleNamespace(
            TLS_CLIENT_METHOD=object(),
            VERIFY_NONE=0,
            VERIFY_PEER=1,
            Context=FakePyOpenSSLContextWithoutChainFile)

        context = _build_pyopenssl_context_from_options(
            ssl_module,
            {
                'certfile': 'combined.pem',
                'cert_reqs': ssl.CERT_NONE,
            })

        assert context.certificate_chain_files == []
        assert context.certificate_files == ['combined.pem']
        assert context.privatekey_files == ['combined.pem']

    def test_ignores_falsey_legacy_file_options(self):
        for falsey in (None, ''):
            with self.subTest(falsey=falsey):
                context = _build_pyopenssl_context_from_options(
                    FakePyOpenSSLModule,
                    {
                        'certfile': falsey,
                        'keyfile': 'unused-key.pem',
                        'ca_certs': falsey,
                    })

                assert context.certificate_chain_files == []
                assert context.certificate_files == []
                assert context.privatekey_files == []
                assert context.verify_locations == []

    def test_supplied_context_check_hostname_promotes_verify_none_to_verify_peer(self):
        context = FakePyOpenSSLContext(
            FakePyOpenSSLModule.TLS_CLIENT_METHOD)
        context.verify_mode = FakePyOpenSSLModule.VERIFY_NONE

        _ensure_pyopenssl_context_requires_verification(
            FakePyOpenSSLModule, context, True)

        assert context.verify_mode == FakePyOpenSSLModule.VERIFY_PEER

    def test_supplied_context_without_check_hostname_preserves_verify_none(self):
        context = FakePyOpenSSLContext(
            FakePyOpenSSLModule.TLS_CLIENT_METHOD)
        context.verify_mode = FakePyOpenSSLModule.VERIFY_NONE

        _ensure_pyopenssl_context_requires_verification(
            FakePyOpenSSLModule, context, False)

        assert context.verify_mode == FakePyOpenSSLModule.VERIFY_NONE

    def test_supplied_context_with_verify_peer_preserves_custom_callback(self):
        context = FakePyOpenSSLContext(
            FakePyOpenSSLModule.TLS_CLIENT_METHOD)
        custom_callback = Mock()
        secure_mode = FakePyOpenSSLModule.VERIFY_PEER | 4
        context.set_verify(secure_mode, custom_callback)

        _ensure_pyopenssl_context_requires_verification(
            FakePyOpenSSLModule, context, True)

        assert context.verify_callback is custom_callback
        assert context.set_verify_calls == [
            (secure_mode, custom_callback)]

    def test_supplied_context_without_verify_peer_flag_is_promoted(self):
        context = FakePyOpenSSLContext(
            FakePyOpenSSLModule.TLS_CLIENT_METHOD)
        context.set_verify(4, Mock())

        with self.assertLogs('cassandra.tls', level='WARNING'):
            _ensure_pyopenssl_context_requires_verification(
                FakePyOpenSSLModule, context, True)

        assert context.verify_mode == (4 | FakePyOpenSSLModule.VERIFY_PEER)

    def test_supplied_context_without_get_verify_mode_promotes_to_verify_peer(self):
        context = FakePyOpenSSLContextWithoutGetVerifyMode(
            FakePyOpenSSLModule.TLS_CLIENT_METHOD)

        with self.assertLogs('cassandra.tls', level='WARNING') as logs:
            _ensure_pyopenssl_context_requires_verification(
                FakePyOpenSSLModule, context, True)

        assert context.verify_mode == FakePyOpenSSLModule.VERIFY_PEER
        assert "replacing its verification callback" in logs.output[0]


class StdlibSSLContextTest(unittest.TestCase):

    def test_non_empty_options_load_system_default_ca_certs(self):
        context = Mock()

        with patch('cassandra.tls.ssl.SSLContext', return_value=context):
            result = _build_ssl_context_from_options(
                {'server_hostname': 'node.example.com'})

        assert result is context
        context.load_default_certs.assert_called_once_with()
        context.load_verify_locations.assert_not_called()

    def test_explicit_ca_certs_do_not_load_system_defaults(self):
        context = Mock()

        with patch('cassandra.tls.ssl.SSLContext', return_value=context):
            result = _build_ssl_context_from_options(
                {'ca_certs': 'ca.pem'})

        assert result is context
        context.load_verify_locations.assert_called_once_with('ca.pem')
        context.load_default_certs.assert_not_called()

    def test_falsey_legacy_file_options_are_ignored(self):
        context = Mock()

        with patch('cassandra.tls.ssl.SSLContext', return_value=context):
            result = _build_ssl_context_from_options({
                'certfile': '',
                'keyfile': '',
                'ca_certs': '',
                'cert_reqs': ssl.CERT_NONE,
            })

        assert result is context
        context.load_cert_chain.assert_not_called()
        context.load_verify_locations.assert_not_called()

    def test_falsey_keyfile_treats_certfile_as_combined_pem(self):
        context = Mock()

        with patch('cassandra.tls.ssl.SSLContext', return_value=context):
            result = _build_ssl_context_from_options({
                'certfile': 'combined.pem',
                'keyfile': '',
                'cert_reqs': ssl.CERT_NONE,
            })

        assert result is context
        context.load_cert_chain.assert_called_once_with('combined.pem', None)
