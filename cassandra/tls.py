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
Internal TLS configuration and certificate validation helpers.

This module keeps reactor-independent TLS behavior in one place.  The
connection and reactor modules remain responsible only for socket lifecycle
and event-loop integration.
"""

import ipaddress
import logging
import ssl
import unicodedata


log = logging.getLogger(__name__)


_STDLIB_PROTOCOL_TO_PYOPENSSL_METHODS = {
    'PROTOCOL_TLS_CLIENT': ('TLS_CLIENT_METHOD', 'TLS_METHOD', 'TLSv1_2_METHOD'),
    'PROTOCOL_TLS': ('TLS_METHOD', 'TLS_CLIENT_METHOD', 'TLSv1_2_METHOD'),
    'PROTOCOL_SSLv23': ('TLS_METHOD', 'TLS_CLIENT_METHOD', 'TLSv1_2_METHOD'),
    'PROTOCOL_TLSv1_2': ('TLSv1_2_METHOD',),
    'PROTOCOL_TLSv1_1': ('TLSv1_1_METHOD',),
    'PROTOCOL_TLSv1': ('TLSv1_METHOD',),
}


_SSL_CONTEXT_OPTION_NAMES = frozenset((
    'ssl_version',
    'cert_reqs',
    'check_hostname',
    'keyfile',
    'certfile',
    'ca_certs',
    'ciphers',
))


def _ssl_options_requiring_new_context(ssl_options):
    """Return endpoint options that cannot safely alter an existing context."""
    return set(ssl_options or {}) & _SSL_CONTEXT_OPTION_NAMES


def _prepare_ssl_options(ssl_options, endpoint_ssl_options):
    """
    Merge caller and endpoint options and derive their default verification.

    Endpoint options override caller options.  An explicitly empty caller
    mapping enables TLS without verification, while endpoint-provided options
    default to verification.  A caller's empty mapping also remains
    unverified when the endpoint contributes only SNI.
    """
    endpoint_has_non_sni_options = bool(
        endpoint_ssl_options and
        any(name != 'server_hostname' for name in endpoint_ssl_options)
    )
    if ssl_options is not None:
        verify_by_default = bool(ssl_options) or endpoint_has_non_sni_options
    else:
        verify_by_default = bool(endpoint_ssl_options)

    merged_options = dict(ssl_options or {})
    merged_options.update(endpoint_ssl_options or {})
    enabled = ssl_options is not None or endpoint_ssl_options is not None
    return merged_options, enabled, verify_by_default


def _stdlib_protocol_from_symbolic_name(protocol_name):
    protocol = getattr(ssl, protocol_name, None)
    if not protocol_name.startswith('PROTOCOL_') or protocol is None:
        raise ValueError(
            'unknown symbolic stdlib SSL protocol %r' % (protocol_name,))
    return protocol


def _build_ssl_context_from_options(ssl_options, verify_by_default=None):
    """Build a stdlib :class:`ssl.SSLContext` from legacy SSL options."""
    ssl_options = ssl_options or {}
    ssl_version = (
        ssl_options.get('ssl_version') or ssl.PROTOCOL_TLS_CLIENT
    )
    if isinstance(ssl_version, str):
        ssl_version = _stdlib_protocol_from_symbolic_name(ssl_version)

    cert_reqs = ssl_options.get('cert_reqs')
    check_hostname = bool(ssl_options.get('check_hostname', False))
    if cert_reqs is None:
        if verify_by_default is None:
            verify_by_default = bool(ssl_options)
        cert_reqs = (
            ssl.CERT_REQUIRED if verify_by_default else ssl.CERT_NONE
        )
    if check_hostname and cert_reqs == ssl.CERT_NONE:
        cert_reqs = ssl.CERT_REQUIRED

    context = ssl.SSLContext(protocol=int(ssl_version))
    context.check_hostname = False
    context.verify_mode = cert_reqs
    context.check_hostname = check_hostname

    certfile = ssl_options.get('certfile')
    if certfile:
        context.load_cert_chain(
            certfile, ssl_options.get('keyfile') or None)

    ca_certs = ssl_options.get('ca_certs')
    if ca_certs:
        context.load_verify_locations(ca_certs)
    elif cert_reqs != ssl.CERT_NONE:
        load_default_certs = getattr(context, 'load_default_certs', None)
        if load_default_certs is not None:
            load_default_certs()

    ciphers = ssl_options.get('ciphers')
    if ciphers:
        context.set_ciphers(ciphers)
    return context


def _wrap_socket_from_context(context, sock, ssl_options, endpoint_address):
    """Wrap a socket using only options accepted by ``SSLContext.wrap_socket``."""
    wrap_option_names = (
        'server_side',
        'do_handshake_on_connect',
        'suppress_ragged_eofs',
        'server_hostname',
    )
    options = {
        name: ssl_options[name]
        for name in wrap_option_names
        if name in ssl_options
    }
    if context.check_hostname and 'server_hostname' not in options:
        options['server_hostname'] = endpoint_address
    return context.wrap_socket(sock, **options)


def _ssl_context_cert_validation_enabled(ssl_context):
    """Return whether a stdlib or pyOpenSSL context verifies peer certificates."""
    if isinstance(ssl_context, ssl.SSLContext):
        return ssl_context.verify_mode != ssl.CERT_NONE

    from OpenSSL import SSL
    return bool(ssl_context.get_verify_mode() & SSL.VERIFY_PEER)


def _ssl_options_cert_validation_enabled(ssl_options):
    """Return whether legacy SSL options request peer-certificate validation."""
    cert_reqs = ssl_options.get('cert_reqs')
    if cert_reqs is None:
        return bool(ssl_options)
    if ssl_options.get('check_hostname', False):
        return True
    if cert_reqs == ssl.CERT_NONE:
        return False
    if cert_reqs in (ssl.CERT_OPTIONAL, ssl.CERT_REQUIRED):
        return True

    try:
        from OpenSSL import SSL
        return bool(cert_reqs & SSL.VERIFY_PEER)
    except (ImportError, TypeError):
        return False


def _default_pyopenssl_ssl_method(ssl_module):
    for method_name in ('TLS_CLIENT_METHOD', 'TLS_METHOD', 'TLSv1_2_METHOD'):
        method = getattr(ssl_module, method_name, None)
        if method is not None:
            return method
    raise ImportError('pyOpenSSL does not expose a secure TLS client method')


def _numeric_constants(module, name_filter):
    constants = {}
    for name in dir(module):
        if not name_filter(name):
            continue
        value = getattr(module, name, None)
        try:
            numeric_value = int(value)
        except (TypeError, ValueError, OverflowError):
            continue
        constants.setdefault(numeric_value, []).append(name)
    return constants


def _stdlib_protocol_names_by_value():
    return _numeric_constants(
        ssl, lambda name: name.startswith('PROTOCOL_'))


def _pyopenssl_method_names_by_value(ssl_module):
    return _numeric_constants(
        ssl_module, lambda name: name.endswith('_METHOD'))


def _pyopenssl_method_from_stdlib_names(ssl_module, protocol_names):
    resolved_methods = []
    unsupported_protocols = []
    for protocol_name in protocol_names:
        method_names = _STDLIB_PROTOCOL_TO_PYOPENSSL_METHODS.get(protocol_name)
        if method_names is None:
            unsupported_protocols.append(protocol_name)
            continue
        for method_name in method_names:
            method = getattr(ssl_module, method_name, None)
            if method is not None:
                resolved_methods.append((protocol_name, method_name, method))
                break
        else:
            raise ImportError(
                'pyOpenSSL does not expose a method for %s' % protocol_name)

    if unsupported_protocols:
        raise ValueError(
            'stdlib SSL protocol %s is not supported for client connections' %
            ', '.join(sorted(unsupported_protocols)))

    numeric_methods = {}
    for protocol_name, method_name, method in resolved_methods:
        try:
            method_key = int(method)
        except (TypeError, ValueError, OverflowError):
            method_key = id(method)
        numeric_methods.setdefault(method_key, []).append(
            (protocol_name, method_name, method))
    if len(numeric_methods) != 1:
        raise ValueError(
            'stdlib SSL protocol aliases %s resolve to different '
            'pyOpenSSL methods' % ', '.join(sorted(protocol_names)))
    return next(iter(numeric_methods.values()))[0][2]


def _pyopenssl_ssl_method_from_stdlib(ssl_module, ssl_version):
    """
    Resolve stdlib protocols without guessing the meaning of ambiguous ints.

    pyOpenSSL exposes its protocol methods as plain integers, and several of
    their values collide with different stdlib protocols. Symbolic stdlib
    names provide an unambiguous serialized representation.
    """
    if ssl_version is None:
        return _default_pyopenssl_ssl_method(ssl_module)

    stdlib_protocols = _stdlib_protocol_names_by_value()

    if isinstance(ssl_version, str):
        _stdlib_protocol_from_symbolic_name(ssl_version)
        return _pyopenssl_method_from_stdlib_names(
            ssl_module, (ssl_version,))

    for protocol_names in stdlib_protocols.values():
        protocol = getattr(ssl, protocol_names[0])
        if (ssl_version.__class__ is protocol.__class__ and
                ssl_version == protocol):
            return _pyopenssl_method_from_stdlib_names(
                ssl_module, protocol_names)

    if isinstance(ssl_version, int):
        protocol_names = stdlib_protocols.get(ssl_version, ())
        pyopenssl_method_names = _pyopenssl_method_names_by_value(
            ssl_module).get(ssl_version, ())

        if protocol_names:
            method = _pyopenssl_method_from_stdlib_names(
                ssl_module, protocol_names)
            if pyopenssl_method_names and int(method) != ssl_version:
                raise ValueError(
                    'plain integer ssl_version %r is ambiguous between '
                    'stdlib protocols %s and pyOpenSSL methods %s; pass an '
                    'ssl.PROTOCOL_* value directly or use its symbolic name' %
                    (
                        ssl_version,
                        ', '.join(sorted(protocol_names)),
                        ', '.join(sorted(pyopenssl_method_names)),
                    )
                )
            return method

        if pyopenssl_method_names:
            return ssl_version

        raise ValueError(
            'plain integer ssl_version %r does not identify a protocol '
            'exposed by stdlib ssl or pyOpenSSL' % ssl_version)

    return ssl_version


def _pyopenssl_verify_mode_from_cert_reqs(ssl_module, cert_reqs):
    # ``cert_reqs`` is a legacy stdlib-style option, so stdlib constants win
    # when their integer values collide with pyOpenSSL verification flags.
    if cert_reqs is None:
        return None
    if cert_reqs == ssl.CERT_NONE:
        return ssl_module.VERIFY_NONE
    if cert_reqs in (ssl.CERT_OPTIONAL, ssl.CERT_REQUIRED):
        return ssl_module.VERIFY_PEER
    return cert_reqs


def _ensure_pyopenssl_context_requires_verification(
        ssl_module, context, check_hostname):
    """
    Make hostname verification fail closed for a caller-supplied context.

    When hostname checking is enabled, this may mutate the context's verify
    mode and callback because pyOpenSSL does not expose a non-mutating way to
    require peer verification. Twisted may also replace a context-level info
    callback when the installed pyOpenSSL version does not support callbacks
    on individual connections.
    """
    if not check_hostname:
        return

    get_verify_mode = getattr(context, 'get_verify_mode', None)
    verify_mode = get_verify_mode() if get_verify_mode is not None else None
    if (verify_mode is not None and
            verify_mode & ssl_module.VERIFY_PEER):
        return

    promoted_verify_mode = ssl_module.VERIFY_PEER
    if get_verify_mode is not None:
        promoted_verify_mode = (
            (verify_mode or ssl_module.VERIFY_NONE) |
            ssl_module.VERIFY_PEER
        )

    log.warning(
        "check_hostname=True requires peer verification; mutating supplied "
        "pyOpenSSL context to use VERIFY_PEER and replacing its verification "
        "callback"
    )
    context.set_verify(
        promoted_verify_mode,
        callback=lambda _connection, _x509, _errnum, _errdepth, ok: ok
    )


def _build_pyopenssl_context_from_options(
        ssl_module, ssl_options, verify_by_default=None):
    """Build a pyOpenSSL context from legacy stdlib-style SSL options."""
    ssl_options = ssl_options or {}
    method = _pyopenssl_ssl_method_from_stdlib(
        ssl_module, ssl_options.get('ssl_version'))
    context = ssl_module.Context(method)

    certfile = ssl_options.get('certfile')
    if certfile:
        use_certificate_chain_file = getattr(
            context, 'use_certificate_chain_file', None)
        if use_certificate_chain_file is not None:
            use_certificate_chain_file(certfile)
        else:
            context.use_certificate_file(certfile)
        context.use_privatekey_file(
            ssl_options.get('keyfile') or certfile)

    ca_certs = ssl_options.get('ca_certs')
    if ca_certs:
        context.load_verify_locations(ca_certs)

    cert_reqs = _pyopenssl_verify_mode_from_cert_reqs(
        ssl_module, ssl_options.get('cert_reqs'))
    if cert_reqs is None:
        if verify_by_default is None:
            verify_by_default = bool(ssl_options)
        cert_reqs = (
            ssl_module.VERIFY_PEER
            if verify_by_default
            else ssl_module.VERIFY_NONE
        )
    if (ssl_options.get('check_hostname', False) and
            not cert_reqs & ssl_module.VERIFY_PEER):
        cert_reqs |= ssl_module.VERIFY_PEER

    context.set_verify(
        cert_reqs,
        callback=lambda _connection, _x509, _errnum, _errdepth, ok: ok
    )
    if (cert_reqs & ssl_module.VERIFY_PEER and
            not ssl_options.get('ca_certs')):
        set_default_verify_paths = getattr(
            context, 'set_default_verify_paths', None)
        if set_default_verify_paths is not None:
            set_default_verify_paths()

    ciphers = ssl_options.get('ciphers')
    if ciphers:
        if isinstance(ciphers, str):
            ciphers = ciphers.encode('ascii')
        context.set_cipher_list(ciphers)
    return context


def _idna_ascii_hostname(hostname):
    """
    Convert a DNS name to ASCII without accepting lossy IDNA 2003 mappings.

    Python's built-in ``idna`` codec maps some distinct IDNA 2008 names to the
    same ASCII name (for example, ``faß.de`` to ``fass.de``). Reject any
    non-ASCII input that does not round-trip through the codec so callers can
    supply the unambiguous IDNA 2008 A-label instead.
    """
    try:
        ascii_hostname = hostname.encode('idna').decode('ascii')
        if any(ord(character) > 127 for character in hostname):
            round_tripped = ascii_hostname.encode('ascii').decode('idna')
            normalized_input = unicodedata.normalize('NFC', hostname).lower()
            normalized_round_trip = unicodedata.normalize(
                'NFC', round_tripped).lower()
            if normalized_input != normalized_round_trip:
                return None
    except (AttributeError, UnicodeError):
        return None
    return ascii_hostname.lower()


def _normalized_hostname(hostname, is_reference=False):
    if is_reference:
        if hostname.endswith('.'):
            hostname = hostname[:-1]
            if hostname.endswith('.'):
                return None
    elif hostname.endswith('.'):
        return None
    return _idna_ascii_hostname(hostname)


def _encode_server_hostname(hostname):
    normalized = _normalized_hostname(hostname, is_reference=True)
    if not normalized:
        raise ValueError(
            "server_hostname %r cannot be safely encoded as an IDNA name" %
            hostname)
    return normalized.encode('ascii')


def _dnsname_match(dn, hostname):
    dn = _normalized_hostname(dn)
    hostname = _normalized_hostname(hostname, is_reference=True)

    if not dn or not hostname:
        return False

    if '*' not in dn:
        return dn == hostname

    dn_labels = dn.split('.')
    hostname_labels = hostname.split('.')
    if (len(dn_labels) != len(hostname_labels) or
            len(dn_labels) < 2 or dn_labels[0] != '*' or
            not hostname_labels[0] or
            any('*' in label for label in dn_labels[1:])):
        return False

    return dn_labels[1:] == hostname_labels[1:]


def _ipaddress_match(cert_ip, hostname):
    try:
        host_ip = ipaddress.ip_address(hostname)
        cert_ip = ipaddress.ip_address(cert_ip)
    except ValueError:
        return False
    return cert_ip == host_ip


def _is_ip_address(hostname):
    try:
        ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return True


def _resolve_pyopenssl_server_names(
        endpoint_address, server_hostname, check_hostname,
        verify_endpoint_address=False):
    """
    Resolve the SNI name and certificate identity for pyOpenSSL reactors.

    SNI proxy endpoints route using an explicit server name but authenticate
    the proxy address. Ordinary endpoints authenticate an explicit
    ``server_hostname`` when present, otherwise their endpoint address.
    """
    expected_name = (
        endpoint_address
        if verify_endpoint_address
        else server_hostname or endpoint_address
    )
    if (server_hostname is None and check_hostname and
            not _is_ip_address(endpoint_address)):
        server_hostname = endpoint_address
    return server_hostname, expected_name


def _pyopenssl_cert_subject_alt_names(cert):
    # Imported lazily because cryptography is an optional driver dependency.
    # pyOpenSSL supplies it whenever this code path is used.
    from cryptography import x509

    try:
        san = cert.to_cryptography().extensions.get_extension_for_class(
            x509.SubjectAlternativeName).value
    except x509.ExtensionNotFound:
        return [], []

    return (
        san.get_values_for_type(x509.DNSName),
        [str(ip) for ip in san.get_values_for_type(x509.IPAddress)]
    )


def _pyopenssl_cert_common_names(cert):
    # Imported lazily for the same reason as the SAN types above.
    from cryptography.x509.oid import NameOID

    return [
        attribute.value
        for attribute in cert.to_cryptography().subject.get_attributes_for_oid(
            NameOID.COMMON_NAME)
    ]


def _validate_pyopenssl_hostname(cert, hostname):
    if cert is None:
        raise ssl.CertificateError(
            "peer did not present a certificate; cannot verify hostname %r" %
            hostname)

    san_dns_names, san_ip_addresses = _pyopenssl_cert_subject_alt_names(cert)
    hostname_is_ip = _is_ip_address(hostname)

    if hostname_is_ip:
        for cert_ip in san_ip_addresses:
            if _ipaddress_match(cert_ip, hostname):
                return
        raise ssl.CertificateError(
            "IP address %r doesn't match certificate IP subjectAltName %r" %
            (hostname, san_ip_addresses))

    for cert_hostname in san_dns_names:
        if _dnsname_match(cert_hostname, hostname):
            return
    if san_dns_names:
        raise ssl.CertificateError(
            "hostname %r doesn't match certificate DNS subjectAltName %r" %
            (hostname, san_dns_names))

    # Match SSLContext/OpenSSL X509_check_host semantics: for a DNS reference,
    # only DNS subjectAltNames suppress commonName fallback. IP subjectAltNames
    # are a different identity type and do not suppress it. Conversely, IP
    # references are handled above and never fall back to commonName. The
    # deprecated ssl.match_hostname() helper differs in the DNS-with-IP-SAN
    # case and is not the compatibility target here.
    common_names = _pyopenssl_cert_common_names(cert)
    for common_name in common_names:
        if _dnsname_match(common_name, hostname):
            return

    raise ssl.CertificateError(
        "hostname %r doesn't match certificate commonName %r" %
        (hostname, common_names))
