# Copyright DataStax, Inc.
#
# Licensed under the DataStax DSE Driver License;
# you may not use this file except in compliance with the License.
#
# You may obtain a copy of the License at
#
# http://www.datastax.com/terms/datastax-dse-driver-license-terms
import unittest

import itertools
import uuid

from cassandra.connection import (ClientRoutesEndPoint, DefaultEndPoint,
                                  SniEndPointFactory, UnixSocketEndPoint)

from unittest.mock import patch


def socket_getaddrinfo(*args):
    return [
        (0, 0, 0, '', ('127.0.0.1', 30002)),
        (0, 0, 0, '', ('127.0.0.2', 30002)),
        (0, 0, 0, '', ('127.0.0.3', 30002))
    ]


@patch('socket.getaddrinfo', socket_getaddrinfo)
class SniEndPointTest(unittest.TestCase):

    endpoint_factory = SniEndPointFactory("proxy.datastax.com", 30002)

    def test_sni_endpoint_properties(self):

        endpoint = self.endpoint_factory.create_from_sni('test')
        assert endpoint.address == 'proxy.datastax.com'
        assert endpoint.port == 30002
        assert endpoint._server_name == 'test'
        assert str(endpoint) == 'proxy.datastax.com:30002:test'

    def test_endpoint_equality(self):
        assert DefaultEndPoint('10.0.0.1') != self.endpoint_factory.create_from_sni('10.0.0.1')

        assert self.endpoint_factory.create_from_sni('10.0.0.1') == self.endpoint_factory.create_from_sni('10.0.0.1')

        assert self.endpoint_factory.create_from_sni('10.0.0.1') != self.endpoint_factory.create_from_sni('10.0.0.0')

        assert self.endpoint_factory.create_from_sni('10.0.0.1') != SniEndPointFactory("proxy.datastax.com", 9999).create_from_sni('10.0.0.1')

    def test_endpoint_resolve(self):
        ips = ['127.0.0.1', '127.0.0.2', '127.0.0.3']
        it = itertools.cycle(ips)

        endpoint = self.endpoint_factory.create_from_sni('test')
        for i in range(10):
            (address, _) = endpoint.resolve()
            assert address == next(it)

    def test_tls_session_cache_key_distinguishes_server_names(self):
        # All SNI endpoints behind a proxy share an address and port, so the
        # server name has to be part of the key or they would share sessions.
        one = self.endpoint_factory.create_from_sni('node1')
        other = self.endpoint_factory.create_from_sni('node2')

        assert one.tls_session_cache_key != other.tls_session_cache_key
        assert one.tls_session_cache_key == \
            self.endpoint_factory.create_from_sni('node1').tls_session_cache_key
        assert one.tls_session_cache_key != DefaultEndPoint(
            'proxy.datastax.com', 30002).tls_session_cache_key


class TlsSessionCacheKeyTest(unittest.TestCase):

    def test_default_endpoint_key(self):
        assert DefaultEndPoint('10.0.0.1', 9042).tls_session_cache_key == ('10.0.0.1', 9042)
        assert DefaultEndPoint('10.0.0.1', 9042).tls_session_cache_key != \
            DefaultEndPoint('10.0.0.1', 9142).tls_session_cache_key

    def test_unix_socket_endpoint_key(self):
        assert UnixSocketEndPoint('/tmp/a').tls_session_cache_key != \
            UnixSocketEndPoint('/tmp/b').tls_session_cache_key

    def test_client_routes_endpoint_key_follows_the_node_not_the_route(self):
        host_id = uuid.uuid4()
        endpoint = ClientRoutesEndPoint(host_id, handler=None,
                                        original_address='10.0.0.1',
                                        original_port=9042)
        other = ClientRoutesEndPoint(uuid.uuid4(), handler=None,
                                     original_address='10.0.0.1',
                                     original_port=9042)

        assert endpoint.tls_session_cache_key == (host_id, '10.0.0.1', 9042)
        assert endpoint.tls_session_cache_key != other.tls_session_cache_key

    def test_an_override_replaces_the_endpoints_own_identity(self):
        # An endpoint built to reach a node another one already describes -- the
        # shard-aware port alias -- carries that node's key so both share one
        # cached session.
        node = DefaultEndPoint('10.0.0.1', 9042)
        alias = DefaultEndPoint('10.0.0.1', 19142)
        assert alias.tls_session_cache_key != node.tls_session_cache_key

        alias._tls_session_cache_key_override = node.tls_session_cache_key

        assert alias.tls_session_cache_key == node.tls_session_cache_key

    def test_an_override_applies_to_every_endpoint_type(self):
        # The override lives on the base property, so a subclass that gives its
        # own identity still honours it.
        endpoints = [DefaultEndPoint('10.0.0.1'),
                     UnixSocketEndPoint('/tmp/a'),
                     ClientRoutesEndPoint(uuid.uuid4(), None, '10.0.0.1', 9042),
                     SniEndPointFactory("proxy", 30002).create_from_sni('node1')]
        for endpoint in endpoints:
            endpoint._tls_session_cache_key_override = ('the', 'node')
            assert endpoint.tls_session_cache_key == ('the', 'node'), endpoint

    def test_keys_are_hashable(self):
        # Keys are used as dict keys in SSLSessionCache.
        for endpoint in (DefaultEndPoint('10.0.0.1'),
                         UnixSocketEndPoint('/tmp/a'),
                         ClientRoutesEndPoint(uuid.uuid4(), None, '10.0.0.1', 9042)):
            hash(endpoint.tls_session_cache_key)
