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

from cassandra.connection import DefaultEndPoint, SniEndPoint, SniEndPointFactory, UnixSocketEndPoint

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

    def test_sni_endpoint_tls_session_cache_key(self):
        """Test that SNI endpoints include server_name in cache key."""
        endpoint1 = self.endpoint_factory.create_from_sni('server1.example.com')
        endpoint2 = self.endpoint_factory.create_from_sni('server2.example.com')

        # Both have same proxy address and port
        assert endpoint1.address == endpoint2.address
        assert endpoint1.port == endpoint2.port

        # But different cache keys due to server_name
        assert endpoint1.tls_session_cache_key != endpoint2.tls_session_cache_key
        assert endpoint1.tls_session_cache_key == ('proxy.datastax.com', 30002, 'server1.example.com')
        assert endpoint2.tls_session_cache_key == ('proxy.datastax.com', 30002, 'server2.example.com')


class DefaultEndPointTest(unittest.TestCase):

    def test_tls_session_cache_key(self):
        """Test that DefaultEndPoint cache key is (address, port)."""
        endpoint = DefaultEndPoint('10.0.0.1', 9042)
        assert endpoint.tls_session_cache_key == ('10.0.0.1', 9042)

        endpoint2 = DefaultEndPoint('10.0.0.1', 9043)
        assert endpoint2.tls_session_cache_key == ('10.0.0.1', 9043)
        assert endpoint.tls_session_cache_key != endpoint2.tls_session_cache_key


class UnixSocketEndPointTest(unittest.TestCase):

    def test_tls_session_cache_key(self):
        """Test that UnixSocketEndPoint cache key is just the path."""
        endpoint = UnixSocketEndPoint('/var/run/scylla.sock')
        assert endpoint.tls_session_cache_key == ('/var/run/scylla.sock',)

        # Different paths should have different keys
        endpoint2 = UnixSocketEndPoint('/tmp/scylla.sock')
        assert endpoint.tls_session_cache_key != endpoint2.tls_session_cache_key
