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

from cassandra.connection import DefaultEndPoint, SniEndPoint, SniEndPointFactory

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
