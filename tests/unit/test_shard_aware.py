# Copyright 2020 ScyllaDB, Inc.
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

import unittest

import logging
import time
from unittest.mock import MagicMock
from concurrent.futures import ThreadPoolExecutor

from cassandra.cluster import ShardAwareOptions
from cassandra.pool import HostConnection, HostDistance
from cassandra.connection import ShardingInfo, DefaultEndPoint
from cassandra.metadata import Murmur3Token
from cassandra.protocol_features import ProtocolFeatures

LOGGER = logging.getLogger(__name__)


class MockSession(MagicMock):
    is_shutdown = False
    keyspace = "ks1"

    def __init__(self, ssl_options=None, ssl_context=None, sharding_info=None,
                 *args, **kwargs):
        super(MockSession, self).__init__(*args, **kwargs)
        self.cluster = MagicMock()
        self.cluster.ssl_options = ssl_options
        self.cluster.ssl_context = ssl_context
        self.cluster.shard_aware_options = ShardAwareOptions()
        self.cluster.connect_timeout = 5
        self.cluster.executor = ThreadPoolExecutor(max_workers=2)
        self.cluster.signal_connection_failure = lambda *args, **kwargs: False
        self.cluster.connection_factory = self.mock_connection_factory
        self.connection_counter = 0
        self.futures = []
        self.sharding_info = sharding_info

    def submit(self, fn, *args, **kwargs):
        logging.info("Scheduling %s with args: %s, kwargs: %s", fn, args, kwargs)
        if not self.is_shutdown:
            f = self.cluster.executor.submit(fn, *args, **kwargs)
            self.futures += [f]
            return f

    def mock_connection_factory(self, *args, **kwargs):
        connection = MagicMock()
        connection.is_shutdown = False
        connection.is_defunct = False
        connection.is_closed = False
        connection.orphaned_threshold_reached = False
        connection.endpoint = args[0]
        sharding_info = self.sharding_info or ShardingInfo(
            shard_id=1, shards_count=4, partitioner="",
            sharding_algorithm="", sharding_ignore_msb=0,
            shard_aware_port=19042, shard_aware_port_ssl=19045)
        connection.features = ProtocolFeatures(
            shard_id=kwargs.get('shard_id', self.connection_counter),
            sharding_info=sharding_info)
        self.connection_counter += 1

        return connection


class TestShardAware(unittest.TestCase):
    def _assert_closed_missing_shard_connection_is_discarded(
            self, use_shard_aware_endpoint):
        host = MagicMock()
        host.endpoint = DefaultEndPoint("1.2.3.4")
        session = MockSession()
        session.cluster.shard_aware_options.disable_shardaware_port = (
            not use_shard_aware_endpoint)
        pool = HostConnection(
            host=host, host_distance=HostDistance.REMOTE, session=session)

        try:
            for future in session.futures:
                future.result()

            requested_shard = 2

            class ClosedConnection(object):
                def __init__(self):
                    self.is_closed = True
                    self.close = MagicMock()
                    self.set_keyspace_blocking = MagicMock()
                    self.features_access_count = 0
                    self._features = ProtocolFeatures(
                        shard_id=requested_shard)

                @property
                def features(self):
                    self.features_access_count += 1
                    return self._features

            closed_connection = ClosedConnection()
            session.cluster.connection_factory = MagicMock(
                return_value=closed_connection)
            pool._connections.clear()
            pool._excess_connections.clear()
            pool._connecting.add(requested_shard)

            pool._open_connection_to_missing_shard(requested_shard)

            assert pool._connections == {}
            assert pool._excess_connections == set()
            assert closed_connection.features_access_count == 0
            closed_connection.set_keyspace_blocking.assert_not_called()
            closed_connection.close.assert_called_once_with()
            assert requested_shard not in pool._connecting

            factory_args = session.cluster.connection_factory.call_args
            expected_endpoint = (
                DefaultEndPoint("1.2.3.4", port=19042)
                if use_shard_aware_endpoint else host.endpoint)
            assert factory_args.args[0] == expected_endpoint
            if use_shard_aware_endpoint:
                assert factory_args.kwargs['shard_id'] == requested_shard
                assert factory_args.kwargs['total_shards'] == 4
            else:
                assert 'shard_id' not in factory_args.kwargs
                assert 'total_shards' not in factory_args.kwargs
        finally:
            pool.shutdown()
            session.cluster.executor.shutdown(wait=True)

    def test_closed_connection_to_missing_shard_is_discarded(self):
        self._assert_closed_missing_shard_connection_is_discarded(
            use_shard_aware_endpoint=True)

    def test_closed_connection_to_missing_shard_fallback_is_discarded(self):
        self._assert_closed_missing_shard_connection_is_discarded(
            use_shard_aware_endpoint=False)

    def test_parsing_and_calculating_shard_id(self):
        """
        Testing the parsing of the options command
        and the calculation getting a shard id from a Murmur3 token
        """
        class OptionsHolder(object):
            options = {
                'SCYLLA_SHARD': ['1'], 
                'SCYLLA_NR_SHARDS': ['12'],
                'SCYLLA_PARTITIONER': ['org.apache.cassandra.dht.Murmur3Partitioner'],
                'SCYLLA_SHARDING_ALGORITHM': ['biased-token-round-robin'],
                'SCYLLA_SHARDING_IGNORE_MSB': ['12']
            }
        shard_id, shard_info = ProtocolFeatures.parse_sharding_info(OptionsHolder().options)

        assert shard_id == 1
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"a").value) == 4
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"b").value) == 6
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"c").value) == 6
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"e").value) == 4
        assert shard_info.shard_id_from_token(Murmur3Token.from_key(b"100000").value) == 2

    def test_advanced_shard_aware_port(self):
        """
        Test that on given a `shard_aware_port` on the OPTIONS message (ShardInfo class)
        the next connections would be open using this port
        """
        host = MagicMock()
        host.endpoint = DefaultEndPoint("1.2.3.4")

        for port, ssl_options, ssl_context in [
                (19042, None, None),
                (19045, {'some_ssl_options': True}, None),
                (19045, {}, None),
                (19045, None, object())]:
            session = MockSession(ssl_options=ssl_options, ssl_context=ssl_context)
            pool = HostConnection(host=host, host_distance=HostDistance.REMOTE, session=session)
            try:
                for f in session.futures:
                    f.result()
                assert len(pool._connections) == 4
                for shard_id, connection in pool._connections.items():
                    assert connection.features.shard_id == shard_id
                    if shard_id == 0:
                        assert connection.endpoint == DefaultEndPoint("1.2.3.4")
                    else:
                        assert connection.endpoint == DefaultEndPoint("1.2.3.4", port=port)
            finally:
                session.cluster.executor.shutdown(wait=True)

    def test_ssl_advanced_shard_aware_port_requires_ssl_port(self):
        """
        Test that SSL connections do not fall back to the plaintext
        shard-aware port when the SSL shard-aware port is unavailable.
        """
        host = MagicMock()
        host.endpoint = DefaultEndPoint("1.2.3.4")
        sharding_info = ShardingInfo(
            shard_id=1, shards_count=4, partitioner="", sharding_algorithm="",
            sharding_ignore_msb=0, shard_aware_port=19042,
            shard_aware_port_ssl=None)
        for label, ssl_options, ssl_context in [
                ('ssl_options', {'some_ssl_options': True}, None),
                ('empty_ssl_options', {}, None),
                ('ssl_context', None, object())]:
            with self.subTest(label=label):
                session = MockSession(
                    ssl_options=ssl_options,
                    ssl_context=ssl_context,
                    sharding_info=sharding_info)
                pool = HostConnection(host=host, host_distance=HostDistance.REMOTE, session=session)

                try:
                    for f in session.futures:
                        f.result()

                    assert pool._get_shard_aware_endpoint() is None
                finally:
                    session.cluster.executor.shutdown(wait=True)

    def test_advanced_shard_aware_cooldown(self):
        """
        `disable_advanced_shard_aware` must suppress the shard-aware endpoint for
        the duration of the cool-down window, then automatically restore it once
        the deadline has passed. The hard-disable flag must suppress the endpoint
        unconditionally.
        """
        host = MagicMock()
        host.endpoint = DefaultEndPoint("1.2.3.4")
        session = MockSession()

        pool = HostConnection(host=host, host_distance=HostDistance.REMOTE, session=session)
        for f in session.futures:
            f.result()

        try:
            # Baseline: shard-aware port is returned.
            endpoint = pool._get_shard_aware_endpoint()
            assert endpoint is not None
            assert endpoint.port == 19042

            # During the cool-down window `_get_shard_aware_endpoint` must return None.
            pool.disable_advanced_shard_aware(600)
            assert pool._get_shard_aware_endpoint() is None

            # Once the deadline has passed, the shard-aware port must be used again.
            pool.advanced_shardaware_block_until = time.time() - 1
            endpoint = pool._get_shard_aware_endpoint()
            assert endpoint is not None
            assert endpoint.port == 19042

            # The hard-disable flag must suppress the endpoint regardless of the timer.
            session.cluster.shard_aware_options.disable_shardaware_port = True
            assert pool._get_shard_aware_endpoint() is None
        finally:
            session.cluster.executor.shutdown(wait=True)
