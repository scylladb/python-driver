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
import uuid

try:
    import unittest2 as unittest
except ImportError:
    import unittest  # noqa

import time
import logging
from mock import MagicMock
from concurrent.futures import ThreadPoolExecutor

from cassandra.cluster import ShardAwareOptions, _Scheduler
from cassandra.policies import ConstantReconnectionPolicy, \
    NoDelayShardReconnectionPolicy, NoConcurrentShardReconnectionPolicy, ShardReconnectionPolicyScope
from cassandra.pool import HostConnection, HostDistance
from cassandra.connection import ShardingInfo, DefaultEndPoint
from cassandra.metadata import Murmur3Token
from cassandra.protocol_features import ProtocolFeatures

LOGGER = logging.getLogger(__name__)


class TestShardAware(unittest.TestCase):
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

        self.assertEqual(shard_id, 1)
        self.assertEqual(shard_info.shard_id_from_token(Murmur3Token.from_key(b"a").value), 4)
        self.assertEqual(shard_info.shard_id_from_token(Murmur3Token.from_key(b"b").value), 6)
        self.assertEqual(shard_info.shard_id_from_token(Murmur3Token.from_key(b"c").value), 6)
        self.assertEqual(shard_info.shard_id_from_token(Murmur3Token.from_key(b"e").value), 4)
        self.assertEqual(shard_info.shard_id_from_token(Murmur3Token.from_key(b"100000").value), 2)

    def test_shard_aware_reconnection_policy_no_delay(self):
        # with NoDelayReconnectionPolicy all the connections should be created right away
        self._test_shard_aware_reconnection_policy(4, NoDelayShardReconnectionPolicy(), 4, 4)

    def test_shard_aware_reconnection_policy_delay(self):
        # with ConstantReconnectionPolicy first connection is created right away, others are delayed
        self._test_shard_aware_reconnection_policy(4, NoConcurrentShardReconnectionPolicy(ShardReconnectionPolicyScope.Cluster, ConstantReconnectionPolicy(1)), 1, 4)

    def _test_shard_aware_reconnection_policy(self, shard_count, shard_reconnection_policy, expected_count, expected_after):
        """
        Test that on given a `shard_aware_port` on the OPTIONS message (ShardInfo class)
        the next connections would be open using this port
        """
        class MockSession(MagicMock):
            is_shutdown = False
            keyspace = "ks1"

            def __init__(self, is_ssl=False, *args, **kwargs):
                super(MockSession, self).__init__(*args, **kwargs)
                self.cluster = MagicMock()
                if is_ssl:
                    self.cluster.ssl_options = {'some_ssl_options': True}
                else:
                    self.cluster.ssl_options = None
                self.cluster.shard_aware_options = ShardAwareOptions()
                self.cluster.executor = ThreadPoolExecutor(max_workers=2)
                self._executor_submit_original = self.cluster.executor.submit
                self.cluster.executor.submit = self._executor_submit
                self.cluster.scheduler = _Scheduler(self.cluster.executor)
                self.cluster.signal_connection_failure = lambda *args, **kwargs: False
                self.cluster.connection_factory = self.mock_connection_factory
                self.connection_counter = 0
                self.shard_reconnection_scheduler = shard_reconnection_policy.new_scheduler(self)
                self.futures = []

            def submit(self, fn, *args, **kwargs):
                if self.is_shutdown:
                    return None
                return self.cluster.executor.submit(fn, *args, **kwargs)

            def _executor_submit(self, fn, *args, **kwargs):
                logging.info("Scheduling %s with args: %s, kwargs: %s", fn, args, kwargs)
                f = self._executor_submit_original(fn, *args, **kwargs)
                self.futures += [f]
                return f

            def mock_connection_factory(self, *args, **kwargs):
                connection = MagicMock()
                connection.is_shutdown = False
                connection.is_defunct = False
                connection.is_closed = False
                connection.orphaned_threshold_reached = False
                connection.endpoint = args[0] 
                sharding_info = ShardingInfo(shard_id=1, shards_count=shard_count, partitioner="", sharding_algorithm="", sharding_ignore_msb=0, shard_aware_port=19042, shard_aware_port_ssl=19045)
                connection.features = ProtocolFeatures(shard_id=kwargs.get('shard_id', self.connection_counter), sharding_info=sharding_info)
                self.connection_counter += 1

                return connection

        host = MagicMock()
        host.host_id = uuid.uuid4()
        host.endpoint = DefaultEndPoint("1.2.3.4")
        session = None
        reconnection_policy = None
        if isinstance(shard_reconnection_policy, NoConcurrentShardReconnectionPolicy):
            reconnection_policy = shard_reconnection_policy.reconnection_policy
        try:
            for port, is_ssl in [(19042, False), (19045, True)]:
                session = MockSession(is_ssl=is_ssl)
                pool = HostConnection(host=host, host_distance=HostDistance.REMOTE, session=session)
                for f in session.futures:
                    f.result()
                assert len(pool._connections) == expected_count
                for shard_id, connection in pool._connections.items():
                    assert connection.features.shard_id == shard_id
                    if shard_id == 0:
                        assert connection.endpoint == DefaultEndPoint("1.2.3.4")
                    else:
                        assert connection.endpoint == DefaultEndPoint("1.2.3.4", port=port)

                sleep_time = 0
                if reconnection_policy:
                    # Check that connections to shards are being established according to the policy
                    # Calculate total time it will need to establish all connections
                    # Sleep half of the time and check that connections are not there yet
                    # Sleep rest of the time + 1 second and check that all connections has been established
                    schedule = reconnection_policy.new_schedule()
                    for _ in range(shard_count):
                        sleep_time += next(schedule)
                    if sleep_time > 0:
                        time.sleep(sleep_time/2)
                        # Check that connection are not being established quicker than expected
                        assert len(pool._connections) < expected_after
                        time.sleep(sleep_time/2 + 1)

                assert len(pool._connections) == expected_after
        finally:
            if session:
                session.cluster.scheduler.shutdown()
                session.cluster.executor.shutdown(wait=True)
