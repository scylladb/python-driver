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
import os
import time
import unittest
from typing import Optional

from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT, Session
from cassandra.policies import LimitedConcurrencyShardConnectionBackoffPolicy, ShardConnectionBackoffScope, \
    ConstantReconnectionPolicy, ShardConnectionBackoffPolicy, NoDelayShardConnectionBackoffPolicy
from cassandra.shard_info import _ShardingInfo

from tests.integration import use_cluster, get_cluster, get_node, TestCluster


def setup_module():
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 4"
    use_cluster('test_cluster', [4])


class RetryPolicyTests(unittest.TestCase):

    @classmethod
    def tearDownClass(cls):
        cluster = get_cluster()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)  # make sure other nodes are restarted

    def test_should_rethrow_on_unvailable_with_default_policy_if_cas(self):
        """
        Tests for the default retry policy in combination with lightweight transactions.

        @since 3.17
        @jira_ticket PYTHON-1007
        @expected_result the query is retried with the default CL, not the serial one.

        @test_category policy
        """
        ep = ExecutionProfile(consistency_level=ConsistencyLevel.ALL,
                              serial_consistency_level=ConsistencyLevel.SERIAL)

        cluster = TestCluster(execution_profiles={EXEC_PROFILE_DEFAULT: ep})
        session = cluster.connect()

        session.execute("CREATE KEYSPACE test_retry_policy_cas WITH replication = {'class':'SimpleStrategy','replication_factor': 3};")
        session.execute("CREATE TABLE test_retry_policy_cas.t (id int PRIMARY KEY, data text);")
        session.execute('INSERT INTO test_retry_policy_cas.t ("id", "data") VALUES (%(0)s, %(1)s)', {'0': 42, '1': 'testing'})

        get_node(2).stop()
        get_node(4).stop()

        # before fix: cassandra.InvalidRequest: Error from server: code=2200 [Invalid query] message="SERIAL is not
        # supported as conditional update commit consistency. ....""

        # after fix: cassandra.Unavailable (expected since replicas are down)
        with self.assertRaises(Unavailable) as cm:
            session.execute("update test_retry_policy_cas.t set data = 'staging' where id = 42 if data ='testing'")

        exception = cm.exception
        self.assertEqual(exception.consistency, ConsistencyLevel.SERIAL)
        self.assertEqual(exception.required_replicas, 2)
        self.assertEqual(exception.alive_replicas, 1)


class ShardBackoffPolicyTests(unittest.TestCase):
    @classmethod
    def tearDownClass(cls):
        cluster = get_cluster()
        cluster.start(wait_for_binary_proto=True, wait_other_notice=True)  # make sure other nodes are restarted

    def test_limited_concurrency_1_connection_per_cluster(self):
        self._test_backoff(
            LimitedConcurrencyShardConnectionBackoffPolicy(
                backoff_policy=ConstantReconnectionPolicy(0.1),
                max_concurrent=1,
                scope=ShardConnectionBackoffScope.Cluster,
            )
        )

    def test_limited_concurrency_2_connection_per_cluster(self):
        self._test_backoff(
            LimitedConcurrencyShardConnectionBackoffPolicy(
                backoff_policy=ConstantReconnectionPolicy(0.1),
                max_concurrent=2,
                scope=ShardConnectionBackoffScope.Cluster,
            )
        )

    def test_limited_concurrency_1_connection_per_host(self):
        self._test_backoff(
            LimitedConcurrencyShardConnectionBackoffPolicy(
                backoff_policy=ConstantReconnectionPolicy(0.1),
                max_concurrent=1,
                scope=ShardConnectionBackoffScope.Host,
            )
        )

    def test_limited_concurrency_2_connection_per_host(self):
        self._test_backoff(
            LimitedConcurrencyShardConnectionBackoffPolicy(
                backoff_policy=ConstantReconnectionPolicy(0.1),
                max_concurrent=1,
                scope=ShardConnectionBackoffScope.Host,
            )
        )

    def test_no_delay(self):
        self._test_backoff(NoDelayShardConnectionBackoffPolicy())

    def _test_backoff(self, shard_connection_backoff_policy: ShardConnectionBackoffPolicy):
        backoff_policy = None
        if isinstance(shard_connection_backoff_policy, LimitedConcurrencyShardConnectionBackoffPolicy):
            backoff_policy = shard_connection_backoff_policy.backoff_policy

        cluster = TestCluster(
            shard_connection_backoff_policy=shard_connection_backoff_policy,
            reconnection_policy=ConstantReconnectionPolicy(0),
        )
        session = cluster.connect()
        sharding_info = get_sharding_info(session)

        # even if backoff is set and there is no sharding info
        # behavior should be the same as if there is no backoff policy
        if not backoff_policy or not sharding_info:
            time.sleep(2)
            expected_connections = 1
            if sharding_info:
                expected_connections = sharding_info.shards_count
            for host_id, connections_count in get_connections_per_host(session).items():
                self.assertEqual(connections_count, expected_connections)
            return

        sleep_time = 0
        schedule = backoff_policy.new_schedule()
        # Calculate total time it will need to establish all connections
        if shard_connection_backoff_policy.scope == ShardConnectionBackoffScope.Cluster:
            for _ in session.hosts:
                for _ in range(sharding_info.shards_count - 1):
                    sleep_time += next(schedule)
            sleep_time /= shard_connection_backoff_policy.max_concurrent
        elif shard_connection_backoff_policy.scope == ShardConnectionBackoffScope.Host:
            for _ in range(sharding_info.shards_count - 1):
                sleep_time += next(schedule)
            sleep_time /= shard_connection_backoff_policy.max_concurrent
        else:
            raise ValueError("Unknown scope {}".format(shard_connection_backoff_policy.scope))

        time.sleep(sleep_time / 2)
        self.assertFalse(
            is_connection_filled(shard_connection_backoff_policy.scope, session, sharding_info.shards_count))
        time.sleep(sleep_time / 2 + 1)
        self.assertTrue(
            is_connection_filled(shard_connection_backoff_policy.scope, session, sharding_info.shards_count))


def is_connection_filled(scope: ShardConnectionBackoffScope, session: Session, shards_count: int) -> bool:
    if scope == ShardConnectionBackoffScope.Cluster:
        expected_connections = shards_count * len(session.hosts)
        total_connections = sum(get_connections_per_host(session).values())
        return expected_connections == total_connections
    elif scope == ShardConnectionBackoffScope.Host:
        expected_connections_per_host = shards_count
        for connections_count in get_connections_per_host(session).values():
            if connections_count < expected_connections_per_host:
                return False
            if connections_count == expected_connections_per_host:
                continue
            assert False, "Expected {} or less connections but got {}".format(expected_connections_per_host,
                                                                              connections_count)
        return True
    else:
        raise ValueError("Unknown scope {}".format(scope))


def get_connections_per_host(session: Session) -> dict[str, int]:
    host_connections = {}
    for host, pool in session._pools.items():
        host_connections[host.host_id] = len(pool._connections)
    return host_connections


def get_sharding_info(session: Session) -> Optional[_ShardingInfo]:
    for host in session.hosts:
        if host.sharding_info:
            return host.sharding_info
    return None
