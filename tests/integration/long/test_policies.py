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
from unittest.mock import Mock

from cassandra import ConsistencyLevel, Unavailable
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT, Session
from cassandra.policies import LimitedConcurrencyShardConnectionBackoffPolicy, ConstantReconnectionPolicy, \
    ShardConnectionBackoffPolicy, NoDelayShardConnectionBackoffPolicy, _ScopeBucket, \
    _NoDelayShardConnectionBackoffScheduler
from cassandra.shard_info import _ShardingInfo

from tests.integration import use_cluster, get_cluster, get_node, TestCluster, remove_cluster


def setup_module():
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 8"
    use_cluster('test_cluster', [2])


def teardown_module():
    remove_cluster()
    del os.environ['SCYLLA_EXT_OPTS']


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
    def test_limited_concurrency_1_connection_per_host(self):
        self._test_backoff(
            LimitedConcurrencyShardConnectionBackoffPolicy(
                backoff_policy=ConstantReconnectionPolicy(0.1),
                max_concurrent=1,
            )
        )

    def test_limited_concurrency_2_connection_per_host(self):
        self._test_backoff(
            LimitedConcurrencyShardConnectionBackoffPolicy(
                backoff_policy=ConstantReconnectionPolicy(0.1),
                max_concurrent=1,
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

        # Collect scheduled calls and execute them right away
        scheduler_calls = []
        original_schedule = cluster.scheduler.schedule
        pending = 0

        def new_schedule(delay, fn, *args, **kwargs):
            nonlocal pending
            pending+=1

            def execute():
                nonlocal pending
                try:
                    fn(*args, **kwargs)
                finally:
                    pending-=1

            scheduler_calls.append((delay, fn, args, kwargs))
            return original_schedule(0, execute)

        cluster.scheduler.schedule = Mock(side_effect=new_schedule)

        session = cluster.connect()
        sharding_info = get_sharding_info(session)

        # Since scheduled calls executed in a separate thread we need to give them some time to complete
        while pending > 0:
            time.sleep(0.01)

        if not sharding_info:
            # If it is not scylla `ShardConnectionBackoffScheduler` should not be involved
            for delay, fn, args, kwargs in scheduler_calls:
                if fn.__self__.__class__ is _ScopeBucket or fn.__self__.__class__ is _NoDelayShardConnectionBackoffScheduler:
                    self.fail(
                        "in non-shard-aware case connection should be created directly, not involving ShardConnectionBackoffScheduler")
            return

        sleep_time = 0
        if backoff_policy:
            schedule = backoff_policy.new_schedule()
            sleep_time = next(iter(schedule))

        # Make sure that all scheduled calls have delay according to policy
        found_related_calls = 0
        for delay, fn, args, kwargs in scheduler_calls:
            if fn.__self__.__class__ is _ScopeBucket or fn.__self__.__class__ is _NoDelayShardConnectionBackoffScheduler:
                found_related_calls += 1
                self.assertEqual(delay, sleep_time)
        self.assertLessEqual(len(session.hosts) * (sharding_info.shards_count - 1), found_related_calls)


def get_sharding_info(session: Session) -> Optional[_ShardingInfo]:
    for host in session.hosts:
        if host.sharding_info:
            return host.sharding_info
    return None
