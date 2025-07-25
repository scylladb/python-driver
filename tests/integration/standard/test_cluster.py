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
import subprocess

import unittest

from collections import deque
from copy import copy
from unittest.mock import Mock, patch
import time
from uuid import uuid4
import logging
import warnings
from packaging.version import Version
import os
import pytest

import cassandra
from cassandra.cluster import NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT, ControlConnection, Cluster
from cassandra.concurrent import execute_concurrent
from cassandra.policies import (RoundRobinPolicy, ExponentialReconnectionPolicy,
                                RetryPolicy, SimpleConvictionPolicy, HostDistance,
                                AddressTranslator, TokenAwarePolicy, HostFilterPolicy)
from cassandra import ConsistencyLevel

from cassandra.query import SimpleStatement, TraceUnavailable, tuple_factory
from cassandra.auth import PlainTextAuthProvider, SaslAuthProvider
from cassandra import connection
from cassandra.connection import DefaultEndPoint

from tests import notwindows, notasyncio
from tests.integration import use_cluster, get_server_versions, CASSANDRA_VERSION, \
    execute_until_pass, execute_with_long_wait_retry, get_node, MockLoggingHandler, get_unsupported_lower_protocol, \
    get_unsupported_upper_protocol, protocolv6, local, CASSANDRA_IP, greaterthanorequalcass30, \
    lessthanorequalcass40, TestCluster, PROTOCOL_VERSION, xfail_scylla, incorrect_test
from tests.integration.util import assert_quiescent_pool_state
from tests.util import assertListEqual
import sys

log = logging.getLogger(__name__)


def setup_module():
    os.environ['SCYLLA_EXT_OPTS'] = "--smp 1"
    use_cluster("cluster_tests", [3], start=True, workloads=None)
    warnings.simplefilter("always")


class IgnoredHostPolicy(RoundRobinPolicy):

    def __init__(self, ignored_hosts):
        self.ignored_hosts = ignored_hosts
        RoundRobinPolicy.__init__(self)

    def distance(self, host):
        if(host.address in self.ignored_hosts):
            return HostDistance.IGNORED
        else:
            return HostDistance.LOCAL


class ClusterTests(unittest.TestCase):
    @local
    def test_ignored_host_up(self):
        """
        Test to ensure that is_up is not set by default on ignored hosts

        @since 3.6
        @jira_ticket PYTHON-551
        @expected_result ignored hosts should have None set for is_up

        @test_category connection
        """
        ignored_host_policy = IgnoredHostPolicy(["127.0.0.2", "127.0.0.3"])
        cluster = TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=ignored_host_policy)}
        )
        cluster.connect()
        for host in cluster.metadata.all_hosts():
            if str(host) == "127.0.0.1:9042":
                assert host.is_up
            else:
                assert host.is_up is None
        cluster.shutdown()

    @local
    def test_host_resolution(self):
        """
        Test to insure A records are resolved appropriately.

        @since 3.3
        @jira_ticket PYTHON-415
        @expected_result hostname will be transformed into IP

        @test_category connection
        """
        cluster = TestCluster(contact_points=["localhost"], connect_timeout=1)
        assert DefaultEndPoint('127.0.0.1') in cluster.endpoints_resolved

    @local
    def test_host_duplication(self):
        """
        Ensure that duplicate hosts in the contact points are surfaced in the cluster metadata

        @since 3.3
        @jira_ticket PYTHON-103
        @expected_result duplicate hosts aren't surfaced in cluster.metadata

        @test_category connection
        """
        cluster = TestCluster(
            contact_points=["localhost", "127.0.0.1", "localhost", "localhost", "localhost"],
            connect_timeout=1
        )
        cluster.connect(wait_for_all_pools=True)
        assert len(cluster.metadata.all_hosts()) == 3
        cluster.shutdown()
        cluster = TestCluster(contact_points=["127.0.0.1", "localhost"], connect_timeout=1)
        cluster.connect(wait_for_all_pools=True)
        assert len(cluster.metadata.all_hosts()) == 3
        cluster.shutdown()

    @local
    def test_raise_error_on_control_connection_timeout(self):
        """
        Test for initial control connection timeout

        test_raise_error_on_control_connection_timeout tests that the driver times out after the set initial connection
        timeout. It first pauses node1, essentially making it unreachable. It then attempts to create a Cluster object
        via connecting to node1 with a timeout of 1 second, and ensures that a NoHostAvailable is raised, along with
        an OperationTimedOut for 1 second.

        @expected_errors NoHostAvailable When node1 is paused, and a connection attempt is made.
        @since 2.6.0
        @jira_ticket PYTHON-206
        @expected_result NoHostAvailable exception should be raised after 1 second.

        @test_category connection
        """

        get_node(1).pause()
        cluster = TestCluster(contact_points=['127.0.0.1'], connect_timeout=1)

        with pytest.raises(NoHostAvailable, match=r"OperationTimedOut\('errors=Timed out creating connection \(1 seconds\)"):
            cluster.connect()
        cluster.shutdown()

        get_node(1).resume()

    def test_basic(self):
        """
        Test basic connection and usage
        """

        cluster = TestCluster()
        session = cluster.connect()
        result = execute_until_pass(session,
            """
            CREATE KEYSPACE clustertests
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
            """)
        assert not result

        result = execute_with_long_wait_retry(session,
            """
            CREATE TABLE clustertests.cf0 (
                a text,
                b text,
                c text,
                PRIMARY KEY (a, b)
            )
            """)
        assert not result

        result = session.execute(
            """
            INSERT INTO clustertests.cf0 (a, b, c) VALUES ('a', 'b', 'c')
            """)
        assert not result

        result = session.execute("SELECT * FROM clustertests.cf0")
        assert [('a', 'b', 'c')] == result

        execute_with_long_wait_retry(session, "DROP KEYSPACE clustertests")

        cluster.shutdown()

    def test_session_host_parameter(self):
        """
        Test for protocol negotiation

        Very that NoHostAvailable is risen in Session.__init__ when there are no valid connections and that
        no error is arisen otherwise, despite maybe being some invalid hosts

        @since 3.9
        @jira_ticket PYTHON-665
        @expected_result NoHostAvailable when the driver is unable to connect to a valid host,
        no exception otherwise

        @test_category connection
        """
        def cleanup():
            """
            When this test fails, the inline .shutdown() calls don't get
            called, so we register this as a cleanup.
            """
            self.cluster_to_shutdown.shutdown()
        self.addCleanup(cleanup)

        # Test with empty list
        self.cluster_to_shutdown = TestCluster(contact_points=[])
        with pytest.raises(NoHostAvailable):
            self.cluster_to_shutdown.connect()
        self.cluster_to_shutdown.shutdown()

        # Test with only invalid
        self.cluster_to_shutdown = TestCluster(contact_points=('1.2.3.4',))
        with pytest.raises(NoHostAvailable):
            self.cluster_to_shutdown.connect()
        self.cluster_to_shutdown.shutdown()

        # Test with valid and invalid hosts
        self.cluster_to_shutdown = TestCluster(contact_points=("127.0.0.1", "127.0.0.2", "1.2.3.4"))
        self.cluster_to_shutdown.connect()
        self.cluster_to_shutdown.shutdown()

    def test_protocol_negotiation(self):
        """
        Test for protocol negotiation

        test_protocol_negotiation tests that the driver will select the correct protocol version to match
        the correct cassandra version. Please note that 2.1.5 has a
        bug https://issues.apache.org/jira/browse/CASSANDRA-9451 that will cause this test to fail
        that will cause this to not pass. It was rectified in 2.1.6

        @since 2.6.0
        @jira_ticket PYTHON-240
        @expected_result the correct protocol version should be selected

        @test_category connection
        """

        cluster = Cluster()
        assert cluster.protocol_version <= cassandra.ProtocolVersion.MAX_SUPPORTED
        session = cluster.connect()
        updated_protocol_version = session._protocol_version
        updated_cluster_version = cluster.protocol_version
        # Make sure the correct protocol was selected by default
        if CASSANDRA_VERSION >= Version('4.0-beta5'):
            assert updated_protocol_version == cassandra.ProtocolVersion.V5
            assert updated_cluster_version == cassandra.ProtocolVersion.V5
        elif CASSANDRA_VERSION >= Version('4.0-a'):
            assert updated_protocol_version == cassandra.ProtocolVersion.V4
            assert updated_cluster_version == cassandra.ProtocolVersion.V4
        elif CASSANDRA_VERSION >= Version('3.11'):
            assert updated_protocol_version == cassandra.ProtocolVersion.V4
            assert updated_cluster_version == cassandra.ProtocolVersion.V4
        elif CASSANDRA_VERSION >= Version('3.0'):
            assert updated_protocol_version == cassandra.ProtocolVersion.V4
            assert updated_cluster_version == cassandra.ProtocolVersion.V4
        elif CASSANDRA_VERSION >= Version('2.2'):
            assert updated_protocol_version == 4
            assert updated_cluster_version == 4
        elif CASSANDRA_VERSION >= Version('2.1'):
            assert updated_protocol_version == 3
            assert updated_cluster_version == 3
        elif CASSANDRA_VERSION >= Version('2.0'):
            assert updated_protocol_version == 2
            assert updated_cluster_version == 2
        else:
            assert updated_protocol_version == 1
            assert updated_cluster_version == 1

        cluster.shutdown()

    def test_invalid_protocol_negotation(self):
        """
        Test for protocol negotiation when explicit versions are set

        If an explicit protocol version that is not compatible with the server version is set
        an exception should be thrown. It should not attempt to negotiate

        for reference supported protocol version to server versions is as follows/

        1.2 -> 1
        2.0 -> 2, 1
        2.1 -> 3, 2, 1
        2.2 -> 4, 3, 2, 1
        3.X -> 4, 3

        @since 3.6.0
        @jira_ticket PYTHON-537
        @expected_result downgrading should not be allowed when explicit protocol versions are set.

        @test_category connection
        """

        upper_bound = get_unsupported_upper_protocol()
        log.debug('got upper_bound of {}'.format(upper_bound))
        if upper_bound is not None:
            cluster = TestCluster(protocol_version=upper_bound)
            with pytest.raises(NoHostAvailable):
                cluster.connect()
            cluster.shutdown()

        lower_bound = get_unsupported_lower_protocol()
        log.debug('got lower_bound of {}'.format(lower_bound))
        if lower_bound is not None:
            cluster = TestCluster(protocol_version=lower_bound)
            with pytest.raises(NoHostAvailable):
                cluster.connect()
            cluster.shutdown()

    def test_connect_on_keyspace(self):
        """
        Ensure clusters that connect on a keyspace, do
        """

        cluster = TestCluster()
        session = cluster.connect()
        result = session.execute(
            """
            INSERT INTO test1rf.test (k, v) VALUES (8889, 8889)
            """)
        assert not result

        result = session.execute("SELECT * FROM test1rf.test")
        assert [(8889, 8889)] == result, "Rows in ResultSet are {0}".format(result.current_rows)

        # test_connect_on_keyspace
        session2 = cluster.connect('test1rf')
        result2 = session2.execute("SELECT * FROM test")
        assert result == result2
        cluster.shutdown()

    def test_set_keyspace_twice(self):
        cluster = TestCluster()
        session = cluster.connect()
        session.execute("USE system")
        session.execute("USE system")
        cluster.shutdown()

    def test_default_connections(self):
        """
        Ensure errors are not thrown when using non-default policies
        """

        TestCluster(
            reconnection_policy=ExponentialReconnectionPolicy(1.0, 600.0),
            conviction_policy_factory=SimpleConvictionPolicy,
            protocol_version=PROTOCOL_VERSION
        )

    def test_connect_to_already_shutdown_cluster(self):
        """
        Ensure you cannot connect to a cluster that's been shutdown
        """
        cluster = TestCluster()
        cluster.shutdown()
        with pytest.raises(Exception):
            cluster.connect()

    def test_auth_provider_is_callable(self):
        """
        Ensure that auth_providers are always callable
        """
        with pytest.raises(TypeError):
            Cluster(auth_provider=1, protocol_version=1)
        c = TestCluster(protocol_version=1)
        with pytest.raises(TypeError):
            setattr(c, 'auth_provider', 1)

    def test_v2_auth_provider(self):
        """
        Check for v2 auth_provider compliance
        """
        bad_auth_provider = lambda x: {'username': 'foo', 'password': 'bar'}
        with pytest.raises(TypeError):
            Cluster(auth_provider=bad_auth_provider, protocol_version=2)
        c = TestCluster(protocol_version=2)
        with pytest.raises(TypeError):
            setattr(c, 'auth_provider', bad_auth_provider)

    def test_conviction_policy_factory_is_callable(self):
        """
        Ensure that conviction_policy_factory are always callable
        """

        with pytest.raises(ValueError):
            Cluster(conviction_policy_factory=1)

    def test_connect_to_bad_hosts(self):
        """
        Ensure that a NoHostAvailable Exception is thrown
        when a cluster cannot connect to given hosts
        """

        cluster = TestCluster(contact_points=['127.1.2.9', '127.1.2.10'],
                              protocol_version=PROTOCOL_VERSION)
        with pytest.raises(NoHostAvailable):
            cluster.connect()

    def test_refresh_schema(self):
        cluster = TestCluster()
        session = cluster.connect()

        original_meta = cluster.metadata.keyspaces
        # full schema refresh, with wait
        cluster.refresh_schema_metadata()
        assert original_meta is not cluster.metadata.keyspaces
        assert original_meta == cluster.metadata.keyspaces

        cluster.shutdown()

    def test_refresh_schema_keyspace(self):
        cluster = TestCluster()
        session = cluster.connect()

        original_meta = cluster.metadata.keyspaces
        original_system_meta = original_meta['system']

        # only refresh one keyspace
        cluster.refresh_keyspace_metadata('system')
        current_meta = cluster.metadata.keyspaces
        assert original_meta is current_meta
        current_system_meta = current_meta['system']
        assert original_system_meta is not current_system_meta
        assert original_system_meta.as_cql_query() == current_system_meta.as_cql_query()
        cluster.shutdown()

    def test_refresh_schema_table(self):
        cluster = TestCluster()
        session = cluster.connect()

        original_meta = cluster.metadata.keyspaces
        original_system_meta = original_meta['system']
        original_system_schema_meta = original_system_meta.tables['local']

        # only refresh one table
        cluster.refresh_table_metadata('system', 'local')
        current_meta = cluster.metadata.keyspaces
        current_system_meta = current_meta['system']
        current_system_schema_meta = current_system_meta.tables['local']
        assert original_meta is current_meta
        assert original_system_meta is current_system_meta
        assert original_system_schema_meta is not current_system_schema_meta
        assert original_system_schema_meta.as_cql_query() == current_system_schema_meta.as_cql_query()
        cluster.shutdown()

    def test_refresh_schema_type(self):
        if get_server_versions()[0] < (2, 1, 0):
            raise unittest.SkipTest('UDTs were introduced in Cassandra 2.1')

        if PROTOCOL_VERSION < 3:
            raise unittest.SkipTest('UDTs are not specified in change events for protocol v2')
            # We may want to refresh types on keyspace change events in that case(?)

        cluster = TestCluster()
        session = cluster.connect()

        keyspace_name = 'test1rf'
        type_name = self._testMethodName

        execute_until_pass(session, 'CREATE TYPE IF NOT EXISTS %s.%s (one int, two text)' % (keyspace_name, type_name))
        original_meta = cluster.metadata.keyspaces
        original_test1rf_meta = original_meta[keyspace_name]
        original_type_meta = original_test1rf_meta.user_types[type_name]

        # only refresh one type
        cluster.refresh_user_type_metadata('test1rf', type_name)
        current_meta = cluster.metadata.keyspaces
        current_test1rf_meta = current_meta[keyspace_name]
        current_type_meta = current_test1rf_meta.user_types[type_name]
        assert original_meta is current_meta
        assert original_test1rf_meta.export_as_string() == current_test1rf_meta.export_as_string()
        assert original_type_meta is not current_type_meta
        assert original_type_meta.as_cql_query() == current_type_meta.as_cql_query()
        cluster.shutdown()

    @local
    @notwindows
    def test_refresh_schema_no_wait(self):
        original_wait_for_responses = connection.Connection.wait_for_responses

        def patched_wait_for_responses(*args, **kwargs):
            # When selecting schema version, replace the real schema UUID with an unexpected UUID
            response = original_wait_for_responses(*args, **kwargs)
            if len(args) > 2 and hasattr(args[2], "query") and "SELECT schema_version FROM system.local WHERE key='local'" in args[2].query:
                new_uuid = uuid4()
                response[1].parsed_rows[0] = (new_uuid,)
            return response

        with patch.object(connection.Connection, "wait_for_responses", patched_wait_for_responses):
            agreement_timeout = 1

            # cluster agreement wait exceeded
            c = TestCluster(max_schema_agreement_wait=agreement_timeout)
            c.connect()
            assert c.metadata.keyspaces

            # cluster agreement wait used for refresh
            original_meta = c.metadata.keyspaces
            start_time = time.time()
            with pytest.raises(Exception, match=r"Schema metadata was not refreshed.*"):
                c.refresh_schema_metadata()
            end_time = time.time()
            assert end_time - start_time >= agreement_timeout
            assert original_meta is c.metadata.keyspaces

            # refresh wait overrides cluster value
            original_meta = c.metadata.keyspaces
            start_time = time.time()
            c.refresh_schema_metadata(max_schema_agreement_wait=0)
            end_time = time.time()
            assert end_time - start_time < agreement_timeout
            assert original_meta is not c.metadata.keyspaces
            assert original_meta == c.metadata.keyspaces

            c.shutdown()

            refresh_threshold = 0.5
            # cluster agreement bypass
            c = TestCluster(max_schema_agreement_wait=0)
            start_time = time.time()
            s = c.connect()
            end_time = time.time()
            assert end_time - start_time < refresh_threshold
            assert c.metadata.keyspaces

            # cluster agreement wait used for refresh
            original_meta = c.metadata.keyspaces
            start_time = time.time()
            c.refresh_schema_metadata()
            end_time = time.time()
            assert end_time - start_time < refresh_threshold
            assert original_meta is not c.metadata.keyspaces
            assert original_meta == c.metadata.keyspaces

            # refresh wait overrides cluster value
            original_meta = c.metadata.keyspaces
            start_time = time.time()
            with pytest.raises(Exception, match=r"Schema metadata was not refreshed.*"):
                c.refresh_schema_metadata(max_schema_agreement_wait=agreement_timeout)
            end_time = time.time()
            assert end_time - start_time >= agreement_timeout
            assert original_meta is c.metadata.keyspaces
            c.shutdown()

    def test_trace(self):
        """
        Ensure trace can be requested for async and non-async queries
        """

        cluster = TestCluster()
        session = cluster.connect()

        result = session.execute( "SELECT * FROM system.local WHERE key='local'", trace=True)
        self._check_trace(result.get_query_trace())

        query = "SELECT * FROM system.local WHERE key='local'"
        statement = SimpleStatement(query)
        result = session.execute(statement, trace=True)
        self._check_trace(result.get_query_trace())

        query = "SELECT * FROM system.local WHERE key='local'"
        statement = SimpleStatement(query)
        result = session.execute(statement)
        assert result.get_query_trace() is None

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2, trace=True)
        future.result()
        self._check_trace(future.get_query_trace())

        statement2 = SimpleStatement(query)
        future = session.execute_async(statement2)
        future.result()
        assert future.get_query_trace() is None

        prepared = session.prepare("SELECT * FROM system.local WHERE key='local'")
        future = session.execute_async(prepared, parameters=(), trace=True)
        future.result()
        self._check_trace(future.get_query_trace())
        cluster.shutdown()

    def test_trace_unavailable(self):
        """
        First checks that TraceUnavailable is arisen if the
        max_wait parameter is negative

        Then checks that TraceUnavailable is arisen if the
        result hasn't been set yet

        @since 3.10
        @jira_ticket PYTHON-196
        @expected_result TraceUnavailable is arisen in both cases

        @test_category query
                """
        cluster = TestCluster()
        self.addCleanup(cluster.shutdown)
        session = cluster.connect()

        query = "SELECT * FROM system.local WHERE key='local'"
        statement = SimpleStatement(query)

        max_retry_count = 10
        for i in range(max_retry_count):
            future = session.execute_async(statement, trace=True)
            future.result()
            try:
                result = future.get_query_trace(-1.0)
                # In case the result has time to come back before this timeout due to a race condition
                self._check_trace(result)
            except TraceUnavailable:
                break
        else:
            raise Exception("get_query_trace didn't raise TraceUnavailable after {} tries".format(max_retry_count))


        for i in range(max_retry_count):
            future = session.execute_async(statement, trace=True)
            try:
                result = future.get_query_trace(max_wait=120)
                # In case the result has been set check the trace
                self._check_trace(result)
            except TraceUnavailable:
                break
        else:
            raise Exception("get_query_trace didn't raise TraceUnavailable after {} tries".format(max_retry_count))

    def test_one_returns_none(self):
        """
        Test ResulSet.one returns None if no rows where found

        @since 3.14
        @jira_ticket PYTHON-947
        @expected_result ResulSet.one is None

        @test_category query
        """
        with TestCluster() as cluster:
            session = cluster.connect()
            assert session.execute("SELECT * from system.local WHERE key='madeup_key'").one() is None

    def test_string_coverage(self):
        """
        Ensure str(future) returns without error
        """

        cluster = TestCluster()
        session = cluster.connect()

        query = "SELECT * FROM system.local WHERE key='local'"
        statement = SimpleStatement(query)
        future = session.execute_async(statement)

        assert query in str(future)
        future.result()

        assert query in str(future)
        assert 'result' in str(future)
        cluster.shutdown()

    def test_can_connect_with_plainauth(self):
        """
        Verify that we can connect setting PlainTextAuthProvider against a
        C* server without authentication set. We also verify a warning is
        issued per connection. This test is here instead of in test_authentication.py
        because the C* server running in that module has auth set.

        @since 3.14
        @jira_ticket PYTHON-940
        @expected_result we can connect, query C* and warning are issued

        @test_category auth
        """
        auth_provider = PlainTextAuthProvider(
            username="made_up_username",
            password="made_up_password"
        )
        self._warning_are_issued_when_auth(auth_provider)

    def test_can_connect_with_sslauth(self):
        """
        Verify that we can connect setting SaslAuthProvider against a
        C* server without authentication set. We also verify a warning is
        issued per connection. This test is here instead of in test_authentication.py
        because the C* server running in that module has auth set.

        @since 3.14
        @jira_ticket PYTHON-940
        @expected_result we can connect, query C* and warning are issued

        @test_category auth
        """
        sasl_kwargs = {'service': 'cassandra',
                       'mechanism': 'PLAIN',
                       'qops': ['auth'],
                       'username': "made_up_username",
                       'password': "made_up_password"}

        auth_provider = SaslAuthProvider(**sasl_kwargs)
        self._warning_are_issued_when_auth(auth_provider)

    def _warning_are_issued_when_auth(self, auth_provider):
        with MockLoggingHandler().set_module_name(connection.__name__) as mock_handler:
            with TestCluster(auth_provider=auth_provider) as cluster:
                session = cluster.connect()
                assert session.execute("SELECT * from system.local WHERE key='local'") is not None

            # Three conenctions to nodes plus the control connection
            auth_warning = mock_handler.get_message_count('warning', "An authentication challenge was not sent")
            assert auth_warning >= 4
            assert auth_warning == mock_handler.get_message_count("debug", "Got ReadyMessage on new connection")

    def test_idle_heartbeat(self):
        interval = 2
        cluster = TestCluster(idle_heartbeat_interval=interval,
                              monitor_reporting_enabled=False)
        if PROTOCOL_VERSION < 3:
            cluster.set_core_connections_per_host(HostDistance.LOCAL, 1)
        session = cluster.connect(wait_for_all_pools=True)

        # This test relies on impl details of connection req id management to see if heartbeats
        # are being sent. May need update if impl is changed
        connection_request_ids = {}
        for h in cluster.get_connection_holders():
            for c in h.get_connections():
                # make sure none are idle (should have startup messages
                assert not c.is_idle
                with c.lock:
                    connection_request_ids[id(c)] = deque(c.request_ids)  # copy of request ids

        # let two heatbeat intervals pass (first one had startup messages in it)
        time.sleep(2 * interval + interval/2)

        connections = [c for holders in cluster.get_connection_holders() for c in holders.get_connections()]

        # make sure requests were sent on all connections
        for c in connections:
            expected_ids = connection_request_ids[id(c)]
            expected_ids.rotate(-1)
            with c.lock:
                assertListEqual(list(c.request_ids), list(expected_ids))

        # assert idle status
        assert all(c.is_idle for c in connections)

        # send messages on all connections
        statements_and_params = [("SELECT release_version FROM system.local WHERE key='local'", ())] * len(cluster.metadata.all_hosts())
        results = execute_concurrent(session, statements_and_params)
        for success, result in results:
            assert success

        # assert not idle status
        assert not any(c.is_idle if not c.is_control_connection else False for c in connections)

        # holders include session pools and cc
        holders = cluster.get_connection_holders()
        assert cluster.control_connection in holders
        assert len(holders) == len(cluster.metadata.all_hosts()) + 1  # hosts pools, 1 for cc

        # include additional sessions
        session2 = cluster.connect(wait_for_all_pools=True)

        holders = cluster.get_connection_holders()
        assert cluster.control_connection in holders
        assert len(holders) == 2 * len(cluster.metadata.all_hosts()) + 1  # 2 sessions' hosts pools, 1 for cc

        cluster._idle_heartbeat.stop()
        cluster._idle_heartbeat.join()
        assert_quiescent_pool_state(cluster)

        cluster.shutdown()

    @patch('cassandra.cluster.Cluster.idle_heartbeat_interval', new=0.1)
    def test_idle_heartbeat_disabled(self):
        assert Cluster.idle_heartbeat_interval

        # heartbeat disabled with '0'
        cluster = TestCluster(idle_heartbeat_interval=0)
        assert cluster.idle_heartbeat_interval == 0
        session = cluster.connect()

        # let two heatbeat intervals pass (first one had startup messages in it)
        time.sleep(2 * Cluster.idle_heartbeat_interval)

        connections = [c for holders in cluster.get_connection_holders() for c in holders.get_connections()]

        # assert not idle status (should never get reset because there is not heartbeat)
        assert not any(c.is_idle for c in connections)

        cluster.shutdown()

    def test_pool_management(self):
        # Ensure that in_flight and request_ids quiesce after cluster operations
        cluster = TestCluster(idle_heartbeat_interval=0)  # no idle heartbeat here, pool management is tested in test_idle_heartbeat
        session = cluster.connect()
        session2 = cluster.connect()

        # prepare
        p = session.prepare("SELECT * FROM system.local WHERE key=?")
        assert session.execute(p, ('local',))

        # simple
        assert session.execute("SELECT * FROM system.local WHERE key='local'")

        # set keyspace
        session.set_keyspace('system')
        session.set_keyspace('system_traces')

        # use keyspace
        session.execute('USE system')
        session.execute('USE system_traces')

        # refresh schema
        cluster.refresh_schema_metadata()
        cluster.refresh_schema_metadata(max_schema_agreement_wait=0)

        assert_quiescent_pool_state(cluster)

        cluster.shutdown()

    @local
    def test_profile_load_balancing(self):
        """
        Tests that profile load balancing policies are honored.

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result Execution Policy should be used when applicable.

        @test_category config_profiles
        """
        query = "select release_version from system.local where key='local'"
        node1 = ExecutionProfile(
            load_balancing_policy=HostFilterPolicy(
                RoundRobinPolicy(), lambda host: host.address == CASSANDRA_IP
            )
        )
        with TestCluster(execution_profiles={'node1': node1}, monitor_reporting_enabled=False) as cluster:
            session = cluster.connect(wait_for_all_pools=True)

            # default is DCA RR for all hosts
            expected_hosts = set(cluster.metadata.all_hosts())
            queried_hosts = set()
            for _ in expected_hosts:
                rs = session.execute(query)
                queried_hosts.add(rs.response_future._current_host)
            assert queried_hosts == expected_hosts

            # by name we should only hit the one
            expected_hosts = set(h for h in cluster.metadata.all_hosts() if h.address == CASSANDRA_IP)
            queried_hosts = set()
            for _ in cluster.metadata.all_hosts():
                rs = session.execute(query, execution_profile='node1')
                queried_hosts.add(rs.response_future._current_host)
            assert queried_hosts == expected_hosts

            # use a copied instance and override the row factory
            # assert last returned value can be accessed as a namedtuple so we can prove something different
            named_tuple_row = rs.one()
            assert isinstance(named_tuple_row, tuple)
            assert named_tuple_row.release_version

            tmp_profile = copy(node1)
            tmp_profile.row_factory = tuple_factory
            queried_hosts = set()
            for _ in cluster.metadata.all_hosts():
                rs = session.execute(query, execution_profile=tmp_profile)
                queried_hosts.add(rs.response_future._current_host)
            assert queried_hosts == expected_hosts
            tuple_row = rs.one()
            assert isinstance(tuple_row, tuple)
            with pytest.raises(AttributeError):
                tuple_row.release_version

            # make sure original profile is not impacted
            assert session.execute(query, execution_profile='node1').one().release_version

    def test_setting_lbp_legacy(self):
        cluster = TestCluster()
        self.addCleanup(cluster.shutdown)
        cluster.load_balancing_policy = RoundRobinPolicy()
        assert list(cluster.load_balancing_policy.make_query_plan()) == []
        cluster.connect()
        assert list(cluster.load_balancing_policy.make_query_plan()) != []

    def test_profile_lb_swap(self):
        """
        Tests that profile load balancing policies are not shared

        Creates two LBP, runs a few queries, and validates that each LBP is execised
        seperately between EP's

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result LBP should not be shared.

        @test_category config_profiles
        """
        query = "select release_version from system.local where key='local'"
        rr1 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        rr2 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        exec_profiles = {'rr1': rr1, 'rr2': rr2}
        with TestCluster(execution_profiles=exec_profiles) as cluster:
            session = cluster.connect(wait_for_all_pools=True)

            # default is DCA RR for all hosts
            expected_hosts = set(cluster.metadata.all_hosts())
            rr1_queried_hosts = set()
            rr2_queried_hosts = set()

            rs = session.execute(query, execution_profile='rr1')
            rr1_queried_hosts.add(rs.response_future._current_host)
            rs = session.execute(query, execution_profile='rr2')
            rr2_queried_hosts.add(rs.response_future._current_host)

            assert rr2_queried_hosts == rr1_queried_hosts

    def test_ta_lbp(self):
        """
        Test that execution profiles containing token aware LBP can be added

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result Queries can run

        @test_category config_profiles
        """
        query = "select release_version from system.local where key='local'"
        ta1 = ExecutionProfile()
        with TestCluster() as cluster:
            session = cluster.connect()
            cluster.add_execution_profile("ta1", ta1)
            rs = session.execute(query, execution_profile='ta1')

    def test_clone_shared_lbp(self):
        """
        Tests that profile load balancing policies are shared on clone

        Creates one LBP clones it, and ensures that the LBP is shared between
        the two EP's

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result LBP is shared

        @test_category config_profiles
        """
        query = "select release_version from system.local where key='local'"
        rr1 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        exec_profiles = {'rr1': rr1}
        with TestCluster(execution_profiles=exec_profiles) as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            assert len(cluster.metadata.all_hosts()) > 1, "We only have one host connected at this point"

            rr1_clone = session.execution_profile_clone_update('rr1', row_factory=tuple_factory)
            cluster.add_execution_profile("rr1_clone", rr1_clone)
            rr1_queried_hosts = set()
            rr1_clone_queried_hosts = set()
            rs = session.execute(query, execution_profile='rr1')
            rr1_queried_hosts.add(rs.response_future._current_host)
            rs = session.execute(query, execution_profile='rr1_clone')
            rr1_clone_queried_hosts.add(rs.response_future._current_host)
            assert rr1_clone_queried_hosts != rr1_queried_hosts

    def test_missing_exec_prof(self):
        """
        Tests to verify that using an unknown profile raises a ValueError

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result ValueError

        @test_category config_profiles
        """
        query = "select release_version from system.local where key='local'"
        rr1 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        rr2 = ExecutionProfile(load_balancing_policy=RoundRobinPolicy())
        exec_profiles = {'rr1': rr1, 'rr2': rr2}
        with TestCluster(execution_profiles=exec_profiles) as cluster:
            session = cluster.connect()
            with pytest.raises(ValueError):
                session.execute(query, execution_profile='rr3')

    @local
    def test_profile_pool_management(self):
        """
        Tests that changes to execution profiles correctly impact our cluster's pooling

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result pools should be correctly updated as EP's are added and removed

        @test_category config_profiles
        """

        node1 = ExecutionProfile(
            load_balancing_policy=HostFilterPolicy(
                RoundRobinPolicy(), lambda host: host.address == "127.0.0.1"
            )
        )
        node2 = ExecutionProfile(
            load_balancing_policy=HostFilterPolicy(
                RoundRobinPolicy(), lambda host: host.address == "127.0.0.2"
            )
        )
        with TestCluster(execution_profiles={EXEC_PROFILE_DEFAULT: node1, 'node2': node2}) as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            pools = session.get_pool_state()
            # there are more hosts, but we connected to the ones in the lbp aggregate
            assert len(cluster.metadata.all_hosts()) > 2
            assert set(h.address for h in pools) == set(('127.0.0.1', '127.0.0.2'))

            # dynamically update pools on add
            node3 = ExecutionProfile(
                load_balancing_policy=HostFilterPolicy(
                    RoundRobinPolicy(), lambda host: host.address == "127.0.0.3"
                )
            )
            cluster.add_execution_profile('node3', node3)
            pools = session.get_pool_state()
            assert set(h.address for h in pools) == set(('127.0.0.1', '127.0.0.2', '127.0.0.3'))

    @local
    def test_add_profile_timeout(self):
        """
        Tests that EP Timeouts are honored.

        @since 3.5
        @jira_ticket PYTHON-569
        @expected_result EP timeouts should override defaults

        @test_category config_profiles
        """
        max_retry_count = 10
        for i in range(max_retry_count):
            node1 = ExecutionProfile(
                load_balancing_policy=HostFilterPolicy(
                    RoundRobinPolicy(), lambda host: host.address == "127.0.0.1"
                )
            )
            with TestCluster(execution_profiles={EXEC_PROFILE_DEFAULT: node1}) as cluster:
                session = cluster.connect(wait_for_all_pools=True)
                pools = session.get_pool_state()
                assert len(cluster.metadata.all_hosts()) > 2
                assert set(h.address for h in pools) == set(('127.0.0.1',))

                node2 = ExecutionProfile(
                    load_balancing_policy=HostFilterPolicy(
                        RoundRobinPolicy(), lambda host: host.address in ["127.0.0.2", "127.0.0.3"]
                    )
                )

                start = time.time()
                try:
                    with pytest.raises(cassandra.OperationTimedOut):
                        cluster.add_execution_profile('profile_{0}'.format(i),
                                      node2, pool_wait_timeout=sys.float_info.min)
                    break
                except AssertionError:
                    end = time.time()
                    assert start == pytest.approx(end, abs=1e-1)
        else:
            raise Exception("add_execution_profile didn't timeout after {0} retries".format(max_retry_count))

    def test_stale_connections_after_shutdown(self):
        """
        Check if any connection/socket left unclosed after cluster.shutdown
        Originates from https://github.com/scylladb/python-driver/issues/120
        """
        for _ in range(10):
            with TestCluster(protocol_version=3) as cluster:
                cluster.connect().execute("SELECT * FROM system_schema.keyspaces")
                time.sleep(1)

            with TestCluster(protocol_version=3) as cluster:
                session = cluster.connect()
                for _ in range(5):
                    session.execute("SELECT * FROM system_schema.keyspaces")

            for _ in range(10):
                with TestCluster(protocol_version=3) as cluster:
                    cluster.connect().execute("SELECT * FROM system_schema.keyspaces")

            for _ in range(10):
                with TestCluster(protocol_version=3) as cluster:
                    cluster.connect()

            result = subprocess.run(["lsof -nP | awk '$3 ~ \":9042\" {print $0}' | grep ''"], shell=True, capture_output=True)
            if result.returncode:
                continue
            assert False, f'Found stale connections: {result.stdout}'

    @notwindows
    @notasyncio  # asyncio can't do timeouts smaller than 1ms, as this test requires
    def test_execute_query_timeout(self):
        with TestCluster() as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            query = "SELECT * FROM system.local"

            # default is passed down
            default_profile = cluster.profile_manager.profiles[EXEC_PROFILE_DEFAULT]
            rs = session.execute(query)
            assert rs.response_future.timeout == default_profile.request_timeout

            # tiny timeout times out as expected
            tmp_profile = copy(default_profile)
            tmp_profile.request_timeout = sys.float_info.min

            max_retry_count = 10
            for _ in range(max_retry_count):
                start = time.time()
                try:
                    with pytest.raises(cassandra.OperationTimedOut):
                        session.execute(query, execution_profile=tmp_profile)
                    break
                except:
                    import traceback
                    traceback.print_exc()
                    end = time.time()
                    assert start == pytest.approx(end, abs=1e-1)
            else:
                raise Exception("session.execute didn't time out in {0} tries".format(max_retry_count))

    def test_replicas_are_queried(self):
        """
        Test that replicas are queried first for TokenAwarePolicy. A table with RF 1
        is created. All the queries should go to that replica when TokenAwarePolicy
        is used.
        Then using HostFilterPolicy the replica is excluded from the considered hosts.
        By checking the trace we verify that there are no more replicas.

        @since 3.5
        @jira_ticket PYTHON-653
        @expected_result the replicas are queried for HostFilterPolicy

        @test_category metadata
        """
        queried_hosts = set()
        tap_profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy())
        )
        with TestCluster(execution_profiles={EXEC_PROFILE_DEFAULT: tap_profile}) as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            session.execute('''
                    CREATE TABLE test1rf.table_with_big_key (
                        k1 int,
                        k2 int,
                        k3 int,
                        k4 int,
                        PRIMARY KEY((k1, k2, k3), k4))''')
            prepared = session.prepare("""SELECT * from test1rf.table_with_big_key
                                          WHERE k1 = ? AND k2 = ? AND k3 = ? AND k4 = ?""")
            for i in range(10):
                result = session.execute(prepared, (i, i, i, i), trace=True)
                trace = result.response_future.get_query_trace(query_cl=ConsistencyLevel.ALL)
                queried_hosts = self._assert_replica_queried(trace, only_replicas=True)
                last_i = i

        hfp_profile = ExecutionProfile(
            load_balancing_policy=HostFilterPolicy(RoundRobinPolicy(),
                     predicate=lambda host: host.address != only_replica)
        )
        only_replica = queried_hosts.pop()
        log = logging.getLogger(__name__)
        log.info("The only replica found was: {}".format(only_replica))
        available_hosts = [host for host in ["127.0.0.1", "127.0.0.2", "127.0.0.3"] if host != only_replica]
        with TestCluster(contact_points=available_hosts,
                         execution_profiles={EXEC_PROFILE_DEFAULT: hfp_profile}) as cluster:

            session = cluster.connect(wait_for_all_pools=True)
            prepared = session.prepare("""SELECT * from test1rf.table_with_big_key
                                          WHERE k1 = ? AND k2 = ? AND k3 = ? AND k4 = ?""")
            for _ in range(10):
                result = session.execute(prepared, (last_i, last_i, last_i, last_i), trace=True)
                trace = result.response_future.get_query_trace(query_cl=ConsistencyLevel.ALL)
                self._assert_replica_queried(trace, only_replicas=False)

            session.execute('''DROP TABLE test1rf.table_with_big_key''')

    @greaterthanorequalcass30
    @lessthanorequalcass40
    @incorrect_test()
    def test_compact_option(self):
        """
        Test the driver can connect with the no_compact option and the results
        are as expected. This test is very similar to the corresponding dtest

        @since 3.12
        @jira_ticket PYTHON-366
        @expected_result only one hosts' metadata will be populated

        @test_category connection
        """
        nc_cluster = TestCluster(no_compact=True)
        nc_session = nc_cluster.connect()

        cluster = TestCluster(no_compact=False)
        session = cluster.connect()

        self.addCleanup(cluster.shutdown)
        self.addCleanup(nc_cluster.shutdown)

        nc_session.set_keyspace("test3rf")
        session.set_keyspace("test3rf")

        nc_session.execute(
            "CREATE TABLE IF NOT EXISTS compact_table (k int PRIMARY KEY, v1 int, v2 int) WITH COMPACT STORAGE;")

        for i in range(1, 5):
            nc_session.execute(
                "INSERT INTO compact_table (k, column1, v1, v2, value) VALUES "
                "({i}, 'a{i}', {i}, {i}, textAsBlob('b{i}'))".format(i=i))
            nc_session.execute(
                "INSERT INTO compact_table (k, column1, v1, v2, value) VALUES "
                "({i}, 'a{i}{i}', {i}{i}, {i}{i}, textAsBlob('b{i}{i}'))".format(i=i))

        nc_results = nc_session.execute("SELECT * FROM compact_table")
        assert set(nc_results.current_rows) == {(1, u'a1', 11, 11, 'b1'),
         (1, u'a11', 11, 11, 'b11'),
         (2, u'a2', 22, 22, 'b2'),
         (2, u'a22', 22, 22, 'b22'),
         (3, u'a3', 33, 33, 'b3'),
         (3, u'a33', 33, 33, 'b33'),
         (4, u'a4', 44, 44, 'b4'),
         (4, u'a44', 44, 44, 'b44')}

        results = session.execute("SELECT * FROM compact_table")
        assert set(results.current_rows) == {(1, 11, 11),
         (2, 22, 22),
         (3, 33, 33),
         (4, 44, 44)}

    def _assert_replica_queried(self, trace, only_replicas=True):
        queried_hosts = set()
        for row in trace.events:
            queried_hosts.add(row.source)
        if only_replicas:
            assert len(queried_hosts) == 1, "The hosts queried where {}".format(queried_hosts)
        else:
            assert len(queried_hosts) > 1, "The host queried was {}".format(queried_hosts)
        return queried_hosts

    def _check_trace(self, trace):
        assert trace.request_type is not None
        assert trace.duration is not None
        assert trace.started_at is not None
        assert trace.coordinator is not None
        assert trace.events is not None


class LocalHostAdressTranslator(AddressTranslator):

    def __init__(self, addr_map=None):
        self.addr_map = addr_map

    def translate(self, addr):
        new_addr = self.addr_map.get(addr)
        return new_addr

@local
class TestAddressTranslation(unittest.TestCase):

    def test_address_translator_basic(self):
        """
        Test host address translation

        Uses a custom Address Translator to map all ip back to one.
        Validates AddressTranslator invocation by ensuring that only meta data associated with single
        host is populated

        @since 3.3
        @jira_ticket PYTHON-69
        @expected_result only one hosts' metadata will be populated

        @test_category metadata
        """
        lh_ad = LocalHostAdressTranslator({'127.0.0.1': '127.0.0.1', '127.0.0.2': '127.0.0.1', '127.0.0.3': '127.0.0.1'})
        c = TestCluster(address_translator=lh_ad)
        c.connect()
        assert len(c.metadata.all_hosts()) == 1
        c.shutdown()

    def test_address_translator_with_mixed_nodes(self):
        """
        Test host address translation

        Uses a custom Address Translator to map ip's of non control_connection nodes to each other
        Validates AddressTranslator invocation by ensuring that metadata for mapped hosts is also mapped

        @since 3.3
        @jira_ticket PYTHON-69
        @expected_result metadata for crossed hosts will also be crossed

        @test_category metadata
        """
        adder_map = {'127.0.0.1': '127.0.0.1', '127.0.0.2': '127.0.0.3', '127.0.0.3': '127.0.0.2'}
        lh_ad = LocalHostAdressTranslator(adder_map)
        c = TestCluster(address_translator=lh_ad)
        c.connect()
        for host in c.metadata.all_hosts():
            assert adder_map.get(host.address) == host.broadcast_address
        c.shutdown()

@local
class ContextManagementTest(unittest.TestCase):
    load_balancing_policy = HostFilterPolicy(
        RoundRobinPolicy(), lambda host: host.address == CASSANDRA_IP
    )
    cluster_kwargs = {'execution_profiles': {EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=
                                                                                    load_balancing_policy)},
                      'schema_metadata_enabled': False,
                      'token_metadata_enabled': False}

    def test_no_connect(self):
        """
        Test cluster context without connecting.

        @since 3.4
        @jira_ticket PYTHON-521
        @expected_result context should still be valid

        @test_category configuration
        """
        with TestCluster() as cluster:
            assert not cluster.is_shutdown
        assert cluster.is_shutdown

    def test_simple_nested(self):
        """
        Test cluster and session contexts nested in one another.

        @since 3.4
        @jira_ticket PYTHON-521
        @expected_result cluster/session should be crated and shutdown appropriately.

        @test_category configuration
        """
        with TestCluster(**self.cluster_kwargs) as cluster:
            with cluster.connect() as session:
                assert not cluster.is_shutdown
                assert not session.is_shutdown
                assert session.execute('select release_version from system.local').one()
            assert session.is_shutdown
        assert cluster.is_shutdown

    def test_cluster_no_session(self):
        """
        Test cluster context without session context.

        @since 3.4
        @jira_ticket PYTHON-521
        @expected_result Session should be created correctly. Cluster should shutdown outside of context

        @test_category configuration
        """
        with TestCluster(**self.cluster_kwargs) as cluster:
            session = cluster.connect()
            assert not cluster.is_shutdown
            assert not session.is_shutdown
            assert session.execute('select release_version from system.local').one()
        assert session.is_shutdown
        assert cluster.is_shutdown

    def test_session_no_cluster(self):
        """
        Test session context without cluster context.

        @since 3.4
        @jira_ticket PYTHON-521
        @expected_result session should be created correctly. Session should shutdown correctly outside of context

        @test_category configuration
        """
        cluster = TestCluster(**self.cluster_kwargs)
        unmanaged_session = cluster.connect()
        with cluster.connect() as session:
            assert not cluster.is_shutdown
            assert not session.is_shutdown
            assert not unmanaged_session.is_shutdown
            assert session.execute('select release_version from system.local').one()
        assert session.is_shutdown
        assert not cluster.is_shutdown
        assert not unmanaged_session.is_shutdown
        unmanaged_session.shutdown()
        assert unmanaged_session.is_shutdown
        assert not cluster.is_shutdown
        cluster.shutdown()
        assert cluster.is_shutdown


class HostStateTest(unittest.TestCase):

    def test_down_event_with_active_connection(self):
        """
        Test to ensure that on down calls to clusters with connections still active don't result in
        a host being marked down. The second part of the test kills the connection then invokes
        on_down, and ensures the state changes for host's metadata.

        @since 3.7
        @jira_ticket PYTHON-498
        @expected_result host should never be toggled down while a connection is active.

        @test_category connection
        """
        with TestCluster() as cluster:
            session = cluster.connect(wait_for_all_pools=True)
            random_host = cluster.metadata.all_hosts()[0]
            cluster.on_down(random_host, False)
            for _ in range(10):
                new_host = cluster.metadata.all_hosts()[0]
                assert new_host.is_up, "Host was not up on iteration {0}".format(_)
                time.sleep(.01)

            pool = session._pools.get(random_host)
            pool.shutdown()
            cluster.on_down(random_host, False)
            was_marked_down = False
            for _ in range(20):
                new_host = cluster.metadata.all_hosts()[0]
                if not new_host.is_up:
                    was_marked_down = True
                    break
                time.sleep(.01)
            assert was_marked_down


@local
class DontPrepareOnIgnoredHostsTest(unittest.TestCase):
    ignored_addresses = ['127.0.0.3']
    ignore_node_3_policy = IgnoredHostPolicy(ignored_addresses)

    def test_prepare_on_ignored_hosts(self):

        cluster = TestCluster(
            execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=self.ignore_node_3_policy)}
        )
        session = cluster.connect()
        cluster.reprepare_on_up, cluster.prepare_on_all_hosts = True, False

        hosts = cluster.metadata.all_hosts()
        session.execute("CREATE KEYSPACE clustertests "
                        "WITH replication = "
                        "{'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.execute("CREATE TABLE clustertests.tab (a text, PRIMARY KEY (a))")
        # assign to an unused variable so cluster._prepared_statements retains
        # reference
        _ = session.prepare("INSERT INTO clustertests.tab (a) VALUES ('a')")  # noqa

        cluster.connection_factory = Mock(wraps=cluster.connection_factory)

        unignored_address = '127.0.0.1'
        unignored_host = next(h for h in hosts if h.address == unignored_address)
        ignored_host = next(h for h in hosts if h.address in self.ignored_addresses)
        unignored_host.is_up = ignored_host.is_up = False

        cluster.on_up(unignored_host)
        cluster.on_up(ignored_host)

        # the length of mock_calls will vary, but all should use the unignored
        # address
        for c in cluster.connection_factory.mock_calls:
            assert unignored_address == c.args[0].address
        cluster.shutdown()


@protocolv6
class BetaProtocolTest(unittest.TestCase):

    @protocolv6
    def test_invalid_protocol_version_beta_option(self):
        """
        Test cluster connection with protocol v6 and beta flag not set

        @since 3.7.0
        @jira_ticket PYTHON-614, PYTHON-1232
        @expected_result client shouldn't connect with V6 and no beta flag set

        @test_category connection
        """


        cluster = TestCluster(protocol_version=cassandra.ProtocolVersion.V6, allow_beta_protocol_version=False)
        try:
            with pytest.raises(NoHostAvailable):
                cluster.connect()
        except Exception as e:
            pytest.fail("Unexpected error encountered {0}".format(e.message))

    @protocolv6
    def test_valid_protocol_version_beta_options_connect(self):
        """
        Test cluster connection with protocol version 5 and beta flag set

        @since 3.7.0
        @jira_ticket PYTHON-614, PYTHON-1232
        @expected_result client should connect with protocol v6 and beta flag set.

        @test_category connection
        """
        cluster = Cluster(protocol_version=cassandra.ProtocolVersion.V6, allow_beta_protocol_version=True)
        session = cluster.connect()
        assert cluster.protocol_version == cassandra.ProtocolVersion.V6
        assert session.execute("select release_version from system.local").one()
        cluster.shutdown()


class DeprecationWarningTest(unittest.TestCase):
    def test_deprecation_warnings_legacy_parameters(self):
        """
        Tests the deprecation warning has been added when using
        legacy parameters

        @since 3.13
        @jira_ticket PYTHON-877
        @expected_result the deprecation warning is emitted

        @test_category logs
        """
        with warnings.catch_warnings(record=True) as w:
            TestCluster(load_balancing_policy=RoundRobinPolicy())
            logging.info(w)
            assert len(w) >= 1
            assert any(["Legacy execution parameters will be removed in 4.0. "
                                 "Consider using execution profiles." in
                            str(wa.message) for wa in w])

    def test_deprecation_warnings_meta_refreshed(self):
        """
        Tests the deprecation warning has been added when enabling
        metadata refreshment

        @since 3.13
        @jira_ticket PYTHON-890
        @expected_result the deprecation warning is emitted

        @test_category logs
        """
        with warnings.catch_warnings(record=True) as w:
            cluster = TestCluster()
            cluster.set_meta_refresh_enabled(True)
            logging.info(w)
            assert len(w) >= 1
            assert any(["Cluster.set_meta_refresh_enabled is deprecated and will be removed in 4.0." in
                            str(wa.message) for wa in w])

    def test_deprecation_warning_default_consistency_level(self):
        """
        Tests the deprecation warning has been added when enabling
        session the default consistency level to session

        @since 3.14
        @jira_ticket PYTHON-935
        @expected_result the deprecation warning is emitted

        @test_category logs
        """
        with warnings.catch_warnings(record=True) as w:
            cluster = TestCluster()
            session = cluster.connect()
            session.default_consistency_level = ConsistencyLevel.ONE
            assert len(w) >= 1
            assert any(["Setting the consistency level at the session level will be removed in 4.0" in
                            str(wa.message) for wa in w])
