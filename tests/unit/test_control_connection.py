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

import gc
import unittest
import weakref

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, ANY, call, patch

from cassandra import (AuthenticationFailed, OperationTimedOut,
                       SchemaTargetType, SchemaChangeType,
                       UnresolvableContactPoints)
from cassandra.protocol import ResultMessage, RESULT_KIND_ROWS
from cassandra.cluster import (Cluster, ControlConnection, _Scheduler,
                               ProfileManager, EXEC_PROFILE_DEFAULT,
                               ExecutionProfile,
                               ControlConnectionQueryFallback,
                               NoHostAvailable, _ControlReconnectionHandler)
from cassandra.pool import Host, _ReconnectionHandler
from cassandra.connection import (ConnectionException, EndPoint, DefaultEndPoint,
                                  DefaultEndPointFactory, UnixSocketEndPoint)
from cassandra.policies import (HostDistance, SimpleConvictionPolicy,
                                RoundRobinPolicy, ConstantReconnectionPolicy,
                                ExponentialReconnectionPolicy,
                                IdentityTranslator)

PEER_IP = "foobar"


class MockMetadata(object):

    def __init__(self):
        self.hosts = {
            'uuid1': Host(endpoint=DefaultEndPoint("192.168.1.0"), conviction_policy_factory=SimpleConvictionPolicy, host_id='uuid1'),
            'uuid2': Host(endpoint=DefaultEndPoint("192.168.1.1"), conviction_policy_factory=SimpleConvictionPolicy, host_id='uuid2'),
            'uuid3': Host(endpoint=DefaultEndPoint("192.168.1.2"), conviction_policy_factory=SimpleConvictionPolicy, host_id='uuid3')
        }
        self._host_id_by_endpoint = {
            DefaultEndPoint("192.168.1.0"): 'uuid1',
            DefaultEndPoint("192.168.1.1"): 'uuid2',
            DefaultEndPoint("192.168.1.2"): 'uuid3',
        }
        for host in self.hosts.values():
            host.set_up()
            host.release_version = "3.11"

        self.cluster_name = None
        self.partitioner = None
        self.token_map = {}
        self.removed_hosts = []

    def get_host(self, endpoint_or_address, port=None):
        if not isinstance(endpoint_or_address, EndPoint):
            for host in self.hosts.values():
                if (host.address == endpoint_or_address and
                        (port is None or host.broadcast_rpc_port is None or host.broadcast_rpc_port == port)):
                    return host
        else:
            host_id = self._host_id_by_endpoint.get(endpoint_or_address)
            return self.hosts.get(host_id)

    def get_host_by_host_id(self, host_id):
        return self.hosts.get(host_id)

    def all_hosts(self):
        return self.hosts.values()

    def rebuild_token_map(self, partitioner, token_map):
        self.partitioner = partitioner
        self.token_map = token_map

    def add_or_return_host(self, host):
        try:
            return self.hosts[host.host_id], False
        except KeyError:
            self._host_id_by_endpoint[host.endpoint] = host.host_id
            self.hosts[host.host_id] = host
            return host, True

    def update_host(self, host, old_endpoint):
        host, created = self.add_or_return_host(host)
        self._host_id_by_endpoint.pop(old_endpoint, False)
        self._host_id_by_endpoint[host.endpoint] = host.host_id

    def all_hosts_items(self):
        return list(self.hosts.items())

    def remove_host_by_host_id(self, host_id, endpoint=None):
        if endpoint and self._host_id_by_endpoint[endpoint] == host_id:
            self._host_id_by_endpoint.pop(endpoint, False)
        self.removed_hosts.append(self.hosts.pop(host_id, False))
        return bool(self.hosts.pop(host_id, False))


class MockCluster(object):

    max_schema_agreement_wait = 5
    profile_manager = ProfileManager()
    reconnection_policy = ConstantReconnectionPolicy(2)
    address_translator = IdentityTranslator()
    down_host = None
    contact_points = []
    is_shutdown = False

    def __init__(self):
        self.metadata = MockMetadata()
        self.added_hosts = []
        self.scheduler = Mock(spec=_Scheduler)
        self.executor = Mock(spec=ThreadPoolExecutor)
        self.profile_manager.profiles[EXEC_PROFILE_DEFAULT] = ExecutionProfile(RoundRobinPolicy())
        self.endpoint_factory = DefaultEndPointFactory().configure(self)
        self.ssl_options = None

    def add_host(self, endpoint, datacenter, rack, signal=False, refresh_nodes=True, host_id=None):
        host = Host(endpoint, SimpleConvictionPolicy, datacenter, rack, host_id=host_id)
        host, _ = self.metadata.add_or_return_host(host)
        self.added_hosts.append(host)
        return host, True

    def remove_host(self, host):
        pass

    def on_up(self, host):
        pass

    def on_down(self, host, is_host_addition, expect_host_to_be_down=False):
        self.down_host = host


def _node_meta_results(local_results, peer_results):
    """
    creates a pair of ResultMessages from (col_names, parsed_rows)
    """
    local_response = ResultMessage(kind=RESULT_KIND_ROWS)
    local_response.column_names = local_results[0]
    local_response.parsed_rows = local_results[1]

    peer_response = ResultMessage(kind=RESULT_KIND_ROWS)
    peer_response.column_names = peer_results[0]
    peer_response.parsed_rows = peer_results[1]

    return peer_response, local_response


class MockConnection(object):

    is_defunct = False
    is_closed = False

    def __init__(self):
        self.endpoint = DefaultEndPoint("192.168.1.0")
        self.original_endpoint = self.endpoint
        self.local_results = [
            ["rpc_address", "schema_version", "cluster_name", "data_center", "rack", "partitioner", "release_version", "tokens", "host_id", "listen_address"],
            [["192.168.1.0", "a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", "2.2.0", ["0", "100", "200"], "uuid1", "192.168.1.0"]]
        ]

        self.peer_results = [
            ["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens", "host_id"],
            [["192.168.1.1", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], "uuid2"],
             ["192.168.1.2", "10.0.0.2", "a", "dc1", "rack1", ["2", "102", "202"], "uuid3"]]
        ]

        self.peer_results_v2 = [
            ["native_address",  "native_port", "peer", "peer_port", "schema_version", "data_center", "rack", "tokens", "host_id"],
            [["192.168.1.1", 9042, "10.0.0.1", 7042, "a", "dc1", "rack1", ["1", "101", "201"], "uuid2"],
             ["192.168.1.2", 9042, "10.0.0.2", 7040, "a", "dc1", "rack1", ["2", "102", "202"], "uuid3"]]
        ]
        self.wait_for_responses = Mock(return_value=_node_meta_results(self.local_results, self.peer_results))


class FakeTime(object):

    def __init__(self):
        self.clock = 0

    def time(self):
        return self.clock

    def sleep(self, amount):
        self.clock += amount


class ControlConnectionTest(unittest.TestCase):

    _matching_schema_preloaded_results = _node_meta_results(
        local_results=(["rpc_address", "schema_version", "cluster_name", "data_center", "rack", "partitioner", "release_version", "tokens", "host_id", "listen_address"],
                       [["192.168.1.0", "a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", "2.2.0", ["0", "100", "200"], "uuid1", "192.168.1.0"]]),
        peer_results=(["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens", "host_id"],
                      [["192.168.1.1", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], "uuid2"],
                       ["192.168.1.2", "10.0.0.2", "a", "dc1", "rack1", ["2", "102", "202"], "uuid3"]]))

    _nonmatching_schema_preloaded_results = _node_meta_results(
        local_results=(["rpc_address", "schema_version", "cluster_name", "data_center", "rack", "partitioner", "release_version", "tokens", "host_id", "listen_address"],
                       [["192.168.1.0", "a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", "2.2.0", ["0", "100", "200"], "uuid1", "192.168.1.0"]]),
        peer_results=(["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens", "host_id"],
                      [["192.168.1.1", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], "uuid2"],
                       ["192.168.1.2", "10.0.0.2", "b", "dc1", "rack1", ["2", "102", "202"], "uuid3"]]))

    def setUp(self):
        self.cluster = MockCluster()
        self.connection = MockConnection()
        self.time = FakeTime()

        self.control_connection = ControlConnection(self.cluster, 1, 0, 0, 0)
        self.control_connection._connection = self.connection
        self.control_connection._time = self.time
        self.cluster.control_connection = self.control_connection

    def _forget_local_host(self):
        endpoint = DefaultEndPoint('192.168.1.0')
        self.cluster.metadata._host_id_by_endpoint.pop(endpoint)
        self.cluster.metadata.hosts.pop('uuid1')

    def _discover_local_host_over_unix(self):
        maintenance_endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        self._forget_local_host()
        self.connection.endpoint = maintenance_endpoint
        self.connection.original_endpoint = maintenance_endpoint
        self.control_connection.refresh_node_list_and_token_map()
        local_host = self.cluster.metadata.get_host_by_host_id('uuid1')
        local_host.set_up()
        return maintenance_endpoint, local_host

    def _refresh_control_connection_over_network(self):
        self.connection.endpoint = DefaultEndPoint('192.168.1.0')
        self.connection.original_endpoint = self.connection.endpoint
        self.control_connection.refresh_node_list_and_token_map()

    def _use_cluster_down_handling(
            self, sessions=(),
            fallback=ControlConnectionQueryFallback.Disabled):
        self.cluster.sessions = list(sessions)
        self.cluster._discount_down_events = True
        self.cluster.allow_control_connection_query_fallback = fallback
        self.cluster.profile_manager = Mock()
        self.cluster.profile_manager.distance.return_value = \
            HostDistance.LOCAL
        # The real method reports whether the executor accepted the work.
        self.cluster.on_down_potentially_blocking = Mock(return_value=True)
        self.cluster._restart_reconnector = Mock(return_value=True)
        self.cluster.on_down = Cluster.on_down.__get__(self.cluster)
        self.cluster.signal_connection_failure = \
            Cluster.signal_connection_failure.__get__(self.cluster)

    def _discount_down_for(self, host):
        """Model a session pool that keeps ``host`` up despite a conviction."""
        session = Mock()
        session.get_pool_state.return_value = {host: {'open_count': 1}}
        self._use_cluster_down_handling([session])
        return session

    def test_wait_for_schema_agreement(self):
        """
        Basic test with all schema versions agreeing
        """
        assert self.control_connection._wait_for_schema_agreement()
        # the control connection should not have slept at all
        assert self.time.clock == 0

    @patch('cassandra.cluster.warn')
    def test_wait_for_schema_agreement_warns_about_deprecation(self, mocked_warn):
        assert self.control_connection.wait_for_schema_agreement()

        mocked_warn.assert_called_once()
        warning_args, warning_kwargs = mocked_warn.call_args
        assert 'ControlConnection.wait_for_schema_agreement is deprecated' in str(warning_args[0])
        assert 'Use Session.wait_for_schema_agreement instead.' in str(warning_args[0])
        assert warning_args[1] is DeprecationWarning
        assert warning_kwargs['stacklevel'] == 2

    def test_wait_for_schema_agreement_uses_preloaded_results_if_given(self):
        """
        wait_for_schema_agreement uses preloaded results if given for shared table queries
        """
        preloaded_results = self._matching_schema_preloaded_results
        assert self.control_connection._wait_for_schema_agreement(preloaded_results=preloaded_results)
        # the control connection should not have slept at all
        assert self.time.clock == 0
        # the connection should not have made any queries if given preloaded results
        assert self.connection.wait_for_responses.call_count == 0

    def test_wait_for_schema_agreement_falls_back_to_querying_if_schemas_dont_match_preloaded_result(self):
        """
        wait_for_schema_agreement requery if schema does not match using preloaded results
        """
        preloaded_results = self._nonmatching_schema_preloaded_results
        assert self.control_connection._wait_for_schema_agreement(preloaded_results=preloaded_results)
        # the control connection should not have slept at all
        assert self.time.clock == 0
        assert self.connection.wait_for_responses.call_count == 1

    def test_wait_for_schema_agreement_fails(self):
        """
        Make sure the control connection sleeps and retries
        """
        # change the schema version on one node
        self.connection.peer_results[1][1][2] = 'b'
        assert not self.control_connection._wait_for_schema_agreement()
        # the control connection should have slept until it hit the limit
        assert self.time.clock >= self.cluster.max_schema_agreement_wait

    def test_wait_for_schema_agreement_skipping(self):
        """
        If rpc_address or schema_version isn't set, the host should be skipped
        """
        # an entry with no schema_version
        self.connection.peer_results[1].append(
            ["192.168.1.3", "10.0.0.3", None, "dc1", "rack1", ["3", "103", "203"]]
        )
        # an entry with a different schema_version and no rpc_address
        self.connection.peer_results[1].append(
            [None, None, "b", "dc1", "rack1", ["4", "104", "204"]]
        )

        # change the schema version on one of the existing entries
        self.connection.peer_results[1][1][3] = 'c'
        self.cluster.metadata.get_host(DefaultEndPoint('192.168.1.1')).is_up = False

        assert self.control_connection._wait_for_schema_agreement()
        assert self.time.clock == 0

    def test_wait_for_schema_agreement_rpc_lookup(self):
        """
        If the rpc_address is 0.0.0.0, the "peer" column should be used instead.
        """
        self.connection.peer_results[1].append(
            ["0.0.0.0", PEER_IP, "b", "dc1", "rack1", ["3", "103", "203"], "uuid6"]
        )
        host = Host(DefaultEndPoint("0.0.0.0"), SimpleConvictionPolicy, host_id='uuid6')
        self.cluster.metadata.hosts[host.host_id] = host
        self.cluster.metadata._host_id_by_endpoint[DefaultEndPoint(PEER_IP)] = host.host_id
        host.is_up = False

        # even though the new host has a different schema version, it's
        # marked as down, so the control connection shouldn't care
        assert self.control_connection._wait_for_schema_agreement()
        assert self.time.clock == 0

        # but once we mark it up, the control connection will care
        host.is_up = True
        assert not self.control_connection._wait_for_schema_agreement()
        assert self.time.clock >= self.cluster.max_schema_agreement_wait


    def test_wait_for_schema_agreement_none_timeout(self):
        """
        When control_connection_timeout is None, wait_for_schema_agreement
        should not raise a TypeError on the min() call.
        """
        cc = ControlConnection(self.cluster, timeout=None,
                               schema_event_refresh_window=0,
                               topology_event_refresh_window=0,
                               status_event_refresh_window=0)
        cc._connection = self.connection
        cc._time = self.time
        assert cc._wait_for_schema_agreement()

    def test_refresh_nodes_and_tokens(self):
        self.control_connection.refresh_node_list_and_token_map()
        meta = self.cluster.metadata
        assert meta.partitioner == 'Murmur3Partitioner'
        assert meta.cluster_name == 'foocluster'

        # check token map
        assert sorted(meta.all_hosts()) == sorted(meta.token_map.keys())
        for token_list in meta.token_map.values():
            assert 3 == len(token_list)

        # check datacenter/rack
        for host in meta.all_hosts():
            assert host.datacenter == "dc1"
            assert host.rack == "rack1"

        assert self.connection.wait_for_responses.call_count == 1

    def test_refresh_sets_local_listen_address_when_rpc_address_changes(self):
        self.connection.local_results[1][0][0] = '192.168.1.4'

        self.control_connection.refresh_node_list_and_token_map()

        local_host = self.cluster.metadata.get_host_by_host_id('uuid1')
        assert local_host.endpoint == DefaultEndPoint('192.168.1.4')
        assert local_host.listen_address == '192.168.1.0'

    def test_refresh_sets_local_addresses_without_token_metadata(self):
        self.control_connection._token_meta_enabled = False
        self.connection.local_results[0].append('broadcast_address')
        self.connection.local_results[1][0].append('10.0.0.1')

        for results in (self.connection.local_results, self.connection.peer_results):
            tokens_index = results[0].index('tokens')
            results[0].pop(tokens_index)
            for row in results[1]:
                row.pop(tokens_index)
        self.control_connection.refresh_node_list_and_token_map()

        local_query = self.connection.wait_for_responses.call_args[0][1]
        local_projection = local_query.query.split(" FROM system.local", 1)[0]
        assert 'listen_address' in local_projection
        assert 'broadcast_address' in local_projection
        assert 'tokens' not in local_projection
        local_host = self.cluster.metadata.get_host_by_host_id('uuid1')
        assert local_host.listen_address == '192.168.1.0'
        assert local_host.broadcast_address == '10.0.0.1'

    def test_refresh_uses_control_endpoint_for_local_unix_host(self):
        maintenance_endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        self._forget_local_host()
        self.connection.endpoint = maintenance_endpoint
        self.connection.original_endpoint = maintenance_endpoint

        self.control_connection.refresh_node_list_and_token_map()

        local_host = self.cluster.metadata.get_host_by_host_id('uuid1')
        assert local_host.endpoint == maintenance_endpoint
        assert local_host.broadcast_rpc_address == '192.168.1.0'
        peer_host = self.cluster.metadata.get_host_by_host_id('uuid2')
        assert peer_host.endpoint == DefaultEndPoint('192.168.1.1')
        assert sorted([local_host, peer_host]) == \
            sorted([peer_host, local_host])

    def test_refresh_checks_unix_local_advertised_endpoint_for_duplicates(self):
        self._forget_local_host()
        self.connection.endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        self.connection.original_endpoint = \
            UnixSocketEndPoint('/tmp/maintenance.sock')
        self.connection.peer_results[1].append([
            '192.168.1.0', '10.0.0.4', 'a', 'dc1', 'rack1',
            ['4', '104', '204'], 'uuid4'])

        self.control_connection.refresh_node_list_and_token_map()

        assert self.cluster.metadata.get_host_by_host_id('uuid4') is None

    def test_refresh_preserves_known_unix_endpoint_when_host_becomes_peer(self):
        maintenance_endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        self._forget_local_host()
        self.connection.endpoint = maintenance_endpoint
        self.connection.original_endpoint = maintenance_endpoint
        self.control_connection.refresh_node_list_and_token_map()

        local_results = (
            self.connection.local_results[0],
            [['192.168.1.1', 'a', 'foocluster', 'dc1', 'rack1',
              'Murmur3Partitioner', '2.2.0', ['1', '101', '201'],
              'uuid2', '192.168.1.1']])
        peer_results = (
            self.connection.peer_results[0],
            [['192.168.1.0', '10.0.0.1', 'a', 'dc1', 'rack1',
              ['0', '100', '200'], 'uuid1'],
             ['192.168.1.2', '10.0.0.2', 'a', 'dc1', 'rack1',
              ['2', '102', '202'], 'uuid3']])
        self.connection.endpoint = DefaultEndPoint('192.168.1.1')
        self.connection.original_endpoint = self.connection.endpoint

        self.control_connection._refresh_node_list_and_token_map(
            self.connection,
            preloaded_results=_node_meta_results(local_results, peer_results))

        local_host = self.cluster.metadata.get_host_by_host_id('uuid1')
        assert local_host.endpoint == maintenance_endpoint

        peer_results[1][0][2] = 'b'
        peers_response, local_response = _node_meta_results(
            local_results, peer_results)
        mismatches = self.control_connection._get_schema_mismatches(
            peers_response, local_response, self.connection.endpoint)
        assert maintenance_endpoint in mismatches['b']

    def test_refresh_uses_factory_for_local_network_host(self):
        self.connection.original_endpoint = DefaultEndPoint('proxy', 9999)

        self.control_connection.refresh_node_list_and_token_map()

        local_host = self.cluster.metadata.get_host_by_host_id('uuid1')
        assert local_host.endpoint == DefaultEndPoint('192.168.1.0')

    def test_schema_query_uses_shard_aware_connection_original_endpoint(self):
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        self.connection.endpoint = DefaultEndPoint('192.168.1.0', 19042)
        self.connection.original_endpoint = host.endpoint
        self.control_connection._uses_peers_v2 = False

        query = self.control_connection._get_peers_query(
            self.control_connection.PeersQueryType.PEERS_SCHEMA,
            self.connection)

        assert query == self.control_connection._SELECT_SCHEMA_PEERS_TEMPLATE \
            .format(nt_col_name='rpc_address')

    def test_defunct_tcp_control_reconnects_when_open_pool_discounts_down(self):
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host.set_up()
        session = Mock()
        session.get_pool_state.return_value = {
            host: {'open_count': 1}}
        self._use_cluster_down_handling([session])
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        assert host.is_up is True
        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_defunct_control_reconnects_when_conviction_is_rejected(self):
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host.set_up()
        self._use_cluster_down_handling()
        self.connection.is_defunct = True
        self.connection.last_error = OperationTimedOut()
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        assert host.is_up is True
        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_defunct_control_reconnects_when_host_is_already_down(self):
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host.set_down()
        self._use_cluster_down_handling()
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_defunct_control_reconnects_when_host_reconnector_is_active(self):
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host.set_down()
        host.get_and_set_reconnection_handler(Mock())
        self._use_cluster_down_handling()
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        assert host.is_up is False
        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_defunct_control_reconnects_when_pool_creation_is_disabled(self):
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host.set_up()
        self._use_cluster_down_handling(
            fallback=ControlConnectionQueryFallback.SkipPoolCreation)
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        assert host.is_up is True
        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_defunct_control_waits_for_dispatched_down_callback(self):
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host.set_up()
        self._use_cluster_down_handling()
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        assert host.is_up is False
        self.cluster.on_down_potentially_blocking.assert_called_once_with(
            host, False, host._down_event_generation)
        self.cluster.executor.submit.assert_not_called()

        self.control_connection.on_down(host)

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_signal_error_reconnects_non_defunct_connection(self):
        self.connection.is_defunct = False
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_signal_error_reconnects_when_host_is_unresolved(self):
        self._forget_local_host()
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_signal_error_does_nothing_after_control_connection_shutdown(self):
        self.control_connection._is_shutdown = True
        self.connection.is_defunct = True
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.executor.submit.assert_not_called()

    def test_signal_error_leaves_an_in_flight_reconnection_alone(self):
        # _reconnect() cancels the handler and restarts its schedule from the
        # initial delay, so repeated errors must not keep resetting the backoff.
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host.set_down()
        self._use_cluster_down_handling()
        self.control_connection._reconnection_handler = Mock()
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.executor.submit.assert_not_called()

    def _make_reconnection_handler(self):
        handler = _ControlReconnectionHandler(
            self.control_connection, self.cluster.scheduler, iter([1.0]))
        self.control_connection._reconnection_handler = handler
        return handler

    def test_reconnection_handler_releases_its_slot_when_it_gives_up(self):
        handler = self._make_reconnection_handler()

        handler.on_exception(ConnectionException('refused'), None)

        assert self.control_connection._reconnection_handler is None

    def test_reconnection_handler_keeps_its_slot_while_it_retries(self):
        for exc in (ConnectionException('refused'),
                    AuthenticationFailed('bad password')):
            with self.subTest(exc=exc):
                handler = self._make_reconnection_handler()

                assert handler.on_exception(exc, 1.0)

                assert self.control_connection._reconnection_handler is handler

    def test_reconnection_handler_never_releases_a_replacement(self):
        handler = self._make_reconnection_handler()
        replacement = self._make_reconnection_handler()

        handler.on_exception(ConnectionException('refused'), None)

        assert self.control_connection._reconnection_handler is replacement

    def test_reconnection_handler_run_leaves_a_replacement_in_the_slot(self):
        # A finishing handler must not clear a replacement that another thread
        # parked in the slot while it was handing its connection over.
        # _set_new_connection() has already released this handler by then, so
        # clearing again can only evict someone else -- and it would do so
        # without cancelling them, leaving a handler that keeps retrying where
        # reconnect() can no longer see it.
        handler = self._make_reconnection_handler()
        self.control_connection._connection = None
        conn = Mock()
        replacement = []

        def install_and_lose_the_race(new_conn):
            # What _set_new_connection() really does -- release the slot and
            # adopt the connection -- plus another thread winning the race to
            # park a fresh handler in the slot it just emptied.
            self.control_connection._reconnection_handler = None
            self.control_connection._connection = new_conn
            replacement.append(self._make_reconnection_handler())

        with patch.object(handler, 'try_reconnect', return_value=conn), \
                patch.object(self.control_connection, '_set_new_connection',
                             side_effect=install_and_lose_the_race):
            handler.run()

        assert self.control_connection._reconnection_handler is replacement[0]

    def test_reconnection_handler_keeps_the_connection_it_installs(self):
        # run() closes the connection it opened, which is right for the host
        # handler that only probes with it, but this one hands it to the
        # control connection.
        handler = self._make_reconnection_handler()
        self.control_connection._connection = None
        conn = Mock()

        with patch.object(handler, 'try_reconnect', return_value=conn):
            handler.run()

        assert self.control_connection._connection is conn
        conn.close.assert_not_called()

    def test_reconnection_handler_keeps_the_connection_a_failed_install_took(self):
        # Installing the new control connection closes the old one, which runs
        # user callbacks, and one of those may raise. The connection is already
        # adopted by then, so it must not be closed on the way out.
        handler = self._make_reconnection_handler()
        self.control_connection._connection = None
        conn = Mock()

        with patch.object(handler, 'try_reconnect', return_value=conn), \
                patch.object(self.control_connection, '_set_new_connection',
                             side_effect=RuntimeError('listener failed')):
            with self.assertRaises(RuntimeError):
                handler.run()

        conn.close.assert_not_called()

    def test_reconnection_handler_closes_a_connection_it_only_probes_with(self):
        # The plain handler, like the host one, only uses the connection to
        # prove the host is reachable, so run() still closes it for it.
        handler = _ReconnectionHandler(self.cluster.scheduler, iter([1.0]),
                                       Mock())
        conn = Mock()

        with patch.object(handler, 'try_reconnect', return_value=conn):
            handler.run()

        conn.close.assert_called_once_with()

    def test_reconnecting_successfully_releases_a_parked_handler(self):
        # A handler installed by an earlier failure is still backing off when
        # an unrelated reconnect succeeds. Left in the slot it would look like
        # a reconnection in progress to every later error.
        handler = self._make_reconnection_handler()
        self.control_connection._connection = None

        with patch.object(self.control_connection, '_reconnect_internal',
                          return_value=Mock()):
            self.control_connection._reconnect()

        assert self.control_connection._reconnection_handler is None
        assert handler._cancelled

    def test_set_new_connection_closes_a_connection_shutdown_beat(self):
        # shutdown() already closed the control connection, so nothing would
        # ever use this one or close it on our behalf.
        self.control_connection._is_shutdown = True
        conn = Mock()

        self.control_connection._set_new_connection(conn)

        conn.close.assert_called_once_with()
        assert self.control_connection._connection is self.connection

    def test_signal_error_reconnects_once_a_reconnection_has_given_up(self):
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host.set_down()
        self._use_cluster_down_handling()
        handler = self._make_reconnection_handler()
        handler.on_exception(ConnectionException('refused'), None)
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_reconnect_collapses_an_attempt_that_has_not_started(self):
        self.cluster.executor.reset_mock()

        self.control_connection.reconnect()
        self.control_connection.reconnect()

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_reconnect_is_queued_again_once_the_attempt_starts(self):
        self.cluster.executor.reset_mock()
        self.control_connection.reconnect()
        self.control_connection._connection = None

        with patch.object(self.control_connection, '_reconnect_internal',
                          return_value=Mock()):
            self.control_connection._reconnect()

        self.control_connection.reconnect()

        assert self.cluster.executor.submit.call_args_list == [
            call(self.control_connection._reconnect),
            call(self.control_connection._reconnect)]

    def test_reconnect_does_not_clear_a_flag_a_newer_attempt_claimed(self):
        # _set_new_connection() drops the pending flag as the connection goes
        # in. If that connection errors before the installing attempt returns,
        # the reconnect() it triggers claims the flag, and the finishing
        # attempt must not clear it -- a further trigger would then queue a
        # second attempt that runs alongside the first.
        self.cluster.executor.reset_mock()
        self.control_connection._connection = None
        install = self.control_connection._set_new_connection

        def install_then_lose_the_connection(conn):
            install(conn)
            self.control_connection.reconnect()

        with patch.object(self.control_connection, '_reconnect_internal',
                          return_value=Mock()), \
                patch.object(self.control_connection, '_set_new_connection',
                             side_effect=install_then_lose_the_connection):
            self.control_connection._reconnect()

        assert self.control_connection._reconnect_pending
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

        self.control_connection.reconnect()

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_reconnect_collapses_an_attempt_while_one_is_running(self):
        # _reconnect_internal() walks the whole query plan and can outlast the
        # idle heartbeat, which calls reconnect() through return_connection().
        # Those calls must not queue a second attempt that would cancel the
        # first one's handler and restart its backoff.
        def reconnect_while_running():
            self.cluster.executor.reset_mock()
            self.control_connection.reconnect()
            raise NoHostAvailable('no host', {})

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=reconnect_while_running):
            self.control_connection._reconnect()

        self.cluster.executor.submit.assert_not_called()

    def test_reconnect_handler_owns_trigger_after_dns_failure(self):
        # A trigger arriving during an attempt must not queue duplicate work
        # when that attempt leaves a backoff handler owning future retries.
        def reconnect_while_running():
            self.cluster.executor.reset_mock()
            self.control_connection.reconnect()
            raise UnresolvableContactPoints({})

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=reconnect_while_running):
            self.control_connection._reconnect()

        self.cluster.executor.submit.assert_not_called()
        assert self.control_connection._reconnection_handler is not None
        assert not self.control_connection._reconnect_pending

    def test_reconnect_preserves_trigger_if_new_handler_already_exhausted(self):
        # A zero-delay finite handler can exhaust on another worker before the
        # attempt that started it reaches _finish_reconnect(). It no longer
        # owns retries at that point, so a trigger collapsed in between must
        # be submitted as follow-up work.
        self.cluster.reconnection_policy = ConstantReconnectionPolicy(
            0, max_attempts=1)
        self.connection.is_defunct = True

        def exhaust_then_trigger(_delay, run):
            run()
            self.control_connection.reconnect()

        self.cluster.scheduler.schedule.side_effect = exhaust_then_trigger
        self.cluster.executor.reset_mock()

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()

        assert self.control_connection._reconnection_handler is None
        assert self.control_connection._reconnect_pending
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_reconnect_retries_when_contact_points_cannot_resolve(self):
        # A DNS failure leaves no connection that can trigger a heartbeat, so
        # it must enter the normal reconnection backoff on its own.
        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=UnresolvableContactPoints({})):
            self.control_connection._reconnect()

        assert self.control_connection._reconnection_handler is not None

    def test_connect_does_not_raise_when_shutdown_beats_it(self):
        # shutdown() ran while _reconnect_internal() was connecting, so
        # _set_new_connection() declined to install anything.
        self.control_connection._connection = None
        self.cluster.protocol_version = 4

        def shut_down_and_connect():
            self.control_connection._is_shutdown = True
            return Mock()

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=shut_down_and_connect):
            self.control_connection.connect()

        assert self.control_connection._connection is None

    def test_reconnect_does_not_park_a_handler_after_another_attempt_won(self):
        # Two attempts can overlap: reconnect() only collapses ones that have
        # not started. If this one fails after the other installed a live
        # connection, a handler parked here would block every later
        # reconnect() for the whole backoff and then replace that connection.
        def lose_the_race():
            self.control_connection._connection_generation += 1
            raise NoHostAvailable('no host', {})

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=lose_the_race):
            self.control_connection._reconnect()

        assert self.control_connection._reconnection_handler is None

    def test_reconnect_parks_a_handler_when_no_attempt_won(self):
        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()

        assert self.control_connection._reconnection_handler is not None

    def test_reconnection_handler_closes_the_connection_it_cannot_hand_off(self):
        # The ControlConnection was collected while the handler was backing
        # off. run() has already marked the connection as handed off, so
        # nothing else is left to close it.
        handler = self._make_reconnection_handler()
        owner = Mock()
        handler.control_connection = weakref.proxy(owner)
        del owner
        gc.collect()
        conn = Mock()

        with patch.object(handler, 'try_reconnect', return_value=conn):
            handler.run()

        conn.close.assert_called_once_with()

    def test_reconnect_defers_to_a_handler_left_by_a_failed_attempt(self):
        self.cluster.executor.reset_mock()
        self.control_connection.reconnect()

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()

        handler = self.control_connection._reconnection_handler
        assert handler is not None

        self.control_connection.reconnect()

        # The handler installed by the failed attempt is retrying on its own
        # schedule; starting another attempt would cancel it and restart that
        # schedule from its initial delay.
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)
        assert self.control_connection._reconnection_handler is handler
        assert not handler._cancelled

    def test_reconnect_is_queued_again_after_a_handler_fails_to_start(self):
        # A handler that never got scheduled retries nothing, so it must not
        # be left in the slot for reconnect() to defer to forever.
        self.cluster.scheduler.schedule.side_effect = RuntimeError(
            'scheduler is shut down')

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            with self.assertRaises(RuntimeError):
                self.control_connection._reconnect()

        assert self.control_connection._reconnection_handler is None

        self.cluster.executor.reset_mock()
        self.control_connection.reconnect()

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_returning_a_defunct_connection_does_not_restart_the_backoff(self):
        # The heartbeat hands a defunct control connection back once per
        # idle_heartbeat_interval for as long as it stays defunct. Each of
        # those must leave the parked handler's schedule alone.
        self.cluster.executor.reset_mock()

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()

        handler = self.control_connection._reconnection_handler
        assert handler is not None
        self.cluster.executor.reset_mock()

        self.connection.is_defunct = True
        for _ in range(3):
            self.control_connection.return_connection(self.connection)

        self.cluster.executor.submit.assert_not_called()
        assert self.control_connection._reconnection_handler is handler
        assert not handler._cancelled

    def test_heartbeat_does_not_restart_an_exhausted_finite_schedule(self):
        self.cluster.reconnection_policy = ConstantReconnectionPolicy(
            0, max_attempts=1)
        self.connection.is_defunct = True

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()
            handler = self.control_connection._reconnection_handler
            assert handler is not None

            # Run the only scheduled retry. Its failure exhausts the finite
            # schedule and releases the handler slot.
            handler.run()

        assert self.control_connection._reconnection_handler is None
        self.cluster.executor.reset_mock()
        self.connection.is_defunct = True

        # ConnectionHeartbeat returns this same defunct connection on every
        # interval; none of those passes may create a new retry schedule.
        for _ in range(3):
            self.control_connection.return_connection(self.connection)

        self.cluster.executor.submit.assert_not_called()

    def test_final_handler_attempt_preserves_heartbeat_trigger(self):
        self.cluster.reconnection_policy = ConstantReconnectionPolicy(
            0, max_attempts=1)
        self.connection.is_defunct = True

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()

        handler = self.control_connection._reconnection_handler
        assert handler is not None
        self.cluster.executor.reset_mock()

        def fail_after_heartbeat():
            self.control_connection.return_connection(self.connection)
            raise NoHostAvailable('no host', {})

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=fail_after_heartbeat):
            # The only scheduled retry receives a heartbeat trigger while it
            # is running and then exhausts the finite schedule.
            handler.run()

        assert self.control_connection._reconnection_handler is None
        assert self.control_connection._reconnection_exhausted_connection \
            is None
        assert self.control_connection._reconnect_pending
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_nonfinal_handler_attempt_consumes_heartbeat_trigger(self):
        self.cluster.reconnection_policy = ConstantReconnectionPolicy(
            0, max_attempts=2)
        self.connection.is_defunct = True

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()

        handler = self.control_connection._reconnection_handler
        assert handler is not None
        self.cluster.executor.reset_mock()

        def fail_after_heartbeat():
            self.control_connection.return_connection(self.connection)
            raise NoHostAvailable('no host', {})

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=fail_after_heartbeat):
            # This retry still has another scheduled attempt to own the
            # heartbeat trigger, so it must not start a new schedule.
            handler.run()

        assert self.control_connection._reconnection_handler is handler
        self.cluster.executor.submit.assert_not_called()

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            handler.run()

        assert self.control_connection._reconnection_handler is None
        assert self.control_connection._reconnection_exhausted_connection \
            is self.connection
        self.cluster.executor.submit.assert_not_called()

    def test_heartbeat_does_not_restart_an_empty_reconnection_schedule(self):
        self.cluster.reconnection_policy = ExponentialReconnectionPolicy(
            1.0, 2.0, max_attempts=0)
        self.connection.is_defunct = True

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()

        assert self.control_connection._reconnection_handler is None
        assert self.control_connection._reconnection_exhausted_connection \
            is self.connection
        self.cluster.executor.reset_mock()

        # No retries means that recurring heartbeat returns must not turn the
        # empty schedule into one fresh immediate attempt per interval.
        for _ in range(3):
            self.control_connection.return_connection(self.connection)

        self.cluster.executor.submit.assert_not_called()

    def test_exhausted_replacement_does_not_suppress_a_later_failure(self):
        self.cluster.reconnection_policy = ConstantReconnectionPolicy(
            0, max_attempts=1)
        host = self.cluster.metadata.get_host_by_host_id('uuid1')

        # Removing the connected host starts a proactive replacement while
        # the existing control connection is still healthy.
        self.control_connection.on_remove(host)
        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()
            handler = self.control_connection._reconnection_handler
            assert handler is not None
            handler.run()

        assert self.control_connection._reconnection_handler is None
        assert self.control_connection._reconnection_exhausted_connection \
            is None

        # A later, independent failure of the old connection must get a new
        # retry schedule rather than being mistaken for the exhausted one.
        self.cluster.executor.reset_mock()
        self.connection.is_defunct = True
        self.control_connection.return_connection(self.connection)

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_failure_between_proactive_retries_starts_a_fresh_schedule(self):
        self.cluster.reconnection_policy = ConstantReconnectionPolicy(
            0, max_attempts=1)
        host = self.cluster.metadata.get_host_by_host_id('uuid1')

        # Start a proactive replacement while the existing control connection
        # is healthy, then park the handler between its scheduled attempts.
        self.control_connection.on_remove(host)
        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            self.control_connection._reconnect()

        handler = self.control_connection._reconnection_handler
        assert handler is not None
        assert handler._failed_connection is None
        self.cluster.executor.reset_mock()

        # With heartbeats disabled, this is the only failure notification the
        # connection supplies. The proactive handler must retain it until its
        # own finite schedule exhausts, then start a failure-owned schedule.
        self.connection.is_defunct = True
        self.control_connection.return_connection(self.connection)
        self.cluster.executor.submit.assert_not_called()

        with patch.object(self.control_connection, '_reconnect_internal',
                          side_effect=NoHostAvailable('no host', {})):
            handler.run()

        assert self.control_connection._reconnection_handler is None
        assert self.control_connection._reconnect_pending
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_reconnect_is_queued_again_after_a_rejected_submission(self):
        self.cluster.executor.reset_mock()
        self.cluster.is_shutdown = True
        self.addCleanup(setattr, self.cluster, 'is_shutdown', False)

        self.control_connection.reconnect()

        self.cluster.executor.submit.assert_not_called()

        self.cluster.is_shutdown = False
        self.control_connection.reconnect()

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_reconnect_is_queued_again_after_a_raising_submission(self):
        self.cluster.executor.reset_mock()
        self.cluster.executor.submit.side_effect = RuntimeError(
            'cannot schedule new futures after shutdown')

        with self.assertRaises(RuntimeError):
            self.control_connection.reconnect()

        self.cluster.executor.submit.side_effect = None
        self.cluster.executor.reset_mock()

        self.control_connection.reconnect()

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_defunct_control_reconnects_when_down_dispatch_is_dropped(self):
        # on_down() marks the host down but the executor refuses the DOWN
        # callback, so nothing else will reconnect the control connection.
        host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host.set_up()
        self._use_cluster_down_handling()
        self.cluster.on_down_potentially_blocking = \
            Cluster.on_down_potentially_blocking.__get__(self.cluster)
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()
        self.cluster.executor.submit.side_effect = [
            RuntimeError('cannot schedule new futures'), Mock()]

        self.control_connection._signal_error()

        assert host.is_up is False
        assert self.cluster.executor.submit.call_args_list[-1] == call(
            self.control_connection._reconnect)

    def test_refresh_network_local_preserves_known_unix_endpoint(self):
        maintenance_endpoint, local_host = \
            self._discover_local_host_over_unix()
        host_index = {local_host: object()}

        self._refresh_control_connection_over_network()

        assert self.cluster.metadata.get_host_by_host_id('uuid1') is local_host
        assert local_host.endpoint == maintenance_endpoint
        assert host_index[local_host] is not None
        assert Cluster.get_control_connection_host(self.cluster) is local_host

        # A DOWN transition discounted because a usable session pool remains
        # queues no control on_down callback, so the reconnect is direct.
        self._discount_down_for(local_host)
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        assert local_host.is_up is True
        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_unix_signal_error_reconnects_if_down_notification_suppressed(self):
        _, local_host = self._discover_local_host_over_unix()
        session = self._discount_down_for(local_host)
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        # The discount is what suppresses the notification here, which means
        # the host really was resolved from the Unix endpoint: an unresolved
        # host would reconnect without ever consulting a session pool.
        session.get_pool_state.assert_called_once_with()
        assert local_host.is_up is True
        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_tcp_route_mismatch_reconnects_if_down_notification_suppressed(self):
        self.control_connection.refresh_node_list_and_token_map()
        local_host = self.cluster.metadata.get_host_by_host_id('uuid1')
        local_host.set_up()
        self.connection.endpoint = DefaultEndPoint('192.168.1.0', 19042)
        self.connection.original_endpoint = local_host.endpoint
        session = self._discount_down_for(local_host)
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        # As above: reaching the discount proves the host was resolved over
        # the original endpoint despite the connection's route mismatch.
        session.get_pool_state.assert_called_once_with()
        assert local_host.is_up is True
        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_route_mismatch_signal_error_waits_for_queued_down_reconnect(self):
        _, local_host = self._discover_local_host_over_unix()
        self._refresh_control_connection_over_network()
        connection_error = ConnectionException('control connection failed')
        self.connection.is_defunct = True
        self.connection.last_error = connection_error
        down_notifications = []

        def transition_host_down(host, *_args, **_kwargs):
            host.set_down()
            down_notifications.append(host)
            return True

        self.cluster.signal_connection_failure = Mock(
            side_effect=transition_host_down)
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.signal_connection_failure.assert_called_once_with(
            local_host, connection_error, is_host_addition=False)
        self.cluster.executor.submit.assert_not_called()

        self.control_connection.on_down(down_notifications.pop())

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_route_mismatch_signal_error_reconnects_if_host_already_down(self):
        _, local_host = self._discover_local_host_over_unix()
        self._refresh_control_connection_over_network()
        local_host.set_down()
        self._use_cluster_down_handling()
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_route_mismatch_signal_error_reconnects_if_host_reconnecting(self):
        _, local_host = self._discover_local_host_over_unix()
        self._refresh_control_connection_over_network()
        local_host.set_down()
        local_host.get_and_set_reconnection_handler(Mock())
        self._use_cluster_down_handling()
        self.connection.is_defunct = True
        self.connection.last_error = ConnectionException(
            'control connection failed')
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        assert local_host.is_up is False
        self.cluster.on_down_potentially_blocking.assert_not_called()
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_remove_matches_control_connection_by_host_id(self):
        maintenance_endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        self._forget_local_host()
        self.connection.endpoint = maintenance_endpoint
        self.connection.original_endpoint = maintenance_endpoint
        self.control_connection.refresh_node_list_and_token_map()
        local_host = self.cluster.metadata.get_host_by_host_id('uuid1')

        self.connection.endpoint = DefaultEndPoint('192.168.1.0')
        self.cluster.metadata.hosts.pop('uuid1')
        self.cluster.metadata._host_id_by_endpoint.pop(maintenance_endpoint)
        self.cluster.executor.reset_mock()

        self.control_connection.on_remove(local_host)

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_down_matches_replacement_at_stale_control_endpoint(self):
        self.control_connection.refresh_node_list_and_token_map()
        old_host = self.cluster.metadata.get_host_by_host_id('uuid1')
        endpoint = old_host.endpoint
        self.cluster.metadata.hosts.pop('uuid1')

        replacement_host = Host(
            endpoint, SimpleConvictionPolicy, host_id='replacement-id')
        replacement_host.set_up()
        self.cluster.metadata.hosts['replacement-id'] = replacement_host
        self.cluster.metadata._host_id_by_endpoint[endpoint] = \
            'replacement-id'

        connection_error = ConnectionException('old control failed')
        self.connection.is_defunct = True
        self.connection.last_error = connection_error
        self.cluster.signal_connection_failure = Mock(return_value=True)
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.signal_connection_failure.assert_called_once_with(
            replacement_host, connection_error, is_host_addition=False)
        self.cluster.executor.submit.assert_not_called()

        self.control_connection.on_down(replacement_host)

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_refresh_unix_local_preserves_known_network_endpoint(self):
        maintenance_endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        local_host = self.cluster.metadata.get_host_by_host_id('uuid1')
        host_index = {local_host: object()}
        self.connection.endpoint = maintenance_endpoint
        self.connection.original_endpoint = maintenance_endpoint

        self.control_connection.refresh_node_list_and_token_map()

        assert self.cluster.metadata.get_host_by_host_id('uuid1') is local_host
        assert local_host.endpoint == DefaultEndPoint('192.168.1.0')
        assert host_index[local_host] is not None

    def test_refresh_nodes_and_tokens_with_invalid_peers(self):
        def refresh_and_validate_added_hosts():
            self.connection.wait_for_responses = Mock(return_value=_node_meta_results(
                self.connection.local_results, self.connection.peer_results))
            self.control_connection.refresh_node_list_and_token_map()
            assert 1 == len(self.cluster.added_hosts)  # only one valid peer found

        # peersV1
        del self.connection.peer_results[:]
        self.connection.peer_results.extend([
            ["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens", "host_id"],
            [["192.168.1.3", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], 'uuid6'],
             # all others are invalid
             [None, None, "a", "dc1", "rack1", ["1", "101", "201"], 'uuid1'],
             ["192.168.1.7", "10.0.0.1", "a", None, "rack1", ["1", "101", "201"], 'uuid2'],
             ["192.168.1.6", "10.0.0.1", "a", "dc1", None, ["1", "101", "201"], 'uuid3'],
             ["192.168.1.5", "10.0.0.1", "a", "dc1", "rack1", None, 'uuid4'],
             ["192.168.1.4", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], None]]])
        refresh_and_validate_added_hosts()

        # peersV2
        del self.cluster.added_hosts[:]
        del self.connection.peer_results[:]
        self.connection.peer_results.extend([
            ["native_address", "native_port", "peer", "peer_port", "schema_version", "data_center", "rack", "tokens", "host_id"],
            [["192.168.1.4", 9042, "10.0.0.1", 7042, "a", "dc1", "rack1", ["1", "101", "201"], "uuid7"],
             # all others are invalid
             [None, 9042, None, 7040, "a", "dc1", "rack1", ["2", "102", "202"], "uuid2"],
             ["192.168.1.5", 9042, "10.0.0.2", 7040, "a", None, "rack1", ["2", "102", "202"], "uuid2"],
             ["192.168.1.5", 9042, "10.0.0.2", 7040, "a", "dc1", None, ["2", "102", "202"], "uuid2"],
             ["192.168.1.5", 9042, "10.0.0.2", 7040, "a", "dc1", "rack1", None, "uuid2"],
             ["192.168.1.5", 9042, "10.0.0.2", 7040, "a", "dc1", "rack1", ["2", "102", "202"], None]]])
        refresh_and_validate_added_hosts()

    def test_change_ip(self):
        """
        Tests node IPs are updated while the nodes themselves are not
        removed or added when their IPs change (the node look up is based on
        host id).
        """
        del self.cluster.added_hosts[:]
        del self.connection.peer_results[:]

        self.connection.peer_results.extend([
            ["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens", "host_id"],
            [["192.168.1.5", "10.0.0.5", "a", "dc1", "rack1", ["2", "102", "202"], 'uuid2'],
             ["192.168.1.6", "10.0.0.6", "a", "dc1", "rack1", ["3", "103", "203"], 'uuid3']]])
        self.connection.wait_for_responses = Mock(
            return_value=_node_meta_results(
                self.connection.local_results, self.connection.peer_results))
        self.control_connection.refresh_node_list_and_token_map()
        # all peers are updated
        assert 0 == len(self.cluster.added_hosts)

        assert self.cluster.metadata.get_host('192.168.1.5')
        assert self.cluster.metadata.get_host('192.168.1.6')

        assert 3 == len(self.cluster.metadata.all_hosts())


    def test_refresh_nodes_and_tokens_uses_preloaded_results_if_given(self):
        """
        refresh_nodes_and_tokens uses preloaded results if given for shared table queries
        """
        preloaded_results = self._matching_schema_preloaded_results
        self.control_connection._refresh_node_list_and_token_map(self.connection, preloaded_results=preloaded_results)
        meta = self.cluster.metadata
        assert meta.partitioner == 'Murmur3Partitioner'
        assert meta.cluster_name == 'foocluster'

        # check token map
        assert sorted(meta.all_hosts()) == sorted(meta.token_map.keys())
        for token_list in meta.token_map.values():
            assert 3 == len(token_list)

        # check datacenter/rack
        for host in meta.all_hosts():
            assert host.datacenter == "dc1"
            assert host.rack == "rack1"

        # the connection should not have made any queries if given preloaded results
        assert self.connection.wait_for_responses.call_count == 0

    def test_refresh_nodes_and_tokens_no_partitioner(self):
        """
        Test handling of an unknown partitioner.
        """
        # set the partitioner column to None
        self.connection.local_results[1][0][5] = None
        self.control_connection.refresh_node_list_and_token_map()
        meta = self.cluster.metadata
        assert meta.partitioner == None
        assert meta.token_map == {}

    def test_refresh_nodes_and_tokens_add_host(self):
        self.connection.peer_results[1].append(
            ["192.168.1.3", "10.0.0.3", "a", "dc1", "rack1", ["3", "103", "203"], "uuid4"]
        )
        self.cluster.scheduler.schedule = lambda delay, f, *args, **kwargs: f(*args, **kwargs)
        self.control_connection.refresh_node_list_and_token_map()
        assert 1 == len(self.cluster.added_hosts)
        assert self.cluster.added_hosts[0].address == "192.168.1.3"
        assert self.cluster.added_hosts[0].datacenter == "dc1"
        assert self.cluster.added_hosts[0].rack == "rack1"
        assert self.cluster.added_hosts[0].host_id == "uuid4"

    def test_refresh_nodes_and_tokens_remove_host(self):
        del self.connection.peer_results[1][1]
        self.control_connection.refresh_node_list_and_token_map()
        assert 1 == len(self.cluster.metadata.removed_hosts)
        assert self.cluster.metadata.removed_hosts[0].address == "192.168.1.2"

    def test_refresh_nodes_and_tokens_timeout(self):

        def bad_wait_for_responses(*args, **kwargs):
            assert kwargs['timeout'] == self.control_connection._timeout
            raise OperationTimedOut()

        self.connection.wait_for_responses = bad_wait_for_responses
        self.control_connection.refresh_node_list_and_token_map()
        self.cluster.executor.submit.assert_called_with(self.control_connection._reconnect)

    @patch('cassandra.cluster.warn')
    def test_refresh_schema_timeout(self, mocked_warn):

        def bad_wait_for_responses(*args, **kwargs):
            self.time.sleep(kwargs['timeout'])
            raise OperationTimedOut()

        self.connection.wait_for_responses = Mock(side_effect=bad_wait_for_responses)
        self.control_connection.refresh_schema()
        assert self.connection.wait_for_responses.call_count == self.cluster.max_schema_agreement_wait / self.control_connection._timeout
        assert self.connection.wait_for_responses.call_args[1]['timeout'] == self.control_connection._timeout
        mocked_warn.assert_not_called()

    def test_handle_topology_change(self):
        event = {
            'change_type': 'NEW_NODE',
            'address': ('1.2.3.4', 9000)
        }
        self.cluster.scheduler.reset_mock()
        self.control_connection._handle_topology_change(event)

        self.cluster.scheduler.schedule_unique.assert_called_once_with(ANY, self.control_connection._refresh_nodes_if_not_up, None)

        event = {
            'change_type': 'REMOVED_NODE',
            'address': ('1.2.3.4', 9000)
        }
        self.cluster.scheduler.reset_mock()
        self.control_connection._handle_topology_change(event)
        self.cluster.scheduler.schedule_unique.assert_called_once_with(ANY, self.cluster.remove_host, None)

        event = {
            'change_type': 'MOVED_NODE',
            'address': ('1.2.3.4', 9000)
        }
        self.cluster.scheduler.reset_mock()
        self.control_connection._handle_topology_change(event)
        self.cluster.scheduler.schedule_unique.assert_called_once_with(ANY, self.control_connection._refresh_nodes_if_not_up, None)

    def test_handle_status_change(self):
        event = {
            'change_type': 'UP',
            'address': ('1.2.3.4', 9000)
        }
        self.cluster.scheduler.reset_mock()
        self.control_connection._handle_status_change(event)
        self.cluster.scheduler.schedule_unique.assert_called_once_with(ANY, self.control_connection.refresh_node_list_and_token_map)

        # do the same with a known Host
        event = {
            'change_type': 'UP',
            'address': ('192.168.1.0', 9042)
        }
        self.cluster.scheduler.reset_mock()
        self.control_connection._handle_status_change(event)
        host = self.cluster.metadata.get_host(DefaultEndPoint('192.168.1.0'))
        self.cluster.scheduler.schedule_unique.assert_called_once_with(ANY, self.cluster.on_up, host)

        self.cluster.scheduler.schedule.reset_mock()
        event = {
            'change_type': 'DOWN',
            'address': ('1.2.3.4', 9000)
        }
        self.control_connection._handle_status_change(event)
        assert not self.cluster.scheduler.schedule.called

        # do the same with a known Host
        event = {
            'change_type': 'DOWN',
            'address': ('192.168.1.0', 9000)
        }
        self.control_connection._handle_status_change(event)
        host = self.cluster.metadata.get_host(DefaultEndPoint('192.168.1.0'))
        assert host is self.cluster.down_host

    def test_handle_schema_change(self):

        change_types = [getattr(SchemaChangeType, attr) for attr in vars(SchemaChangeType) if attr[0] != '_']
        for change_type in change_types:
            event = {
                'target_type': SchemaTargetType.TABLE,
                'change_type': change_type,
                'keyspace': 'ks1',
                'table': 'table1'
            }
            self.cluster.scheduler.reset_mock()
            self.control_connection._handle_schema_change(event)
            self.cluster.scheduler.schedule_unique.assert_called_once_with(ANY, self.control_connection.refresh_schema, **event)

            self.cluster.scheduler.reset_mock()
            event['target_type'] = SchemaTargetType.KEYSPACE
            del event['table']
            self.control_connection._handle_schema_change(event)
            self.cluster.scheduler.schedule_unique.assert_called_once_with(ANY, self.control_connection.refresh_schema, **event)

    def test_refresh_disabled(self):
        cluster = MockCluster()

        schema_event = {
            'target_type': SchemaTargetType.TABLE,
            'change_type': SchemaChangeType.CREATED,
            'keyspace': 'ks1',
            'table': 'table1'
        }

        status_event = {
            'change_type': 'UP',
            'address': ('1.2.3.4', 9000)
        }

        topo_event = {
            'change_type': 'MOVED_NODE',
            'address': ('1.2.3.4', 9000)
        }

        cc_no_schema_refresh = ControlConnection(cluster, 1, -1, 0, 0)
        cluster.scheduler.reset_mock()

        # no call on schema refresh
        cc_no_schema_refresh._handle_schema_change(schema_event)
        assert not cluster.scheduler.schedule.called
        assert not cluster.scheduler.schedule_unique.called

        # topo and status changes as normal
        cc_no_schema_refresh._handle_status_change(status_event)
        cc_no_schema_refresh._handle_topology_change(topo_event)
        cluster.scheduler.schedule_unique.assert_has_calls([call(ANY, cc_no_schema_refresh.refresh_node_list_and_token_map),
                                                            call(ANY, cc_no_schema_refresh._refresh_nodes_if_not_up, None)])

        cc_no_topo_refresh = ControlConnection(cluster, 1, 0, -1, 0)
        cluster.scheduler.reset_mock()

        # no call on topo refresh
        cc_no_topo_refresh._handle_topology_change(topo_event)
        assert not cluster.scheduler.schedule.called
        assert not cluster.scheduler.schedule_unique.called

        # schema and status change refresh as normal
        cc_no_topo_refresh._handle_status_change(status_event)
        cc_no_topo_refresh._handle_schema_change(schema_event)
        cluster.scheduler.schedule_unique.assert_has_calls([call(ANY, cc_no_topo_refresh.refresh_node_list_and_token_map),
                                                            call(0.0, cc_no_topo_refresh.refresh_schema,
                                                                 **schema_event)])

    def test_refresh_nodes_and_tokens_add_host_detects_port(self):
        del self.connection.peer_results[:]
        self.connection.peer_results.extend(self.connection.peer_results_v2)
        self.connection.peer_results[1].append(
            ["192.168.1.3", 555, "10.0.0.3", 666, "a", "dc1", "rack1", ["3", "103", "203"], "uuid4"]
        )
        self.connection.wait_for_responses = Mock(return_value=_node_meta_results(
            self.connection.local_results, self.connection.peer_results))
        self.cluster.scheduler.schedule = lambda delay, f, *args, **kwargs: f(*args, **kwargs)
        self.control_connection.refresh_node_list_and_token_map()
        assert 1 == len(self.cluster.added_hosts)
        assert self.cluster.added_hosts[0].endpoint.address == "192.168.1.3"
        assert self.cluster.added_hosts[0].endpoint.port == 555
        assert self.cluster.added_hosts[0].broadcast_rpc_address == "192.168.1.3"
        assert self.cluster.added_hosts[0].broadcast_rpc_port == 555
        assert self.cluster.added_hosts[0].broadcast_address == "10.0.0.3"
        assert self.cluster.added_hosts[0].broadcast_port == 666
        assert self.cluster.added_hosts[0].datacenter == "dc1"
        assert self.cluster.added_hosts[0].rack == "rack1"

    def test_refresh_nodes_and_tokens_add_host_detects_invalid_port(self):
        del self.connection.peer_results[:]
        self.connection.peer_results.extend(self.connection.peer_results_v2)
        self.connection.peer_results[1].append(
            ["192.168.1.3", -1, "10.0.0.3", 0, "a", "dc1", "rack1", ["3", "103", "203"], "uuid4"]
        )
        self.connection.wait_for_responses = Mock(return_value=_node_meta_results(
            self.connection.local_results, self.connection.peer_results))
        self.cluster.scheduler.schedule = lambda delay, f, *args, **kwargs: f(*args, **kwargs)
        self.control_connection.refresh_node_list_and_token_map()
        assert 1 == len(self.cluster.added_hosts)
        assert self.cluster.added_hosts[0].endpoint.address == "192.168.1.3"
        assert self.cluster.added_hosts[0].endpoint.port == 9042  # fallback default
        assert self.cluster.added_hosts[0].broadcast_rpc_address == "192.168.1.3"
        assert self.cluster.added_hosts[0].broadcast_rpc_port == None
        assert self.cluster.added_hosts[0].broadcast_address == "10.0.0.3"
        assert self.cluster.added_hosts[0].broadcast_port == None
        assert self.cluster.added_hosts[0].datacenter == "dc1"
        assert self.cluster.added_hosts[0].rack == "rack1"


class EventTimingTest(unittest.TestCase):
    """
    A simple test to validate that event scheduling happens in order
    Added for PYTHON-358
    """
    def setUp(self):
        self.cluster = MockCluster()
        self.connection = MockConnection()
        self.time = FakeTime()

        # Use 2 for the schema_event_refresh_window which is what we would normally default to.
        self.control_connection = ControlConnection(self.cluster, 1, 2, 0, 0)
        self.control_connection._connection = self.connection
        self.control_connection._time = self.time

    def test_event_delay_timing(self):
        """
        Submits a wide array of events make sure that each is scheduled to occur in the order they were received
        """
        prior_delay = 0
        for _ in range(100):
            for change_type in ('CREATED', 'DROPPED', 'UPDATED'):
                event = {
                    'change_type': change_type,
                    'keyspace': '1',
                    'table': 'table1'
                }
                # This is to increment the fake time, we don't actually sleep here.
                self.time.sleep(.001)
                self.cluster.scheduler.reset_mock()
                self.control_connection._handle_schema_change(event)
                self.cluster.scheduler.mock_calls
                # Grabs the delay parameter from the scheduler invocation
                current_delay = self.cluster.scheduler.mock_calls[0][1][0]
                assert prior_delay < current_delay
                prior_delay = current_delay
