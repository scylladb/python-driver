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

import unittest
import uuid

from concurrent.futures import Future, ThreadPoolExecutor
from unittest.mock import Mock, ANY, call, patch

from cassandra import OperationTimedOut, SchemaTargetType, SchemaChangeType
from cassandra.protocol import ResultMessage, RESULT_KIND_ROWS
from cassandra.cluster import (Cluster, ControlConnection, Session, _Scheduler,
                               ProfileManager, EXEC_PROFILE_DEFAULT,
                               ExecutionProfile)
from cassandra.pool import Host
from cassandra.connection import (ConnectionException, EndPoint,
                                  DefaultEndPoint, DefaultEndPointFactory,
                                  UnixSocketEndPoint)
from cassandra.policies import (DCAwareRoundRobinPolicy, HostDistance,
                                SimpleConvictionPolicy, RoundRobinPolicy,
                                ConstantReconnectionPolicy, IdentityTranslator)

PEER_IP = "foobar"

HOST_ID_1 = uuid.UUID(int=1)
HOST_ID_2 = uuid.UUID(int=2)
HOST_ID_3 = uuid.UUID(int=3)
HOST_ID_4 = uuid.UUID(int=4)
HOST_ID_6 = uuid.UUID(int=6)
HOST_ID_7 = uuid.UUID(int=7)


class MockMetadata(object):

    def __init__(self):
        self.hosts = {
            HOST_ID_1: Host(endpoint=DefaultEndPoint("192.168.1.0"), conviction_policy_factory=SimpleConvictionPolicy, host_id=HOST_ID_1),
            HOST_ID_2: Host(endpoint=DefaultEndPoint("192.168.1.1"), conviction_policy_factory=SimpleConvictionPolicy, host_id=HOST_ID_2),
            HOST_ID_3: Host(endpoint=DefaultEndPoint("192.168.1.2"), conviction_policy_factory=SimpleConvictionPolicy, host_id=HOST_ID_3)
        }
        self._host_id_by_endpoint = {
            DefaultEndPoint("192.168.1.0"): HOST_ID_1,
            DefaultEndPoint("192.168.1.1"): HOST_ID_2,
            DefaultEndPoint("192.168.1.2"): HOST_ID_3,
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

    def add_host(self, endpoint, datacenter, rack, signal=False,
                 refresh_nodes=True, host_id=None,
                 reconcile_pools_on_failure=False):
        host = Host(endpoint, SimpleConvictionPolicy, datacenter, rack, host_id=host_id)
        host, _ = self.metadata.add_or_return_host(host)
        self.added_hosts.append(host)
        return host, True

    def remove_host(self, host, trigger_reconciliation=True):
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
            ["rpc_address", "schema_version", "cluster_name", "data_center", "rack", "partitioner", "release_version", "tokens", "host_id"],
            [["192.168.1.0", "a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", "2.2.0", ["0", "100", "200"], HOST_ID_1]]
        ]

        self.peer_results = [
            ["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens", "host_id"],
            [["192.168.1.1", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], HOST_ID_2],
             ["192.168.1.2", "10.0.0.2", "a", "dc1", "rack1", ["2", "102", "202"], HOST_ID_3]]
        ]

        self.peer_results_v2 = [
            ["native_address",  "native_port", "peer", "peer_port", "schema_version", "data_center", "rack", "tokens", "host_id"],
            [["192.168.1.1", 9042, "10.0.0.1", 7042, "a", "dc1", "rack1", ["1", "101", "201"], HOST_ID_2],
             ["192.168.1.2", 9042, "10.0.0.2", 7040, "a", "dc1", "rack1", ["2", "102", "202"], HOST_ID_3]]
        ]
        self.wait_for_responses = Mock(return_value=_node_meta_results(self.local_results, self.peer_results))

    def close(self):
        self.is_closed = True


class FakeTime(object):

    def __init__(self):
        self.clock = 0

    def time(self):
        return self.clock

    def sleep(self, amount):
        self.clock += amount


class ControlConnectionTest(unittest.TestCase):

    _matching_schema_preloaded_results = _node_meta_results(
        local_results=(["rpc_address", "schema_version", "cluster_name", "data_center", "rack", "partitioner", "release_version", "tokens", "host_id"],
                       [["192.168.1.0", "a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", "2.2.0", ["0", "100", "200"], HOST_ID_1]]),
        peer_results=(["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens", "host_id"],
                      [["192.168.1.1", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], HOST_ID_2],
                       ["192.168.1.2", "10.0.0.2", "a", "dc1", "rack1", ["2", "102", "202"], HOST_ID_3]]))

    _nonmatching_schema_preloaded_results = _node_meta_results(
        local_results=(["rpc_address", "schema_version", "cluster_name", "data_center", "rack", "partitioner", "release_version", "tokens", "host_id"],
                       [["192.168.1.0", "a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", "2.2.0", ["0", "100", "200"], HOST_ID_1]]),
        peer_results=(["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens", "host_id"],
                      [["192.168.1.1", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], HOST_ID_2],
                       ["192.168.1.2", "10.0.0.2", "b", "dc1", "rack1", ["2", "102", "202"], HOST_ID_3]]))

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
        self.cluster.metadata.hosts.pop(HOST_ID_1)

    def _discover_local_host_over_unix(self):
        maintenance_endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        self._forget_local_host()
        self.connection.endpoint = maintenance_endpoint
        self.connection.original_endpoint = maintenance_endpoint
        self.control_connection.refresh_node_list_and_token_map()
        local_host = self.cluster.metadata.get_host_by_host_id(HOST_ID_1)
        local_host.set_up()
        return maintenance_endpoint, local_host

    def _refresh_control_connection_over_network(self):
        self.connection.endpoint = DefaultEndPoint('192.168.1.0')
        self.connection.original_endpoint = self.connection.endpoint
        self.control_connection.refresh_node_list_and_token_map()

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
            ["0.0.0.0", PEER_IP, "b", "dc1", "rack1", ["3", "103", "203"], HOST_ID_6]
        )
        host = Host(DefaultEndPoint("0.0.0.0"), SimpleConvictionPolicy, host_id=HOST_ID_6)
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

    def test_refresh_uses_control_endpoint_for_local_unix_host(self):
        maintenance_endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        self._forget_local_host()
        self.connection.endpoint = maintenance_endpoint
        self.connection.original_endpoint = maintenance_endpoint

        self.control_connection.refresh_node_list_and_token_map()

        local_host = self.cluster.metadata.get_host_by_host_id(HOST_ID_1)
        assert local_host.endpoint == maintenance_endpoint
        assert local_host.broadcast_rpc_address == '192.168.1.0'
        peer_host = self.cluster.metadata.get_host_by_host_id(HOST_ID_2)
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
            ['4', '104', '204'], HOST_ID_4])

        self.control_connection.refresh_node_list_and_token_map()

        assert self.cluster.metadata.get_host_by_host_id(HOST_ID_4) is None

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
              HOST_ID_2]])
        peer_results = (
            self.connection.peer_results[0],
            [['192.168.1.0', '10.0.0.1', 'a', 'dc1', 'rack1',
              ['0', '100', '200'], HOST_ID_1],
             ['192.168.1.2', '10.0.0.2', 'a', 'dc1', 'rack1',
              ['2', '102', '202'], HOST_ID_3]])
        self.connection.endpoint = DefaultEndPoint('192.168.1.1')
        self.connection.original_endpoint = self.connection.endpoint

        self.control_connection._refresh_node_list_and_token_map(
            self.connection,
            preloaded_results=_node_meta_results(local_results, peer_results))

        local_host = self.cluster.metadata.get_host_by_host_id(HOST_ID_1)
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

        local_host = self.cluster.metadata.get_host_by_host_id(HOST_ID_1)
        assert local_host.endpoint == DefaultEndPoint('192.168.1.0')

    def test_schema_query_uses_shard_aware_connection_original_endpoint(self):
        host = self.cluster.metadata.get_host_by_host_id(HOST_ID_1)
        self.connection.endpoint = DefaultEndPoint('192.168.1.0', 19042)
        self.connection.original_endpoint = host.endpoint
        self.control_connection._uses_peers_v2 = False

        query = self.control_connection._get_peers_query(
            self.control_connection.PeersQueryType.PEERS_SCHEMA,
            self.connection)

        assert query == self.control_connection._SELECT_SCHEMA_PEERS_TEMPLATE \
            .format(nt_col_name='rpc_address')

    def test_refresh_network_local_preserves_known_unix_endpoint(self):
        maintenance_endpoint, local_host = \
            self._discover_local_host_over_unix()
        host_index = {local_host: object()}

        self._refresh_control_connection_over_network()

        assert self.cluster.metadata.get_host_by_host_id(HOST_ID_1) is local_host
        assert local_host.endpoint == maintenance_endpoint
        assert host_index[local_host] is not None
        assert Cluster.get_control_connection_host(self.cluster) is local_host

        connection_error = ConnectionException('control connection failed')
        self.connection.is_defunct = True
        self.connection.last_error = connection_error
        # Model a conviction whose DOWN transition is discounted because a
        # usable session pool remains: no control on_down callback is queued.
        self.cluster.signal_connection_failure = Mock(return_value=True)
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.signal_connection_failure.assert_called_once_with(
            local_host, connection_error, is_host_addition=False)
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_unix_signal_error_reconnects_if_down_notification_suppressed(self):
        _, local_host = self._discover_local_host_over_unix()
        connection_error = ConnectionException('control connection failed')
        self.connection.is_defunct = True
        self.connection.last_error = connection_error
        self.cluster.signal_connection_failure = Mock(return_value=True)
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.signal_connection_failure.assert_called_once_with(
            local_host, connection_error, is_host_addition=False)
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_tcp_route_mismatch_reconnects_if_down_notification_suppressed(self):
        self.control_connection.refresh_node_list_and_token_map()
        local_host = self.cluster.metadata.get_host_by_host_id(HOST_ID_1)
        self.connection.endpoint = DefaultEndPoint('192.168.1.0', 19042)
        self.connection.original_endpoint = local_host.endpoint
        connection_error = ConnectionException('control connection failed')
        self.connection.is_defunct = True
        self.connection.last_error = connection_error
        self.cluster.signal_connection_failure = Mock(return_value=True)
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.signal_connection_failure.assert_called_once_with(
            local_host, connection_error, is_host_addition=False)
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
        connection_error = ConnectionException('control connection failed')
        self.connection.is_defunct = True
        self.connection.last_error = connection_error
        self.cluster.signal_connection_failure = Mock(return_value=True)
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.signal_connection_failure.assert_called_once_with(
            local_host, connection_error, is_host_addition=False)
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_route_mismatch_signal_error_reconnects_if_host_reconnecting(self):
        _, local_host = self._discover_local_host_over_unix()
        self._refresh_control_connection_over_network()
        local_host.get_and_set_reconnection_handler(Mock())
        connection_error = ConnectionException('control connection failed')
        self.connection.is_defunct = True
        self.connection.last_error = connection_error

        def transition_without_notification(host, *_args, **_kwargs):
            host.set_down()
            return True

        self.cluster.signal_connection_failure = Mock(
            side_effect=transition_without_notification)
        self.cluster.executor.reset_mock()

        self.control_connection._signal_error()

        self.cluster.signal_connection_failure.assert_called_once_with(
            local_host, connection_error, is_host_addition=False)
        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_remove_matches_control_connection_by_host_id(self):
        maintenance_endpoint = UnixSocketEndPoint('/tmp/maintenance.sock')
        self._forget_local_host()
        self.connection.endpoint = maintenance_endpoint
        self.connection.original_endpoint = maintenance_endpoint
        self.control_connection.refresh_node_list_and_token_map()
        local_host = self.cluster.metadata.get_host_by_host_id(HOST_ID_1)

        self.connection.endpoint = DefaultEndPoint('192.168.1.0')
        self.cluster.metadata.hosts.pop(HOST_ID_1)
        self.cluster.metadata._host_id_by_endpoint.pop(maintenance_endpoint)
        self.cluster.executor.reset_mock()

        self.control_connection.on_remove(local_host)

        self.cluster.executor.submit.assert_called_once_with(
            self.control_connection._reconnect)

    def test_down_matches_replacement_at_stale_control_endpoint(self):
        self.control_connection.refresh_node_list_and_token_map()
        old_host = self.cluster.metadata.get_host_by_host_id(HOST_ID_1)
        endpoint = old_host.endpoint
        self.cluster.metadata.hosts.pop(HOST_ID_1)

        replacement_host = Host(
            endpoint, SimpleConvictionPolicy, host_id=HOST_ID_4)
        replacement_host.set_up()
        self.cluster.metadata.hosts[HOST_ID_4] = replacement_host
        self.cluster.metadata._host_id_by_endpoint[endpoint] = \
            HOST_ID_4

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
        local_host = self.cluster.metadata.get_host_by_host_id(HOST_ID_1)
        host_index = {local_host: object()}
        self.connection.endpoint = maintenance_endpoint
        self.connection.original_endpoint = maintenance_endpoint

        self.control_connection.refresh_node_list_and_token_map()

        assert self.cluster.metadata.get_host_by_host_id(HOST_ID_1) is local_host
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
             [["192.168.1.3", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], HOST_ID_6],
             # all others are invalid
             [None, None, "a", "dc1", "rack1", ["1", "101", "201"], HOST_ID_1],
             ["192.168.1.7", "10.0.0.1", "a", None, "rack1", ["1", "101", "201"], HOST_ID_2],
             ["192.168.1.6", "10.0.0.1", "a", "dc1", None, ["1", "101", "201"], HOST_ID_3],
             ["192.168.1.5", "10.0.0.1", "a", "dc1", "rack1", None, HOST_ID_4],
             ["192.168.1.8", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], "not-a-uuid"],
             ["192.168.1.9", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], uuid.UUID(int=0)],
             ["192.168.1.4", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], None]]])
        refresh_and_validate_added_hosts()

        # peersV2
        del self.cluster.added_hosts[:]
        del self.connection.peer_results[:]
        self.connection.peer_results.extend([
            ["native_address", "native_port", "peer", "peer_port", "schema_version", "data_center", "rack", "tokens", "host_id"],
             [["192.168.1.4", 9042, "10.0.0.1", 7042, "a", "dc1", "rack1", ["1", "101", "201"], HOST_ID_7],
             # all others are invalid
             [None, 9042, None, 7040, "a", "dc1", "rack1", ["2", "102", "202"], HOST_ID_2],
             ["192.168.1.5", 9042, "10.0.0.2", 7040, "a", None, "rack1", ["2", "102", "202"], HOST_ID_2],
             ["192.168.1.5", 9042, "10.0.0.2", 7040, "a", "dc1", None, ["2", "102", "202"], HOST_ID_2],
             ["192.168.1.5", 9042, "10.0.0.2", 7040, "a", "dc1", "rack1", None, HOST_ID_2],
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
            [["192.168.1.5", "10.0.0.5", "a", "dc1", "rack1", ["2", "102", "202"], HOST_ID_2],
             ["192.168.1.6", "10.0.0.6", "a", "dc1", "rack1", ["3", "103", "203"], HOST_ID_3]]])
        self.connection.wait_for_responses = Mock(
            return_value=_node_meta_results(
                self.connection.local_results, self.connection.peer_results))
        self.control_connection.refresh_node_list_and_token_map()
        # all peers are updated
        assert 0 == len(self.cluster.added_hosts)

        assert self.cluster.metadata.get_host('192.168.1.5')
        assert self.cluster.metadata.get_host('192.168.1.6')

        assert 3 == len(self.cluster.metadata.all_hosts())

    def test_same_endpoint_with_new_host_id_removes_old_session_pool(self):
        cluster = Cluster()
        self.addCleanup(cluster.shutdown)
        cluster.control_connection.shutdown()

        hosts = []
        for host_id, address in (
                (HOST_ID_1, "192.168.1.0"),
                (HOST_ID_2, "192.168.1.1"),
                (HOST_ID_3, "192.168.1.2")):
            host, _ = cluster.add_host(
                DefaultEndPoint(address), datacenter="dc1", rack="rack1",
                signal=False, host_id=host_id)
            host.set_up()
            hosts.append(host)

        old_host = hosts[1]
        old_pool = Mock(host=old_host, is_shutdown=False)
        retained_pools = {
            host: Mock(
                host=host, is_shutdown=False,
                host_distance=HostDistance.LOCAL)
            for host in (hosts[0], hosts[2])
        }
        removal_future = Future()
        addition_future = Future()
        session = Session.__new__(Session)
        session.cluster = cluster
        session._pools = dict(retained_pools)
        session._pools[old_host] = old_pool
        session.is_shutdown = False

        def submit(fn, *args, **kwargs):
            fn(*args, **kwargs)
            return removal_future

        session.submit = submit
        session._profile_manager = Mock()
        session._profile_manager.distance.return_value = HostDistance.LOCAL
        session.add_or_renew_pool = Mock(return_value=addition_future)
        session.shutdown = Mock()
        cluster.sessions.add(session)

        connection = MockConnection()
        connection.peer_results[1][0][-1] = HOST_ID_4
        connection.wait_for_responses = Mock(
            return_value=_node_meta_results(
                connection.local_results, connection.peer_results))
        control_connection = ControlConnection(cluster, 1, 2, 0, 0)
        control_connection._connection = connection
        cluster.control_connection = control_connection
        control_connection.refresh_node_list_and_token_map()

        assert connection.wait_for_responses.call_count == 1
        assert old_host not in session._pools
        old_pool.shutdown.assert_called_once_with()
        assert cluster.metadata.get_host_by_host_id(HOST_ID_2) is None
        replacement = cluster.metadata.get_host_by_host_id(HOST_ID_4)
        assert replacement is not None
        assert replacement.endpoint == old_host.endpoint
        session.add_or_renew_pool.assert_called_once_with(
            replacement, is_host_addition=True,
            on_add_reconnection=ANY)

        removal_future.set_result(None)
        session.add_or_renew_pool.assert_called_once_with(
            replacement, is_host_addition=True,
            on_add_reconnection=ANY)

        replacement_pool = Mock(
            host=replacement, is_shutdown=False,
            host_distance=HostDistance.LOCAL)
        session._pools[replacement] = replacement_pool
        addition_future.set_result(True)

        assert session._pools[replacement] is replacement_pool

    def test_failed_same_endpoint_replacement_reconciles_surviving_pools(self):
        class ReplacementFirstDCAwareRoundRobinPolicy(
                DCAwareRoundRobinPolicy):
            def on_add(self, host):
                super().on_add(host)
                dc = self._dc(host)
                with self._hosts_lock:
                    current_hosts = self._dc_live_hosts[dc]
                    self._dc_live_hosts[dc] = (host,) + tuple(
                        current_host for current_host in current_hosts
                        if current_host != host)

        cluster = Cluster(
            load_balancing_policy=ReplacementFirstDCAwareRoundRobinPolicy(
                local_dc="dc1", used_hosts_per_remote_dc=1))
        self.addCleanup(cluster.shutdown)
        cluster.control_connection.shutdown()

        hosts = []
        for host_id, address, datacenter in (
                (HOST_ID_1, "192.168.1.0", "dc1"),
                (HOST_ID_2, "192.168.1.1", "dc2"),
                (HOST_ID_3, "192.168.1.2", "dc2")):
            host, _ = cluster.add_host(
                DefaultEndPoint(address), datacenter=datacenter, rack="rack1",
                signal=False, host_id=host_id)
            host.set_up()
            hosts.append(host)

        local_host, old_host, promoted_host = hosts
        # The custom policy models a policy whose newest host preempts an
        # existing eligible host. Seed the old host last so it starts as the
        # only eligible remote host.
        for host in (local_host, promoted_host, old_host):
            cluster.profile_manager.on_add(host)
        assert cluster.profile_manager.distance(old_host) == HostDistance.REMOTE
        assert cluster.profile_manager.distance(promoted_host) == HostDistance.IGNORED

        old_pool = Mock(
            host=old_host, is_shutdown=False,
            host_distance=HostDistance.REMOTE)
        local_pool = Mock(
            host=local_host, is_shutdown=False,
            host_distance=HostDistance.LOCAL)
        removal_future = Future()
        replacement_future = Future()
        promoted_future = Future()
        session = Session.__new__(Session)
        session.cluster = cluster
        session._pools = {local_host: local_pool, old_host: old_pool}
        session.is_shutdown = False

        def submit(fn, *args, **kwargs):
            fn(*args, **kwargs)
            return removal_future

        session.submit = submit
        session._profile_manager = cluster.profile_manager
        def add_or_renew_pool(host, is_host_addition,
                              on_add_reconnection=None):
            if is_host_addition:
                assert on_add_reconnection is not None
                return replacement_future
            assert host is promoted_host
            assert cluster.profile_manager.distance(host) == HostDistance.REMOTE
            return promoted_future

        session.add_or_renew_pool = Mock(side_effect=add_or_renew_pool)
        session.shutdown = Mock()
        cluster.sessions.add(session)

        listener = Mock()
        cluster.register_listener(listener)

        connection = MockConnection()
        connection.peer_results[1][0][3] = "dc2"
        connection.peer_results[1][0][-1] = HOST_ID_4
        connection.peer_results[1][1][3] = "dc2"
        connection.wait_for_responses = Mock(
            return_value=_node_meta_results(
                connection.local_results, connection.peer_results))
        control_connection = ControlConnection(cluster, 1, 2, 0, 0)
        control_connection._connection = connection
        cluster.control_connection = control_connection

        control_connection.refresh_node_list_and_token_map()

        replacement = cluster.metadata.get_host_by_host_id(HOST_ID_4)
        assert replacement is not None
        assert cluster.profile_manager.distance(replacement) == HostDistance.REMOTE
        assert cluster.profile_manager.distance(promoted_host) == HostDistance.IGNORED
        session.add_or_renew_pool.assert_called_once_with(
            replacement, is_host_addition=True,
            on_add_reconnection=ANY)

        # Completing removal must not race the replacement with another pool
        # creation attempt.
        removal_future.set_result(None)
        session.add_or_renew_pool.assert_called_once_with(
            replacement, is_host_addition=True,
            on_add_reconnection=ANY)

        replacement_future.set_result(False)

        assert cluster.profile_manager.distance(replacement) == HostDistance.IGNORED
        assert cluster.profile_manager.distance(promoted_host) == HostDistance.REMOTE
        assert replacement.is_currently_reconnecting()
        assert session.add_or_renew_pool.call_args_list == [
            call(replacement, is_host_addition=True,
                 on_add_reconnection=ANY),
            call(promoted_host, False),
        ]
        listener.on_add.assert_not_called()

    def test_same_control_endpoint_with_new_host_id_does_not_reconnect(self):
        cluster = Cluster()
        self.addCleanup(cluster.shutdown)
        cluster.control_connection.shutdown()

        old_host, _ = cluster.add_host(
            DefaultEndPoint("192.168.1.0"), datacenter="dc1", rack="rack1",
            signal=False, host_id=HOST_ID_1)

        published_connection = MockConnection()
        candidate_connection = MockConnection()
        candidate_connection.local_results[1][0][-1] = HOST_ID_4
        candidate_connection.wait_for_responses = Mock(
            return_value=_node_meta_results(
                candidate_connection.local_results,
                candidate_connection.peer_results))

        control_connection = ControlConnection(cluster, 1, 2, 0, 0)
        control_connection._connection = published_connection
        control_connection.reconnect = Mock()
        cluster.control_connection = control_connection
        control_connection._refresh_node_list_and_token_map(
            candidate_connection)

        control_connection.reconnect.assert_not_called()
        assert candidate_connection.wait_for_responses.call_count == 1
        assert control_connection._connection is published_connection
        assert cluster.metadata.get_host_by_host_id(HOST_ID_1) is None
        replacement = cluster.metadata.get_host_by_host_id(HOST_ID_4)
        assert replacement is not None
        assert replacement.endpoint == old_host.endpoint

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
            ["192.168.1.3", "10.0.0.3", "a", "dc1", "rack1", ["3", "103", "203"], HOST_ID_4]
        )
        self.cluster.scheduler.schedule = lambda delay, f, *args, **kwargs: f(*args, **kwargs)
        self.control_connection.refresh_node_list_and_token_map()
        assert 1 == len(self.cluster.added_hosts)
        assert self.cluster.added_hosts[0].address == "192.168.1.3"
        assert self.cluster.added_hosts[0].datacenter == "dc1"
        assert self.cluster.added_hosts[0].rack == "rack1"
        assert self.cluster.added_hosts[0].host_id == HOST_ID_4

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
            ["192.168.1.3", 555, "10.0.0.3", 666, "a", "dc1", "rack1", ["3", "103", "203"], HOST_ID_4]
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
            ["192.168.1.3", -1, "10.0.0.3", 0, "a", "dc1", "rack1", ["3", "103", "203"], HOST_ID_4]
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
