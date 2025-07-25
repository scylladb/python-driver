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

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, ANY, call

from cassandra import OperationTimedOut, SchemaTargetType, SchemaChangeType
from cassandra.protocol import ResultMessage, RESULT_KIND_ROWS
from cassandra.cluster import ControlConnection, _Scheduler, ProfileManager, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.pool import Host
from cassandra.connection import EndPoint, DefaultEndPoint, DefaultEndPointFactory
from cassandra.policies import (SimpleConvictionPolicy, RoundRobinPolicy,
                                ConstantReconnectionPolicy, IdentityTranslator)

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
        self._host_id_by_endpoint[host.endpoint] = host.host_id
        self._host_id_by_endpoint.pop(old_endpoint, False)

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

    def __init__(self):
        self.endpoint = DefaultEndPoint("192.168.1.0")
        self.original_endpoint = self.endpoint
        self.local_results = [
            ["rpc_address", "schema_version", "cluster_name", "data_center", "rack", "partitioner", "release_version", "tokens", "host_id"],
            [["192.168.1.0", "a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", "2.2.0", ["0", "100", "200"], "uuid1"]]
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
        local_results=(["rpc_address", "schema_version", "cluster_name", "data_center", "rack", "partitioner", "release_version", "tokens", "host_id"],
                       [["192.168.1.0", "a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", "2.2.0", ["0", "100", "200"], "uuid1"]]),
        peer_results=(["rpc_address", "peer", "schema_version", "data_center", "rack", "tokens", "host_id"],
                      [["192.168.1.1", "10.0.0.1", "a", "dc1", "rack1", ["1", "101", "201"], "uuid2"],
                       ["192.168.1.2", "10.0.0.2", "a", "dc1", "rack1", ["2", "102", "202"], "uuid3"]]))

    _nonmatching_schema_preloaded_results = _node_meta_results(
        local_results=(["rpc_address", "schema_version", "cluster_name", "data_center", "rack", "partitioner", "release_version", "tokens", "host_id"],
                       [["192.168.1.0", "a", "foocluster", "dc1", "rack1", "Murmur3Partitioner", "2.2.0", ["0", "100", "200"], "uuid1"]]),
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

    def test_wait_for_schema_agreement(self):
        """
        Basic test with all schema versions agreeing
        """
        assert self.control_connection.wait_for_schema_agreement()
        # the control connection should not have slept at all
        assert self.time.clock == 0

    def test_wait_for_schema_agreement_uses_preloaded_results_if_given(self):
        """
        wait_for_schema_agreement uses preloaded results if given for shared table queries
        """
        preloaded_results = self._matching_schema_preloaded_results
        assert self.control_connection.wait_for_schema_agreement(preloaded_results=preloaded_results)
        # the control connection should not have slept at all
        assert self.time.clock == 0
        # the connection should not have made any queries if given preloaded results
        assert self.connection.wait_for_responses.call_count == 0

    def test_wait_for_schema_agreement_falls_back_to_querying_if_schemas_dont_match_preloaded_result(self):
        """
        wait_for_schema_agreement requery if schema does not match using preloaded results
        """
        preloaded_results = self._nonmatching_schema_preloaded_results
        assert self.control_connection.wait_for_schema_agreement(preloaded_results=preloaded_results)
        # the control connection should not have slept at all
        assert self.time.clock == 0
        assert self.connection.wait_for_responses.call_count == 1

    def test_wait_for_schema_agreement_fails(self):
        """
        Make sure the control connection sleeps and retries
        """
        # change the schema version on one node
        self.connection.peer_results[1][1][2] = 'b'
        assert not self.control_connection.wait_for_schema_agreement()
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

        assert self.control_connection.wait_for_schema_agreement()
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
        assert self.control_connection.wait_for_schema_agreement()
        assert self.time.clock == 0

        # but once we mark it up, the control connection will care
        host.is_up = True
        assert not self.control_connection.wait_for_schema_agreement()
        assert self.time.clock >= self.cluster.max_schema_agreement_wait

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

    def test_refresh_schema_timeout(self):

        def bad_wait_for_responses(*args, **kwargs):
            self.time.sleep(kwargs['timeout'])
            raise OperationTimedOut()

        self.connection.wait_for_responses = Mock(side_effect=bad_wait_for_responses)
        self.control_connection.refresh_schema()
        assert self.connection.wait_for_responses.call_count == self.cluster.max_schema_agreement_wait / self.control_connection._timeout
        assert self.connection.wait_for_responses.call_args[1]['timeout'] == self.control_connection._timeout

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
