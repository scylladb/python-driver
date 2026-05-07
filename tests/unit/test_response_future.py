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

from collections import deque
from threading import RLock
from unittest.mock import Mock, MagicMock, ANY

from cassandra import ConsistencyLevel, Unavailable, SchemaTargetType, SchemaChangeType, OperationTimedOut
from cassandra.cluster import Session, ResponseFuture, NoHostAvailable, ProtocolVersion, ControlConnectionQueryFallback
from cassandra.connection import Connection, ConnectionException
from cassandra.protocol import (ReadTimeoutErrorMessage, WriteTimeoutErrorMessage,
                                UnavailableErrorMessage, ResultMessage, QueryMessage,
                                OverloadedErrorMessage, IsBootstrappingErrorMessage,
                                PreparedQueryNotFound, PrepareMessage, ServerError,
                                RESULT_KIND_ROWS, RESULT_KIND_SET_KEYSPACE,
                                RESULT_KIND_SCHEMA_CHANGE, RESULT_KIND_PREPARED,
                                ProtocolHandler)
from cassandra.policies import RetryPolicy, ExponentialBackoffRetryPolicy
from cassandra.pool import NoConnectionsAvailable
from cassandra.query import SimpleStatement
from tests.util import assertEqual, assertIsInstance
import pytest


class ResponseFutureTests(unittest.TestCase):

    def make_basic_session(self):
        s = Mock(spec=Session)
        s.row_factory = lambda col_names, rows: [(col_names, rows)]
        s.cluster.control_connection._tablets_routing_v1 = False
        s.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Disabled
        return s

    def make_pool(self):
        pool = Mock()
        pool.is_shutdown = False
        pool.borrow_connection.return_value = [Mock(), Mock()]
        return pool

    def make_control_connection(self):
        connection = Mock(spec=Connection)
        connection.endpoint = 'control-host'
        connection.lock = RLock()
        connection.in_flight = 0
        connection.max_request_id = 100
        connection.request_ids = deque()
        connection._requests = {}
        connection.orphaned_request_ids = set()
        connection.orphaned_threshold = 75
        connection.orphaned_threshold_reached = False
        connection.is_control_connection = True
        connection.get_request_id.return_value = 7
        connection.send_msg.return_value = 128
        return connection

    def make_session(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value = self.make_pool()
        return session

    def make_response_future(self, session):
        query = SimpleStatement("SELECT * FROM foo")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
        return ResponseFuture(session, message, query, 1)

    def make_mock_response(self, col_names, rows):
        return Mock(spec=ResultMessage, kind=RESULT_KIND_ROWS, column_names=col_names, parsed_rows=rows, paging_state=None, col_types=None)

    def test_result_message(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']
        pool = session._pools.get.return_value
        pool.is_shutdown = False

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()

        rf.session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY, routing_key=ANY, keyspace=ANY, table=ANY)

        connection.send_msg.assert_called_once_with(rf.message, 1, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])

        expected_result = (object(), object())
        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))
        result = rf.result()[0]
        assert result == expected_result

    def test_unknown_result_class(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()
        rf._set_result(None, None, None, object())
        with pytest.raises(ConnectionException):
            rf.result()

    def test_set_keyspace_result(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        result = Mock(spec=ResultMessage,
                      kind=RESULT_KIND_SET_KEYSPACE,
                      results="keyspace1")
        rf._set_result(None, None, None, result)
        rf._set_keyspace_completed({})
        assert not rf.result()

    def test_schema_change_result(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        event_results={'target_type': SchemaTargetType.TABLE, 'change_type': SchemaChangeType.CREATED,
                       'keyspace': "keyspace1", "table": "table1"}
        result = Mock(spec=ResultMessage,
                      kind=RESULT_KIND_SCHEMA_CHANGE,
                      schema_change_event=event_results)
        connection = Mock()
        rf._set_result(None, connection, None, result)
        session.submit.assert_called_once_with(ANY, ANY, rf, connection, **event_results)

    def test_other_result_message_kind(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()
        result = Mock(spec=ResultMessage, kind=999, results=[1, 2, 3])
        rf._set_result(None, None, None, result)
        assert rf.result()[0] == result

    def test_heartbeat_defunct_deadlock(self):
        """
        Heartbeat defuncts all connections and clears request queues. Response future times out and even
        if it has been removed from request queue, timeout exception must be thrown. Otherwise event loop
        will deadlock on eventual ResponseFuture.result() call.

        PYTHON-1044
        """

        connection = MagicMock(spec=Connection)
        connection._requests = {}
        connection.in_flight = 5
        connection.orphaned_request_ids = set()

        pool = Mock()
        pool.is_shutdown = False
        pool.borrow_connection.return_value = [connection, 1]

        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = [Mock(), Mock()]
        session._pools.get.return_value = pool

        query = SimpleStatement("SELECT * FROM foo")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf.send_request()

        # Simulate Connection.error_all_requests() after heartbeat defuncts
        connection._requests = {}

        # Simulate ResponseFuture timing out
        rf._on_timeout()
        with pytest.raises(OperationTimedOut, match="Connection defunct by heartbeat") as exc_info:
            rf.result()
        assert exc_info.value.timeout == 1
        assert exc_info.value.in_flight == 5

    def test_read_timeout_error_message(self):
        session = self.make_session()
        query = SimpleStatement("SELECT * FROM foo")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf.send_request()

        result = Mock(spec=ReadTimeoutErrorMessage, info={"data_retrieved": "", "required_responses":2,
                                                           "received_responses":1, "consistency": 1})
        rf._set_result(None, None, None, result)

        with pytest.raises(Exception):
            rf.result()

    def test_write_timeout_error_message(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf.send_request()

        result = Mock(spec=WriteTimeoutErrorMessage, info={"write_type": 1, "required_responses":2,
                                                           "received_responses":1, "consistency": 1})
        rf._set_result(None, None, None, result)
        with pytest.raises(Exception):
            rf.result()

    def test_unavailable_error_message(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        query.retry_policy = Mock()
        query.retry_policy.on_unavailable.return_value = (RetryPolicy.RETHROW, None)
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf._query_retries = 1
        rf.send_request()

        result = Mock(spec=UnavailableErrorMessage, info={"required_replicas":2, "alive_replicas": 1, "consistency": 1})
        rf._set_result(None, None, None, result)
        with pytest.raises(Exception):
            rf.result()

    def test_request_error_with_prepare_message(self):
        session = self.make_session()
        query = SimpleStatement("SELECT * FROM foobar")
        retry_policy = Mock()
        retry_policy.on_request_error.return_value = (RetryPolicy.RETHROW, None)
        message = PrepareMessage(query=query)

        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf._query_retries = 1
        rf.send_request()
        result = Mock(spec=OverloadedErrorMessage)
        result.to_exception.return_value = result
        rf._set_result(None, None, None, result)
        assert isinstance(rf._final_exception, OverloadedErrorMessage)

        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf._query_retries = 1
        rf.send_request()
        result = Mock(spec=ConnectionException)
        rf._set_result(None, None, None, result)
        assert isinstance(rf._final_exception, ConnectionException)

    def test_retry_policy_says_ignore(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        retry_policy = Mock()
        retry_policy.on_unavailable.return_value = (RetryPolicy.IGNORE, None)
        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf.send_request()

        result = Mock(spec=UnavailableErrorMessage, info={})
        rf._set_result(None, None, None, result)
        assert not rf.result()

    def test_retry_policy_says_retry(self):
        session = self.make_session()
        pool = session._pools.get.return_value

        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.QUORUM)

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        retry_policy = Mock()
        retry_policy.on_unavailable.return_value = (RetryPolicy.RETRY, ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf.send_request()

        rf.session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY, routing_key=ANY, keyspace=ANY, table=ANY)
        connection.send_msg.assert_called_once_with(rf.message, 1, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])

        result = Mock(spec=UnavailableErrorMessage, info={})
        host = Mock()
        rf._set_result(host, None, None, result)

        rf.session.cluster.scheduler.schedule.assert_called_once_with(ANY, rf._retry_task, True, host)
        assert 1 == rf._query_retries

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 2)

        # simulate the executor running this
        rf._retry_task(True, host)

        # it should try again with the same host since this was
        # an UnavailableException
        rf.session._pools.get.assert_called_with(host)
        pool.borrow_connection.assert_called_with(timeout=ANY, routing_key=ANY, keyspace=ANY, table=ANY)
        connection.send_msg.assert_called_with(rf.message, 2, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])

    def test_retry_with_different_host(self):
        session = self.make_session()
        pool = session._pools.get.return_value

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.message.consistency_level = ConsistencyLevel.QUORUM
        rf.send_request()

        rf.session._pools.get.assert_called_once_with('ip1')
        pool.borrow_connection.assert_called_once_with(timeout=ANY, routing_key=ANY, keyspace=ANY, table=ANY)
        connection.send_msg.assert_called_once_with(rf.message, 1, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])
        assert ConsistencyLevel.QUORUM == rf.message.consistency_level

        result = Mock(spec=OverloadedErrorMessage, info={})
        host = Mock()
        rf._set_result(host, None, None, result)

        rf.session.cluster.scheduler.schedule.assert_called_once_with(ANY, rf._retry_task, False, host)
        # query_retries does get incremented for Overloaded/Bootstrapping errors (since 3.18)
        assert 1 == rf._query_retries

        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 2)
        # simulate the executor running this
        rf._retry_task(False, host)

        # it should try with a different host
        rf.session._pools.get.assert_called_with('ip2')
        pool.borrow_connection.assert_called_with(timeout=ANY, routing_key=ANY, keyspace=ANY, table=ANY)
        connection.send_msg.assert_called_with(rf.message, 2, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])

        # the consistency level should be the same
        assert ConsistencyLevel.QUORUM == rf.message.consistency_level

    def test_all_retries_fail(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()
        rf.session._pools.get.assert_called_once_with('ip1')

        result = Mock(spec=IsBootstrappingErrorMessage, info={})
        host = Mock()
        rf._set_result(host, None, None, result)

        # simulate the executor running this
        rf.session.cluster.scheduler.schedule.assert_called_once_with(ANY, rf._retry_task, False, host)

        rf._retry_task(False, host)

        # it should try with a different host
        rf.session._pools.get.assert_called_with('ip2')

        result = Mock(spec=IsBootstrappingErrorMessage, info={})
        rf._set_result(host, None, None, result)

        # simulate the executor running this
        rf.session.cluster.scheduler.schedule.assert_called_with(ANY, rf._retry_task, False, host)
        rf._retry_task(False, host)

        with pytest.raises(NoHostAvailable):
            rf.result()

    def test_exponential_retry_policy_fail(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        query = SimpleStatement("SELECT * FROM foo")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
        rf = ResponseFuture(session, message, query, 1, retry_policy=ExponentialBackoffRetryPolicy(2))
        rf.send_request()
        rf.session._pools.get.assert_called_once_with('ip1')

        result = Mock(spec=IsBootstrappingErrorMessage, info={})
        host = Mock()
        rf._set_result(host, None, None, result)

        # simulate the executor running this
        rf.session.cluster.scheduler.schedule.assert_called_once_with(ANY, rf._retry_task, False, host)

        delay = rf.session.cluster.scheduler.schedule.mock_calls[-1][1][0]
        assert delay > 0.05
        rf._retry_task(False, host)

    def test_all_pools_shutdown(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools.get.return_value.is_shutdown = True

        rf = ResponseFuture(session, Mock(), Mock(), 1)
        rf.send_request()
        with pytest.raises(NoHostAvailable):
            rf.result()

    def test_control_connection_fallback_disabled_by_default(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1']
        session._pools = {}
        connection = self.make_control_connection()
        session.cluster.control_connection._connection = connection

        rf = self.make_response_future(session)
        rf.send_request()

        connection.send_msg.assert_not_called()
        with pytest.raises(NoHostAvailable):
            rf.result()

    def test_control_connection_fallback_updates_connection_keyspace(self):
        session = self.make_basic_session()
        session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1']
        session._pools = {}

        def set_keyspace_for_all_pools(keyspace, callback):
            session.keyspace = keyspace
            callback({})

        session._set_keyspace_for_all_pools.side_effect = set_keyspace_for_all_pools

        connection = self.make_control_connection()
        connection.keyspace = 'oldks'
        session.cluster.control_connection._connection = connection
        control_host = Mock(endpoint=connection.endpoint)
        session.cluster.get_control_connection_host.return_value = control_host

        rf = self.make_response_future(session)
        assert rf.send_request()

        result = Mock(spec=ResultMessage, kind=RESULT_KIND_SET_KEYSPACE, new_keyspace='newks')
        connection.send_msg.call_args[1]['cb'](result)

        assert connection.keyspace == 'newks'
        assert session.keyspace == 'newks'
        assert rf.result().current_rows == []

    def test_control_connection_fallback_when_no_usable_pools(self):
        session = self.make_basic_session()
        session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.SkipPoolCreation
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']
        session._pools = {}
        connection = self.make_control_connection()
        session.cluster.control_connection._connection = connection
        control_host = Mock(endpoint=connection.endpoint)
        session.cluster.get_control_connection_host.return_value = control_host

        rf = self.make_response_future(session)
        assert rf.send_request()

        connection.send_msg.assert_called_once_with(
            rf.message, 7, cb=ANY, encoder=ProtocolHandler.encode_message,
            decoder=ProtocolHandler.decode_message, result_metadata=[])
        assert connection.in_flight == 1
        assert rf.attempted_hosts == [control_host]

        cb = connection.send_msg.call_args[1]['cb']
        expected_result = (object(), object())
        cb(self.make_mock_response(expected_result[0], expected_result[1]))

        assert connection.in_flight == 0
        assert rf.result()[0] == expected_result

    def test_control_connection_fallback_retries_after_server_error(self):
        session = self.make_basic_session()
        session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1']
        session._pools = {}
        connection = self.make_control_connection()
        connection.get_request_id.side_effect = [7, 8]
        session.cluster.control_connection._connection = connection
        control_host = Mock(endpoint=connection.endpoint)
        session.cluster.get_control_connection_host.return_value = control_host

        rf = self.make_response_future(session)
        assert rf.send_request()

        first_response = Mock(spec=ServerError, info={})
        first_response.summary = 'boom'
        first_response.to_exception.return_value = first_response
        connection.send_msg.call_args[1]['cb'](first_response)

        rf.session.cluster.scheduler.schedule.assert_called_once_with(ANY, rf._retry_task, False, control_host)

        # The retry decision must come from the future state, not the live connection reference.
        rf._connection = Mock(is_control_connection=False)

        rf._retry_task(False, control_host)

        assert connection.send_msg.call_count == 2
        assert connection.send_msg.call_args_list[1][0][0] is rf.message
        assert connection.send_msg.call_args_list[1][0][1] == 8
        assert rf.attempted_hosts == [control_host, control_host]

        expected_result = (object(), object())
        connection.send_msg.call_args_list[1][1]['cb'](
            self.make_mock_response(expected_result[0], expected_result[1]))

        assert connection.in_flight == 0
        assert rf.result()[0] == expected_result

    def test_control_connection_fallback_fetches_next_page(self):
        session = self.make_basic_session()
        session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1']
        session._pools = {}
        connection = self.make_control_connection()
        connection.get_request_id.side_effect = [7, 8]
        session.cluster.control_connection._connection = connection
        control_host = Mock(endpoint=connection.endpoint)
        session.cluster.get_control_connection_host.return_value = control_host

        rf = self.make_response_future(session)
        assert rf.send_request()

        first_response = self.make_mock_response(['col'], [(1,)])
        first_response.paging_state = b'next-page'
        connection.send_msg.call_args[1]['cb'](first_response)

        assert rf.result().current_rows == [(['col'], [(1,)])]
        assert rf.has_more_pages

        rf.start_fetching_next_page()

        assert connection.send_msg.call_count == 2
        assert connection.send_msg.call_args_list[1][0][0] is rf.message
        assert connection.send_msg.call_args_list[1][0][1] == 8
        assert rf.message.paging_state == b'next-page'

        second_response = self.make_mock_response(['col'], [(2,)])
        connection.send_msg.call_args_list[1][1]['cb'](second_response)

        assert connection.in_flight == 0
        assert rf.result().current_rows == [(['col'], [(2,)])]

    def test_control_connection_fallback_reprepares_prepared_statement(self):
        session = self.make_basic_session()
        session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
        session.cluster.protocol_version = ProtocolVersion.V4
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1']
        session._pools = {}
        session.submit.side_effect = lambda fn, *args, **kwargs: fn(*args, **kwargs)

        query_id = b'a' * 16
        prepared_statement = Mock(
            query_id=query_id,
            query_string="SELECT * FROM foobar",
            keyspace="FooKeyspace",
            result_metadata=[],
            result_metadata_id=None)
        session.cluster._prepared_statements = {query_id: prepared_statement}

        connection = self.make_control_connection()
        connection.keyspace = "FooKeyspace"
        connection.get_request_id.side_effect = [7, 8, 9]
        session.cluster.control_connection._connection = connection
        control_host = Mock(endpoint=connection.endpoint)
        session.cluster.get_control_connection_host.return_value = control_host

        rf = self.make_response_future(session)
        rf.prepared_statement = prepared_statement
        assert rf.send_request()

        missing = Mock(spec=PreparedQueryNotFound, info=query_id)
        connection.send_msg.call_args_list[0][1]['cb'](missing)

        assert connection.send_msg.call_count == 2
        prepare_message = connection.send_msg.call_args_list[1][0][0]
        assert isinstance(prepare_message, PrepareMessage)
        assert prepare_message.query == "SELECT * FROM foobar"
        assert connection.send_msg.call_args_list[1][0][1] == 8

        prepared_response = Mock(
            spec=ResultMessage,
            kind=RESULT_KIND_PREPARED,
            query_id=query_id,
            column_metadata=[],
            result_metadata_id=None)
        connection.send_msg.call_args_list[1][1]['cb'](prepared_response)

        assert connection.send_msg.call_count == 3
        assert connection.send_msg.call_args_list[2][0][0] is rf.message
        assert connection.send_msg.call_args_list[2][0][1] == 9

        expected_result = (['col'], [(1,)])
        connection.send_msg.call_args_list[2][1]['cb'](
            self.make_mock_response(expected_result[0], expected_result[1]))

        assert connection.in_flight == 0
        assert rf.result()[0] == expected_result

    def test_control_connection_fallback_not_used_when_pool_can_serve(self):
        session = self.make_basic_session()
        session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1']
        pool = Mock(is_shutdown=False)
        pool.borrow_connection.side_effect = NoConnectionsAvailable()
        session._pools = {'ip1': pool}
        connection = self.make_control_connection()
        session.cluster.control_connection._connection = connection

        rf = self.make_response_future(session)
        rf.send_request()

        connection.send_msg.assert_not_called()
        with pytest.raises(NoHostAvailable):
            rf.result()

    def test_control_connection_fallback_orphans_stream_on_timeout(self):
        session = self.make_basic_session()
        session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1']
        session._pools = {}
        connection = self.make_control_connection()
        session.cluster.control_connection._connection = connection

        def send_msg(message, request_id, cb, **kwargs):
            connection._requests[request_id] = (cb, kwargs.get('decoder'), kwargs.get('result_metadata'))
            return 128

        connection.send_msg.side_effect = send_msg

        rf = self.make_response_future(session)
        rf.send_request()
        rf._on_timeout()

        assert 7 in connection.orphaned_request_ids
        assert connection.in_flight == 1
        with pytest.raises(OperationTimedOut):
            rf.result()

    def test_control_connection_fallback_timeout_without_metadata_host_uses_connection_endpoint(self):
        session = self.make_basic_session()
        session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = []
        session._pools = {}
        session.cluster.get_control_connection_host.return_value = None
        connection = self.make_control_connection()
        session.cluster.control_connection._connection = connection

        def send_msg(message, request_id, cb, **kwargs):
            connection._requests[request_id] = (cb, kwargs.get('decoder'), kwargs.get('result_metadata'))
            return 128

        connection.send_msg.side_effect = send_msg

        rf = self.make_response_future(session)
        assert rf.send_request()
        rf._on_timeout()

        with pytest.raises(OperationTimedOut) as exc_info:
            rf.result()

        assert exc_info.value.errors == {
            'control-host': 'Client request timeout. See Session.execute[_async](timeout)'
        }

    def test_first_pool_shutdown(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']
        # first return a pool with is_shutdown=True, then is_shutdown=False
        pool_shutdown = self.make_pool()
        pool_shutdown.is_shutdown = True
        pool_ok = self.make_pool()
        pool_ok.is_shutdown = True
        session._pools.get.side_effect = [pool_shutdown, pool_ok]

        rf = self.make_response_future(session)
        rf.send_request()

        expected_result = (object(), object())
        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))

        result = rf.result()[0]
        assert result == expected_result

    def test_timeout_getting_connection_from_pool(self):
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = ['ip1', 'ip2']

        # the first pool will raise an exception on borrow_connection()
        exc = NoConnectionsAvailable()
        first_pool = Mock(is_shutdown=False)
        first_pool.borrow_connection.side_effect = exc

        # the second pool will return a connection
        second_pool = Mock(is_shutdown=False)
        connection = Mock(spec=Connection)
        second_pool.borrow_connection.return_value = (connection, 1)

        session._pools.get.side_effect = [first_pool, second_pool]

        rf = self.make_response_future(session)
        rf.send_request()

        expected_result = (object(), object())
        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))
        assert rf.result()[0] == expected_result

        # make sure the exception is recorded correctly
        assert rf._errors == {'ip1': exc}

    def test_callback(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        callback = Mock()
        expected_result = (object(), object())
        arg = "positional"
        kwargs = {'one': 1, 'two': 2}
        rf.add_callback(callback, arg, **kwargs)

        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))

        result = rf.result()[0]
        assert result == expected_result

        callback.assert_called_once_with([expected_result], arg, **kwargs)

        # this should get called immediately now that the result is set
        rf.add_callback(assertEqual, [expected_result])

    def test_errback(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        rf = ResponseFuture(session, message, query, 1)
        rf._query_retries = 1
        rf.send_request()

        rf.add_errback(assertIsInstance, Exception)

        result = Mock(spec=UnavailableErrorMessage, info={"required_replicas":2, "alive_replicas": 1, "consistency": 1})
        result.to_exception.return_value = Exception()

        rf._set_result(None, None, None, result)
        with pytest.raises(Exception):
            rf.result()

        # this should get called immediately now that the error is set
        rf.add_errback(assertIsInstance, Exception)

    def test_multiple_callbacks(self):
        session = self.make_session()
        rf = self.make_response_future(session)
        rf.send_request()

        callback = Mock()
        expected_result = (object(), object())
        arg = "positional"
        kwargs = {'one': 1, 'two': 2}
        rf.add_callback(callback, arg, **kwargs)

        callback2 = Mock()
        arg2 = "another"
        kwargs2 = {'three': 3, 'four': 4}
        rf.add_callback(callback2, arg2, **kwargs2)

        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))

        result = rf.result()[0]
        assert result == expected_result

        callback.assert_called_once_with([expected_result], arg, **kwargs)
        callback2.assert_called_once_with([expected_result], arg2, **kwargs2)

    def test_multiple_errbacks(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        retry_policy = Mock()
        retry_policy.on_unavailable.return_value = (RetryPolicy.RETHROW, None)
        rf = ResponseFuture(session, message, query, 1, retry_policy=retry_policy)
        rf.send_request()

        callback = Mock()
        arg = "positional"
        kwargs = {'one': 1, 'two': 2}
        rf.add_errback(callback, arg, **kwargs)

        callback2 = Mock()
        arg2 = "another"
        kwargs2 = {'three': 3, 'four': 4}
        rf.add_errback(callback2, arg2, **kwargs2)

        expected_exception = Unavailable("message", 1, 2, 3)
        result = Mock(spec=UnavailableErrorMessage, info={"required_replicas":2, "alive_replicas": 1, "consistency": 1})
        result.to_exception.return_value = expected_exception
        rf._set_result(None, None, None, result)
        rf._event.set()
        with pytest.raises(Exception):
            rf.result()

        callback.assert_called_once_with(expected_exception, arg, **kwargs)
        callback2.assert_called_once_with(expected_exception, arg2, **kwargs2)

    def test_add_callbacks(self):
        session = self.make_session()
        query = SimpleStatement("INSERT INFO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)

        # test errback
        rf = ResponseFuture(session, message, query, 1)
        rf._query_retries = 1
        rf.send_request()

        rf.add_callbacks(
            callback=assertEqual, callback_args=([{'col': 'val'}],),
            errback=assertIsInstance, errback_args=(Exception,))

        result = Mock(spec=UnavailableErrorMessage,
                      info={"required_replicas":2, "alive_replicas": 1, "consistency": 1})
        result.to_exception.return_value = Exception()
        rf._set_result(None, None, None, result)
        with pytest.raises(Exception):
            rf.result()

        # test callback
        rf = ResponseFuture(session, message, query, 1)
        rf.send_request()

        callback = Mock()
        expected_result = (object(), object())
        arg = "positional"
        kwargs = {'one': 1, 'two': 2}
        rf.add_callbacks(
            callback=callback, callback_args=(arg,), callback_kwargs=kwargs,
            errback=assertIsInstance, errback_args=(Exception,))

        rf._set_result(None, None, None, self.make_mock_response(expected_result[0], expected_result[1]))
        assert rf.result()[0] == expected_result

        callback.assert_called_once_with([expected_result], arg, **kwargs)

    def test_prepared_query_not_found(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()

        session.cluster.protocol_version = ProtocolVersion.V4
        session.cluster._prepared_statements = MagicMock(dict)
        prepared_statement = session.cluster._prepared_statements.__getitem__.return_value
        prepared_statement.query_string = "SELECT * FROM foobar"
        prepared_statement.keyspace = "FooKeyspace"
        rf._connection.keyspace = "FooKeyspace"

        result = Mock(spec=PreparedQueryNotFound, info='a' * 16)
        rf._set_result(None, None, None, result)

        assert session.submit.call_args
        args, kwargs = session.submit.call_args
        assert rf._reprepare == args[-5]
        assert isinstance(args[-4], PrepareMessage)
        assert args[-4].query == "SELECT * FROM foobar"

    def test_prepared_query_not_found_bad_keyspace(self):
        session = self.make_session()
        pool = session._pools.get.return_value
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()

        session.cluster.protocol_version = ProtocolVersion.V4
        session.cluster._prepared_statements = MagicMock(dict)
        prepared_statement = session.cluster._prepared_statements.__getitem__.return_value
        prepared_statement.query_string = "SELECT * FROM foobar"
        prepared_statement.keyspace = "FooKeyspace"
        rf._connection.keyspace = "BarKeyspace"

        result = Mock(spec=PreparedQueryNotFound, info='a' * 16)
        rf._set_result(None, None, None, result)
        with pytest.raises(ValueError):
            rf.result()

    def test_repeat_orig_query_after_succesful_reprepare(self):
        query_id = b'abc123'  # Just a random binary string so we don't hit id mismatch exception
        session = self.make_session()
        rf = self.make_response_future(session)

        response = Mock(spec=ResultMessage,
                        kind=RESULT_KIND_PREPARED,
                        result_metadata_id='foo')
        response.results = (None, None, None, None, None)
        response.query_id = query_id

        rf._query = Mock(return_value=True)
        rf._execute_after_prepare('host', None, None, response)
        rf._query.assert_called_once_with('host')

        rf.prepared_statement = Mock()
        rf.prepared_statement.query_id = query_id
        rf._query = Mock(return_value=True)
        rf._execute_after_prepare('host', None, None, response)
        rf._query.assert_called_once_with('host')

    def test_timeout_does_not_release_stream_id(self):
        """
        Make sure that stream ID is not reused immediately after client-side
        timeout. Otherwise, a new request could reuse the stream ID and would
        risk getting a response for the old, timed out query.
        """
        session = self.make_basic_session()
        session.cluster._default_load_balancing_policy.make_query_plan.return_value = [Mock(endpoint='ip1'), Mock(endpoint='ip2')]
        pool = self.make_pool()
        session._pools.get.return_value = pool
        connection = Mock(spec=Connection, lock=RLock(), _requests={}, request_ids=deque(),
                orphaned_request_ids=set(), orphaned_threshold=256, in_flight=3)
        pool.borrow_connection.return_value = (connection, 1)

        rf = self.make_response_future(session)
        rf.send_request()

        connection._requests[1] = (connection._handle_options_response, ProtocolHandler.decode_message, [])

        rf._on_timeout()
        pool.return_connection.assert_called_once_with(connection, stream_was_orphaned=True)
        with pytest.raises(OperationTimedOut, match="Client request timeout") as exc_info:
            rf.result()
        assert exc_info.value.timeout == 1
        assert exc_info.value.in_flight == 3

        assert len(connection.request_ids) == 0, \
            "Request IDs should be empty but it's not: {}".format(connection.request_ids)

    def test_single_host_query_plan_exhausted_after_one_retry(self):
        """
        Test that when a specific host is provided, the query plan is properly
        exhausted after one attempt and doesn't cause infinite retries.
        
        This test reproduces the issue where providing a single host in the query plan
        (via the host parameter) would cause infinite retries on server errors because
        the query_plan was a list instead of an iterator.
        """
        session = self.make_basic_session()
        pool = self.make_pool()
        session._pools.get.return_value = pool
        
        # Create a specific host
        specific_host = Mock()
        
        connection = Mock(spec=Connection)
        pool.borrow_connection.return_value = (connection, 1)
        
        query = SimpleStatement("INSERT INTO foo (a, b) VALUES (1, 2)")
        message = QueryMessage(query=query, consistency_level=ConsistencyLevel.ONE)
        
        # Create ResponseFuture with a specific host (this is the key to reproducing the bug)
        rf = ResponseFuture(session, message, query, 1, host=specific_host)
        rf.send_request()
        
        # Verify initial request was sent
        rf.session._pools.get.assert_called_once_with(specific_host)
        pool.borrow_connection.assert_called_once_with(timeout=ANY, routing_key=ANY, keyspace=ANY, table=ANY)
        connection.send_msg.assert_called_once_with(rf.message, 1, cb=ANY, encoder=ProtocolHandler.encode_message, decoder=ProtocolHandler.decode_message, result_metadata=[])
        
        # Simulate a ServerError response (which triggers RETRY_NEXT_HOST by default)
        result = Mock(spec=ServerError, info={})
        result.to_exception.return_value = result
        rf._set_result(specific_host, None, None, result)
        
        # The retry should be scheduled
        rf.session.cluster.scheduler.schedule.assert_called_once_with(ANY, rf._retry_task, False, specific_host)
        assert 1 == rf._query_retries
        
        # Reset mocks to track next calls
        pool.borrow_connection.reset_mock()
        connection.send_msg.reset_mock()
        
        # Now simulate the retry task executing
        # The bug would cause this to succeed and retry again infinitely
        # The fix ensures the iterator is exhausted after the first try
        rf._retry_task(False, specific_host)
        
        # After the retry, send_request should be called but the query_plan iterator
        # should be exhausted, so no new request should be sent
        # Instead, it should set a NoHostAvailable exception
        assert rf._final_exception is not None
        assert isinstance(rf._final_exception, NoHostAvailable)
