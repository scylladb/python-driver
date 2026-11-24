# Copyright ScyllaDB, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0

import gc
import logging
import time
import weakref

from unittest.mock import Mock

import pytest

from cassandra import (ConsistencyLevel, InvalidRequest, OperationTimedOut,
                       ProtocolVersion, ReadTimeout, WriteTimeout)
from cassandra.cluster import ResponseFuture, ControlConnectionQueryFallback
from cassandra.connection import (Connection, ConnectionBusy, ConnectionException,
                                  ConnectionSendError, ConnectionShutdown, DefaultEndPoint)
from cassandra.policies import RetryPolicy, FallthroughRetryPolicy
from cassandra.protocol import (QueryMessage, ReadTimeoutErrorMessage,
                                WriteTimeoutErrorMessage, ServerError, ExecuteMessage,
                                PreparedQueryNotFound, PrepareMessage, ResultMessage,
                                RESULT_KIND_PREPARED, RESULT_KIND_SET_KEYSPACE)
from cassandra.query import SimpleStatement
from cassandra import WriteType
from tests.unit import test_response_future as response_helpers
from tests.unit.utils import move_host


@pytest.fixture
def helpers():
    helper = response_helpers.ResponseFutureTests()
    helper.setUp()
    return helper


def make_fallback(helpers, policy, is_idempotent=False):
    session = helpers.make_session()
    session._pools = {}
    session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
    session.cluster.control_connection.reconnect = Mock()
    host = helpers.host1
    connection = Connection(DefaultEndPoint('127.0.0.1'))
    connection.close = Mock()
    connection.push = Mock()
    connection.is_control_connection = True
    connection._control_connection_host = host
    session.cluster.control_connection._connection = connection
    session.cluster.get_control_connection_host.return_value = host
    query = SimpleStatement('UPDATE ks.t SET n = n + 1 WHERE k = 0',
                            is_idempotent=is_idempotent)
    message = QueryMessage(query.query_string, ConsistencyLevel.QUORUM)
    future = ResponseFuture(session, message, query, 10, retry_policy=policy)
    return future, connection, host


def run_retry(future):
    call = future.session.cluster.scheduler.schedule_with_shutdown.call_args
    call.args[2](*call.args[3:], **call.kwargs)


@pytest.mark.parametrize('idempotent', [False, True])
@pytest.mark.parametrize('decision', [RetryPolicy.RETRY, RetryPolicy.RETRY_NEXT_HOST,
                                     RetryPolicy.RETHROW, RetryPolicy.IGNORE])
def test_control_failed_push_policy_after_cleanup(helpers, idempotent, decision):
    policy = Mock(spec=RetryPolicy)
    future, connection, host = make_fallback(helpers, policy, idempotent)
    control = future.session.cluster.control_connection
    queued = []

    def fail(data):
        queued.append(data)
        raise OSError('partially queued')

    connection.push.side_effect = fail
    control.return_connection = Mock(wraps=control.return_connection)

    def decide(query, consistency, error, retry_num):
        assert query.is_idempotent is idempotent
        assert consistency == ConsistencyLevel.QUORUM
        assert isinstance(error, ConnectionSendError)
        assert retry_num == 0
        assert connection.is_defunct
        assert connection.in_flight == 0
        assert not connection._requests
        assert not connection.orphaned_request_ids
        assert not future._control_connection_requests
        assert not control._application_sessions
        assert not control._application_session_claims
        assert control._application_requests_in_flight == 0
        assert future._connection is None
        assert future._req_id is None
        control.return_connection.assert_called_once_with(connection)
        return decision, ConsistencyLevel.LOCAL_QUORUM, 0.5

    policy.on_request_error.side_effect = decide
    assert future.send_request()
    assert len(queued) == 1
    policy.on_request_error.assert_called_once()
    # The failed request stream is returned once, after it was removed from
    # the deque by get_request_id(). No orphan survives the failed push.
    assert connection.request_ids.count(0) == 1
    if decision == RetryPolicy.RETHROW:
        assert future._final_exception is future._errors[host]
    elif decision == RetryPolicy.IGNORE:
        assert future._is_final() and future._final_result is None
    else:
        scheduled = future.session.cluster.scheduler.schedule_with_shutdown.call_args
        assert scheduled.args[0] == 0.5
        assert future.message.consistency_level == ConsistencyLevel.LOCAL_QUORUM
        # A recovered pool can serve the exact policy-selected destination.
        destination = host if decision == RetryPolicy.RETRY else helpers.host2
        pool, replacement = helpers.make_owned_pool(destination, request_id=17)
        future.session._pools[destination] = pool
        future.query_plan = iter([host, helpers.host2])
        run_retry(future)
        replacement.send_msg.assert_called_once()
        assert future._current_host is destination
        assert future._req_id == 17
        assert connection.push.call_count == 1


@pytest.mark.parametrize('idempotent', [False, True])
def test_fallthrough_control_push_never_resends(helpers, idempotent):
    policy = Mock(wraps=FallthroughRetryPolicy())
    future, connection, _host = make_fallback(helpers, policy, idempotent)
    connection.push.side_effect = OSError('partially queued')
    assert future.send_request()
    assert isinstance(future._final_exception, ConnectionSendError)
    policy.on_request_error.assert_called_once()
    assert connection.push.call_count == 1
    future.session.cluster.scheduler.schedule_with_shutdown.assert_not_called()


@pytest.mark.parametrize('failure', ['server', 'connection'])
@pytest.mark.parametrize('another_host', [False, True])
def test_next_host_never_returns_to_original_control_host(helpers, failure, another_host):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = RetryPolicy.RETRY_NEXT_HOST, None
    future, connection, host = make_fallback(helpers, policy)
    assert future.send_request()
    if failure == 'server':
        error = ServerError(0, 'server error', {})
    else:
        error = ConnectionException('lost after send', host.endpoint)
    if failure == 'server':
        callback = connection._requests.pop(future._req_id)[0]
        callback(error)
    else:
        connection.error_all_requests(error)
    original = future._errors[host]
    # A duplicate coordinator in an extension query plan is still the same
    # retry destination, and must be skipped.
    future.query_plan = iter([host, helpers.host2])
    if another_host:
        pool, replacement = helpers.make_owned_pool(helpers.host2)
        future.session._pools[helpers.host2] = pool
    future._query = Mock(wraps=future._query)
    run_retry(future)
    assert all(call.args[0] is not host for call in future._query.call_args_list)
    assert connection.push.call_count == 1
    if another_host:
        replacement.send_msg.assert_called_once()
        assert future._current_host is helpers.host2
    else:
        assert future._final_exception is original


@pytest.mark.parametrize('timeout_kind,consistency', [
    ('read', ConsistencyLevel.QUORUM),
    ('batch_log', ConsistencyLevel.QUORUM),
    ('batch_log', ConsistencyLevel.EACH_QUORUM),
])
@pytest.mark.parametrize('unusable', ['missing', 'busy', 'down'])
def test_default_same_host_retry_preserves_timeout(helpers, timeout_kind, consistency, unusable):
    session = helpers.make_session()
    pool, connection = helpers.make_owned_pool(helpers.host1)
    session._pools[helpers.host1] = pool
    query = SimpleStatement('SELECT * FROM ks.t', is_idempotent=True)
    future = ResponseFuture(session, QueryMessage(query.query_string, consistency),
                            query, 10, retry_policy=RetryPolicy())
    assert future.send_request()
    info = dict(consistency=consistency, required_responses=2,
                received_responses=2)
    if timeout_kind == 'read':
        response = ReadTimeoutErrorMessage(0, 'read timeout', dict(info, data_retrieved=False))
        expected_type = ReadTimeout
    else:
        response = WriteTimeoutErrorMessage(0, 'batch timeout', dict(info, write_type=WriteType.BATCH_LOG))
        expected_type = WriteTimeout
    future._set_result(helpers.host1, connection, pool, response)
    if unusable == 'missing':
        del session._pools[helpers.host1]
    elif unusable == 'busy':
        pool.borrow_connection.side_effect = ConnectionBusy('busy')
    else:
        helpers.host1.is_up = False
    run_retry(future)
    assert connection.send_msg.call_count == 1
    session._pools[helpers.host2].borrow_connection.assert_not_called()
    assert isinstance(future._final_exception, expected_type)
    assert future._final_exception.consistency == consistency


def test_policy_exception_after_control_cleanup(helpers):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.side_effect = ValueError('policy failed')
    future, connection, _ = make_fallback(helpers, policy)
    connection.push.side_effect = OSError('partially queued')
    assert future.send_request()
    assert future._final_exception is policy.on_request_error.side_effect
    assert connection.in_flight == 0
    assert not future._control_connection_requests


@pytest.mark.parametrize('kind,consistency', [
    ('read', ConsistencyLevel.QUORUM),
    ('batch_log', ConsistencyLevel.QUORUM),
    ('batch_log', ConsistencyLevel.EACH_QUORUM),
])
def test_default_same_host_timeout_retries_on_control_connection(helpers, kind, consistency):
    future, connection, host = make_fallback(helpers, RetryPolicy(), True)
    future.message.consistency_level = consistency
    assert future.send_request()
    info = dict(consistency=consistency, required_responses=2,
                received_responses=2)
    if kind == 'read':
        response = ReadTimeoutErrorMessage(0, 'read timeout', dict(info, data_retrieved=False))
    else:
        response = WriteTimeoutErrorMessage(0, 'batch timeout', dict(info, write_type=WriteType.BATCH_LOG))
    callback = connection._requests.pop(future._req_id)[0]
    callback(response)
    run_retry(future)
    assert connection.push.call_count == 2
    assert future._current_host is host
    assert future.message.consistency_level == consistency
    assert connection.in_flight == 1


@pytest.mark.parametrize('original,requested,expected', [
    (ConsistencyLevel.QUORUM, ConsistencyLevel.LOCAL_QUORUM, ConsistencyLevel.LOCAL_QUORUM),
    (ConsistencyLevel.SERIAL, ConsistencyLevel.ONE, ConsistencyLevel.SERIAL),
])
def test_control_retry_honors_custom_consistency_and_serial_guard(helpers, original, requested, expected):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = RetryPolicy.RETRY, requested
    future, connection, host = make_fallback(helpers, policy)
    future.message.consistency_level = original
    assert future.send_request()
    callback = connection._requests.pop(future._req_id)[0]
    callback(ServerError(0, 'server error', {}))
    run_retry(future)
    assert connection.push.call_count == 2
    assert future.message.consistency_level == expected
    assert future._current_host is host


def test_retry_next_host_can_use_replacement_control_coordinator(helpers):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = RetryPolicy.RETRY_NEXT_HOST, None
    future, connection, host = make_fallback(helpers, policy)
    assert future.send_request()
    callback = connection._requests.pop(future._req_id)[0]
    callback(ServerError(0, 'server error', {}))
    replacement = Connection(helpers.host2.endpoint)
    replacement.is_control_connection = True
    replacement.close = Mock()
    replacement.push = Mock()
    future.session.cluster.control_connection._connection = replacement
    future.session.cluster.get_control_connection_host.return_value = helpers.host2
    run_retry(future)
    assert connection.push.call_count == 1
    replacement.push.assert_called_once()
    assert future._current_host is helpers.host2


def test_retry_host_identity_handles_missing_ids_and_endpoint_only_hosts(helpers):
    first = helpers.make_host('127.0.0.1')
    same = helpers.make_host('127.0.0.1')
    other = helpers.make_host('127.0.0.2')
    first = Mock(spec=type(first), host_id=None, endpoint=first.endpoint)
    same = Mock(spec=type(same), host_id=None, endpoint=same.endpoint)
    other = Mock(spec=type(other), host_id=None, endpoint=other.endpoint)
    assert ResponseFuture._same_retry_host(first, same)
    assert not ResponseFuture._same_retry_host(first, other)
    assert ResponseFuture._same_retry_host(first.endpoint, first)
    assert not ResponseFuture._same_retry_host(other.endpoint, first)


def test_immediate_control_retry_keeps_replacement_stream(helpers):
    policy = Mock(spec=RetryPolicy)
    future, connection, host = make_fallback(helpers, policy)
    connection.push.side_effect = OSError('partially queued')
    replacement = Connection(host.endpoint)
    replacement.is_control_connection = True
    replacement.close = Mock()
    replacement.push = Mock()
    replacement.request_ids.clear()
    replacement.request_ids.append(17)

    def decide(*args, **kwargs):
        assert connection.in_flight == 0
        future.session.cluster.control_connection._connection = replacement
        return RetryPolicy.RETRY, None

    def schedule(delay, abort, fn, *args, **kwargs):
        fn(*args, **kwargs)

    policy.on_request_error.side_effect = decide
    future.session.cluster.scheduler.schedule_with_shutdown.side_effect = schedule
    assert future.send_request()
    replacement.push.assert_called_once()
    assert future._connection is replacement
    assert future._req_id == 17
    assert 17 in replacement._requests
    assert replacement.in_flight == 1
    assert connection.in_flight == 0


def test_same_host_retry_internal_keyspace_failure_keeps_destination(helpers):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None)
    future, connection, host = make_fallback(helpers, policy, True)
    future.session.keyspace = 'ks1'
    connection.keyspace = 'ks1'
    assert future.send_request()
    connection._requests.pop(future._req_id)[0](ServerError(0, 'original error', {}))
    original = future._errors[host]
    replacement = Connection(host.endpoint)
    replacement.is_control_connection = True
    replacement.close = Mock()
    replacement.push = Mock()
    future.session.cluster.control_connection._connection = replacement
    run_retry(future)
    assert replacement.push.call_count == 1
    assert replacement.keyspace is None
    replacement._requests.pop(future._req_id)[0](ConnectionException('lost internal USE', host.endpoint))
    pool, other = helpers.make_owned_pool(helpers.host2, request_id=17)
    future.session._pools[helpers.host2] = pool
    future.query_plan = iter([helpers.host2])
    submitted = future.session.submit.call_args
    submitted.args[0](*submitted.args[1:], **submitted.kwargs)
    other.send_msg.assert_not_called()
    assert policy.on_request_error.call_count == 1
    assert future._final_exception is original


def test_same_host_retry_reprepare_failure_preserves_cause(helpers):
    session = helpers.make_session()
    pool, connection = helpers.make_owned_pool(helpers.host1)
    session._pools[helpers.host1] = pool
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = RetryPolicy.RETRY, None
    query = SimpleStatement('SELECT * FROM ks.t', is_idempotent=True)
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.QUORUM),
                            query, 10, retry_policy=policy)
    assert future.send_request()
    future._set_result(helpers.host1, connection, pool, ServerError(0, 'original error', {}))
    run_retry(future)
    original = future._errors[helpers.host1]
    del session._pools[helpers.host1]
    future.query_plan = iter([helpers.host2])
    future._execute_after_prepare(helpers.host1, connection, pool,
                                  ConnectionException('lost internal PREPARE'),
                                  retry_context=(True, helpers.host1, original,
                                                 future._page_generation))
    session._pools[helpers.host2].borrow_connection.assert_not_called()
    assert future._final_exception is original
    policy.on_request_error.assert_called_once()


def test_same_host_retry_does_not_restrict_speculation_or_next_page(helpers):
    session = helpers.make_session()
    first_pool, first = helpers.make_owned_pool(helpers.host1)
    second_pool, second = helpers.make_owned_pool(helpers.host2)
    session._pools[helpers.host1] = first_pool
    session._pools[helpers.host2] = second_pool
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = RetryPolicy.RETRY, None
    query = SimpleStatement('SELECT * FROM ks.t', is_idempotent=True)
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.QUORUM),
                            query, 10, retry_policy=policy)
    assert future.send_request()
    future._set_result(helpers.host1, first, first_pool, ServerError(0, 'original error', {}))
    run_retry(future)
    assert first.send_msg.call_count == 2
    future.query_plan = iter([helpers.host2])
    assert future.send_request(error_no_hosts=False, is_speculative=True)
    second.send_msg.assert_called_once()

    future._set_final_result([])
    future._paging_state = b'next page'
    del session._pools[helpers.host1]
    second.send_msg.reset_mock()
    future.start_fetching_next_page()
    second.send_msg.assert_called_once()


def test_same_host_retry_rejects_replacement_control_coordinator(helpers):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = RetryPolicy.RETRY, None
    future, connection, host = make_fallback(helpers, policy)
    assert future.send_request()
    connection._requests.pop(future._req_id)[0](ServerError(0, 'original error', {}))
    original = future._errors[host]
    replacement = Connection(helpers.host2.endpoint)
    replacement.is_control_connection = True
    replacement.close = Mock()
    replacement.push = Mock()
    future.session.cluster.control_connection._connection = replacement
    future.session.cluster.get_control_connection_host.return_value = helpers.host2
    run_retry(future)
    replacement.push.assert_not_called()
    assert future._final_exception is original


def test_control_retry_validates_selected_connection_not_live_reference(helpers):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = RetryPolicy.RETRY_NEXT_HOST, None
    future, connection, host = make_fallback(helpers, policy)
    assert future.send_request()
    connection._requests.pop(future._req_id)[0](ServerError(0, 'original error', {}))
    original = future._errors[host]
    # Outer selection appears to use another coordinator; the selected physical
    # connection still belongs to the excluded host. Model reconnect replacing
    # the live control reference before its metadata getter runs.
    future.session.cluster.get_control_connection_host.return_value = helpers.host2
    run_retry(future)
    assert connection.push.call_count == 1
    assert future._final_exception is original
    assert connection.in_flight == 0


def test_same_host_control_retry_stops_at_client_deadline(helpers):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = RetryPolicy.RETRY, None
    future, connection, host = make_fallback(helpers, policy)
    assert future.send_request()
    connection._requests.pop(future._req_id)[0](ServerError(0, 'original error', {}))
    sends = connection.push.call_count
    future._start_time = time.time() - future.timeout - 1
    run_retry(future)
    assert connection.push.call_count == sends
    assert isinstance(future._final_exception, OperationTimedOut)


@pytest.mark.parametrize('prepare_succeeds', [False, True])
def test_speculative_reprepare_keeps_its_attempt(helpers, prepare_succeeds):
    session = helpers.make_session()
    session.cluster.protocol_version = ProtocolVersion.V4
    session.submit.side_effect = lambda fn, *args, **kwargs: fn(*args, **kwargs)
    first_pool, first = helpers.make_owned_pool(helpers.host1, request_id=1)
    second_pool, second = helpers.make_owned_pool(helpers.host2, request_id=2)
    first.keyspace = second.keyspace = None
    session._pools[helpers.host1] = first_pool
    session._pools[helpers.host2] = second_pool
    query = SimpleStatement('SELECT * FROM ks.t', is_idempotent=True)
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None)
    qid = b'prepared-query-id'
    prepared = Mock(query_id=qid, query_string=query.query_string, keyspace=None,
                    result_metadata=[], result_metadata_id=None)
    session.cluster._prepared_statements = {qid: prepared}
    future = ResponseFuture(session, ExecuteMessage(qid, [], ConsistencyLevel.QUORUM),
                            query, 10, retry_policy=policy, prepared_statement=prepared)
    assert future.send_request()
    first.send_msg.call_args.kwargs['cb'](ServerError(0, 'original failure', {}))
    run_retry(future)
    assert future.send_request(is_speculative=True)
    assert second.send_msg.call_count == 1
    second.send_msg.call_args.kwargs['cb'](PreparedQueryNotFound(0x2500, 'not prepared', qid))
    assert second.send_msg.call_count == 2
    assert isinstance(second.send_msg.call_args.args[0], PrepareMessage)
    if prepare_succeeds:
        response = Mock(spec=ResultMessage, kind=RESULT_KIND_PREPARED, query_id=qid,
                        bind_metadata=[], column_metadata=[], result_metadata_id=None)
    else:
        response = ConnectionException('lost speculative PREPARE')
    second.send_msg.call_args.kwargs['cb'](response)
    assert first.send_msg.call_count == 2
    assert future._final_exception is None
    if prepare_succeeds:
        assert second.send_msg.call_count == 3
        assert second.send_msg.call_args.args[0] is future.message
    else:
        assert second.send_msg.call_count == 2


@pytest.mark.parametrize('decision', [RetryPolicy.RETRY, RetryPolicy.RETRY_NEXT_HOST])
def test_first_control_attempt_uses_physical_coordinator(helpers, decision):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = decision, None
    future, connection, host = make_fallback(helpers, policy)
    # Reconnect has published another live reference after physical selection.
    future.session.cluster.get_control_connection_host.return_value = helpers.host2
    assert future.send_request()
    connection._requests.pop(future._req_id)[0](ServerError(0, 'original error', {}))
    assert host in future._errors
    assert isinstance(future._errors[host], ServerError)
    assert not isinstance(future._errors.get(helpers.host2), ServerError)
    destination = host if decision == RetryPolicy.RETRY else helpers.host2
    pool, replacement = helpers.make_owned_pool(destination, request_id=17)
    future.session._pools[destination] = pool
    future.query_plan = iter([host, helpers.host2])
    run_retry(future)
    replacement.send_msg.assert_called_once()


@pytest.mark.parametrize('tuple_entry', [False, True])
@pytest.mark.parametrize('decision', [RetryPolicy.RETRY, RetryPolicy.RETRY_NEXT_HOST])
def test_expired_retry_does_not_orphan_reused_pool_stream(helpers, decision, tuple_entry):
    session = helpers.make_session()
    pool, connection = helpers.make_owned_pool(helpers.host1, request_id=7)
    session._pools[helpers.host1] = pool
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (decision, None, 1)
    query = SimpleStatement('SELECT * FROM ks.t', is_idempotent=True)
    first = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.QUORUM),
                           query, 10, retry_policy=policy)
    assert first.send_request()
    connection._requests.pop(7)(ServerError(0, 'retry original', {}))
    assert connection.in_flight == 0
    second = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.QUORUM),
                            query, 10)
    assert second.send_request()
    other_callback = connection._requests[7]
    if tuple_entry:
        connection._requests[7] = (other_callback, None, None)
    other_request = connection._requests[7]
    pool.return_connection.reset_mock()
    first._start_time -= 100
    run_retry(first)
    assert isinstance(first._final_exception, OperationTimedOut)
    assert connection._requests.get(7) is other_request
    pool.return_connection.assert_not_called()
    assert 7 not in connection.orphaned_request_ids
    assert connection.in_flight == 1
    assert not second._is_final()


def test_next_host_keeps_stale_control_coordinator_host_id(helpers):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY_NEXT_HOST, None)
    future, old_connection, old_host = make_fallback(helpers, policy, True)
    control = future.session.cluster.control_connection
    assert control._get_host_for_connection(old_connection) is old_host
    new_host = move_host(old_host, DefaultEndPoint('127.0.0.9'))
    metadata = future.session.cluster.metadata
    metadata.remove_host(old_host)
    metadata.add_or_return_host(new_host)
    control = future.session.cluster.control_connection
    assert control._get_host_for_connection(old_connection) is None
    assert future.send_request()
    old_connection._requests.pop(future._req_id)[0](ServerError(0, 'unknown outcome', {}))
    replacement = Connection(new_host.endpoint)
    replacement.is_control_connection = True
    replacement._control_connection_host = new_host
    replacement.close = Mock()
    replacement.push = Mock()
    control._connection = replacement
    future.session.cluster.get_control_connection_host.return_value = new_host
    run_retry(future)
    assert new_host.host_id == old_host.host_id
    replacement.push.assert_not_called()


@pytest.mark.parametrize('continuation', ['success', 'connection_error', 'prepare_error', 'next_page', 'other_success'])
def test_overlapping_same_host_retry_keeps_primary_prepare_destination(helpers, continuation):
    session = helpers.make_session()
    session.cluster.protocol_version = ProtocolVersion.V4
    session.submit.side_effect = lambda fn, *args, **kwargs: fn(*args, **kwargs)
    pool_a, conn_a = helpers.make_owned_pool(helpers.host1, request_id=1)
    pool_b, conn_b = helpers.make_owned_pool(helpers.host2, request_id=2)
    conn_a.keyspace = conn_b.keyspace = None
    request_ids = iter(range(1, 10))

    def borrow(**kwargs):
        conn_a.in_flight += 1
        return conn_a, next(request_ids)

    pool_a.borrow_connection.side_effect = borrow
    session._pools[helpers.host1] = pool_a
    session._pools[helpers.host2] = pool_b
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None)
    query = SimpleStatement('SELECT * FROM ks.t', is_idempotent=True)
    qid = b'prepared-query-id'
    prepared = Mock(query_id=qid, query_string=query.query_string, keyspace=None,
                    result_metadata=[], result_metadata_id=None)
    session.cluster._prepared_statements = {qid: prepared}
    future = ResponseFuture(session, ExecuteMessage(qid, [], ConsistencyLevel.QUORUM),
                            query, 10, retry_policy=policy, prepared_statement=prepared)
    assert future.send_request()
    primary_callback = conn_a._requests.pop(1)
    assert future.send_request(is_speculative=True)
    speculative_callback = conn_b._requests.pop(2)
    primary_callback(ServerError(0, 'primary error', {}))
    run_retry(future)
    assert conn_a.send_msg.call_count == 2
    conn_a._requests.pop(2)(PreparedQueryNotFound(0x2500, 'not prepared', qid))
    assert isinstance(conn_a.send_msg.call_args.args[0], PrepareMessage)
    prepared_id = conn_a.send_msg.call_args.args[1]
    speculative_callback(ServerError(0, 'speculative error', {}))
    run_retry(future)
    assert conn_b.send_msg.call_count == 2
    response = Mock(spec=ResultMessage, kind=RESULT_KIND_PREPARED, query_id=qid,
                    bind_metadata=[], column_metadata=[], result_metadata_id=None)
    if continuation in ('next_page', 'other_success'):
        rows = helpers.make_mock_response(['v'], [(1,)])
        rows.custom_payload = rows.trace_id = rows.warnings = None
        rows.result_metadata_id = None
        rows.column_metadata = rows.column_types = []
        rows.paging_state = b'next page'
        conn_b._requests.pop(2)(rows)
        assert future.has_more_pages
        before = conn_a.send_msg.call_count
        if continuation == 'next_page':
            future.start_fetching_next_page()
            assert conn_a.send_msg.call_count == before + 1
            before = conn_a.send_msg.call_count
        conn_a._requests.pop(prepared_id)(response)
        assert conn_a.send_msg.call_count == before
        if continuation == 'other_success':
            prepared.update_result_metadata.assert_called_once_with([], None)
        assert conn_b.send_msg.call_count == 2
    else:
        if continuation == 'connection_error':
            response = ConnectionException('lost primary PREPARE')
        elif continuation == 'prepare_error':
            response = ServerError(0, 'failed primary PREPARE', {})
        original = future._errors[helpers.host1]
        future.timeout = None
        conn_a._requests.pop(prepared_id)(response)
        if continuation in ('connection_error', 'prepare_error'):
            assert conn_a.send_msg.call_count == 3
            assert future._final_exception is original
            assert future._query_retries == 2
        else:
            assert conn_a.send_msg.call_count == 4
            assert conn_a.send_msg.call_args.args[0] is future.message
        assert conn_b.send_msg.call_count == 2
    if continuation not in ('connection_error', 'prepare_error'):
        assert future._final_exception is None


@pytest.mark.parametrize('failure', ['connection', 'server'])
def test_overlapping_retry_keeps_internal_use_destination(helpers, failure):
    session = helpers.make_session()
    session.keyspace = 'ks1'
    session.submit.side_effect = lambda fn, *args, **kwargs: fn(*args, **kwargs)
    session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
    pool_a, conn_a = helpers.make_owned_pool(helpers.host1, request_id=1)
    session._pools = {helpers.host1: pool_a}
    control_connection = Connection(helpers.host1.endpoint)
    control_connection.is_control_connection = True
    control_connection._control_connection_host = helpers.host1
    control_connection.close = Mock()
    control_connection.push = Mock()
    session.cluster.control_connection._connection = control_connection
    session.cluster.get_control_connection_host.return_value = helpers.host1
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None)
    query = SimpleStatement('SELECT * FROM ks1.t', is_idempotent=True)
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.QUORUM),
                            query, 10, retry_policy=policy)
    assert future.send_request()
    conn_a.send_msg.call_args.kwargs['cb'](ServerError(0, 'primary error', {}))
    original = future._errors[helpers.host1]
    session._pools = {}
    run_retry(future)
    control_connection.push.assert_called_once()
    assert b'USE ks1' in control_connection.push.call_args.args[0]
    use_callback = control_connection._requests[future._req_id][0]
    pool_b, conn_b = helpers.make_owned_pool(helpers.host2, request_id=2)
    session._pools[helpers.host2] = pool_b
    assert future.send_request(is_speculative=True)
    conn_b.send_msg.call_args.kwargs['cb'](ServerError(0, 'speculative error', {}))
    run_retry(future)
    assert conn_b.send_msg.call_count == 2
    control_connection.is_defunct = True
    response = (ConnectionException('internal USE failed', helpers.host1.endpoint)
                if failure == 'connection' else ServerError(0, 'internal USE failed', {}))
    use_callback(response)
    assert conn_b.send_msg.call_count == 2
    assert future._final_exception is original


def test_retry_context_survives_same_session_application_use(helpers):
    session = helpers.make_session()
    session.cluster.protocol_version = ProtocolVersion.V4
    session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.SkipPoolCreation
    session.submit.side_effect = lambda fn, *args, **kwargs: fn(*args, **kwargs)
    pool_a, conn_a = helpers.make_owned_pool(helpers.host1)
    conn_a.keyspace = None
    session._pools = {helpers.host1: pool_a}
    control_connection = Connection(helpers.host1.endpoint)
    control_connection.is_control_connection = True
    control_connection._control_connection_host = helpers.host1
    control_connection.close = Mock()
    control_connection.push = Mock()
    session.cluster.control_connection._connection = control_connection
    session.cluster.get_control_connection_host.return_value = helpers.host1
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None)
    qid = b'prepared-query-id'
    query = SimpleStatement('SELECT * FROM t', is_idempotent=True)
    prepared = Mock(query_id=qid, query_string=query.query_string, keyspace=None,
                    result_metadata=[], result_metadata_id=None)
    session.cluster._prepared_statements = {qid: prepared}
    future = ResponseFuture(session, ExecuteMessage(qid, [], ConsistencyLevel.QUORUM),
                            query, 10, retry_policy=policy, prepared_statement=prepared)
    assert future.send_request()
    original = ServerError(0, 'original error', {})
    conn_a._requests.pop(1)(original)
    original = future._errors[helpers.host1]
    session._pools = {}
    use_query = SimpleStatement('USE otherks')
    use_future = ResponseFuture(session, QueryMessage(use_query.query_string, ConsistencyLevel.ONE),
                                use_query, 10)
    assert use_future.send_request()
    assert session.cluster.control_connection._application_keyspace_change is not None
    run_retry(future)
    callback = control_connection._requests.pop(future._req_id)[0]
    callback(PreparedQueryNotFound(0x2500, 'not prepared', qid))
    assert b'SELECT * FROM t' in control_connection.push.call_args.args[0]
    prepare_callback = control_connection._requests.pop(future._req_id)[0]
    pool_b, conn_b = helpers.make_owned_pool(helpers.host2)
    session._pools[helpers.host2] = pool_b
    control_connection.is_defunct = True
    prepare_callback(ConnectionException('PREPARE failed', helpers.host1.endpoint))
    conn_b.send_msg.assert_not_called()
    assert future._final_exception is original


@pytest.mark.parametrize('unusable', ['busy', 'capacity'])
def test_internal_use_success_app_unusable_retains_original_cause(helpers, unusable):
    session = helpers.make_session()
    session.keyspace = 'ks1'
    session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Fallback
    pool, pooled = helpers.make_owned_pool(helpers.host1)
    session._pools = {helpers.host1: pool}
    control = Connection(helpers.host1.endpoint)
    control.is_control_connection = True
    control._control_connection_host = helpers.host1
    control.close = Mock()
    control.push = Mock()
    session.cluster.control_connection._connection = control
    session.cluster.get_control_connection_host.return_value = helpers.host1
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None)
    query = SimpleStatement('SELECT * FROM ks1.t', is_idempotent=True)
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.QUORUM),
                            query, 10, retry_policy=policy)
    assert future.send_request()
    pooled._requests.pop(1)(ServerError(0, 'original error', {}))
    original = future._errors[helpers.host1]
    session._pools = {}
    run_retry(future)
    use_callback = control._requests.pop(future._req_id)[0]
    if unusable == 'busy':
        control._socket_writable = False
    else:
        control.in_flight = control.max_request_id + 1
    use_callback(Mock(spec=ResultMessage, kind=RESULT_KIND_SET_KEYSPACE, new_keyspace='ks1'))
    assert future._final_exception is original


def test_initial_speculative_prepare_cannot_resend_next_page(helpers):
    session = helpers.make_session()
    session.cluster.protocol_version = ProtocolVersion.V4
    session.submit.side_effect = lambda fn, *args, **kwargs: fn(*args, **kwargs)
    pool_a, a = helpers.make_owned_pool(helpers.host1)
    pool_b, b = helpers.make_owned_pool(helpers.host2)
    a.keyspace = b.keyspace = None
    session._pools[helpers.host1] = pool_a
    session._pools[helpers.host2] = pool_b
    query = SimpleStatement('SELECT * FROM t', is_idempotent=True)
    qid = b'prepared-query-id'
    prepared = Mock(query_id=qid, query_string=query.query_string, keyspace=None,
                    result_metadata=[], result_metadata_id=None)
    session.cluster._prepared_statements = {qid: prepared}
    future = ResponseFuture(session, ExecuteMessage(qid, [], ConsistencyLevel.QUORUM),
                            query, 10, prepared_statement=prepared)
    assert future.send_request()
    primary = a._requests.pop(1)
    assert future.send_request(is_speculative=True)
    b._requests.pop(1)(PreparedQueryNotFound(0x2500, 'not prepared', qid))
    prepared_callback = b._requests.pop(1)
    rows = helpers.make_mock_response(['v'], [(1,)])
    rows.custom_payload = rows.trace_id = rows.warnings = rows.result_metadata_id = None
    rows.column_metadata = rows.column_types = []
    rows.paging_state = b'next-page'
    primary(rows)
    future.start_fetching_next_page()
    count = b.send_msg.call_count
    response = Mock(spec=ResultMessage, kind=RESULT_KIND_PREPARED, query_id=qid,
                    bind_metadata=[], column_metadata=[], result_metadata_id=None)
    prepared_callback(response)
    assert b.send_msg.call_count == count


def test_second_deadline_check_keeps_reused_control_stream(helpers):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None)
    first, connection, _host = make_fallback(helpers, policy, True)
    assert first.send_request()
    stream = first._req_id
    connection._requests.pop(stream)[0](ServerError(0, 'original error', {}))
    connection.request_ids.appendleft(stream)
    second = ResponseFuture(first.session, QueryMessage(first.query.query_string, ConsistencyLevel.QUORUM),
                            first.query, 10)
    assert second.send_request()
    assert second._req_id == stream
    other_callback = connection._requests[stream][0]
    same_host = first._retry_same_host

    def expire_after_first_check(*args, **kwargs):
        first._start_time -= 100
        return same_host(*args, **kwargs)

    first._retry_same_host = expire_after_first_check
    run_retry(first)
    assert isinstance(first._final_exception, OperationTimedOut)
    assert connection._requests.get(stream, (None,))[0] is other_callback
    assert stream not in connection.orphaned_request_ids
    assert connection.in_flight == 1
    assert not second._is_final()


def test_retries_release_completed_failed_send_frames(helpers, caplog):
    # Captured exception logs intentionally retain application traceback locals.
    caplog.set_level(logging.CRITICAL)
    class EncodedFrame(bytearray):
        pass

    frames = []

    def encode(*args, **kwargs):
        frame = EncodedFrame(1024 * 1024)
        frames.append(weakref.ref(frame))
        return frame

    def push(_frame):
        raise OSError('push failed')

    session = helpers.make_session()
    host = helpers.host1
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None, 0.1)
    query = SimpleStatement('SELECT * FROM t')
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                            query, None, retry_policy=policy)
    future._protocol_handler = Mock(encode_message=encode)

    def replace():
        pool, _ = helpers.make_owned_pool(host)
        connection = Connection(host.endpoint)
        connection.close = Mock()
        connection.push = push

        def borrow(**kwargs):
            connection.in_flight += 1
            return connection, connection.get_request_id()

        def return_connection(conn, **kwargs):
            conn.in_flight -= 1

        pool.borrow_connection.side_effect = borrow
        pool.return_connection.side_effect = return_connection
        session._pools[host] = pool

    replace()
    assert future.send_request()
    for _ in range(4):
        error = future._errors[host]
        assert isinstance(error, ConnectionSendError)
        cause, traceback = error.__cause__, error.__traceback__
        locations = []
        while traceback:
            locations.append((traceback.tb_frame.f_code.co_name, traceback.tb_lineno))
            traceback = traceback.tb_next
        original_traceback = error.__traceback__
        replace()
        scheduler = future.session.cluster.scheduler.schedule_with_shutdown
        scheduled = scheduler.call_args
        # Mocks retain errors through parent call histories too. Real policy
        # callbacks and the scheduler do not retain a log of completed calls.
        session.reset_mock()
        policy.reset_mock()
        scheduled.args[2](*scheduled.args[3:], **scheduled.kwargs)
        del scheduled
        assert error.__cause__ is cause
        assert error.__traceback__ is original_traceback
        actual_locations = []
        traceback = error.__traceback__
        while traceback:
            actual_locations.append((traceback.tb_frame.f_code.co_name, traceback.tb_lineno))
            traceback = traceback.tb_next
        assert actual_locations == locations
        gc.collect()
        # This test retains the preceding error/cause for inspection. Its
        # application push frame may retain one payload, and the current error
        # retains one more; older retry contexts must be released.
        assert sum(ref() is not None for ref in frames) <= 2


def test_late_internal_use_success_records_physical_keyspace(helpers):
    future, connection, _host = make_fallback(helpers, RetryPolicy(), True)
    future.session.keyspace = 'ks1'
    assert future.send_request()
    assert connection.keyspace is None
    callback = connection._requests.pop(future._req_id)[0]
    future._set_final_exception(RuntimeError('done'))
    callback(Mock(spec=ResultMessage, kind=RESULT_KIND_SET_KEYSPACE, new_keyspace='ks1'))
    assert connection.keyspace == 'ks1'
    connection.push.assert_called_once()


def test_same_host_retry_uses_current_endpoint_for_same_node(helpers):
    session = helpers.make_session()
    old_host = helpers.host1
    pool, connection = helpers.make_owned_pool(old_host)
    session._pools = {old_host: pool}
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None)
    query = SimpleStatement('SELECT * FROM t')
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                            query, 10, retry_policy=policy)
    assert future.send_request()
    connection._requests.pop(1)(ServerError(0, 'original error', {}))
    current_host = move_host(old_host, DefaultEndPoint('127.0.0.9'))
    session.cluster.metadata.remove_host(old_host)
    session.cluster.metadata.add_or_return_host(current_host)
    current_pool, current_connection = helpers.make_owned_pool(current_host)
    session._pools = {current_host: current_pool}
    run_retry(future)
    assert current_host.host_id == old_host.host_id
    assert future._current_host is current_host
    current_connection.send_msg.assert_called_once()
    connection.send_msg.assert_called_once()


def test_shared_retry_error_preserves_application_frames(helpers):
    session = helpers.make_session()
    query = SimpleStatement('SELECT * FROM t')
    first = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                           query, None)
    second = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                            query, None)
    class SharedShutdown(ConnectionShutdown):
        traceback_reads = 0

        @property
        def __traceback__(self):
            self.traceback_reads += 1
            return BaseException.__traceback__.__get__(self)

    shared = SharedShutdown('shared shutdown')
    first._set_final_exception(shared)

    def application():
        marker = object()
        try:
            first.result()
        except ConnectionShutdown:
            yield marker
        yield marker

    caller = application()
    marker = next(caller)
    second.query_plan = iter([])
    second._retry_task(False, helpers.host1, retry_error=shared)
    assert caller.gi_frame.f_locals['marker'] is marker
    assert next(caller) is marker
    assert second._final_exception is shared
    assert shared.traceback_reads == 0


@pytest.mark.parametrize('stale', [False, True])
def test_same_host_retry_rechecks_after_borrow(helpers, stale):
    session = helpers.make_session()
    pool, connection = helpers.make_owned_pool(helpers.host1)
    session._pools = {helpers.host1: pool}
    query = SimpleStatement('SELECT * FROM t')
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY, None)
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                            query, 10, retry_policy=policy)
    assert future.send_request()
    connection._requests.pop(1)(ServerError(0, 'original error', {}))
    connection.send_msg.reset_mock()
    pool.return_connection.reset_mock()
    borrow = pool.borrow_connection.side_effect

    def changed_while_borrowing(**kwargs):
        if stale:
            result = borrow(**kwargs)
            future._page_generation += 1
            return result
        future._start_time -= 100
        raise ConnectionBusy('deadline elapsed while waiting')

    pool.borrow_connection.side_effect = changed_while_borrowing
    run_retry(future)
    connection.send_msg.assert_not_called()
    if stale:
        pool.return_connection.assert_called_once_with(connection)
        assert connection.in_flight == 0
        assert not future._is_final()
    else:
        assert isinstance(future._final_exception, OperationTimedOut)


def test_retry_retains_scheduled_generation_during_page_transition(helpers, monkeypatch):
    session = helpers.make_session()
    pool, connection = helpers.make_owned_pool(helpers.host1)
    session._pools = {helpers.host1: pool}
    query = SimpleStatement('SELECT * FROM t')
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                            query, 10)
    original = RuntimeError('original error')
    remaining = ResponseFuture._time_remaining

    def page_changes_during_retry(current):
        if current is future:
            current._page_generation += 1
        return remaining.__get__(current)

    monkeypatch.setattr(ResponseFuture, '_time_remaining', property(page_changes_during_retry))
    future._retry_task(True, helpers.host1, 0, retry_error=original)
    connection.send_msg.assert_not_called()
    pool.borrow_connection.assert_not_called()
    assert not future._is_final()


@pytest.mark.parametrize('borrow_succeeds', [False, True])
def test_next_host_retry_cannot_fail_new_page_after_borrow(helpers, borrow_succeeds):
    session = helpers.make_session()
    first_pool, first_connection = helpers.make_owned_pool(helpers.host1)
    pool, connection = helpers.make_owned_pool(helpers.host2)
    session._pools = {helpers.host1: first_pool, helpers.host2: pool}
    query = SimpleStatement('SELECT * FROM t')
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY_NEXT_HOST, None)
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                            query, 10, retry_policy=policy)
    assert future.send_request()
    first_connection._requests.pop(1)(ServerError(0, 'old page error', {}))
    borrow = pool.borrow_connection.side_effect

    def page_changes_during_borrow(**kwargs):
        future._page_generation += 1
        if borrow_succeeds:
            return borrow(**kwargs)
        raise ConnectionBusy('old retry cannot borrow')

    pool.borrow_connection.side_effect = page_changes_during_borrow
    run_retry(future)
    connection.send_msg.assert_not_called()
    assert not future._is_final()
    assert connection.in_flight == 0
    if borrow_succeeds:
        pool.return_connection.assert_called_once_with(connection)
    else:
        pool.return_connection.assert_not_called()

def test_same_host_prepare_borrow_cannot_fail_new_page(helpers):
    session = helpers.make_session()
    pool, connection = helpers.make_owned_pool(helpers.host1)
    session._pools = {helpers.host1: pool}
    query = SimpleStatement('SELECT * FROM t')
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                            query, 10)
    original = RuntimeError('old page error')
    retry_context = (True, helpers.host1, original, 0)
    borrow = pool.borrow_connection.side_effect
    def page_changes_during_prepare(**kwargs):
        result = borrow(**kwargs)
        future._page_generation += 1
        return result
    pool.borrow_connection.side_effect = page_changes_during_prepare
    future._reprepare(PrepareMessage(query.query_string), helpers.host1, connection, pool,
                      retry_context=retry_context, page_generation=0)
    connection.send_msg.assert_not_called()
    pool.return_connection.assert_called_once_with(connection)
    assert connection.in_flight == 0
    assert not future._is_final()

def test_stale_control_borrow_cannot_send_new_page(helpers):
    future, connection, host = make_fallback(helpers, RetryPolicy(), True)
    borrow = future._borrow_control_connection
    def page_changes_during_control_borrow(*args):
        request_id = borrow(*args)
        future._page_generation += 1
        return request_id
    future._borrow_control_connection = page_changes_during_control_borrow
    context = (True, host, RuntimeError('old page error'), 0)
    future._query_control_connection(retry_context=context)
    connection.push.assert_not_called()
    assert connection.in_flight == 0
    assert not future._is_final()


def test_next_host_control_exhaustion_after_deadline(helpers):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (RetryPolicy.RETRY_NEXT_HOST, None)
    future, initial_connection, host = make_fallback(helpers, policy)
    assert future.send_request()
    initial_connection._requests.pop(future._req_id)[0](ServerError(0, 'original error', {}))
    replacement = Connection(helpers.host2.endpoint)
    replacement.is_control_connection = True
    replacement._control_connection_host = helpers.host2
    replacement.close = Mock()
    replacement.push = Mock()
    future.session.cluster.control_connection._connection = replacement
    future.session.cluster.get_control_connection_host.return_value = helpers.host2
    future.query_plan = iter([])

    def expires_while_borrowing(connection):
        future._start_time -= 100
        raise ConnectionBusy('control borrow outlasted deadline')
    future._borrow_control_connection = expires_while_borrowing
    run_retry(future)
    assert isinstance(future._final_exception, OperationTimedOut)
    replacement.push.assert_not_called()


@pytest.mark.parametrize('same_host', [False, True])
def test_control_shutdown_during_host_resolution_preserves_error(helpers, same_host):
    future, connection, host = make_fallback(helpers, Mock(spec=RetryPolicy))
    future.timeout = None
    original = ConnectionException('original application failure')
    control = future.session.cluster.control_connection

    class ClosingControl:
        reads = 0

        @property
        def _connection(self):
            self.reads += 1
            return connection if self.reads == 1 else None

        def __getattr__(self, name):
            return getattr(control, name)

    future.session.cluster.control_connection = ClosingControl()
    future.session.cluster.get_control_connection_host.return_value = None
    future.query_plan = iter([])
    context = (same_host, host, original, future._page_generation)
    if same_host:
        future._retry_same_host(host, original, context)
    else:
        future.send_request(retry_context=context)
    assert future._final_exception is original
    connection.push.assert_not_called()


@pytest.mark.parametrize('same_host', [None, False, True])
def test_control_binding_conflict_preserves_retry_error(helpers, same_host):
    future, connection, host = make_fallback(helpers, RetryPolicy(), True)
    future.session.keyspace = 'ks1'
    control = future.session.cluster.control_connection
    other_session = helpers.make_session()
    other_session.keyspace = 'ks2'
    assert control._attach_application_session('ks2', other_session) is None
    original = ConnectionException('original application failure')
    context = None if same_host is None else (
        same_host, host if same_host else helpers.host2, original, 0)
    future.query_plan = iter([])
    future.send_request(retry_context=context)
    if context is None:
        assert isinstance(future._final_exception, InvalidRequest)
    else:
        assert future._final_exception is original
    connection.push.assert_not_called()
    assert connection.in_flight == 0
    assert not control._application_session_claims


@pytest.mark.parametrize('expired', [False, True])
@pytest.mark.parametrize('failure', ['server', 'decoder', 'unexpected'])
def test_next_host_internal_use_error_preserves_retry_error(helpers, expired, failure):
    future, connection, host = make_fallback(helpers, RetryPolicy(), True)
    future.session.keyspace = 'ks1'
    original = ConnectionException('original application failure')
    context = (False, helpers.host2, original, 0)
    future.query_plan = iter([])
    assert future.send_request(retry_context=context)
    callback = connection._requests.pop(future._req_id)[0]
    if expired:
        future._start_time -= 100
    if failure == 'server':
        response = ServerError(0, 'internal USE failure', {})
    elif failure == 'decoder':
        response = ValueError('internal USE decode failure')
    else:
        response = object()
    callback(response)
    if expired:
        assert isinstance(future._final_exception, OperationTimedOut)
    else:
        assert future._final_exception is original
    assert connection.push.call_count == 1
    assert connection.in_flight == 0
    assert not future._control_connection_requests
    assert not future.session.cluster.control_connection._application_internal_keyspace_changes


@pytest.mark.parametrize('marker_kind', ['application', 'internal'])
@pytest.mark.parametrize('same_host', [False, True])
@pytest.mark.parametrize('state', ['initial', 'retry', 'expired', 'stale'])
def test_application_use_retry_blocked_by_keyspace_marker(
        helpers, marker_kind, same_host, state):
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (
        RetryPolicy.RETRY if same_host else RetryPolicy.RETRY_NEXT_HOST, None)
    seed, connection, host = make_fallback(helpers, policy)
    session = seed.session
    session.keyspace = 'ks1'
    query = SimpleStatement('USE ks2')
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                            query, 10, retry_policy=policy)
    assert future.send_request()
    connection._requests.pop(future._req_id)[0](ServerError(0, 'original application error', {}))
    original = future._errors[host]
    control = session.cluster.control_connection
    if not same_host:
        connection = Connection(helpers.host2.endpoint)
        connection.is_control_connection = True
        connection._control_connection_host = helpers.host2
        connection.close = Mock()
        connection.push = Mock()
        control._connection = connection
        session.cluster.get_control_connection_host.return_value = helpers.host2
    if marker_kind == 'application':
        other_session = session
        other_query = SimpleStatement('USE ks3')
    else:
        other_session = helpers.make_session()
        other_session.cluster = session.cluster
        other_session._pools = {}
        other_session.keyspace = 'ks1'
        other_query = SimpleStatement('SELECT * FROM ks1.t')
    other = ResponseFuture(other_session, QueryMessage(other_query.query_string, ConsistencyLevel.ONE),
                           other_query, 10)
    assert other.send_request()
    active_marker = control._application_keyspace_change
    internal_markers = set(control._application_internal_keyspace_changes)
    other_request = connection._requests[other._req_id]
    sends = connection.push.call_count
    if state != 'initial':
        if state == 'expired':
            future._start_time -= 100
        elif state == 'stale':
            future._page_generation += 1
        run_retry(future)
        if state == 'expired':
            assert isinstance(future._final_exception, OperationTimedOut)
        elif state == 'stale':
            assert not future._is_final()
        else:
            assert future._final_exception is original
    else:
        initial = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                                 query, 10)
        assert initial.send_request()
        assert isinstance(initial._final_exception, InvalidRequest)
    assert connection.push.call_count == sends
    assert connection._requests[other._req_id] is other_request
    assert connection.in_flight == 1
    assert control._application_keyspace_change is active_marker
    assert control._application_internal_keyspace_changes == internal_markers
    assert not other._is_final()


@pytest.mark.parametrize('terminal_path', ['same_host', 'next_host', 'control_setup', 'prepare'])
@pytest.mark.parametrize('completion', ['current', 'other_success', 'next_page'])
def test_retry_error_finalization_keeps_admitting_page(helpers, monkeypatch, terminal_path, completion):
    session = helpers.make_session()
    pool_a, conn_a = helpers.make_owned_pool(helpers.host1)
    pool_b, conn_b = helpers.make_owned_pool(helpers.host2, request_id=2)
    session._pools = {helpers.host1: pool_a, helpers.host2: pool_b}
    session.cluster.allow_control_connection_query_fallback = ControlConnectionQueryFallback.Disabled
    policy = Mock(spec=RetryPolicy)
    policy.on_request_error.return_value = (
        RetryPolicy.RETRY_NEXT_HOST if terminal_path == 'next_host' else RetryPolicy.RETRY, None)
    query = SimpleStatement('SELECT * FROM ks.t', is_idempotent=True)
    future = ResponseFuture(session, QueryMessage(query.query_string, ConsistencyLevel.ONE),
                            query, 10, retry_policy=policy)
    assert future.send_request()
    first_callback = conn_a._requests.pop(1)
    assert future.send_request(is_speculative=True)
    speculative_callback = conn_b._requests.pop(2)
    first_callback(ServerError(0, 'old page error', {}))
    original = future._errors[helpers.host1]
    context = (terminal_path != 'next_host', helpers.host1, original, 0)
    pool_a.borrow_connection.side_effect = ConnectionBusy('same host unavailable')
    errback = Mock()
    future.add_errback(errback)
    finalize = future._set_final_exception
    observed = []

    def rows(value, paging_state=None):
        response = helpers.make_mock_response(['v'], [(value,)])
        response.custom_payload = response.trace_id = response.warnings = None
        response.result_metadata_id = None
        response.column_metadata = response.column_types = []
        response.paging_state = paging_state
        return response

    def interleave_before_finalization(response, *args, **kwargs):
        assert response is original
        if completion != 'current':
            speculative_callback(rows(1, b'next page'))
            assert future._is_final()
            if completion == 'next_page':
                session.cluster._default_load_balancing_policy.make_query_plan.return_value = [helpers.host2]
                future.start_fetching_next_page()
                assert future._page_generation == 1
                assert not future._is_final()
                observed.append(conn_b._requests[2])
            future._timer.cancel.reset_mock()
        finalize(response, *args, **kwargs)

    monkeypatch.setattr(future, '_set_final_exception', interleave_before_finalization)
    if terminal_path in ('same_host', 'next_host'):
        run_retry(future)
    elif terminal_path == 'control_setup':
        future._control_connection_failed(context)
    else:
        future._retry_after_prepare_failure(False, context)
    monkeypatch.setattr(future, '_set_final_exception', finalize)
    if completion == 'current':
        assert future._final_exception is original
        errback.assert_called_once_with(original)
    else:
        assert future._final_exception is None
        errback.assert_not_called()
        future._timer.cancel.assert_not_called()
        if completion == 'next_page':
            assert not future._event.is_set()
            assert conn_b._requests[2] is observed[0]
            assert conn_b.in_flight == 1
            conn_b._requests.pop(2)(rows(2))
            assert future.result().current_rows == [(['v'], [(2,)])]
            assert conn_b.in_flight == 0
        else:
            assert future.result().current_rows == [(['v'], [(1,)])]
