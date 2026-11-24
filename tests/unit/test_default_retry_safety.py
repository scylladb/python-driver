# Copyright ScyllaDB, Inc.
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

from unittest.mock import Mock

import pytest

from cassandra import ConsistencyLevel as CL, OperationTimedOut, WriteType
from cassandra.cluster import ResponseFuture
from cassandra.connection import ConnectionException, ConnectionSendError, ConnectionShutdown
from cassandra.policies import RetryPolicy
from cassandra.protocol import (
    IsBootstrappingErrorMessage, OverloadedErrorMessage, PrepareMessage, QueryMessage,
    ReadTimeoutErrorMessage, ServerError, WriteTimeoutErrorMessage)
from cassandra.query import SimpleStatement
from tests.unit import test_response_future as future_helpers


@pytest.fixture
def harness():
    helper = future_helpers.ResponseFutureTests()
    helper.setUp()
    return helper


def run_retry(future):
    schedule = future.session.cluster.scheduler.schedule_with_shutdown
    args = schedule.call_args.args
    args[2](*args[3:], **schedule.call_args.kwargs)


@pytest.mark.parametrize('marked', [False, True])
@pytest.mark.parametrize('error', [
    ServerError(0, 'unknown execution', {}),
    OverloadedErrorMessage(0x1001, 'possibly executed', {}),
    ConnectionException('lost after sending'),
    ConnectionShutdown('closed after sending'),
    ConnectionSendError('partly queued push'),
])
def test_uncertain_policy_requires_idempotency(marked, error):
    query = SimpleStatement('UPDATE counters SET n = n + 1 WHERE k = 0', is_idempotent=marked)
    expected = RetryPolicy.RETRY_NEXT_HOST if marked else RetryPolicy.RETHROW
    assert RetryPolicy().on_request_error(query, CL.QUORUM, error, 0) == (expected, None)


@pytest.mark.parametrize('marked', [False, True])
def test_deadline_is_terminal(marked):
    query = SimpleStatement('SELECT * FROM t', is_idempotent=marked)
    assert RetryPolicy().on_request_error(query, CL.ONE, OperationTimedOut(), 0) == (RetryPolicy.RETHROW, None)


@pytest.mark.parametrize('error', [
    ServerError(0, 'prepare failed', {}),
    OverloadedErrorMessage(0x1001, 'prepare overloaded', {}),
    ConnectionShutdown('closed after prepare'),
])
def test_prepare_response_keeps_next_host_failover(harness, error):
    future, (first, second), _ = harness.make_failed_push_future(RetryPolicy())
    future.query = None  # Session.prepare supplies no application Statement.
    future.message = PrepareMessage('SELECT * FROM t')
    first.push.side_effect = None
    assert future.send_request()
    callback = next(iter(first._requests.values()))[0]
    callback(error)
    assert future._final_exception is None
    run_retry(future)
    second.push.assert_called_once()
    assert future._current_host is harness.host2
    assert isinstance(future.message, PrepareMessage)


def test_prepare_deadline_remains_terminal():
    assert RetryPolicy().on_request_error(None, None, OperationTimedOut(), 0) == (RetryPolicy.RETHROW, None)


@pytest.mark.parametrize('error_type', [IsBootstrappingErrorMessage])
def test_nonexecution_response_remains_retryable(error_type):
    query = SimpleStatement('SELECT * FROM t')
    error = error_type(error_type.error_code, 'not admitted', {})
    assert RetryPolicy().on_request_error(query, CL.QUORUM, error, 0) == (RetryPolicy.RETRY_NEXT_HOST, None)


@pytest.mark.parametrize('marked', [False, True])
@pytest.mark.parametrize('kind', ['server', 'overload', 'connection', 'push'])
def test_uncertain_pooled_send_count_and_target(harness, marked, kind):
    future, (first, second), queued = harness.make_failed_push_future(RetryPolicy(), marked)
    if kind != 'push':
        first.push.side_effect = None
    assert future.send_request()
    if kind != 'push':
        if kind == 'server':
            response = ServerError(0, 'unknown execution', {})
        elif kind == 'overload':
            response = OverloadedErrorMessage(0x1001, 'possibly executed', {})
        else:
            response = ConnectionShutdown('lost after sending')
        callback = next(iter(first._requests.values()))[0]
        callback(response)
    if marked:
        run_retry(future)
        second.push.assert_called_once()
        assert future._current_host is harness.host2
    else:
        assert future._final_exception is not None
        future.session.cluster.scheduler.schedule_with_shutdown.assert_not_called()
        second.push.assert_not_called()
    first.push.assert_called_once()


@pytest.mark.parametrize('outgoing', [CL.QUORUM, CL.LOCAL_QUORUM])
@pytest.mark.parametrize('kind', ['read', 'batch'])
@pytest.mark.parametrize('fallback', [False, True])
@pytest.mark.parametrize('marked', [False, True])
def test_default_timeout_keeps_wire_consistency_and_error_metadata(harness, outgoing, kind, fallback, marked):
    if fallback:
        session = harness._make_fallback_session()
        connection = harness.make_control_connection()
        session.cluster.control_connection._connection = connection
        host = harness.make_host(connection.endpoint)
        connection.endpoint = host.endpoint
        session.cluster.metadata.add_or_return_host(host)
        connection._control_connection_host = host
        session.cluster.get_control_connection_host.return_value = host
    else:
        session = harness.make_session()
        host = harness.host1
        connection = Mock()
        session._pools[host].borrow_connection.return_value = (connection, 1)
    query = SimpleStatement('SELECT * FROM t' if kind == 'read' else 'BEGIN BATCH', is_idempotent=marked)
    # This test builds the message directly; its consistency controls the resend.
    query.consistency_level = CL.ALL
    future = ResponseFuture(session, QueryMessage(query.query_string, outgoing), query, 10)
    sent = []
    connection.send_msg.side_effect = lambda message, *args, **kwargs: sent.append(message.consistency_level) or 128
    assert future.send_request()
    info = dict(consistency=CL.ONE, required_responses=2, received_responses=2)
    if kind == 'read':
        info['data_retrieved'] = False
        response = ReadTimeoutErrorMessage(0x1200, 'actual server timeout', info)
    else:
        info['write_type'] = WriteType.BATCH_LOG
        response = WriteTimeoutErrorMessage(0x1100, 'actual server timeout', info)
    future._set_result(host, connection, None if fallback else session._pools[host], response)
    should_retry = kind == 'read' or marked
    if should_retry:
        run_retry(future)
        assert sent == [outgoing, outgoing]
        future._set_result(host, connection, None if fallback else session._pools[host], response)
    else:
        session.cluster.scheduler.schedule_with_shutdown.assert_not_called()
        assert sent == [outgoing]
    assert connection.send_msg.call_count == (2 if should_retry else 1)
    assert future._current_host == host
    assert future._event.is_set(), 'Timeout sequence must terminate the request'
    with pytest.raises(type(response.to_exception())) as caught:
        future.result()
    assert caught.value.consistency == CL.ONE
    assert caught.value.required_responses == 2
    assert caught.value.received_responses == 2
    assert response.info == info
    assert 'actual server timeout' in str(caught.value)


@pytest.mark.parametrize('marked', [False, True])
@pytest.mark.parametrize('attempt', [0, 1])
def test_batch_log_is_once_and_marked(marked, attempt):
    query = SimpleStatement('BEGIN BATCH', is_idempotent=marked)
    decision = RetryPolicy().on_write_timeout(query, CL.ONE, WriteType.BATCH_LOG, 2, 0, attempt)
    assert decision == (RetryPolicy.RETRY if marked and attempt == 0 else RetryPolicy.RETHROW, None)


@pytest.mark.parametrize('write_type', [WriteType.SIMPLE, WriteType.BATCH, WriteType.COUNTER, WriteType.UNLOGGED_BATCH])
@pytest.mark.parametrize('marked', [False, True])
def test_other_write_timeouts_are_terminal(marked, write_type):
    query = SimpleStatement('UPDATE t SET v = 1 WHERE k = 0', is_idempotent=marked)
    assert RetryPolicy().on_write_timeout(query, CL.QUORUM, write_type, 2, 1, 0) == (RetryPolicy.RETHROW, None)


@pytest.mark.parametrize('kind', ['read', 'batch'])
def test_custom_timeout_policy_retains_callback_metadata_and_explicit_consistency(harness, kind):
    session = harness.make_session()
    query = SimpleStatement('SELECT * FROM t')
    future = ResponseFuture(session, QueryMessage(query.query_string, CL.QUORUM), query, 10)
    policy = Mock(spec=RetryPolicy)
    method = policy.on_read_timeout if kind == 'read' else policy.on_write_timeout
    method.return_value = (RetryPolicy.RETRY, CL.LOCAL_QUORUM, 0.5)
    future._retry_policy = policy
    future.send_request()
    info = dict(consistency=CL.ONE, required_responses=2, received_responses=2)
    if kind == 'read':
        info['data_retrieved'] = False
        response = ReadTimeoutErrorMessage(0x1200, 'timeout', info)
    else:
        info['write_type'] = WriteType.BATCH_LOG
        response = WriteTimeoutErrorMessage(0x1100, 'timeout', info)
    future._set_result(harness.host1, None, None, response)
    method.assert_called_once_with(query, retry_num=0, **info)
    assert future.message.consistency_level == CL.LOCAL_QUORUM
    assert session.cluster.scheduler.schedule_with_shutdown.call_args.args[0] == 0.5


@pytest.mark.parametrize('kind', ['server', 'connection', 'batch'])
def test_unmarked_fallback_response_is_terminal(harness, kind):
    session = harness._make_fallback_session()
    connection = harness.make_control_connection()
    session.cluster.control_connection._connection = connection
    session.cluster.get_control_connection_host.return_value = harness.make_host(connection.endpoint)
    query = SimpleStatement('UPDATE counters SET n = n + 1 WHERE k = 0')
    future = ResponseFuture(session, QueryMessage(query.query_string, CL.QUORUM), query, 10)
    assert future.send_request()
    if kind == 'server':
        error = ServerError(0, 'unknown execution', {})
    elif kind == 'connection':
        error = ConnectionShutdown('lost after sending')
    else:
        error = WriteTimeoutErrorMessage(0x1100, 'batch timeout', dict(
            consistency=CL.ONE, write_type=WriteType.BATCH_LOG,
            required_responses=2, received_responses=0))
    connection.send_msg.call_args.kwargs['cb'](error)
    assert future._final_exception is not None
    session.cluster.scheduler.schedule_with_shutdown.assert_not_called()
    connection.send_msg.assert_called_once()
