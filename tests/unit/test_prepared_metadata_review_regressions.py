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

from unittest.mock import Mock, patch

import pytest

from cassandra import ConsistencyLevel, InvalidRequest
from cassandra._prepared_metadata import protocol_handler_snapshot
from cassandra.cluster import ResponseFuture
from cassandra.connection import Connection, ConnectionException
from cassandra.protocol import (
    ErrorMessage, ExecuteMessage, PreparedQueryNotFound, ProtocolHandler,
    RESULT_KIND_PREPARED, RESULT_KIND_SET_KEYSPACE, ResultMessage,
)
from cassandra.query import SimpleStatement
from tests.unit import test_response_future as fixtures
from tests.unit.utils import run_isolated_subprocess


def _case():
    case = fixtures.ResponseFutureTests()
    case.setUp()
    return case


def _future():
    case = _case()
    session = case.make_session()
    session.cluster.protocol_version = 4
    metadata = [('ks', 'tbl', 'old_col', Mock())]
    prepared = case._make_prepared_statement(metadata, b'old-id')
    connection = Mock(spec=Connection)
    connection.keyspace = 'ks'
    future = case._make_execute_response_future(
        session, connection, prepared)
    future._connection = connection
    session.cluster._prepared_statements = {prepared.query_id: prepared}
    session.submit.side_effect = lambda fn, *a, **kw: fn(*a, **kw)
    return case, session, prepared, connection, future


def _advance_page(case, future):
    rows = case._make_rows_response()
    rows.paging_state = b'next-page'
    future._set_result(case.host1, None, None, rows)
    assert future._event.is_set()
    with patch.object(future, 'send_request'):
        future.start_fetching_next_page()
    assert not future._event.is_set()


def _prepared_response(prepared):
    return Mock(
        spec=ResultMessage, kind=RESULT_KIND_PREPARED,
        query_id=prepared.query_id, bind_metadata=[],
        column_metadata=[('ks', 'tbl', 'late_col', Mock())],
        result_metadata_id=b'late-id')


@pytest.mark.parametrize('outcome', ['success', 'mismatch', 'error',
                                    'connection-error'])
def test_previous_page_reprepare_response_is_ignored(outcome):
    case, session, prepared, connection, future = _future()
    pool = session._pools[case.host1]
    missing = Mock(spec=PreparedQueryNotFound, info=prepared.query_id,
                   custom_payload=None, trace_id=None, warnings=None)
    future._set_result(case.host1, connection, pool, missing)
    assert connection.send_msg.call_count == 1
    callback = connection.send_msg.call_args.kwargs['cb']
    _advance_page(case, future)
    snapshot = prepared._result_metadata_snapshot
    message = future.message
    if outcome in ('success', 'mismatch'):
        response = _prepared_response(prepared)
        if outcome == 'mismatch':
            response.query_id = b'wrong-id'
    elif outcome == 'error':
        response = Mock(spec=ErrorMessage)
        response.to_exception.return_value = InvalidRequest('late prepare')
    else:
        response = ConnectionException('late prepare', case.host1)

    callback(response)

    assert prepared._result_metadata_snapshot == snapshot
    assert future.message is message
    assert future._page_generation == 1
    assert future._final_exception is None
    assert not future._event.is_set()
    assert connection.send_msg.call_count == 1


def test_previous_page_queued_reprepare_does_not_send():
    case, session, prepared, connection, future = _future()
    queued = []
    session.submit.side_effect = lambda fn, *a, **kw: queued.append(
        (fn, a, kw))
    future._set_result(
        case.host1, connection, session._pools[case.host1],
        Mock(spec=PreparedQueryNotFound, info=prepared.query_id,
                   custom_payload=None, trace_id=None, warnings=None))
    assert len(queued) == 1
    _advance_page(case, future)

    fn, args, kwargs = queued[0]
    fn(*args, **kwargs)

    connection.send_msg.assert_not_called()
    assert not future._event.is_set()


@pytest.mark.parametrize('path', ['pool', 'control'])
def test_reprepare_dispatch_cannot_capture_the_next_page(path):
    case, session, prepared, connection, future = _future()
    method = ('_query' if path == 'pool' else '_query_control_connection')
    original = getattr(future, method)
    if path == 'control':
        connection = case.make_control_connection()
    response = _prepared_response(prepared)

    def advance_before_dispatch(*args, **kwargs):
        _advance_page(case, future)
        return original(*args, **kwargs)

    with patch.object(future, method, side_effect=advance_before_dispatch):
        future._execute_after_prepare(
            case.host1, connection if path == 'control' else None,
            None, response)

    connection.send_msg.assert_not_called()
    assert future._page_generation == 1
    assert future.message.paging_state == b'next-page'
    assert not future._event.is_set()


def test_reprepare_keyspace_send_failure_cannot_fail_the_next_page():
    case, session, prepared, connection, future = _future()
    with patch.object(future, '_send_control_connection_message',
                      return_value=0) as send:
        future._set_control_connection_keyspace(
            connection, case.host1, 'ks', page_generation=0)
    callback = send.call_args.kwargs['cb']

    def fail_after_page_advance(*args, **kwargs):
        _advance_page(case, future)
        return None

    with patch.object(future, '_send_control_connection_message',
                      side_effect=fail_after_page_advance):
        callback(Mock(spec=ResultMessage, kind=RESULT_KIND_SET_KEYSPACE,
                      new_keyspace='ks'))

    assert future._final_exception is None
    assert not future._event.is_set()


def test_reprepare_error_conversion_cannot_fail_the_next_page():
    case, session, prepared, connection, future = _future()
    response = Mock(spec=ErrorMessage)

    def advance_before_error():
        _advance_page(case, future)
        return InvalidRequest('old-page error')

    response.to_exception.side_effect = advance_before_error
    future._execute_after_prepare(case.host1, None, None, response)

    assert future._final_exception is None
    assert not future._event.is_set()


def test_direct_future_preserves_explicit_full_metadata_request():
    case = _case()
    session = case.make_session()
    metadata = [('ks', 'tbl', 'col', Mock())]
    prepared = case._make_prepared_statement(metadata, b'id')
    message = ExecuteMessage(
        prepared.query_id, [], ConsistencyLevel.ONE,
        skip_meta=False, result_metadata_id=b'id')

    future = ResponseFuture(
        session, message, SimpleStatement('SELECT * FROM foo'), 1,
        prepared_statement=prepared)

    assert future.message.skip_meta is False
    assert message.skip_meta is False


@pytest.mark.parametrize('mismatch', [False, True])
def test_direct_future_validates_and_freezes_explicit_metadata(mismatch):
    case = _case()
    session = case.make_session()
    metadata = [('ks', 'tbl', 'current_col', Mock())]
    prepared = case._make_prepared_statement(metadata, b'id')
    supplied = list(metadata)
    if mismatch:
        supplied = [('ks', 'tbl', 'old_col', Mock())]
    message = ExecuteMessage(
        prepared.query_id, [], ConsistencyLevel.ONE,
        skip_meta=True, result_metadata_id=b'id')

    future = ResponseFuture(
        session, message, SimpleStatement('SELECT * FROM foo'), 1,
        prepared_statement=prepared, bound_result_metadata=supplied,
        result_metadata_decoder_context=fixtures._decoder_context())
    supplied.clear()

    if mismatch:
        assert future.message.skip_meta is False
        assert not future._bound_result_metadata
    else:
        assert future.message.skip_meta is True
        assert future._bound_result_metadata == tuple(metadata)
    assert message.skip_meta is True


def test_application_cython_parser_does_not_gain_cache_eligibility():
    from cassandra.protocol import HAVE_CYTHON, cython_protocol_handler
    if not HAVE_CYTHON:
        pytest.skip('Cython parser unavailable')
    from cassandra.obj_parser import ListParser

    handler = cython_protocol_handler(ListParser())

    assert protocol_handler_snapshot(handler) is handler
    assert '_prepared_metadata_cache_token' not in vars(handler)
    assert protocol_handler_snapshot(ProtocolHandler) is not ProtocolHandler


def test_udt_mapping_truth_callback_can_reenter_cache():
    # Bound a cache-lock regression in its own process; terminate on failure.
    run_isolated_subprocess(
        'from cassandra.cqltypes import UserType, Int32Type\n'
        'class Reentrant(type):\n'
        '    def __bool__(cls):\n'
        '        UserType.evict_udt_class("review", "truth")\n'
        '        return True\n'
        'class Mapping(metaclass=Reentrant):\n'
        '    pass\n'
        'try:\n'
        '    descriptor = UserType.make_udt_class(\n'
        '        "review", "truth", ("n",), (Int32Type,), Mapping)\n'
        '    assert descriptor.mapped_class is Mapping\n'
        'finally:\n'
        '    UserType.evict_udt_class("review", "truth")\n',
        timeout=10,
    )
