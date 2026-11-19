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

from unittest.mock import Mock

import pytest

from cassandra import ConsistencyLevel
from cassandra._prepared_metadata import protocol_handler_snapshot
from cassandra.cluster import ResponseFuture
from cassandra.protocol import ExecuteMessage, ProtocolHandler
from cassandra.query import SimpleStatement
from tests.unit import test_response_future as fixtures
from tests.unit.utils import run_isolated_subprocess



def _case():
    case = fixtures.ResponseFutureTests()
    case.setUp()
    return case


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
