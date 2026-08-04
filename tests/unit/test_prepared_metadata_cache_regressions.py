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

import io
import struct
from collections import defaultdict
from threading import Lock
from unittest.mock import Mock, patch

import pytest

from cassandra import ConsistencyLevel, type_codes as type_code_constants
from cassandra.cluster import (
    Cluster,
    Session,
    _PREPARED_METADATA_HANDLER_SNAPSHOTS,
    _prepared_metadata_decoder_context,
    _prepared_metadata_protocol_handler_snapshot,
)
from cassandra.cqltypes import Int32Type, UserType
from cassandra.protocol import (
    ProtocolHandler,
    RESULT_KIND_ROWS,
    ResultMessage,
)
from cassandra.protocol_features import ProtocolFeatures
from cassandra.query import BoundStatement, PreparedStatement


_KEYSPACE = 'prepared_metadata_cache_regressions'
_TABLE = 'values_by_id'


@pytest.fixture(autouse=True)
def clear_prepared_metadata_handler_snapshots():
    with patch.dict(
            _PREPARED_METADATA_HANDLER_SNAPSHOTS, {}, clear=True):
        yield


def _pack_string(value):
    encoded = value.encode('utf-8')
    return struct.pack('>H', len(encoded)) + encoded


def _pack_value(value):
    return struct.pack('>i', len(value)) + value


def _rows_body(column_name, type_description, value, no_metadata=False):
    flags = ResultMessage._NO_METADATA_FLAG if no_metadata else 0
    body = [
        struct.pack('>i', RESULT_KIND_ROWS),
        struct.pack('>i', flags),
        struct.pack('>i', 1),
    ]
    if not no_metadata:
        body.extend((
            _pack_string(_KEYSPACE),
            _pack_string(_TABLE),
            _pack_string(column_name),
            type_description,
        ))
    body.extend((
        struct.pack('>i', 1),
        _pack_value(value),
    ))
    return b''.join(body)


def _int_rows_body(value, no_metadata=False):
    return _rows_body(
        'value',
        struct.pack('>H', type_code_constants.Int32Type),
        struct.pack('>i', value),
        no_metadata,
    )


def _udt_rows_body(value, no_metadata=False):
    udt_description = b''.join((
        struct.pack('>H', type_code_constants.UserType),
        _pack_string(_KEYSPACE),
        _pack_string('address'),
        struct.pack('>H', 1),
        _pack_string('number'),
        struct.pack('>H', type_code_constants.Int32Type),
    ))
    udt_value = _pack_value(struct.pack('>i', value))
    return _rows_body('address', udt_description, udt_value, no_metadata)


def _decode_rows(message_class, body, user_type_map=None, result_metadata=None):
    return message_class.recv_body(
        io.BytesIO(body),
        protocol_version=4,
        protocol_features=ProtocolFeatures(),
        user_type_map=user_type_map or {},
        result_metadata=result_metadata,
        column_encryption_policy=None,
    )


class _TimesTenInt32Type(Int32Type):

    @staticmethod
    def deserialize(byts, protocol_version):
        return Int32Type.deserialize(byts, protocol_version) * 10


class _CustomResultMessage(ResultMessage):
    type_codes = ResultMessage.type_codes.copy()
    type_codes[type_code_constants.Int32Type] = _TimesTenInt32Type


class _CustomProtocolHandler(ProtocolHandler):
    message_types_by_opcode = ProtocolHandler.message_types_by_opcode.copy()
    message_types_by_opcode[ResultMessage.opcode] = _CustomResultMessage


class Address:

    def __init__(self, number):
        self.number = number


def _prepared_statement(result_metadata, result_metadata_id):
    return PreparedStatement(
        column_metadata=[],
        query_id=b'query-id',
        routing_key_indexes=None,
        query='SELECT address FROM values_by_id',
        keyspace=_KEYSPACE,
        protocol_version=4,
        result_metadata=result_metadata,
        result_metadata_id=result_metadata_id,
    )


def _configure_execute_session(session):
    session.cluster._config_mode = object()
    session.cluster.allow_beta_protocol_version = False
    session._protocol_version = 4
    session.default_fetch_size = 5000
    session.use_client_timestamp = False
    session._metrics = None
    session.keyspace = _KEYSPACE

    profile = Mock()
    profile.consistency_level = ConsistencyLevel.ONE
    profile.serial_consistency_level = None
    profile.continuous_paging_options = None
    profile.retry_policy = Mock()
    profile.row_factory = Mock()
    profile.load_balancing_policy = Mock()
    profile.speculative_execution_policy = None
    session._maybe_get_execution_profile.return_value = profile


def _capture_execute(session, prepared_statement):
    _configure_execute_session(session)
    bound = BoundStatement(prepared_statement).bind(())
    with patch('cassandra.cluster.ResponseFuture') as response_future:
        Session._create_response_future(
            session,
            bound,
            parameters=None,
            trace=False,
            custom_payload=None,
            timeout=1,
        )

    call = response_future.call_args
    return call.args[1], call.kwargs['bound_result_metadata']


def test_late_udt_registration_forces_result_metadata_refresh():
    UserType.evict_udt_class(_KEYSPACE, 'address')
    try:
        initial = _decode_rows(ResultMessage, _udt_rows_body(7))
        cached_metadata = initial.column_metadata
        prepared = _prepared_statement(cached_metadata, b'metadata-id')

        cluster = object.__new__(Cluster)
        cluster.protocol_version = 4
        cluster._user_types = defaultdict(dict)
        cluster.sessions = ()
        cluster._prepared_statements = {prepared.query_id: prepared}
        cluster._prepared_statement_lock = Lock()

        cluster.register_user_type(_KEYSPACE, 'address', Address)

        # A metadata-less response would still deserialize through the UDT class
        # cached before registration, which has no mapped_class.
        stale = _decode_rows(
            ResultMessage,
            _udt_rows_body(7, no_metadata=True),
            user_type_map=cluster._user_types,
            result_metadata=cached_metadata,
        )
        assert not isinstance(stale.parsed_rows[0][0], Address)

        # Registering a mapping is a client-side metadata change: the server's
        # metadata id cannot detect it. Invalidate the id so the next execute
        # requests definitions and rebuilds the UDT type with the new mapping.
        assert prepared.result_metadata_id is None

        session = Mock(spec=Session)
        session.cluster = cluster
        session.client_protocol_handler = ProtocolHandler
        message, _ = _capture_execute(session, prepared)
        assert message.skip_meta is False
        assert message.result_metadata_id is None

        refreshed = _decode_rows(
            ResultMessage,
            _udt_rows_body(7),
            user_type_map=cluster._user_types,
            result_metadata=cached_metadata,
        )
        assert isinstance(refreshed.parsed_rows[0][0], Address)
        assert refreshed.parsed_rows[0][0].number == 7
    finally:
        UserType.evict_udt_class(_KEYSPACE, 'address')


def test_failed_udt_registration_still_invalidates_prepared_metadata():
    cached_metadata = [('ks', 'tbl', 'address', Mock())]
    prepared = _prepared_statement(cached_metadata, b'metadata-id')
    old_context = object()
    prepared._update_result_metadata(
        cached_metadata,
        b'metadata-id',
        _prepared_metadata_decoder_context(
            _prepared_metadata_protocol_handler_snapshot(ProtocolHandler),
            old_context),
    )

    session = Mock()
    session.user_type_registered.side_effect = RuntimeError('registration failed')
    cluster = object.__new__(Cluster)
    cluster.protocol_version = 4
    cluster._user_types = defaultdict(dict)
    cluster.sessions = (session,)
    cluster._prepared_statements = {prepared.query_id: prepared}
    cluster._prepared_statement_lock = Lock()
    cluster._prepared_metadata_context = old_context

    with pytest.raises(RuntimeError, match='registration failed'):
        cluster.register_user_type(_KEYSPACE, 'address', Address)

    assert cluster._user_types[_KEYSPACE]['address'] is Address
    assert cluster._prepared_metadata_context is not old_context
    assert prepared.result_metadata_id is None


def test_udt_registration_invalidates_before_session_callbacks():
    cached_metadata = [('ks', 'tbl', 'address', Mock())]
    prepared = _prepared_statement(cached_metadata, b'metadata-id')
    old_context = object()
    prepared._update_result_metadata(
        cached_metadata,
        b'metadata-id',
        _prepared_metadata_decoder_context(
            _prepared_metadata_protocol_handler_snapshot(ProtocolHandler),
            old_context),
    )

    cluster = object.__new__(Cluster)
    cluster.protocol_version = 4
    cluster._user_types = defaultdict(dict)
    cluster._prepared_statements = {prepared.query_id: prepared}
    cluster._prepared_statement_lock = Lock()
    cluster._prepared_metadata_context = old_context

    execute_session = Mock(spec=Session)
    execute_session.cluster = cluster
    execute_session.client_protocol_handler = ProtocolHandler
    observed = {}
    registration_session = Mock()

    def inspect_during_registration(*args):
        observed['transition_context'] = \
            cluster._prepared_metadata_context
        transition_decoder_context = \
            _prepared_metadata_decoder_context(
                _prepared_metadata_protocol_handler_snapshot(
                    ProtocolHandler),
                observed['transition_context'],
            )
        # Simulate a full-metadata response racing with registration and
        # publishing descriptors under the transition context.
        prepared._update_result_metadata(
            cached_metadata,
            b'transition-metadata-id',
            transition_decoder_context,
        )
        observed['message'], _ = _capture_execute(
            execute_session, prepared)

    registration_session.user_type_registered.side_effect = \
        inspect_during_registration
    cluster.sessions = (registration_session,)

    cluster.register_user_type(_KEYSPACE, 'address', Address)

    assert observed['transition_context'] is None
    assert observed['message'].skip_meta is False
    assert observed['message'].result_metadata_id is None
    assert cluster._prepared_metadata_context is not None


def test_protocol_handler_change_forces_result_metadata_refresh():
    default_result = _decode_rows(ResultMessage, _int_rows_body(7))
    cached_metadata = default_result.column_metadata

    session = Mock(spec=Session)
    session.client_protocol_handler = ProtocolHandler
    session._protocol_version = 4
    session.default_timeout = 1
    session.cluster.metadata = Mock()
    session.cluster.column_encryption_policy = None
    session.cluster.prepare_on_all_hosts = False

    response = Mock()
    response.query_id = b'query-id'
    response.bind_metadata = []
    response.pk_indexes = None
    response.column_metadata = cached_metadata
    response.result_metadata_id = b'metadata-id'
    response.is_lwt = False

    prepare_future = Mock()
    prepare_future.result.return_value.one.return_value = response
    prepare_future.custom_payload = None
    with patch('cassandra.cluster.ResponseFuture', return_value=prepare_future):
        prepared = Session.prepare(session, 'SELECT value FROM values_by_id')

    # With NO_METADATA, even the custom ResultMessage must use the default type
    # cached by PREPARE and therefore cannot apply its custom decoder.
    stale = _decode_rows(
        _CustomResultMessage,
        _int_rows_body(7, no_metadata=True),
        result_metadata=cached_metadata,
    )
    assert stale.parsed_rows == [(7,)]

    session.client_protocol_handler = _CustomProtocolHandler
    message, bound_result_metadata = _capture_execute(session, prepared)

    # A cache produced by another handler is not safe to use for NO_METADATA.
    # Send the empty id sentinel and suppress skip so the server returns column
    # definitions for the current handler to interpret.
    assert message.skip_meta is False
    assert message.result_metadata_id is None
    assert not bound_result_metadata

    refreshed = _decode_rows(_CustomResultMessage, _int_rows_body(7))
    assert refreshed.parsed_rows == [(70,)]

    # Custom handlers expose mutable opcode/type maps. Even after one full
    # response, class identity cannot prove that their decoding configuration
    # is unchanged, so they remain ineligible for skip-metadata.
    prepared._update_result_metadata(
        refreshed.column_metadata, b'custom-metadata-id', None)
    message, bound_result_metadata = _capture_execute(session, prepared)
    assert message.skip_meta is False
    assert message.result_metadata_id is None
    assert not bound_result_metadata


def test_builtin_protocol_handler_map_change_forces_result_metadata_refresh(
        monkeypatch):
    default_result = _decode_rows(ResultMessage, _int_rows_body(7))
    cached_metadata = default_result.column_metadata
    cluster_context = object()
    prepared = _prepared_statement(cached_metadata, b'metadata-id')
    handler_snapshot = \
        _prepared_metadata_protocol_handler_snapshot(ProtocolHandler)
    prepared._update_result_metadata(
        cached_metadata,
        b'metadata-id',
        _prepared_metadata_decoder_context(
            handler_snapshot,
            cluster_context),
    )

    session = Mock(spec=Session)
    session.client_protocol_handler = ProtocolHandler
    session.cluster._prepared_metadata_context = cluster_context

    monkeypatch.setitem(
        ProtocolHandler.message_types_by_opcode,
        ResultMessage.opcode,
        _CustomResultMessage,
    )
    modified_handler = \
        _prepared_metadata_protocol_handler_snapshot(ProtocolHandler)
    assert modified_handler is ProtocolHandler
    assert _prepared_metadata_decoder_context(
        modified_handler, cluster_context) is None

    message, bound_result_metadata = _capture_execute(session, prepared)

    assert message.skip_meta is False
    assert message.result_metadata_id is None
    assert not bound_result_metadata

    # A future that already captured the old built-in configuration remains
    # internally consistent even after the public source map changes.
    frozen_response = handler_snapshot.decode_message(
        protocol_version=4,
        protocol_features=ProtocolFeatures(),
        user_type_map={},
        stream_id=0,
        flags=0,
        opcode=ResultMessage.opcode,
        body=_int_rows_body(7, no_metadata=True),
        decompressor=None,
        result_metadata=cached_metadata,
    )
    current_response = _CustomProtocolHandler.decode_message(
        protocol_version=4,
        protocol_features=ProtocolFeatures(),
        user_type_map={},
        stream_id=0,
        flags=0,
        opcode=ResultMessage.opcode,
        body=_int_rows_body(7),
        decompressor=None,
        result_metadata=None,
    )
    assert frozen_response.parsed_rows == [(7,)]
    assert current_response.parsed_rows == [(70,)]

    # Even metadata decoded after the customization cannot make an arbitrary
    # application ResultMessage eligible for metadata-less responses.
    prepared._update_result_metadata(
        current_response.column_metadata,
        b'custom-metadata-id',
        None,
    )
    message, bound_result_metadata = _capture_execute(session, prepared)
    assert message.skip_meta is False
    assert message.result_metadata_id is None
    assert not bound_result_metadata


def test_session_reuses_its_protocol_handler_snapshot():
    cached_metadata = [('ks', 'tbl', 'value', Mock())]
    cluster_context = object()
    prepared = _prepared_statement(cached_metadata, b'metadata-id')
    prepared._update_result_metadata(
        cached_metadata,
        b'metadata-id',
        _prepared_metadata_decoder_context(
            _prepared_metadata_protocol_handler_snapshot(ProtocolHandler),
            cluster_context),
    )
    session = Mock(spec=Session)
    session.client_protocol_handler = ProtocolHandler
    session.cluster._prepared_metadata_context = cluster_context

    with patch(
            'cassandra.cluster.'
            '_prepared_metadata_protocol_handler_snapshot',
            wraps=_prepared_metadata_protocol_handler_snapshot) as snapshot:
        first_message, _ = _capture_execute(session, prepared)
        second_message, _ = _capture_execute(session, prepared)

    assert snapshot.call_count == 1
    assert first_message.skip_meta is True
    assert second_message.skip_meta is True


def test_full_metadata_response_recovers_after_udt_transition():
    cached_metadata = [('ks', 'tbl', 'address', Mock())]
    prepared = _prepared_statement(cached_metadata, None)
    cluster_context = object()
    handler_snapshot = \
        _prepared_metadata_protocol_handler_snapshot(ProtocolHandler)
    prepared._update_result_metadata(
        cached_metadata,
        b'new-metadata-id',
        _prepared_metadata_decoder_context(
            handler_snapshot, cluster_context),
    )
    session = Mock(spec=Session)
    session.client_protocol_handler = ProtocolHandler
    session.cluster._prepared_metadata_context = cluster_context

    message, bound_result_metadata = _capture_execute(session, prepared)

    assert message.skip_meta is True
    assert message.result_metadata_id == b'new-metadata-id'
    assert bound_result_metadata == tuple(cached_metadata)


def test_non_subclassable_result_customization_falls_back_to_full_metadata(
        monkeypatch):
    class _NonSubclassableResultMessage(ResultMessage):

        def __init_subclass__(cls, **kwargs):
            raise TypeError('must not be subclassed')

    monkeypatch.setitem(
        ProtocolHandler.message_types_by_opcode,
        ResultMessage.opcode,
        _NonSubclassableResultMessage,
    )

    assert _prepared_metadata_protocol_handler_snapshot(
        ProtocolHandler) is ProtocolHandler


def test_custom_result_comparison_error_falls_back_to_full_metadata(
        monkeypatch):
    class RaisingComparisonMeta(type(ResultMessage)):

        def __eq__(cls, other):
            raise ValueError('comparison is application code')

        __hash__ = type.__hash__

    class _RaisingComparisonResultMessage(
            ResultMessage, metaclass=RaisingComparisonMeta):
        pass

    monkeypatch.setitem(
        ProtocolHandler.message_types_by_opcode,
        ResultMessage.opcode,
        _RaisingComparisonResultMessage,
    )

    assert _prepared_metadata_protocol_handler_snapshot(
        ProtocolHandler) is ProtocolHandler


def test_handler_snapshot_does_not_register_its_internal_result_class():
    with patch.dict(
            _PREPARED_METADATA_HANDLER_SNAPSHOTS, {}, clear=True), \
            patch('cassandra.protocol.register_class') as register_class:
        _prepared_metadata_protocol_handler_snapshot(ProtocolHandler)

    register_class.assert_not_called()


def test_handler_snapshot_freezes_all_result_message_attributes(monkeypatch):
    handler_snapshot = \
        _prepared_metadata_protocol_handler_snapshot(ProtocolHandler)
    result_snapshot = handler_snapshot.message_types_by_opcode[
        ResultMessage.opcode]
    original_metadata_id_default = result_snapshot.result_metadata_id

    monkeypatch.setattr(ResultMessage, 'result_metadata_id', object())

    assert result_snapshot.result_metadata_id is \
        original_metadata_id_default
