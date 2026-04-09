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
import unittest

from unittest.mock import Mock

from cassandra import ProtocolVersion, UnsupportedOperation
from cassandra.protocol import (
    PrepareMessage, QueryMessage, ExecuteMessage, UnsupportedOperation,
    _PAGING_OPTIONS_FLAG, _WITH_SERIAL_CONSISTENCY_FLAG,
    _PAGE_SIZE_FLAG, _WITH_PAGING_STATE_FLAG,
    _SKIP_METADATA_FLAG,
    BatchMessage, ResultMessage,
    RESULT_KIND_ROWS
)
from cassandra.protocol_features import ProtocolFeatures
from cassandra.query import BatchType
from cassandra.marshal import uint32_unpack
from cassandra.cluster import ContinuousPagingOptions
import pytest


class MessageTest(unittest.TestCase):

    def test_prepare_message(self):
        """
        Test to check the appropriate calls are made

        @since 3.9
        @jira_ticket PYTHON-713
        @expected_result the values are correctly written

        @test_category connection
        """
        message = PrepareMessage("a")
        io = Mock()

        message.send_body(io, 4)
        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',)])

        io.reset_mock()
        message.send_body(io, 5)

        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',), (b'\x00\x00\x00\x00',)])

    def test_execute_message(self):
        message = ExecuteMessage('1', [], 4)
        io = Mock()

        message.send_body(io, 4)
        self._check_calls(io, [(b'\x00\x01',), (b'1',), (b'\x00\x04',), (b'\x01',), (b'\x00\x00',)])

        io.reset_mock()
        message.result_metadata_id = 'foo'
        message.send_body(io, 5)

        self._check_calls(io, [(b'\x00\x01',), (b'1',),
                               (b'\x00\x03',), (b'foo',),
                               (b'\x00\x04',),
                               (b'\x00\x00\x00\x01',), (b'\x00\x00',)])

    def test_execute_message_skip_meta_flag(self):
        """skip_meta=True must set _SKIP_METADATA_FLAG (0x02) in the flags byte."""
        message = ExecuteMessage('1', [], 4, skip_meta=True)
        mock_io = Mock()

        message.send_body(mock_io, 4)
        # flags byte should be VALUES_FLAG | SKIP_METADATA_FLAG = 0x01 | 0x02 = 0x03
        self._check_calls(mock_io, [(b'\x00\x01',), (b'1',), (b'\x00\x04',), (b'\x03',), (b'\x00\x00',)])

    def test_execute_message_scylla_metadata_id_v4(self):
        """result_metadata_id should be written on protocol v4 when set (Scylla extension)."""
        message = ExecuteMessage('1', [], 4)
        message.result_metadata_id = b'foo'
        mock_io = Mock()

        message.send_body(mock_io, 4)
        # metadata_id written before query params (same position as v5)
        self._check_calls(mock_io, [(b'\x00\x01',), (b'1',),
                                    (b'\x00\x03',), (b'foo',),
                                    (b'\x00\x04',), (b'\x01',), (b'\x00\x00',)])

    def test_recv_results_prepared_scylla_extension_reads_metadata_id(self):
        """
        When use_metadata_id is True (Scylla extension), result_metadata_id must be
        read from the PREPARE response even for protocol v4.
        """
        # Build a minimal valid PREPARE response binary (no bind/result columns):
        #   query_id:          short(2) + b'ab'
        #   result_metadata_id: short(3) + b'xyz'  <-- only present when extension active
        #   prepared flags:    int(1) = global_tables_spec
        #   colcount:          int(0)
        #   num_pk_indexes:    int(0)
        #   ksname:            short(2) + b'ks'
        #   cfname:            short(2) + b'tb'
        #   result flags:      int(4) = no_metadata
        #   result colcount:   int(0)
        buf = io.BytesIO(
            struct.pack('>H', 2) + b'ab'   # query_id
            + struct.pack('>H', 3) + b'xyz'  # result_metadata_id
            + struct.pack('>i', 1)           # prepared flags: global_tables_spec
            + struct.pack('>i', 0)           # colcount = 0
            + struct.pack('>i', 0)           # num_pk_indexes = 0
            + struct.pack('>H', 2) + b'ks'  # ksname
            + struct.pack('>H', 2) + b'tb'  # cfname
            + struct.pack('>i', 4)           # result flags: no_metadata
            + struct.pack('>i', 0)           # result colcount = 0
        )

        features_with_extension = ProtocolFeatures(use_metadata_id=True)
        msg = ResultMessage(kind=4)  # RESULT_KIND_PREPARED = 4
        msg.recv_results_prepared(buf, protocol_version=4,
                                  protocol_features=features_with_extension,
                                  user_type_map={})
        assert msg.query_id == b'ab'
        assert msg.result_metadata_id == b'xyz'

    def test_recv_results_prepared_no_extension_skips_metadata_id(self):
        """
        Without use_metadata_id, result_metadata_id must NOT be read on protocol v4.
        The buffer must NOT contain a metadata_id field.
        """
        buf = io.BytesIO(
            struct.pack('>H', 2) + b'ab'   # query_id
            # no result_metadata_id
            + struct.pack('>i', 1)           # prepared flags: global_tables_spec
            + struct.pack('>i', 0)           # colcount = 0
            + struct.pack('>i', 0)           # num_pk_indexes = 0
            + struct.pack('>H', 2) + b'ks'  # ksname
            + struct.pack('>H', 2) + b'tb'  # cfname
            + struct.pack('>i', 4)           # result flags: no_metadata
            + struct.pack('>i', 0)           # result colcount = 0
        )

        features_without_extension = ProtocolFeatures(use_metadata_id=False)
        msg = ResultMessage(kind=4)
        msg.recv_results_prepared(buf, protocol_version=4,
                                  protocol_features=features_without_extension,
                                  user_type_map={})
        assert msg.query_id == b'ab'
        assert msg.result_metadata_id is None

    def test_recv_results_metadata_changed_flag(self):
        """
        When _METADATA_ID_FLAG (0x0008) is set in a ROWS result,
        recv_results_metadata must read and store the new result_metadata_id
        sent by the server (METADATA_CHANGED signal), and still populate
        column_metadata normally.
        """
        # Wire layout for a ROWS result with METADATA_CHANGED:
        #   flags:             int(0x0008)  = _METADATA_ID_FLAG
        #   colcount:          int(0)
        #   result_metadata_id: short(4) + b'new1'
        # (no columns — colcount=0 — to keep the buffer minimal)
        buf = io.BytesIO(
            struct.pack('>i', 0x0008)          # flags: METADATA_ID_FLAG
            + struct.pack('>i', 0)             # colcount = 0
            + struct.pack('>H', 4) + b'new1'   # result_metadata_id = b'new1'
        )
        msg = ResultMessage(kind=RESULT_KIND_ROWS)
        msg.recv_results_metadata(buf, user_type_map={})
        assert msg.result_metadata_id == b'new1'
        assert msg.column_metadata == []

    def test_recv_results_metadata_no_metadata_flag_skips_metadata_id(self):
        """
        When _NO_METADATA_FLAG (0x0004) is set, recv_results_metadata returns
        early and must NOT read or set result_metadata_id, even if the caller
        mistakenly sets _METADATA_ID_FLAG alongside it.
        """
        # flags = _NO_METADATA_FLAG (0x0004), colcount = 0
        buf = io.BytesIO(
            struct.pack('>i', 0x0004)  # flags: NO_METADATA
            + struct.pack('>i', 0)     # colcount = 0
        )
        msg = ResultMessage(kind=RESULT_KIND_ROWS)
        msg.recv_results_metadata(buf, user_type_map={})
        assert not hasattr(msg, 'result_metadata_id') or msg.result_metadata_id is None
        assert not hasattr(msg, 'column_metadata') or msg.column_metadata is None

    def test_query_message(self):
        """
        Test to check the appropriate calls are made

        @since 3.9
        @jira_ticket PYTHON-713
        @expected_result the values are correctly written

        @test_category connection
        """
        message = QueryMessage("a", 3)
        io = Mock()

        message.send_body(io, 4)
        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',), (b'\x00\x03',), (b'\x00',)])

        io.reset_mock()
        message.send_body(io, 5)
        self._check_calls(io, [(b'\x00\x00\x00\x01',), (b'a',), (b'\x00\x03',), (b'\x00\x00\x00\x00',)])

    def _check_calls(self, io, expected):
        assert tuple(c[1] for c in io.write.mock_calls) == tuple(expected)

    def test_prepare_flag(self):
        """
        Test to check the prepare flag is properly set, This should only happen for V5 at the moment.

        @since 3.9
        @jira_ticket PYTHON-694, PYTHON-713
        @expected_result the values are correctly written

        @test_category connection
        """
        message = PrepareMessage("a")
        io = Mock()
        for version in ProtocolVersion.SUPPORTED_VERSIONS:
            message.send_body(io, version)
            if ProtocolVersion.uses_prepare_flags(version):
                assert len(io.write.mock_calls) == 3
            else:
                assert len(io.write.mock_calls) == 2
            io.reset_mock()

    def test_prepare_flag_with_keyspace(self):
        message = PrepareMessage("a", keyspace='ks')
        io = Mock()

        for version in ProtocolVersion.SUPPORTED_VERSIONS:
            if ProtocolVersion.uses_keyspace_flag(version):
                message.send_body(io, version)
                self._check_calls(io, [
                    (b'\x00\x00\x00\x01',),
                    (b'a',),
                    (b'\x00\x00\x00\x01',),
                    (b'\x00\x02',),
                    (b'ks',),
                ])
            else:
                with pytest.raises(UnsupportedOperation):
                    message.send_body(io, version)
            io.reset_mock()

    def test_keyspace_flag_raises_before_v5(self):
        keyspace_message = QueryMessage('a', consistency_level=3, keyspace='ks')
        io = Mock(name='io')

        with pytest.raises(UnsupportedOperation, match='Keyspaces.*set'):
            keyspace_message.send_body(io, protocol_version=4)
        io.assert_not_called()

    def test_keyspace_written_with_length(self):
        io = Mock(name='io')
        base_expected = [
            (b'\x00\x00\x00\x01',),
            (b'a',),
            (b'\x00\x03',),
            (b'\x00\x00\x00\x80',),  # options w/ keyspace flag
        ]

        QueryMessage('a', consistency_level=3, keyspace='ks').send_body(
            io, protocol_version=5
        )
        self._check_calls(io, base_expected + [
            (b'\x00\x02',),  # length of keyspace string
            (b'ks',),
        ])

        io.reset_mock()

        QueryMessage('a', consistency_level=3, keyspace='keyspace').send_body(
            io, protocol_version=5
        )
        self._check_calls(io, base_expected + [
            (b'\x00\x08',),  # length of keyspace string
            (b'keyspace',),
        ])

    def test_batch_message_with_keyspace(self):
        self.maxDiff = None
        io = Mock(name='io')
        batch = BatchMessage(
            batch_type=BatchType.LOGGED,
            queries=((False, 'stmt a', ('param a',)),
                     (False, 'stmt b', ('param b',)),
                     (False, 'stmt c', ('param c',))
                     ),
            consistency_level=3,
            keyspace='ks'
        )
        batch.send_body(io, protocol_version=5)
        self._check_calls(io,
            ((b'\x00',), (b'\x00\x03',), (b'\x00',),
             (b'\x00\x00\x00\x06',), (b'stmt a',),
             (b'\x00\x01',), (b'\x00\x00\x00\x07',), ('param a',),
             (b'\x00',), (b'\x00\x00\x00\x06',), (b'stmt b',),
             (b'\x00\x01',), (b'\x00\x00\x00\x07',), ('param b',),
             (b'\x00',), (b'\x00\x00\x00\x06',), (b'stmt c',),
             (b'\x00\x01',), (b'\x00\x00\x00\x07',), ('param c',),
             (b'\x00\x03',),
             (b'\x00\x00\x00\x80',), (b'\x00\x02',), (b'ks',))
        )
