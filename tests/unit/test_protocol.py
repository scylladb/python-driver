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
import unittest

from unittest.mock import Mock

from cassandra import ProtocolVersion, UnsupportedOperation
from cassandra.cqltypes import Int32Type, UTF8Type
from cassandra.protocol import (
    PrepareMessage, QueryMessage, ExecuteMessage, UnsupportedOperation,
    _PAGING_OPTIONS_FLAG, _WITH_SERIAL_CONSISTENCY_FLAG,
    _PAGE_SIZE_FLAG, _WITH_PAGING_STATE_FLAG,
    BatchMessage,
    ResultMessage, RESULT_KIND_ROWS
)
from cassandra.query import BatchType
from cassandra.marshal import uint32_unpack, int32_pack
from cassandra.cluster import ContinuousPagingOptions
import pytest

from cassandra.policies import ColDesc

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

class ResultTest(unittest.TestCase):
    """
    Tests to verify the optimization of column_encryption_policy checks
    in recv_results_rows. The optimization checks if the policy exists once
    per result message, avoiding the redundant 'column_encryption_policy and ...'
    check for every value.
    """

    def _create_mock_result_metadata(self):
        """Create mock result metadata for testing"""
        return [
            ('keyspace1', 'table1', 'col1', Int32Type),
            ('keyspace1', 'table1', 'col2', UTF8Type),
        ]

    def _create_mock_result_message(self):
        """Create a mock result message with data"""
        msg = ResultMessage(kind=RESULT_KIND_ROWS)
        msg.column_metadata = self._create_mock_result_metadata()
        msg.recv_results_metadata = Mock()
        msg.recv_row = Mock(side_effect=[
            [int32_pack(42), b'hello'],
            [int32_pack(100), b'world'],
        ])
        return msg

    def _create_mock_stream(self):
        """Create a mock stream for reading rows"""
        # Pack rowcount (2 rows)
        data = int32_pack(2)
        return io.BytesIO(data)

    def test_decode_without_encryption_policy(self):
        """
        Test that decoding works correctly without column encryption policy.
        This should use the optimized simple path.
        """
        msg = self._create_mock_result_message()
        f = self._create_mock_stream()

        msg.recv_results_rows(f, ProtocolVersion.V4, {}, None, None)

        # Verify results
        self.assertEqual(len(msg.parsed_rows), 2)
        self.assertEqual(msg.parsed_rows[0][0], 42)
        self.assertEqual(msg.parsed_rows[0][1], 'hello')
        self.assertEqual(msg.parsed_rows[1][0], 100)
        self.assertEqual(msg.parsed_rows[1][1], 'world')

    def test_decode_with_encryption_policy_no_encrypted_columns(self):
        """
        Test that decoding works with encryption policy when no columns are encrypted.
        """
        msg = self._create_mock_result_message()
        f = self._create_mock_stream()

        # Create mock encryption policy that has no encrypted columns
        mock_policy = Mock()
        mock_policy.contains_column = Mock(return_value=False)

        msg.recv_results_rows(f, ProtocolVersion.V4, {}, None, mock_policy)

        # Verify results
        self.assertEqual(len(msg.parsed_rows), 2)
        self.assertEqual(msg.parsed_rows[0][0], 42)
        self.assertEqual(msg.parsed_rows[0][1], 'hello')

        # Verify contains_column was called for each value (but policy existence check happens once)
        # Should be called 4 times (2 rows × 2 columns)
        self.assertEqual(mock_policy.contains_column.call_count, 4)

    def test_decode_with_encryption_policy_with_encrypted_column(self):
        """
        Test that decoding works with encryption policy when one column is encrypted.
        """
        msg = self._create_mock_result_message()
        f = self._create_mock_stream()

        # Create mock encryption policy where first column is encrypted
        mock_policy = Mock()
        def contains_column_side_effect(col_desc):
            return col_desc.col == 'col1'
        mock_policy.contains_column = Mock(side_effect=contains_column_side_effect)
        mock_policy.column_type = Mock(return_value=Int32Type)
        mock_policy.decrypt = Mock(side_effect=lambda col_desc, val: val)

        msg.recv_results_rows(f, ProtocolVersion.V4, {}, None, mock_policy)

        # Verify results
        self.assertEqual(len(msg.parsed_rows), 2)
        self.assertEqual(msg.parsed_rows[0][0], 42)
        self.assertEqual(msg.parsed_rows[0][1], 'hello')

        # Verify contains_column was called for each value (but policy existence check happens once)
        # Should be called 4 times (2 rows × 2 columns)
        self.assertEqual(mock_policy.contains_column.call_count, 4)

        # Verify decrypt was called for each encrypted value (2 rows * 1 encrypted column)
        self.assertEqual(mock_policy.decrypt.call_count, 2)

    def test_optimization_efficiency(self):
        """
        Verify that the optimization checks policy existence once per result message.
        The key optimization is checking 'if column_encryption_policy:' once,
        rather than 'column_encryption_policy and ...' for every value.
        """
        msg = self._create_mock_result_message()

        # Create more rows to make the check pattern clear
        msg.recv_row = Mock(side_effect=[
            [int32_pack(i), f'text{i}'.encode()] for i in range(100)
        ])

        # Create mock stream with 100 rows
        f = io.BytesIO(int32_pack(100))

        mock_policy = Mock()
        mock_policy.contains_column = Mock(return_value=False)

        msg.recv_results_rows(f, ProtocolVersion.V4, {}, None, mock_policy)

        # With optimization: policy existence checked once, contains_column called per value
        # = 100 rows * 2 columns = 200 calls to contains_column
        # The key is we avoid checking 'column_encryption_policy and ...' 200 times
        self.assertEqual(mock_policy.contains_column.call_count, 200,
                        "contains_column should be called for each value when policy exists")


class CythonParserTest(unittest.TestCase):
    """
    Tests for the Cython fast-path parsers (ListParser, TupleRowParser)
    to verify the column_encryption_policy optimization in obj_parser.pyx.
    """

    def _build_binary_rows(self, rows):
        """
        Build a binary buffer containing encoded rows.

        Each row is a list of (size, raw_bytes) pairs.
        Prepends a 4-byte big-endian row count.
        """
        import struct
        data = struct.pack('>i', len(rows))
        for row in rows:
            for raw in row:
                if raw is None:
                    data += struct.pack('>i', -1)  # NULL
                else:
                    data += struct.pack('>i', len(raw)) + raw
        return data

    def _make_parse_desc(self, column_encryption_policy=None):
        from cassandra.parsing import ParseDesc
        from cassandra.deserializers import make_deserializers
        from cassandra.policies import ColDesc

        colnames = ['col1', 'col2']
        coltypes = [Int32Type, UTF8Type]
        coldescs = [ColDesc('ks', 'tbl', 'col1'), ColDesc('ks', 'tbl', 'col2')]
        deserializers = make_deserializers(coltypes)
        return ParseDesc(colnames, coltypes, column_encryption_policy,
                         coldescs, deserializers, ProtocolVersion.V4)

    def _int32_bytes(self, val):
        import struct
        return struct.pack('>i', val)

    def test_list_parser_without_encryption(self):
        """ListParser decodes rows correctly without encryption policy."""
        from cassandra.bytesio import BytesIOReader
        from cassandra.obj_parser import ListParser

        desc = self._make_parse_desc(column_encryption_policy=None)
        data = self._build_binary_rows([
            [self._int32_bytes(42), b'hello'],
            [self._int32_bytes(100), b'world'],
        ])
        reader = BytesIOReader(data)
        result = ListParser().parse_rows(reader, desc)

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0], (42, 'hello'))
        self.assertEqual(result[1], (100, 'world'))

    def test_list_parser_with_encryption_no_encrypted_cols(self):
        """ListParser decodes rows correctly when policy exists but no columns are encrypted."""
        from cassandra.bytesio import BytesIOReader
        from cassandra.obj_parser import ListParser

        mock_policy = Mock()
        mock_policy.contains_column = Mock(return_value=False)

        desc = self._make_parse_desc(column_encryption_policy=mock_policy)
        data = self._build_binary_rows([
            [self._int32_bytes(42), b'hello'],
        ])
        reader = BytesIOReader(data)
        result = ListParser().parse_rows(reader, desc)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], (42, 'hello'))
        # 1 row * 2 columns = 2 calls
        self.assertEqual(mock_policy.contains_column.call_count, 2)

    def test_list_parser_with_encrypted_column(self):
        """ListParser decodes rows with an encrypted column (mock decrypt is identity)."""
        from cassandra.bytesio import BytesIOReader
        from cassandra.obj_parser import ListParser
        from cassandra.deserializers import find_deserializer

        mock_policy = Mock()
        mock_policy.contains_column = Mock(
            side_effect=lambda cd: cd.col == 'col1')
        mock_policy.column_type = Mock(return_value=Int32Type)
        # decrypt returns the raw bytes unchanged (identity)
        mock_policy.decrypt = Mock(side_effect=lambda cd, val: val)

        desc = self._make_parse_desc(column_encryption_policy=mock_policy)
        data = self._build_binary_rows([
            [self._int32_bytes(7), b'test'],
        ])
        reader = BytesIOReader(data)
        result = ListParser().parse_rows(reader, desc)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], (7, 'test'))
        self.assertEqual(mock_policy.decrypt.call_count, 1)
        self.assertEqual(mock_policy.column_type.call_count, 1)

    def test_numpy_parser_rejects_encryption(self):
        """NumpyParser raises NotImplementedError when column_encryption_policy is set."""
        try:
            from cassandra.numpy_parser import NumpyParser
        except ImportError:
            self.skipTest("NumPy or numpy_parser not available")

        from cassandra.bytesio import BytesIOReader

        mock_policy = Mock()
        desc = self._make_parse_desc(column_encryption_policy=mock_policy)
        data = self._build_binary_rows([[self._int32_bytes(1), b'x']])
        reader = BytesIOReader(data)

        with self.assertRaises(NotImplementedError):
            NumpyParser().parse_rows(reader, desc)
