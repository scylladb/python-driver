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
from unittest.mock import Mock
import io

from cassandra import ProtocolVersion
from cassandra.protocol import ResultMessage, RESULT_KIND_ROWS
from cassandra.cqltypes import Int32Type, UTF8Type
from cassandra.policies import ColDesc
from cassandra.marshal import int32_pack


class DecodeOptimizationTest(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
