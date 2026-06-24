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
import struct

from cassandra import ProtocolVersion, UnsupportedOperation
from cassandra.protocol import (
    PrepareMessage, QueryMessage, ExecuteMessage, UnsupportedOperation,
    _PAGING_OPTIONS_FLAG, _WITH_SERIAL_CONSISTENCY_FLAG,
    _PAGE_SIZE_FLAG, _WITH_PAGING_STATE_FLAG,
    BatchMessage,
    _UNSET_VALUE, write_value, ProtocolHandler
)
from cassandra.query import BatchType
from cassandra.marshal import uint32_unpack, int32_pack, uint16_pack
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

class WriteQueryParamsBufferAccumulationTest(unittest.TestCase):
    """
    Tests for the buffer accumulation optimization in
    _QueryMessage._write_query_params().

    The optimization replaces per-parameter write_value(f, param) calls with
    list.append + b"".join + single f.write().  These tests verify the
    serialized bytes are identical to the original write_value() behaviour.
    """

    # -- helpers ----------------------------------------------------------

    @staticmethod
    def _reference_write_value_bytes(params):
        """Build expected bytes using the original write_value() function."""
        buf = io.BytesIO()
        buf.write(uint16_pack(len(params)))
        for p in params:
            write_value(buf, p)
        return buf.getvalue()

    @staticmethod
    def _execute_msg_bytes(msg, protocol_version):
        """Serialize an ExecuteMessage and return the raw bytes."""
        buf = io.BytesIO()
        msg.send_body(buf, protocol_version)
        return buf.getvalue()

    # -- basic write_value parity -----------------------------------------

    def test_normal_params(self):
        """Normal (non-NULL, non-UNSET) byte-string parameters."""
        params = [b'hello', b'world', b'\x00\x01\x02']
        expected = self._reference_write_value_bytes(params)
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=1)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)

    def test_null_params(self):
        """NULL parameters must serialize as int32(-1)."""
        params = [None, None]
        expected = self._reference_write_value_bytes(params)
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=1)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)

    def test_unset_params(self):
        """UNSET parameters must serialize as int32(-2)."""
        params = [_UNSET_VALUE, _UNSET_VALUE]
        expected = self._reference_write_value_bytes(params)
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=4)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)

    def test_mixed_params(self):
        """Mix of normal, NULL and UNSET params in one message."""
        params = [b'data', None, _UNSET_VALUE, b'more', None]
        expected = self._reference_write_value_bytes(params)
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=1)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)

    def test_empty_bytes_param(self):
        """An empty bytes value (length 0) must differ from NULL (length -1)."""
        params = [b'']
        expected = self._reference_write_value_bytes(params)
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=1)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)
        # Verify it's NOT serialized as NULL
        null_bytes = int32_pack(-1)
        param_section_start = raw.find(expected)
        param_section = raw[param_section_start:param_section_start + len(expected)]
        self.assertNotIn(null_bytes, param_section[2:])  # skip the uint16 count

    def test_empty_query_params_list(self):
        """An empty params list should write count=0 and nothing else."""
        params = []
        expected = self._reference_write_value_bytes(params)
        self.assertEqual(expected, uint16_pack(0))
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=1)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)

    def test_none_query_params(self):
        """When query_params is None, no param block should be written."""
        msg1 = ExecuteMessage(query_id=b'qid', query_params=None,
                              consistency_level=1)
        msg2 = ExecuteMessage(query_id=b'qid', query_params=[b'x'],
                              consistency_level=1)
        raw1 = self._execute_msg_bytes(msg1, protocol_version=4)
        raw2 = self._execute_msg_bytes(msg2, protocol_version=4)
        # raw1 should be shorter (no param section)
        self.assertLess(len(raw1), len(raw2))

    def test_large_vector_param(self):
        """Large parameter simulating a high-dimensional vector embedding."""
        # 768-dimensional float32 vector = 3072 bytes
        vector_bytes = struct.pack('768f', *([0.123456] * 768))
        params = [vector_bytes]
        expected = self._reference_write_value_bytes(params)
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=1)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)

    def test_query_message_with_params(self):
        """QueryMessage (not just ExecuteMessage) uses the same code path."""
        params = [b'val1', None, b'val2']
        expected = self._reference_write_value_bytes(params)
        msg = QueryMessage(query='SELECT * FROM t WHERE k=? AND v=? AND w=?',
                           consistency_level=1,
                           query_params=params)
        raw = io.BytesIO()
        msg.send_body(raw, protocol_version=4)
        self.assertIn(expected, raw.getvalue())

    def test_proto_v3_vs_v4_params(self):
        """The param encoding should be identical across protocol versions."""
        params = [b'abc', None, b'xyz']
        msg_v3 = ExecuteMessage(query_id=b'qid', query_params=params,
                                consistency_level=1)
        msg_v4 = ExecuteMessage(query_id=b'qid', query_params=params,
                                consistency_level=1)
        raw_v3 = self._execute_msg_bytes(msg_v3, protocol_version=3)
        raw_v4 = self._execute_msg_bytes(msg_v4, protocol_version=4)
        expected = self._reference_write_value_bytes(params)
        self.assertIn(expected, raw_v3)
        self.assertIn(expected, raw_v4)

    def test_encode_message_roundtrip(self):
        """Full encode_message path exercises header + body framing."""
        params = [b'roundtrip']
        msg = QueryMessage(query='SELECT 1',
                           consistency_level=1,
                           query_params=params)
        # encode_message returns the full on-wire frame
        frame = ProtocolHandler.encode_message(msg, stream_id=1,
                                               protocol_version=4,
                                               compressor=None,
                                               allow_beta_protocol_version=False)
        # The frame should contain the param bytes somewhere inside
        expected_param_bytes = self._reference_write_value_bytes(params)
        # frame may be memoryview/bytearray; convert to bytes for assertIn
        frame_bytes = bytes(frame)
        self.assertIn(expected_param_bytes, frame_bytes)

    def test_many_params(self):
        """50 parameters to exercise the accumulation loop at scale."""
        params = [b'param_%03d' % i for i in range(50)]
        expected = self._reference_write_value_bytes(params)
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=1)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)

    def test_single_null_param(self):
        """Regression: a single NULL param should serialize correctly."""
        params = [None]
        expected = self._reference_write_value_bytes(params)
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=1)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)

    def test_single_unset_param(self):
        """Regression: a single UNSET param should serialize correctly."""
        params = [_UNSET_VALUE]
        expected = self._reference_write_value_bytes(params)
        msg = ExecuteMessage(query_id=b'qid', query_params=params,
                             consistency_level=4)
        raw = self._execute_msg_bytes(msg, protocol_version=4)
        self.assertIn(expected, raw)

