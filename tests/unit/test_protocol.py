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
from io import BytesIO

from unittest.mock import Mock

from cassandra import ProtocolVersion, UnsupportedOperation
from cassandra.protocol import (
    PrepareMessage,
    QueryMessage,
    ExecuteMessage,
    UnsupportedOperation,
    _PAGING_OPTIONS_FLAG,
    _WITH_SERIAL_CONSISTENCY_FLAG,
    _PAGE_SIZE_FLAG,
    _WITH_PAGING_STATE_FLAG,
    BatchMessage,
    BytesReader,
    ProtocolHandler,
    SupportedMessage,
    ReadyMessage,
    write_stringmultimap,
)
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
        self._check_calls(
            io, [(b"\x00\x01",), (b"1",), (b"\x00\x04",), (b"\x01",), (b"\x00\x00",)]
        )

        io.reset_mock()
        message.result_metadata_id = 'foo'
        message.send_body(io, 5)

        self._check_calls(
            io,
            [
                (b"\x00\x01",),
                (b"1",),
                (b'\x00\x03',),
                (b"foo",),
                (b'\x00\x04',),
                (b'\x00\x00\x00\x01',),
                (b"\x00\x00",),
            ],
        )

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
        self._check_calls(
            io, [(b"\x00\x00\x00\x01",), (b"a",), (b"\x00\x03",), (b"\x00",)]
        )

        io.reset_mock()
        message.send_body(io, 5)
        self._check_calls(
            io,
            [(b"\x00\x00\x00\x01",), (b"a",), (b"\x00\x03",), (b"\x00\x00\x00\x00",)],
        )

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
                self._check_calls(
                    io,
                    [
                        (b'\x00\x00\x00\x01',),
                        (b'a',),
                        (b'\x00\x00\x00\x01',),
                        (b'\x00\x02',),
                        (b'ks',),
                    ],
                )
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
        self._check_calls(
            io,
            base_expected
            + [
                (b'\x00\x02',),  # length of keyspace string
                (b'ks',),
            ],
        )

        io.reset_mock()

        QueryMessage('a', consistency_level=3, keyspace='keyspace').send_body(
            io, protocol_version=5
        )
        self._check_calls(
            io,
            base_expected
            + [
                (b'\x00\x08',),  # length of keyspace string
                (b'keyspace',),
            ],
        )

    def test_batch_message_with_keyspace(self):
        self.maxDiff = None
        io = Mock(name='io')
        batch = BatchMessage(
            batch_type=BatchType.LOGGED,
            queries=(
                (False, "stmt a", ("param a",)),
                (False, 'stmt b', ('param b',)),
                (False, "stmt c", ("param c",)),
            ),
            consistency_level=3,
            keyspace="ks",
        )
        batch.send_body(io, protocol_version=5)
        self._check_calls(
            io,
            (
                (b"\x00",),
                (b"\x00\x03",),
                (b"\x00",),
                (b"\x00\x00\x00\x06",),
                (b"stmt a",),
                (b"\x00\x01",),
                (b"\x00\x00\x00\x07",),
                ("param a",),
                (b"\x00",),
                (b"\x00\x00\x00\x06",),
                (b"stmt b",),
                (b"\x00\x01",),
                (b"\x00\x00\x00\x07",),
                ("param b",),
                (b"\x00",),
                (b"\x00\x00\x00\x06",),
                (b"stmt c",),
                (b"\x00\x01",),
                (b"\x00\x00\x00\x07",),
                ("param c",),
                (b"\x00\x03",),
                (b"\x00\x00\x00\x80",),
                (b"\x00\x02",),
                (b"ks",),
            ),
        )


class BytesReaderTest(unittest.TestCase):
    """Tests for the BytesReader class used in decode_message."""

    def test_read_exact(self):
        r = BytesReader(b"abcdef")
        self.assertEqual(r.read(3), b"abc")
        self.assertEqual(r.read(3), b"def")

    def test_read_sequential(self):
        r = BytesReader(b"\x00\x01\x02\x03")
        self.assertEqual(r.read(1), b"\x00")
        self.assertEqual(r.read(2), b"\x01\x02")
        self.assertEqual(r.read(1), b"\x03")

    def test_read_zero_bytes(self):
        r = BytesReader(b"abc")
        self.assertEqual(r.read(0), b"")
        self.assertEqual(r.read(3), b"abc")

    def test_read_all_no_args(self):
        r = BytesReader(b"hello")
        self.assertEqual(r.read(), b"hello")

    def test_read_all_negative(self):
        r = BytesReader(b"hello")
        self.assertEqual(r.read(-1), b"hello")

    def test_read_all_after_partial(self):
        r = BytesReader(b"hello world")
        r.read(6)
        self.assertEqual(r.read(), b"world")

    def test_read_past_end_raises(self):
        r = BytesReader(b"abc")
        with self.assertRaises(EOFError):
            r.read(4)

    def test_read_past_end_after_partial(self):
        r = BytesReader(b"abc")
        r.read(2)
        with self.assertRaises(EOFError):
            r.read(2)

    def test_empty_data(self):
        r = BytesReader(b"")
        self.assertEqual(r.read(), b"")
        self.assertEqual(r.read(0), b"")
        with self.assertRaises(EOFError):
            r.read(1)

    def test_memoryview_input(self):
        data = b"hello world"
        r = BytesReader(memoryview(data))
        result = r.read(5)
        self.assertIsInstance(result, bytes)
        self.assertEqual(result, b"hello")

    def test_return_type_is_bytes(self):
        r = BytesReader(b"\x00\x01\x02")
        result = r.read(3)
        self.assertIsInstance(result, bytes)

    def test_remaining_buffer(self):
        r = BytesReader(b"header_row_data")
        r.read(7)  # consume "header_"
        buf, pos = r.remaining_buffer()
        self.assertEqual(buf, b"header_row_data")
        self.assertEqual(pos, 7)
        self.assertEqual(buf[pos:], b"row_data")

    def test_remaining_buffer_at_start(self):
        r = BytesReader(b"all_data")
        buf, pos = r.remaining_buffer()
        self.assertEqual(pos, 0)
        self.assertEqual(buf, b"all_data")


class DecodeMessageTest(unittest.TestCase):
    """
    End-to-end tests for ProtocolHandler.decode_message using BytesReader.

    These verify that real message types round-trip through the decode path
    that now uses BytesReader instead of io.BytesIO.
    """

    def _decode(self, opcode, body):
        return ProtocolHandler.decode_message(
            protocol_version=ProtocolVersion.MAX_SUPPORTED,
            protocol_features=None,
            user_type_map={},
            stream_id=0,
            flags=0,
            opcode=opcode,
            body=body,
            decompressor=None,
            result_metadata=None,
        )

    def test_ready_message_empty_body(self):
        """ReadyMessage has an empty body (opcode 0x02)."""
        msg = self._decode(0x02, b"")
        self.assertIsInstance(msg, ReadyMessage)
        self.assertEqual(msg.stream_id, 0)
        self.assertIsNone(msg.trace_id)
        self.assertIsNone(msg.custom_payload)

    def test_supported_message_with_body(self):
        """SupportedMessage reads a stringmultimap from body (opcode 0x06)."""
        buf = BytesIO()
        write_stringmultimap(
            buf,
            {
                "CQL_VERSION": ["3.4.5"],
                "COMPRESSION": ["lz4", "snappy"],
            },
        )
        body = buf.getvalue()
        msg = self._decode(0x06, body)
        self.assertIsInstance(msg, SupportedMessage)
        self.assertEqual(msg.cql_versions, ["3.4.5"])
        self.assertEqual(msg.options["COMPRESSION"], ["lz4", "snappy"])

    def test_decode_with_memoryview_body(self):
        """decode_message should accept a memoryview body (BytesReader materializes it)."""
        buf = BytesIO()
        write_stringmultimap(buf, {"CQL_VERSION": ["3.0.0"]})
        body = memoryview(buf.getvalue())
        msg = self._decode(0x06, body)
        self.assertIsInstance(msg, SupportedMessage)
        self.assertEqual(msg.cql_versions, ["3.0.0"])
