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

from typing import ClassVar
from unittest.mock import Mock

from cassandra import ConsistencyLevel, ProtocolVersion, UnsupportedOperation
from cassandra.protocol import (
    PrepareMessage, QueryMessage, ExecuteMessage,
    BatchMessage, StartupMessage, OptionsMessage, RegisterMessage,
    AuthResponseMessage, ProtocolHandler, _MessageType,
    ResultMessage, RESULT_KIND_ROWS
)
from cassandra.protocol_features import ProtocolFeatures
from cassandra.query import BatchType
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

    def test_execute_message_skip_meta_flag_with_extension(self):
        """
        skip_meta=True must set _SKIP_METADATA_FLAG (0x02) in the flags byte when
        the connection negotiated SCYLLA_USE_METADATA_ID, and the metadata id
        field must be written on the wire.
        """
        message = ExecuteMessage('1', [], 4, skip_meta=True, result_metadata_id=b'foo')
        mock_io = Mock()

        message.send_body(mock_io, 4, ProtocolFeatures(use_metadata_id=True))
        # flags byte should be VALUES_FLAG | SKIP_METADATA_FLAG = 0x01 | 0x02 = 0x03
        self._check_calls(mock_io, [(b'\x00\x01',), (b'1',),
                                    (b'\x00\x03',), (b'foo',),
                                    (b'\x00\x04',), (b'\x03',), (b'\x00\x00',)])

    def test_execute_message_skip_meta_suppressed_without_extension(self):
        """
        skip_meta=True must NOT reach the wire on a pre-v5 connection that did not
        negotiate SCYLLA_USE_METADATA_ID: without the metadata-id mechanism, a
        schema change after PREPARE would leave the driver decoding rows with
        stale cached metadata. The metadata id field must not be written either.
        """
        message = ExecuteMessage('1', [], 4, skip_meta=True, result_metadata_id=b'foo')
        mock_io = Mock()

        message.send_body(mock_io, 4)
        # flags byte contains only VALUES_FLAG; no metadata id field
        self._check_calls(mock_io, [(b'\x00\x01',), (b'1',), (b'\x00\x04',), (b'\x01',), (b'\x00\x00',)])

    def test_execute_message_v5_native_skip_meta_not_set(self):
        """
        On a native protocol v5 connection (no Scylla extension), skip_meta=True must
        NOT set _SKIP_METADATA_FLAG. Upstream never emitted the flag on any version, and
        this PR keeps native v5 byte-identical to upstream — enabling skip on native v5 is
        a separate, out-of-scope behavior change. The metadata id field is still written
        (it is part of the v5 EXECUTE frame layout), so only VALUES_FLAG is set.
        """
        message = ExecuteMessage('1', [], 4, skip_meta=True)
        mock_io = Mock()

        message.send_body(mock_io, 5)
        # v5 wire layout:
        #   query_id:           short(1) + b'1'
        #   result_metadata_id: short(0) + b''  (sentinel — None on init)
        #   consistency:        short(4) = ONE
        #   flags (4-byte int): VALUES_FLAG(0x01) only — skip is NOT set on native v5
        #   param count:        short(0)
        self._check_calls(mock_io, [
            (b'\x00\x01',), (b'1',),
            (b'\x00\x00',), (b'',),
            (b'\x00\x04',),
            (b'\x00\x00\x00\x01',), (b'\x00\x00',),
        ])

    def test_execute_message_v5_with_extension_sets_skip_flag(self):
        """
        skip is extension-driven, not version-driven: on a v5 connection that ALSO
        negotiated SCYLLA_USE_METADATA_ID, skip_meta=True does set _SKIP_METADATA_FLAG.
        This also confirms _SKIP_METADATA_FLAG actually reaches the wire (it was dead code
        upstream) whenever the extension gates it on.
        """
        message = ExecuteMessage('1', [], 4, skip_meta=True)
        mock_io = Mock()

        message.send_body(mock_io, 5, ProtocolFeatures(use_metadata_id=True))
        # flags (4-byte int): VALUES_FLAG(0x01) | SKIP_METADATA_FLAG(0x02) = 0x03
        self._check_calls(mock_io, [
            (b'\x00\x01',), (b'1',),
            (b'\x00\x00',), (b'',),
            (b'\x00\x04',),
            (b'\x00\x00\x00\x03',), (b'\x00\x00',),
        ])

    def test_execute_message_scylla_metadata_id_v4(self):
        """result_metadata_id should be written on protocol v4 when the connection negotiated the Scylla extension."""
        message = ExecuteMessage('1', [], 4, result_metadata_id=b'foo')
        mock_io = Mock()

        message.send_body(mock_io, 4, ProtocolFeatures(use_metadata_id=True))
        # metadata_id written before query params (same position as v5)
        self._check_calls(mock_io, [(b'\x00\x01',), (b'1',),
                                    (b'\x00\x03',), (b'foo',),
                                    (b'\x00\x04',), (b'\x01',), (b'\x00\x00',)])

    def test_execute_message_scylla_metadata_id_none_writes_sentinel(self):
        """
        When the connection negotiated the extension but result_metadata_id is None
        (e.g. LWT statement or mixed cluster), send_body must still write the field
        as an empty string sentinel (\\x00\\x00) so the frame layout matches what
        the server expects.
        """
        message = ExecuteMessage('1', [], 4)
        # result_metadata_id intentionally left as None
        mock_io = Mock()

        message.send_body(mock_io, 4, ProtocolFeatures(use_metadata_id=True))
        # empty sentinel: \x00\x00 (zero-length short) + b'' (zero bytes), then normal query params
        self._check_calls(mock_io, [(b'\x00\x01',), (b'1',),
                                    (b'\x00\x00',), (b'',),
                                    (b'\x00\x04',), (b'\x01',), (b'\x00\x00',)])

    def test_execute_message_v5_metadata_id_none_writes_sentinel(self):
        """
        On protocol v5, result_metadata_id is always written (uses_prepared_metadata).
        When result_metadata_id is None (e.g. LWT statement or mixed cluster where the
        statement was prepared before the extension was active), send_body must write an
        empty sentinel instead of crashing with TypeError.
        """
        message = ExecuteMessage('1', [], 4)
        # result_metadata_id intentionally left as None; use_metadata_id stays False (v5 native path)
        mock_io = Mock()

        message.send_body(mock_io, 5)
        # v5 always writes metadata_id: None → empty sentinel \x00\x00 + b'', then query params
        # v5 uses 4-byte flags: VALUES_FLAG = \x00\x00\x00\x01
        self._check_calls(mock_io, [(b'\x00\x01',), (b'1',),
                                    (b'\x00\x00',), (b'',),
                                    (b'\x00\x04',),
                                    (b'\x00\x00\x00\x01',), (b'\x00\x00',)])

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

    def test_recv_results_prepared_v5_reads_metadata_id(self):
        """
        On protocol v5, ProtocolVersion.uses_prepared_metadata() is True, so
        result_metadata_id must be read from the PREPARE response even when
        use_metadata_id is False (native v5 path, not the Scylla extension).
        """
        buf = io.BytesIO(
            struct.pack('>H', 2) + b'ab'   # query_id
            + struct.pack('>H', 3) + b'xyz'  # result_metadata_id (always present on v5)
            + struct.pack('>i', 1)           # prepared flags: global_tables_spec
            + struct.pack('>i', 0)           # colcount = 0
            + struct.pack('>i', 0)           # num_pk_indexes = 0
            + struct.pack('>H', 2) + b'ks'  # ksname
            + struct.pack('>H', 2) + b'tb'  # cfname
            + struct.pack('>i', 4)           # result flags: no_metadata
            + struct.pack('>i', 0)           # result colcount = 0
        )

        features_no_extension = ProtocolFeatures(use_metadata_id=False)
        msg = ResultMessage(kind=4)  # RESULT_KIND_PREPARED = 4
        msg.recv_results_prepared(buf, protocol_version=5,
                                  protocol_features=features_no_extension,
                                  user_type_map={})
        assert msg.query_id == b'ab'
        assert msg.result_metadata_id == b'xyz'

    def test_recv_results_metadata_reads_metadata_id_on_change(self):
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
        # recv_results_metadata returns early on NO_METADATA; result_metadata_id
        # must never be set as an instance attribute (it is not a class default).
        # column_metadata is a class attribute defaulting to None and must remain so.
        assert not hasattr(msg, 'result_metadata_id')
        assert msg.column_metadata is None

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


class ProtocolFeaturesPlumbingTest(unittest.TestCase):
    """
    The negotiated ProtocolFeatures must flow from encode_message into each
    message's send_body, so serialization can emit fields belonging to
    protocol extensions exactly on the connections that negotiated them.
    """

    class CapturingMessage(_MessageType):
        opcode = 0x00
        name = 'CAPTURE'

        def __init__(self):
            self.seen_features = []

        def send_body(self, f, protocol_version, protocol_features=None):
            self.seen_features.append(protocol_features)

    def test_encode_message_forwards_protocol_features_to_send_body(self):
        features = ProtocolFeatures()
        msg = self.CapturingMessage()
        ProtocolHandler.encode_message(msg, stream_id=0, protocol_version=4, compressor=None,
                                       allow_beta_protocol_version=False, protocol_features=features)
        assert msg.seen_features == [features]
        assert msg.seen_features[0] is features

    def test_encode_message_forwards_protocol_features_when_compressing(self):
        features = ProtocolFeatures()
        msg = self.CapturingMessage()
        ProtocolHandler.encode_message(msg, stream_id=0, protocol_version=4, compressor=lambda body: body,
                                       allow_beta_protocol_version=False, protocol_features=features)
        assert msg.seen_features[0] is features

    def test_encode_message_fails_without_protocol_features(self):
        msg = self.CapturingMessage()

        with pytest.raises(TypeError, match='positional argument'):
            ProtocolHandler.encode_message(msg, stream_id=0, protocol_version=4, compressor=None,
                                       allow_beta_protocol_version=False)


class FrameByteIdentityTest(unittest.TestCase):
    """
    Threading ProtocolFeatures into serialization is pure plumbing: with no
    extension consuming it (and for all-default features), every frame must be
    byte-identical to what the driver produced before the parameter existed.
    The expected frames below were captured from the pre-change encoder.
    """

    EXPECTED_FRAMES: ClassVar[dict] = {
        'startup_v4': '0400000701000000160001000b43514c5f56455253494f4e0005332e342e35',
        'options_v4': '040000070500000000',
        'register_v4': '040000070b000000220002000f544f504f4c4f47595f4348414e4745000d5354415455535f4348414e4745',
        'auth_response_v4': '040000070f0000000e0000000a00757365720070617373',
        'prepare_v4': '0400000709000000220000001e53454c454354202a2046524f4d206b732e74205748455245206b203d203f',
        'prepare_v5_keyspace': '0500000709000000270000001b53454c454354202a2046524f4d2074205748455245206b203d203f0000000100026b73',
        'query_v3': '0300000707000000270000001253454c454354202a2046524f4d206b732e74000434000013880008000462d53c8abac0',
        'execute_v3': '030000070a00000033000412345678000a2d000200000002000100000003616263000000640000000b504147494e475354415445000000003ade68b1',
        'batch_v3': '030000070d00000043000002000000001f494e5345525420494e544f206b732e7420286b292056414c5545532028312900000100041234567800010000000200020001200000000006a11e3d',
        'query_v4': '0400000707000000270000001253454c454354202a2046524f4d206b732e74000434000013880008000462d53c8abac0',
        'execute_v4': '040000070a00000033000412345678000a2d000200000002000100000003616263000000640000000b504147494e475354415445000000003ade68b1',
        'batch_v4': '040000070d00000043000002000000001f494e5345525420494e544f206b732e7420286b292056414c5545532028312900000100041234567800010000000200020001200000000006a11e3d',
        'query_v5': '05000007070000002a0000001253454c454354202a2046524f4d206b732e74000400000034000013880008000462d53c8abac0',
        'execute_v5': '050000070a0000003c0004123456780004aabbccdd000a0000002d000200000002000100000003616263000000640000000b504147494e475354415445000000003ade68b1',
        'batch_v5': '050000070d00000046000002000000001f494e5345525420494e544f206b732e7420286b292056414c5545532028312900000100041234567800010000000200020001000000200000000006a11e3d',
    }

    @staticmethod
    def _make_cases():
        cases = [
            ('startup_v4', StartupMessage(cqlversion="3.4.5", options={}), 4),
            ('options_v4', OptionsMessage(), 4),
            ('register_v4', RegisterMessage(["TOPOLOGY_CHANGE", "STATUS_CHANGE"]), 4),
            ('auth_response_v4', AuthResponseMessage(b"\x00user\x00pass"), 4),
            ('prepare_v4', PrepareMessage("SELECT * FROM ks.t WHERE k = ?"), 4),
            ('prepare_v5_keyspace', PrepareMessage("SELECT * FROM t WHERE k = ?", keyspace="ks"), 5),
        ]
        for pv in (3, 4, 5):
            cases.append((
                'query_v%d' % pv,
                QueryMessage("SELECT * FROM ks.t", ConsistencyLevel.QUORUM,
                             serial_consistency_level=ConsistencyLevel.SERIAL,
                             fetch_size=5000, timestamp=1234567890123456),
                pv,
            ))
            cases.append((
                'execute_v%d' % pv,
                ExecuteMessage(b"\x12\x34\x56\x78", [b"\x00\x01", b"abc"],
                               ConsistencyLevel.LOCAL_ONE, fetch_size=100,
                               paging_state=b"PAGINGSTATE",
                               result_metadata_id=b"\xaa\xbb\xcc\xdd" if pv >= 5 else None,
                               timestamp=987654321),
                pv,
            ))
            cases.append((
                'batch_v%d' % pv,
                BatchMessage(BatchType.LOGGED,
                             [(False, "INSERT INTO ks.t (k) VALUES (1)", []),
                              (True, b"\x12\x34\x56\x78", [b"\x00\x02"])],
                             ConsistencyLevel.ONE, timestamp=111222333),
                pv,
            ))
        return cases

    def _assert_frames(self, protocol_features):
        for name, msg, pv in self._make_cases():
            frame = ProtocolHandler.encode_message(
                msg, stream_id=7, protocol_version=pv, compressor=None,
                allow_beta_protocol_version=False, protocol_features=protocol_features)
            assert frame.hex() == self.EXPECTED_FRAMES[name], name

    def test_frames_without_features(self):
        self._assert_frames(None)

    def test_frames_with_default_features(self):
        self._assert_frames(ProtocolFeatures())
