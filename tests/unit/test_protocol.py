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

from cassandra import ConsistencyLevel, ProtocolVersion, UnsupportedOperation
from cassandra.protocol import (
    PrepareMessage, QueryMessage, ExecuteMessage, UnsupportedOperation,
    BatchMessage, StartupMessage, OptionsMessage, RegisterMessage,
    AuthResponseMessage, ProtocolHandler, _MessageType
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

    EXPECTED_FRAMES = {
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
