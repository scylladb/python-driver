import unittest
from io import BytesIO

from cassandra import ConsistencyLevel, ProtocolVersion
from cassandra.protocol import ExecuteMessage
from cassandra.protocol_features import ProtocolFeatures
from cassandra.tablets import Tablets, Tablet, choose_tablet_version_block, random_tablet_version_block

class TabletsTest(unittest.TestCase):
    def compare_ranges(self, tablets, ranges):
        assert len(tablets) == len(ranges)

        for idx, tablet in enumerate(tablets):
            assert tablet.first_token == ranges[idx][0], "First token is not correct in tablet: {}".format(tablet)
            assert tablet.last_token == ranges[idx][1], "Last token is not correct in tablet: {}".format(tablet)

    def test_add_tablet_to_empty_tablets(self):
        tablets = Tablets({("test_ks", "test_tb"): []})
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-6917529027641081857, -4611686018427387905, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-6917529027641081857, -4611686018427387905)])

    def test_add_tablet_at_the_beggining(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-6917529027641081857, -4611686018427387905, None)]})

        tablets.add_tablet("test_ks", "test_tb", Tablet(-8611686018427387905, -7917529027641081857, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-8611686018427387905, -7917529027641081857),
                                           (-6917529027641081857, -4611686018427387905)])

    def test_add_tablet_at_the_end(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-6917529027641081857, -4611686018427387905, None)]})

        tablets.add_tablet("test_ks", "test_tb", Tablet(-1, 2305843009213693951, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-6917529027641081857, -4611686018427387905),
                                           (-1, 2305843009213693951)])

    def test_add_tablet_in_the_middle(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-6917529027641081857, -4611686018427387905, None), 
                                                    Tablet(-1, 2305843009213693951, None)]},)
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-4611686018427387905, -2305843009213693953, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-6917529027641081857, -4611686018427387905),
                                           (-4611686018427387905, -2305843009213693953),
                                           (-1, 2305843009213693951)])

    def test_add_tablet_intersecting(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-6917529027641081857, -4611686018427387905, None), 
                                                    Tablet(-4611686018427387905, -2305843009213693953, None),
                                                    Tablet(-2305843009213693953, -1, None),
                                                    Tablet(-1, 2305843009213693951, None)]})
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-3611686018427387905, -6, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-6917529027641081857, -4611686018427387905),
                                           (-3611686018427387905, -6),
                                           (-1, 2305843009213693951)])

    def test_add_tablet_intersecting_with_first(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-8611686018427387905, -7917529027641081857, None),
                                                    Tablet(-6917529027641081857, -4611686018427387905, None)]})
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-8011686018427387905, -7987529027641081857, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-8011686018427387905, -7987529027641081857),
                                           (-6917529027641081857, -4611686018427387905)])

    def test_add_tablet_intersecting_with_last(self):
        tablets = Tablets({("test_ks", "test_tb"): [Tablet(-8611686018427387905, -7917529027641081857, None),
                                                    Tablet(-6917529027641081857, -4611686018427387905, None)]})
        
        tablets.add_tablet("test_ks", "test_tb", Tablet(-5011686018427387905, -2987529027641081857, None))
        
        tablets_list = tablets._tablets.get(("test_ks", "test_tb"))

        self.compare_ranges(tablets_list, [(-8611686018427387905, -7917529027641081857),
                                           (-5011686018427387905, -2987529027641081857)])


class GetTabletForKeyTest(unittest.TestCase):
    """Tests for Tablets.get_tablet_for_key."""

    def test_found(self):
        t1 = Tablet(0, 100, [("host1", 0)])
        t2 = Tablet(100, 200, [("host2", 0)])
        t3 = Tablet(200, 300, [("host3", 0)])
        tablets = Tablets({("ks", "tb"): [t1, t2, t3]})

        class Token:
            def __init__(self, v):
                self.value = v

        result = tablets.get_tablet_for_key("ks", "tb", Token(150))
        self.assertIs(result, t2)

    def test_not_found_empty(self):
        tablets = Tablets({})

        class Token:
            def __init__(self, v):
                self.value = v

        self.assertIsNone(tablets.get_tablet_for_key("ks", "tb", Token(50)))

    def test_not_found_outside_range(self):
        t1 = Tablet(100, 200, [("host1", 0)])
        tablets = Tablets({("ks", "tb"): [t1]})

        class Token:
            def __init__(self, v):
                self.value = v

        # Token value 50 is not > first_token (100) of the tablet whose
        # last_token (200) is >= 50, so no match.
        self.assertIsNone(tablets.get_tablet_for_key("ks", "tb", Token(50)))


class TabletVersionBlockTest(unittest.TestCase):
    """Tests for tablet_version_block encoding used by TABLETS_ROUTING_V2."""

    def _server_block_matches(self, version, block):
        """Reimplements the server's locator::compare_tablet_version_block."""
        block_value = block & 0x0F
        block_index = (block & 0xF0) >> 4
        hash_block = (version >> (block_index * 4)) & 0x0F
        return hash_block == block_value

    def test_choose_tablet_version_block_matches_server(self):
        """Every block produced by the driver must match the server's check."""
        version = 0x0123456789ABCDEF
        # The index is chosen randomly; sample enough times to exercise many indices.
        for _ in range(256):
            block = choose_tablet_version_block(version)
            self.assertTrue(self._server_block_matches(version, block),
                f"Block 0x{block:02X} did not match server check for version 0x{version:016X}")

    def test_choose_tablet_version_block_covers_all_indices(self):
        """Over many calls the random index selection should probe every block
        index, so that any server-side version change is eventually detected."""
        version = 0xFFFFFFFFFFFFFFFF  # All nibbles are 0xF
        seen_indices = set()
        # 16 indices; 1000 draws makes a missing index astronomically unlikely.
        for _ in range(1000):
            block = choose_tablet_version_block(version)
            seen_indices.add((block >> 4) & 0xF)
        self.assertEqual(seen_indices, set(range(16)))

    def test_choose_tablet_version_block_matches_server_for_signed_version(self):
        """tablet_version is decoded as a *signed* 64-bit int (LongType), so a
        version with the high bit set is stored negative in the driver while the
        server treats it as unsigned. The block the driver emits must still match
        the server's check computed on the unsigned value (sign-boundary guard)."""
        for unsigned in (0x8000000000000000, 0xDEADBEEFCAFEBABE, 0xFFFFFFFFFFFFFFFF):
            signed = unsigned - (1 << 64)  # how LongType stores a high-bit value
            self.assertLess(signed, 0)
            # Sample enough times to exercise every one of the 16 block indices.
            for _ in range(256):
                block = choose_tablet_version_block(signed)
                self.assertTrue(
                    self._server_block_matches(unsigned, block),
                    f"signed version {signed} (unsigned 0x{unsigned:016X}) produced "
                    f"block 0x{block:02X} that failed the server check")

    def test_random_tablet_version_block_returns_byte(self):
        """Verify random_tablet_version_block returns a value in [0, 255]."""
        for _ in range(100):
            block = random_tablet_version_block()
            self.assertIsInstance(block, int)
            self.assertGreaterEqual(block, 0)
            self.assertLessEqual(block, 255)

    def test_from_row_stores_tablet_version(self):
        """Tablet.from_row stores the tablet_version it is given (the V2 payload field)."""
        version = 0xDEADBEEFCAFEBABE
        tablet = Tablet.from_row(-100, 100, [("host1", 0), ("host2", 1)], tablet_version=version)
        self.assertIsNotNone(tablet)
        self.assertEqual(tablet.tablet_version, version)
        self.assertEqual(tablet.first_token, -100)
        self.assertEqual(tablet.last_token, 100)
        self.assertEqual(len(tablet.replicas), 2)


class ExecuteMessageSerializationTest(unittest.TestCase):
    """ExecuteMessage.send_body decides whether to emit the tablet_version_block
    from the serving connection's negotiated protocol features, so the message no
    longer has to be copied per send attempt (TABLETS_ROUTING_V2)."""

    # V4 keeps the encoding minimal: no prepared-metadata id, single-byte flags.
    PROTOCOL_VERSION = ProtocolVersion.V4

    def _make_message(self, tablet_version_block):
        return ExecuteMessage(
            query_id=b"\x01\x02\x03\x04",
            query_params=[],
            consistency_level=ConsistencyLevel.ONE,
            tablet_version_block=tablet_version_block,
        )

    def _encode_body(self, message, protocol_features):
        f = BytesIO()
        message.send_body(f, self.PROTOCOL_VERSION, protocol_features)
        return f.getvalue()

    def test_block_appended_on_v2_connection(self):
        """A V2 connection appends exactly one trailing byte carrying the
        precomputed tablet_version_block."""
        message = self._make_message(0x7A)
        v2 = self._encode_body(message, ProtocolFeatures(tablets_routing_v2=True))
        plain = self._encode_body(message, ProtocolFeatures(tablets_routing_v2=False))
        self.assertEqual(v2, plain + bytes([0x7A]))

    def test_block_absent_without_v2(self):
        """No trailing byte when the connection did not negotiate V2, and passing
        no features at all is equivalent to V2 being off."""
        message = self._make_message(0x7A)
        no_features = self._encode_body(message, None)
        v2_off = self._encode_body(message, ProtocolFeatures(tablets_routing_v2=False))
        self.assertEqual(no_features, v2_off)
        self.assertNotIn(bytes([0x7A]), no_features[-1:])

    def test_missing_block_coalesces_to_zero_on_v2(self):
        """On a V2 connection the server reads exactly one trailing byte per
        EXECUTE, so a message whose block was never computed must still emit a
        zero byte to keep the frame in sync."""
        message = self._make_message(None)
        v2 = self._encode_body(message, ProtocolFeatures(tablets_routing_v2=True))
        plain = self._encode_body(message, ProtocolFeatures(tablets_routing_v2=False))
        self.assertEqual(v2, plain + bytes([0x00]))

    def test_same_message_encodes_consistently_across_connections(self):
        """The same shared message instance yields the V2 or non-V2 framing purely
        from the features argument, so it is safe to encode concurrently on
        connections with different capabilities without copying it."""
        message = self._make_message(0x3C)
        first = self._encode_body(message, ProtocolFeatures(tablets_routing_v2=True))
        second_plain = self._encode_body(message, ProtocolFeatures(tablets_routing_v2=False))
        first_again = self._encode_body(message, ProtocolFeatures(tablets_routing_v2=True))
        self.assertEqual(first, first_again)
        self.assertEqual(first, second_plain + bytes([0x3C]))
