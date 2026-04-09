import unittest
from uuid import UUID

from cassandra.tablets import Tablets, Tablet

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


class TabletReplicaDictTest(unittest.TestCase):
    """Tests for Tablet._replica_dict cached lookup."""

    def test_replica_dict_built_from_replicas(self):
        u1 = UUID('12345678-1234-5678-1234-567812345678')
        u2 = UUID('87654321-4321-8765-4321-876543218765')
        t = Tablet(0, 100, [(u1, 3), (u2, 7)])
        self.assertEqual(t._replica_dict, {u1: 3, u2: 7})

    def test_replica_dict_empty_when_no_replicas(self):
        t = Tablet(0, 100, None)
        self.assertEqual(t._replica_dict, {})

    def test_replica_dict_contains_host(self):
        u1 = UUID('12345678-1234-5678-1234-567812345678')
        u2 = UUID('87654321-4321-8765-4321-876543218765')
        u3 = UUID('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')
        t = Tablet(0, 100, [(u1, 3), (u2, 7)])
        self.assertIn(u1, t._replica_dict)
        self.assertIn(u2, t._replica_dict)
        self.assertNotIn(u3, t._replica_dict)

    def test_replica_dict_shard_lookup(self):
        u1 = UUID('12345678-1234-5678-1234-567812345678')
        u2 = UUID('87654321-4321-8765-4321-876543218765')
        t = Tablet(0, 100, [(u1, 3), (u2, 7)])
        self.assertEqual(t._replica_dict.get(u1), 3)
        self.assertEqual(t._replica_dict.get(u2), 7)
        self.assertIsNone(t._replica_dict.get(UUID('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')))

    def test_replica_contains_host_id_uses_dict(self):
        u1 = UUID('12345678-1234-5678-1234-567812345678')
        u2 = UUID('87654321-4321-8765-4321-876543218765')
        t = Tablet(0, 100, [(u1, 3), (u2, 7)])
        self.assertTrue(t.replica_contains_host_id(u1))
        self.assertTrue(t.replica_contains_host_id(u2))
        self.assertFalse(t.replica_contains_host_id(UUID('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee')))

    def test_replicas_stored_as_tuple(self):
        t = Tablet(0, 100, [("host1", 0), ("host2", 1)])
        self.assertIsInstance(t.replicas, tuple)

    def test_replica_dict_from_iterator(self):
        """Ensure _replica_dict is correctly built even when replicas is a
        one-shot iterator (generator), not a reusable list."""
        u1 = UUID('12345678-1234-5678-1234-567812345678')
        u2 = UUID('87654321-4321-8765-4321-876543218765')

        def gen():
            yield (u1, 3)
            yield (u2, 7)

        t = Tablet(0, 100, gen())
        self.assertEqual(t.replicas, ((u1, 3), (u2, 7)))
        self.assertEqual(t._replica_dict, {u1: 3, u2: 7})
        self.assertTrue(t.replica_contains_host_id(u1))
        self.assertTrue(t.replica_contains_host_id(u2))
