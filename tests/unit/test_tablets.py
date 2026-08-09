import unittest
import uuid

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


class DropTabletsByHostIdTest(unittest.TestCase):
    """
    Regression tests: drop_tablets_by_host_id must delete a table's key
    from _tablets entirely once its tablet list becomes empty, so that
    table_has_tablets() (and any other truthiness/membership check on
    _tablets, e.g. bool(tablets)) correctly reflects that no tablets are
    left -- rather than leaving a stale empty list behind.
    """

    def test_drop_last_tablet_removes_table_key(self):
        host_id = uuid.uuid4()
        t1 = Tablet(0, 100, [(host_id, 0)])
        tablets = Tablets({("ks", "tb"): [t1]})

        assert tablets.table_has_tablets("ks", "tb") is True

        tablets.drop_tablets_by_host_id(host_id)

        # The no-tablet fast path must now be correctly signalled: the key
        # must be gone from the dict (not just left as an empty list), and
        # table_has_tablets must report False.
        assert ("ks", "tb") not in tablets._tablets
        assert tablets.table_has_tablets("ks", "tb") is False
        assert bool(tablets) is False

    def test_drop_some_tablets_keeps_remaining(self):
        removed_host_id = uuid.uuid4()
        remaining_host_id = uuid.uuid4()
        t1 = Tablet(0, 100, [(removed_host_id, 0)])
        t2 = Tablet(100, 200, [(remaining_host_id, 0)])
        tablets = Tablets({("ks", "tb"): [t1, t2]})

        tablets.drop_tablets_by_host_id(removed_host_id)

        assert ("ks", "tb") in tablets._tablets
        assert tablets._tablets[("ks", "tb")] == [t2]
        assert tablets.table_has_tablets("ks", "tb") is True
        assert bool(tablets) is True

    def test_drop_last_tablet_for_one_table_keeps_other_tables(self):
        host_id = uuid.uuid4()
        other_host_id = uuid.uuid4()
        t1 = Tablet(0, 100, [(host_id, 0)])
        t2 = Tablet(0, 100, [(other_host_id, 0)])
        tablets = Tablets({("ks", "tb1"): [t1], ("ks", "tb2"): [t2]})

        tablets.drop_tablets_by_host_id(host_id)

        assert ("ks", "tb1") not in tablets._tablets
        assert tablets.table_has_tablets("ks", "tb1") is False
        assert ("ks", "tb2") in tablets._tablets
        assert tablets.table_has_tablets("ks", "tb2") is True
        # Overall dict is still non-empty because tb2 still has tablets.
        assert bool(tablets) is True

    def test_drop_by_host_id_none_is_noop(self):
        tablets = Tablets({("ks", "tb"): []})
        tablets.drop_tablets_by_host_id(None)
        assert ("ks", "tb") in tablets._tablets
