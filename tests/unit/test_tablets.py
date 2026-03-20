import unittest

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


class BisectLeftFallbackTest(unittest.TestCase):
    """Tests for the pure-Python bisect_left fallback.

    On Python >= 3.10 the stdlib C implementation is used, but we keep a
    fallback for older interpreters.  The original fallback had a bug: the
    key=None branch executed ``return`` (returning None) instead of
    ``return lo``.  These tests exercise the fallback directly regardless
    of Python version.
    """

    def _get_fallback(self):
        """Import the module and grab the fallback even on Python >= 3.10."""
        import importlib, types, sys as _sys

        # Re-execute the module body with a fake sys.version_info < 3.10
        # so the else-branch is taken and the fallback is defined.
        source_path = "cassandra/tablets.py"
        with open(source_path) as f:
            source = f.read()

        fake_mod = types.ModuleType("_tablets_fallback")
        fake_mod.__file__ = source_path

        # Patch sys.version_info to (3, 9) so the else branch runs
        original_vi = _sys.version_info
        _sys.version_info = (3, 9, 0, "final", 0)
        try:
            exec(compile(source, source_path, "exec"), fake_mod.__dict__)
        finally:
            _sys.version_info = original_vi

        return fake_mod.bisect_left

    def test_key_none_returns_int(self):
        """The key=None branch must return an int, not None (bug fix)."""
        bisect_left = self._get_fallback()
        result = bisect_left([1, 3, 5, 7], 4)
        self.assertEqual(result, 2)

    def test_key_none_empty_list(self):
        bisect_left = self._get_fallback()
        self.assertEqual(bisect_left([], 42), 0)

    def test_key_none_insert_at_start(self):
        bisect_left = self._get_fallback()
        self.assertEqual(bisect_left([10, 20, 30], 5), 0)

    def test_key_none_insert_at_end(self):
        bisect_left = self._get_fallback()
        self.assertEqual(bisect_left([10, 20, 30], 40), 3)

    def test_key_none_exact_match(self):
        bisect_left = self._get_fallback()
        # bisect_left returns leftmost position
        self.assertEqual(bisect_left([10, 20, 20, 30], 20), 1)

    def test_with_key_function(self):
        """key= branch should still work correctly."""
        bisect_left = self._get_fallback()
        pairs = [(1, "a"), (3, "b"), (5, "c"), (7, "d")]
        idx = bisect_left(pairs, 4, key=lambda p: p[0])
        self.assertEqual(idx, 2)

    def test_lo_negative_raises(self):
        bisect_left = self._get_fallback()
        with self.assertRaises(ValueError):
            bisect_left([1, 2, 3], 2, lo=-1)


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
