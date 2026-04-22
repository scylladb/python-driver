"""
Unit tests for apply_parameters caching in _CassandraType.
"""
import unittest
from cassandra.cqltypes import (
    MapType, SetType, ListType, TupleType,
    Int32Type, UTF8Type, FloatType, DoubleType, BooleanType,
    _CassandraType,
)


class TestApplyParametersCache(unittest.TestCase):

    def setUp(self):
        _CassandraType._apply_parameters_cache.clear()

    def test_cache_returns_same_object(self):
        """Repeated apply_parameters calls return the exact same class object."""
        result1 = MapType.apply_parameters([UTF8Type, Int32Type])
        result2 = MapType.apply_parameters([UTF8Type, Int32Type])
        self.assertIs(result1, result2)

    def test_cache_different_subtypes_different_results(self):
        """Different subtype combinations produce different cached classes."""
        r1 = MapType.apply_parameters([UTF8Type, Int32Type])
        r2 = MapType.apply_parameters([Int32Type, UTF8Type])
        self.assertIsNot(r1, r2)

    def test_cache_different_base_types(self):
        """Different base types with same subtypes produce different classes."""
        r1 = SetType.apply_parameters([Int32Type])
        r2 = ListType.apply_parameters([Int32Type])
        self.assertIsNot(r1, r2)

    def test_cached_type_has_correct_subtypes(self):
        """Cached types preserve their subtype information."""
        result = MapType.apply_parameters([UTF8Type, FloatType])
        self.assertEqual(result.subtypes, (UTF8Type, FloatType))
        # Call again, verify cache hit still has correct subtypes
        result2 = MapType.apply_parameters([UTF8Type, FloatType])
        self.assertEqual(result2.subtypes, (UTF8Type, FloatType))

    def test_cached_type_has_correct_cassname(self):
        """Cached types preserve their cassname."""
        result = SetType.apply_parameters([DoubleType])
        self.assertEqual(result.cassname, SetType.cassname)

    def test_cached_type_with_names(self):
        """Caching works correctly with named parameters (UDT-style)."""
        r1 = TupleType.apply_parameters([Int32Type, UTF8Type], names=['id', 'name'])
        r2 = TupleType.apply_parameters([Int32Type, UTF8Type], names=['id', 'name'])
        self.assertIs(r1, r2)

    def test_different_names_different_cache_entries(self):
        """Different names produce different cached classes."""
        r1 = TupleType.apply_parameters([Int32Type, UTF8Type], names=['id', 'name'])
        r2 = TupleType.apply_parameters([Int32Type, UTF8Type], names=['key', 'value'])
        self.assertIsNot(r1, r2)

    def test_names_none_vs_no_names(self):
        """Passing names=None and not passing names use the same cache entry."""
        r1 = MapType.apply_parameters([UTF8Type, Int32Type], names=None)
        r2 = MapType.apply_parameters([UTF8Type, Int32Type])
        self.assertIs(r1, r2)

    def test_tuple_subtypes_accepted(self):
        """Both list and tuple subtypes produce the same cached result."""
        r1 = MapType.apply_parameters([UTF8Type, Int32Type])
        r2 = MapType.apply_parameters((UTF8Type, Int32Type))
        self.assertIs(r1, r2)

    def test_cache_populated(self):
        """The cache dict is populated after apply_parameters calls."""
        _CassandraType._apply_parameters_cache.clear()
        MapType.apply_parameters([UTF8Type, Int32Type])
        self.assertGreater(len(_CassandraType._apply_parameters_cache), 0)

    def test_cache_clear_forces_new_creation(self):
        """Clearing the cache forces new type creation."""
        r1 = MapType.apply_parameters([UTF8Type, Int32Type])
        _CassandraType._apply_parameters_cache.clear()
        r2 = MapType.apply_parameters([UTF8Type, Int32Type])
        # After clearing, we get a new class (different object identity)
        self.assertIsNot(r1, r2)
        # But they should be functionally equivalent
        self.assertEqual(r1.subtypes, r2.subtypes)


if __name__ == '__main__':
    unittest.main()
