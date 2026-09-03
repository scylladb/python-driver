"""
Unit tests for apply_parameters caching in _CassandraType.
"""
import threading
import time
import unittest
import unittest.mock
from cassandra.cqltypes import (
    MapType, SetType, ListType, TupleType, VectorType,
    Int32Type, UTF8Type, FloatType, DoubleType, BooleanType,
    _CassandraType,
)


# Local stand-ins for exercising apply_parameters() with zero subtypes (as
# happens for real types like CompositeType/ColumnToCollectionType, which
# allow num_subtypes == 'UNKNOWN'). A leading underscore keeps
# CassandraTypeType.__new__ from registering these -- and the dynamic
# classes apply_parameters() creates from them -- in the shared, global
# _casstypes/_cqltypes registries, so these tests can't leak state into
# (or collide with) unrelated tests that look up real type names.
class _CacheTestZeroArgTypeA(_CassandraType):
    typename = 'org.apache.cassandra.db.marshal._CacheTestZeroArgTypeA'
    num_subtypes = 'UNKNOWN'


class _CacheTestZeroArgTypeB(_CassandraType):
    typename = 'org.apache.cassandra.db.marshal._CacheTestZeroArgTypeB'
    num_subtypes = 'UNKNOWN'


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

    def test_empty_names_list_does_not_crash(self):
        """
        An explicit empty `names` list (as opposed to omitted/None) must not
        blow up the cache-key computation. This happens for types that accept
        zero subtypes (num_subtypes == 'UNKNOWN'), e.g. when parsing a bare
        'CompositeType()' or 'ColumnToCollectionType()' cass type string:
        both `subtypes` and `names` come back as `[]` from the parser.

        Regression test: `tuple(names) if names else names` left an empty
        list unconverted (since `[]` is falsy), which is unhashable and
        raised TypeError when used inside the cache-key tuple.
        """
        result = _CacheTestZeroArgTypeA.apply_parameters([], [])
        self.assertEqual(result.subtypes, ())
        # Second call must hit the cache path (same code path, same crash risk)
        result2 = _CacheTestZeroArgTypeA.apply_parameters([], [])
        self.assertIs(result, result2)

        # Different class, same degenerate arguments -> distinct cache entry
        other = _CacheTestZeroArgTypeB.apply_parameters([], [])
        self.assertIsNot(result, other)

    def test_empty_names_list_vs_none_both_hashable(self):
        """
        `names=[]` and `names=None` are distinct cache keys (`()` vs `None`),
        so they are *not* required to share a cache entry -- but neither may
        raise, and both must produce a correctly-shaped (empty-subtypes)
        result.
        """
        r1 = _CacheTestZeroArgTypeA.apply_parameters([], [])
        r2 = _CacheTestZeroArgTypeA.apply_parameters([], None)
        self.assertEqual(r1.subtypes, ())
        self.assertEqual(r2.subtypes, ())

    def test_vector_type_does_not_use_shared_cache(self):
        """
        VectorType overrides apply_parameters() completely and never reads or
        writes _CassandraType._apply_parameters_cache: it is keyed only on
        (subtype, vector_size) via its own type() call, so it cannot collide
        with -- or be corrupted by -- the generic cache added for
        Map/Set/List/Tuple/Composite types.

        This guards against a regression where the shared cache is
        (re)connected to VectorType.apply_parameters() without including
        vector_size in the key, which would risk returning a wrongly-sized
        cached vector type for a different dimension.
        """
        _CassandraType._apply_parameters_cache.clear()

        v3 = VectorType.apply_parameters([FloatType, 3], None)
        v4 = VectorType.apply_parameters([FloatType, 4], None)

        # Different dimensions must produce distinct, correctly-shaped types.
        self.assertIsNot(v3, v4)
        self.assertEqual(v3.vector_size, 3)
        self.assertEqual(v4.vector_size, 4)
        self.assertEqual(v3.subtype, FloatType)
        self.assertEqual(v4.subtype, FloatType)

        # VectorType.apply_parameters() must not have touched the shared cache.
        self.assertEqual(len(_CassandraType._apply_parameters_cache), 0)

    def test_mutating_caller_names_list_does_not_corrupt_cached_class(self):
        """
        `names` is normalized to a tuple for the cache key, but the class
        created by apply_parameters() must store that *same normalized*
        value in `fieldnames` -- never the caller's original object.

        Regression test: if `fieldnames` aliased the caller's mutable list
        instead of the normalized tuple, mutating that list after the call
        would silently change `fieldnames` on the cached class for every
        future cache hit, since the cache stores (and returns) the class
        object itself, not a copy.
        """
        names = ['id', 'name']
        result = TupleType.apply_parameters([Int32Type, UTF8Type], names=names)
        self.assertEqual(result.fieldnames, ('id', 'name'))

        # Mutate the caller's original list after the class has been created
        # and cached.
        names.append('extra')
        names[0] = 'clobbered'

        # The cached class must be unaffected by the mutation.
        self.assertEqual(result.fieldnames, ('id', 'name'))

        # A subsequent cache-hit call (using a fresh, unmutated list so the
        # cache key still matches) must return the same, still-uncorrupted
        # class.
        result2 = TupleType.apply_parameters([Int32Type, UTF8Type], names=['id', 'name'])
        self.assertIs(result, result2)
        self.assertEqual(result2.fieldnames, ('id', 'name'))

    def test_concurrent_apply_parameters_same_args_returns_identical_class(self):
        """
        The get-then-create-then-set sequence in apply_parameters() must be
        synchronized: concurrent callers racing on the same
        (cls, subtypes, names) with an empty cache must all observe the
        exact same class object, not merely equal ones.

        Regression test: without a lock guarding the miss path, multiple
        threads can each miss the cache, each create their own class via
        type(), and each stomp on the cache entry in turn -- leaving some
        callers holding a class object that is not the one now cached (and
        is not `is`-identical to what other threads got back).

        Under the GIL, the unguarded window between the cache-miss check and
        the cache-set is normally too narrow to hit reliably with plain
        threads (the race exists, but "many iterations" would be needed to
        catch it by luck). To get a deterministic, non-flaky repro instead
        of relying on scheduling luck, this test widens that exact window by
        making `cass_parameterized_type_with()` -- which apply_parameters()
        calls in between the get and the set -- briefly sleep. That's the
        real code path under test, just slowed down enough for concurrent
        threads to reliably land inside it together. With the miss path
        properly locked, this must still be safe (just serialized); without
        the lock, it reliably produces distinct class objects.
        """
        _CassandraType._apply_parameters_cache.clear()

        orig_cass_parameterized_type_with = MapType.cass_parameterized_type_with.__func__

        def slow_cass_parameterized_type_with(cls, subtypes, full=False):
            time.sleep(0.01)
            return orig_cass_parameterized_type_with(cls, subtypes, full=full)

        n_threads = 16
        results = [None] * n_threads
        start_barrier = threading.Barrier(n_threads)

        def worker(idx):
            # Synchronize thread start as tightly as possible to maximize
            # the chance of them all racing into the cache-miss path
            # together.
            start_barrier.wait()
            results[idx] = MapType.apply_parameters([UTF8Type, Int32Type], names=['k', 'v'])

        with unittest.mock.patch.object(
                MapType, 'cass_parameterized_type_with',
                classmethod(slow_cass_parameterized_type_with)):
            threads = [threading.Thread(target=worker, args=(i,)) for i in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

        first = results[0]
        for i, r in enumerate(results):
            self.assertIs(r, first, "thread %d got a different class object" % i)

        # And the cache itself must hold that exact same object.
        cache_key = (MapType, (UTF8Type, Int32Type), ('k', 'v'))
        self.assertIs(_CassandraType._apply_parameters_cache[cache_key], first)


if __name__ == '__main__':
    unittest.main()
