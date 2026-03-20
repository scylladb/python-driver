# Copyright ScyllaDB, Inc.
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

"""
Unit tests for the deserializer caches in deserializers.pyx.

Validates cache hit/miss behaviour, bounded eviction, the
clear_deserializer_caches() API (needed after runtime Des* overrides),
and the get_deserializer_cache_sizes() diagnostic helper.
"""

import unittest

from tests.unit.cython.utils import cythontest

try:
    from cassandra.deserializers import (
        clear_deserializer_caches,
        find_deserializer,
        get_deserializer_cache_sizes,
        make_deserializers,
    )

    _HAS_DESERIALIZERS = True
except ImportError:
    _HAS_DESERIALIZERS = False

from cassandra import cqltypes


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class DeserializerCacheTest(unittest.TestCase):
    """Tests for find_deserializer / make_deserializers caching."""

    def setUp(self):
        if _HAS_DESERIALIZERS:
            clear_deserializer_caches()

    def tearDown(self):
        if _HAS_DESERIALIZERS:
            clear_deserializer_caches()

    # -- find_deserializer cache -------------------------------------------

    @cythontest
    def test_find_cache_hit_same_object(self):
        """Repeated calls for the same cqltype return the same instance."""
        d1 = find_deserializer(cqltypes.Int32Type)
        d2 = find_deserializer(cqltypes.Int32Type)
        self.assertIs(d1, d2)

    @cythontest
    def test_find_cache_miss_different_types(self):
        """Different cqltypes produce different deserializer instances."""
        d_int = find_deserializer(cqltypes.Int32Type)
        d_utf = find_deserializer(cqltypes.UTF8Type)
        self.assertIsNot(d_int, d_utf)

    @cythontest
    def test_find_returns_correct_deserializer_class(self):
        """The returned deserializer class name matches the cqltype."""
        d = find_deserializer(cqltypes.Int32Type)
        self.assertEqual(type(d).__name__, "DesInt32Type")

    # -- make_deserializers cache ------------------------------------------

    @cythontest
    def test_make_cache_hit_same_object(self):
        """Repeated calls with the same type list return the same array."""
        types = [cqltypes.Int32Type, cqltypes.UTF8Type]
        r1 = make_deserializers(types)
        r2 = make_deserializers(types)
        self.assertIs(r1, r2)

    @cythontest
    def test_make_cache_correct_length(self):
        """Returned array has the right number of entries."""
        types = [cqltypes.Int32Type, cqltypes.UTF8Type, cqltypes.BooleanType]
        result = make_deserializers(types)
        self.assertEqual(len(result), 3)

    # -- clear_deserializer_caches -----------------------------------------

    @cythontest
    def test_clear_invalidates_find_cache(self):
        """After clearing, find_deserializer returns a new instance."""
        d1 = find_deserializer(cqltypes.Int32Type)
        clear_deserializer_caches()
        d2 = find_deserializer(cqltypes.Int32Type)
        # New instance, but same deserializer class
        self.assertIsNot(d1, d2)
        self.assertEqual(type(d1).__name__, type(d2).__name__)

    @cythontest
    def test_clear_invalidates_make_cache(self):
        """After clearing, make_deserializers returns a new array."""
        types = [cqltypes.Int32Type, cqltypes.UTF8Type]
        r1 = make_deserializers(types)
        clear_deserializer_caches()
        r2 = make_deserializers(types)
        self.assertIsNot(r1, r2)

    # -- get_deserializer_cache_sizes --------------------------------------

    @cythontest
    def test_cache_sizes_empty_after_clear(self):
        """Sizes are (0, 0) immediately after clearing."""
        find_size, make_size = get_deserializer_cache_sizes()
        self.assertEqual(find_size, 0)
        self.assertEqual(make_size, 0)

    @cythontest
    def test_cache_sizes_increment(self):
        """Sizes reflect the number of cached entries."""
        find_deserializer(cqltypes.Int32Type)
        find_deserializer(cqltypes.UTF8Type)
        make_deserializers([cqltypes.Int32Type, cqltypes.UTF8Type])

        find_size, make_size = get_deserializer_cache_sizes()
        self.assertEqual(find_size, 2)
        self.assertEqual(make_size, 1)

    # -- bounded eviction --------------------------------------------------

    @cythontest
    def test_find_cache_bounded_size(self):
        """find_deserializer cache should not exceed 256 entries."""
        # Create 300 distinct cqltype objects via apply_parameters.
        # Each ListType.apply_parameters() call creates a fresh class.
        inner_types = [
            cqltypes.Int32Type,
            cqltypes.UTF8Type,
            cqltypes.BooleanType,
            cqltypes.DoubleType,
            cqltypes.LongType,
        ]
        distinct_types = []
        for i in range(300):
            # Create ListType(inner) — each apply_parameters returns a new
            # class object, so these are all distinct cache keys.
            inner = inner_types[i % len(inner_types)]
            ct = cqltypes.ListType.apply_parameters([inner])
            distinct_types.append(ct)

        for ct in distinct_types:
            find_deserializer(ct)

        find_size, _ = get_deserializer_cache_sizes()
        self.assertLessEqual(
            find_size,
            256,
            "find_deserializer cache should be bounded to 256, got %d" % find_size,
        )

    @cythontest
    def test_make_cache_bounded_size(self):
        """make_deserializers cache should not exceed 256 entries."""
        inner_types = [
            cqltypes.Int32Type,
            cqltypes.UTF8Type,
            cqltypes.BooleanType,
            cqltypes.DoubleType,
            cqltypes.LongType,
        ]
        for i in range(300):
            inner = inner_types[i % len(inner_types)]
            ct = cqltypes.ListType.apply_parameters([inner])
            make_deserializers([ct])

        _, make_size = get_deserializer_cache_sizes()
        self.assertLessEqual(
            make_size,
            256,
            "make_deserializers cache should be bounded to 256, got %d" % make_size,
        )
