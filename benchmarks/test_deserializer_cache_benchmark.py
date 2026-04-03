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
Benchmarks for find_deserializer / make_deserializers with and without caching.

Run with: pytest benchmarks/test_deserializer_cache_benchmark.py -v
"""

import pytest

from cassandra import cqltypes
from cassandra.deserializers import (
    find_deserializer,
    make_deserializers,
)


# ---------------------------------------------------------------------------
# Reference: original uncached implementations (copied from master)
# ---------------------------------------------------------------------------

_classes = {}


def _init_classes():
    """Lazily initialize the class lookup dict from deserializers module."""
    if not _classes:
        from cassandra import deserializers as mod

        for name in dir(mod):
            obj = getattr(mod, name)
            if isinstance(obj, type):
                _classes[name] = obj


def find_deserializer_uncached(cqltype):
    """Original implementation without caching."""
    _init_classes()

    name = "Des" + cqltype.__name__
    if name in _classes:
        cls = _classes[name]
    elif issubclass(cqltype, cqltypes.ListType):
        from cassandra.deserializers import DesListType

        cls = DesListType
    elif issubclass(cqltype, cqltypes.SetType):
        from cassandra.deserializers import DesSetType

        cls = DesSetType
    elif issubclass(cqltype, cqltypes.MapType):
        from cassandra.deserializers import DesMapType

        cls = DesMapType
    elif issubclass(cqltype, cqltypes.UserType):
        from cassandra.deserializers import DesUserType

        cls = DesUserType
    elif issubclass(cqltype, cqltypes.TupleType):
        from cassandra.deserializers import DesTupleType

        cls = DesTupleType
    elif issubclass(cqltype, cqltypes.DynamicCompositeType):
        from cassandra.deserializers import DesDynamicCompositeType

        cls = DesDynamicCompositeType
    elif issubclass(cqltype, cqltypes.CompositeType):
        from cassandra.deserializers import DesCompositeType

        cls = DesCompositeType
    elif issubclass(cqltype, cqltypes.ReversedType):
        from cassandra.deserializers import DesReversedType

        cls = DesReversedType
    elif issubclass(cqltype, cqltypes.FrozenType):
        from cassandra.deserializers import DesFrozenType

        cls = DesFrozenType
    else:
        from cassandra.deserializers import GenericDeserializer

        cls = GenericDeserializer

    return cls(cqltype)


def make_deserializers_uncached(ctypes):
    """Original implementation without caching."""
    from cassandra.deserializers import obj_array

    return obj_array([find_deserializer_uncached(ct) for ct in ctypes])


# ---------------------------------------------------------------------------
# Test type sets
# ---------------------------------------------------------------------------

SIMPLE_TYPES = [
    cqltypes.Int32Type,
    cqltypes.UTF8Type,
    cqltypes.BooleanType,
    cqltypes.DoubleType,
    cqltypes.LongType,
]

MIXED_TYPES = [
    cqltypes.Int32Type,
    cqltypes.UTF8Type,
    cqltypes.BooleanType,
    cqltypes.DoubleType,
    cqltypes.LongType,
    cqltypes.FloatType,
    cqltypes.TimestampType,
    cqltypes.UUIDType,
    cqltypes.InetAddressType,
    cqltypes.DecimalType,
]


# ---------------------------------------------------------------------------
# Correctness tests
# ---------------------------------------------------------------------------


class TestDeserializerCacheCorrectness:
    """Verify the cached implementation returns equivalent deserializers."""

    @pytest.mark.parametrize("cqltype", SIMPLE_TYPES + MIXED_TYPES)
    def test_find_deserializer_returns_correct_type(self, cqltype):
        cached = find_deserializer(cqltype)
        uncached = find_deserializer_uncached(cqltype)
        assert type(cached).__name__ == type(uncached).__name__

    def test_find_deserializer_cache_hit_same_object(self):
        d1 = find_deserializer(cqltypes.Int32Type)
        d2 = find_deserializer(cqltypes.Int32Type)
        assert d1 is d2

    def test_make_deserializers_returns_correct_length(self):
        result = make_deserializers(SIMPLE_TYPES)
        assert len(result) == len(SIMPLE_TYPES)

    def test_make_deserializers_cache_hit_same_object(self):
        r1 = make_deserializers(SIMPLE_TYPES)
        r2 = make_deserializers(SIMPLE_TYPES)
        # Should be the exact same cached object
        assert r1 is r2


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------


class TestFindDeserializerBenchmark:
    """Benchmark find_deserializer cached vs uncached."""

    # --- Single simple type ---

    @pytest.mark.benchmark(group="find_deser_simple")
    def test_uncached_simple(self, benchmark):
        benchmark(find_deserializer_uncached, cqltypes.Int32Type)

    @pytest.mark.benchmark(group="find_deser_simple")
    def test_cached_simple(self, benchmark):
        # Cache is already warm from correctness tests or previous iterations
        find_deserializer(cqltypes.Int32Type)  # ensure warm
        benchmark(find_deserializer, cqltypes.Int32Type)


class TestMakeDeserializersBenchmark:
    """Benchmark make_deserializers cached vs uncached."""

    # --- 5 simple types ---

    @pytest.mark.benchmark(group="make_deser_5types")
    def test_uncached_5types(self, benchmark):
        benchmark(make_deserializers_uncached, SIMPLE_TYPES)

    @pytest.mark.benchmark(group="make_deser_5types")
    def test_cached_5types(self, benchmark):
        make_deserializers(SIMPLE_TYPES)  # ensure warm
        benchmark(make_deserializers, SIMPLE_TYPES)

    # --- 10 mixed types ---

    @pytest.mark.benchmark(group="make_deser_10types")
    def test_uncached_10types(self, benchmark):
        benchmark(make_deserializers_uncached, MIXED_TYPES)

    @pytest.mark.benchmark(group="make_deser_10types")
    def test_cached_10types(self, benchmark):
        make_deserializers(MIXED_TYPES)  # ensure warm
        benchmark(make_deserializers, MIXED_TYPES)
