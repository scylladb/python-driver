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
Benchmarks for lookup_casstype_simple / parse_casstype_args with and without
LRU caching.

Run with: pytest benchmarks/test_casstype_cache_benchmark.py -v

Requires the ``pytest-benchmark`` plugin.
Skipped automatically when the dependency is unavailable.
"""

import re

import pytest

pytest.importorskip("pytest_benchmark")

from cassandra.cqltypes import (
    apache_cassandra_type_prefix,
    casstype_scanner,
    lookup_casstype,
    lookup_casstype_simple,
    parse_casstype_args,
    trim_if_startswith,
    _casstypes,
    mkUnrecognizedType,
)


# ---------------------------------------------------------------------------
# Reference: original uncached implementations
# ---------------------------------------------------------------------------


def lookup_casstype_simple_uncached(casstype):
    """Original implementation without LRU cache."""
    shortname = trim_if_startswith(casstype, apache_cassandra_type_prefix)
    try:
        typeclass = _casstypes[shortname]
    except KeyError:
        typeclass = mkUnrecognizedType(casstype)
    return typeclass


def parse_casstype_args_uncached(typestring):
    """Original implementation without LRU cache."""
    tokens, remainder = casstype_scanner.scan(typestring)
    if remainder:
        raise ValueError("weird characters %r at end" % remainder)

    args = [([], [])]
    for tok in tokens:
        if tok == "(":
            args.append(([], []))
        elif tok == ")":
            types, names = args.pop()
            prev_types, prev_names = args[-1]
            prev_types[-1] = prev_types[-1].apply_parameters(types, names)
        else:
            types, names = args[-1]
            parts = re.split(":|=>", tok)
            tok = parts.pop()
            if parts:
                names.append(parts[0])
            else:
                names.append(None)

            try:
                ctype = int(tok)
            except ValueError:
                ctype = lookup_casstype_simple_uncached(tok)
            types.append(ctype)

    return args[0][0][0]


def lookup_casstype_uncached(casstype):
    """Original lookup_casstype without any LRU caching underneath."""
    from cassandra.cqltypes import CassandraType, CassandraTypeType

    if isinstance(casstype, (CassandraType, CassandraTypeType)):
        return casstype
    if "(" not in casstype:
        return lookup_casstype_simple_uncached(casstype)
    try:
        return parse_casstype_args_uncached(casstype)
    except (ValueError, AssertionError, IndexError) as e:
        raise ValueError("Don't know how to parse type string %r: %s" % (casstype, e))


# ---------------------------------------------------------------------------
# Test type strings
# ---------------------------------------------------------------------------

SIMPLE_TYPES = [
    "UTF8Type",
    "Int32Type",
    "BooleanType",
    "DoubleType",
    "LongType",
    "FloatType",
    "TimestampType",
    "UUIDType",
    "InetAddressType",
    "DecimalType",
]

SIMPLE_TYPES_FQ = [apache_cassandra_type_prefix + t for t in SIMPLE_TYPES]

PARAMETERIZED_TYPES = [
    "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)",
    "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)",
    "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UUIDType)",
    "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))",
]

# Realistic mix: what a schema parse would encounter
MIXED_TYPES = SIMPLE_TYPES_FQ + PARAMETERIZED_TYPES


# ---------------------------------------------------------------------------
# Helper to clear any LRU caches on the module functions (no-op if uncached)
# ---------------------------------------------------------------------------


def _clear_caches():
    """Clear LRU caches if they exist, otherwise no-op."""
    for fn in (lookup_casstype_simple, parse_casstype_args):
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


# ---------------------------------------------------------------------------
# Correctness tests
# ---------------------------------------------------------------------------


class TestCasstypeCacheCorrectness:
    """Verify cached functions return the same types as uncached."""

    @pytest.mark.parametrize("typestr", SIMPLE_TYPES)
    def test_simple_type_matches(self, typestr):
        _clear_caches()
        assert lookup_casstype_simple(typestr) == lookup_casstype_simple_uncached(
            typestr
        )

    @pytest.mark.parametrize("typestr", SIMPLE_TYPES_FQ)
    def test_fq_simple_type_matches(self, typestr):
        _clear_caches()
        assert lookup_casstype_simple(typestr) == lookup_casstype_simple_uncached(
            typestr
        )

    @pytest.mark.parametrize("typestr", PARAMETERIZED_TYPES)
    def test_parameterized_type_matches(self, typestr):
        _clear_caches()
        cached = parse_casstype_args(typestr)
        uncached = parse_casstype_args_uncached(typestr)
        assert str(cached) == str(uncached)

    @pytest.mark.parametrize("typestr", MIXED_TYPES)
    def test_lookup_casstype_matches(self, typestr):
        _clear_caches()
        cached = lookup_casstype(typestr)
        uncached = lookup_casstype_uncached(typestr)
        assert str(cached) == str(uncached)


# ---------------------------------------------------------------------------
# Benchmarks: lookup_casstype_simple
# ---------------------------------------------------------------------------


class TestLookupCasstypeSimpleBenchmark:
    """Benchmark lookup_casstype_simple cached vs uncached."""

    # --- Short name (no prefix stripping) ---

    @pytest.mark.benchmark(group="simple_short")
    def test_uncached_short(self, benchmark):
        benchmark(lookup_casstype_simple_uncached, "Int32Type")

    @pytest.mark.benchmark(group="simple_short")
    def test_cached_short(self, benchmark):
        _clear_caches()
        lookup_casstype_simple("Int32Type")  # warm cache
        benchmark(lookup_casstype_simple, "Int32Type")

    # --- Fully-qualified name (prefix stripping) ---

    @pytest.mark.benchmark(group="simple_fq")
    def test_uncached_fq(self, benchmark):
        fq = apache_cassandra_type_prefix + "Int32Type"
        benchmark(lookup_casstype_simple_uncached, fq)

    @pytest.mark.benchmark(group="simple_fq")
    def test_cached_fq(self, benchmark):
        _clear_caches()
        fq = apache_cassandra_type_prefix + "Int32Type"
        lookup_casstype_simple(fq)  # warm cache
        benchmark(lookup_casstype_simple, fq)

    # --- Batch of 10 different types ---

    @pytest.mark.benchmark(group="simple_batch10")
    def test_uncached_batch10(self, benchmark):
        def run():
            for t in SIMPLE_TYPES:
                lookup_casstype_simple_uncached(t)

        benchmark(run)

    @pytest.mark.benchmark(group="simple_batch10")
    def test_cached_batch10(self, benchmark):
        _clear_caches()
        for t in SIMPLE_TYPES:
            lookup_casstype_simple(t)  # warm cache

        def run():
            for t in SIMPLE_TYPES:
                lookup_casstype_simple(t)

        benchmark(run)


# ---------------------------------------------------------------------------
# Benchmarks: parse_casstype_args
# ---------------------------------------------------------------------------


class TestParseCasstypeArgsBenchmark:
    """Benchmark parse_casstype_args cached vs uncached."""

    # --- Simple parameterized type: MapType(UTF8Type, Int32Type) ---

    @pytest.mark.benchmark(group="parse_map")
    def test_uncached_map(self, benchmark):
        benchmark(parse_casstype_args_uncached, PARAMETERIZED_TYPES[0])

    @pytest.mark.benchmark(group="parse_map")
    def test_cached_map(self, benchmark):
        _clear_caches()
        parse_casstype_args(PARAMETERIZED_TYPES[0])  # warm cache
        benchmark(parse_casstype_args, PARAMETERIZED_TYPES[0])

    # --- Nested: MapType(UTF8Type, ListType(Int32Type)) ---

    @pytest.mark.benchmark(group="parse_nested")
    def test_uncached_nested(self, benchmark):
        benchmark(parse_casstype_args_uncached, PARAMETERIZED_TYPES[3])

    @pytest.mark.benchmark(group="parse_nested")
    def test_cached_nested(self, benchmark):
        _clear_caches()
        parse_casstype_args(PARAMETERIZED_TYPES[3])  # warm cache
        benchmark(parse_casstype_args, PARAMETERIZED_TYPES[3])

    # --- Batch of all 4 parameterized types ---

    @pytest.mark.benchmark(group="parse_batch4")
    def test_uncached_batch4(self, benchmark):
        def run():
            for t in PARAMETERIZED_TYPES:
                parse_casstype_args_uncached(t)

        benchmark(run)

    @pytest.mark.benchmark(group="parse_batch4")
    def test_cached_batch4(self, benchmark):
        _clear_caches()
        for t in PARAMETERIZED_TYPES:
            parse_casstype_args(t)  # warm cache

        def run():
            for t in PARAMETERIZED_TYPES:
                parse_casstype_args(t)

        benchmark(run)


# ---------------------------------------------------------------------------
# Benchmarks: lookup_casstype (end-to-end, realistic mix)
# ---------------------------------------------------------------------------


class TestLookupCasstypeBenchmark:
    """Benchmark the full lookup_casstype with a realistic type mix."""

    @pytest.mark.benchmark(group="lookup_mixed")
    def test_uncached_mixed(self, benchmark):
        def run():
            for t in MIXED_TYPES:
                lookup_casstype_uncached(t)

        benchmark(run)

    @pytest.mark.benchmark(group="lookup_mixed")
    def test_cached_mixed(self, benchmark):
        _clear_caches()
        for t in MIXED_TYPES:
            lookup_casstype(t)  # warm cache

        def run():
            for t in MIXED_TYPES:
                lookup_casstype(t)

        benchmark(run)
