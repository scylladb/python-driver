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
Benchmarks for named_tuple_factory with and without namedtuple class caching.

Run with: pytest benchmarks/test_named_tuple_factory_benchmark.py -v
"""

import re
import warnings
from collections import namedtuple

import pytest

from cassandra.query import named_tuple_factory, _named_tuple_cache
from cassandra.util import _sanitize_identifiers


# ---------------------------------------------------------------------------
# Reference: original uncached implementation (copied from master)
# ---------------------------------------------------------------------------

NON_ALPHA_REGEX = re.compile("[^a-zA-Z0-9]")
START_BADCHAR_REGEX = re.compile("^[^a-zA-Z0-9]*")
END_BADCHAR_REGEX = re.compile("[^a-zA-Z0-9_]*$")

_clean_name_cache_old = {}


def _clean_column_name_old(name):
    try:
        return _clean_name_cache_old[name]
    except KeyError:
        clean = NON_ALPHA_REGEX.sub(
            "_", START_BADCHAR_REGEX.sub("", END_BADCHAR_REGEX.sub("", name))
        )
        _clean_name_cache_old[name] = clean
        return clean


def named_tuple_factory_uncached(colnames, rows):
    """Original implementation without caching (for benchmark comparison)."""
    clean_column_names = map(_clean_column_name_old, colnames)
    try:
        Row = namedtuple("Row", clean_column_names)
    except SyntaxError:
        raise
    except Exception:
        clean_column_names = list(map(_clean_column_name_old, colnames))
        Row = namedtuple("Row", _sanitize_identifiers(clean_column_names))
    return [Row(*row) for row in rows]


# ---------------------------------------------------------------------------
# Test data generators
# ---------------------------------------------------------------------------


def make_colnames(n):
    return tuple(f"col_{i}" for i in range(n))


def make_rows(ncols, nrows):
    return [tuple(range(ncols)) for _ in range(nrows)]


# ---------------------------------------------------------------------------
# Correctness tests
# ---------------------------------------------------------------------------


class TestNamedTupleFactoryCorrectness:
    """Verify the cached implementation matches the uncached one."""

    @pytest.mark.parametrize("ncols", [1, 5, 10, 20])
    @pytest.mark.parametrize("nrows", [1, 10, 100])
    def test_results_match(self, ncols, nrows):
        colnames = make_colnames(ncols)
        rows = make_rows(ncols, nrows)
        _named_tuple_cache.clear()
        cached_result = named_tuple_factory(colnames, rows)
        uncached_result = named_tuple_factory_uncached(colnames, rows)
        assert len(cached_result) == len(uncached_result)
        for cr, ur in zip(cached_result, uncached_result):
            assert tuple(cr) == tuple(ur)
            assert cr._fields == ur._fields

    def test_cache_hit_returns_same_class(self):
        colnames = ("name", "age", "email")
        rows1 = [("Alice", 30, "a@b.com")]
        rows2 = [("Bob", 25, "b@c.com")]
        _named_tuple_cache.clear()
        result1 = named_tuple_factory(colnames, rows1)
        result2 = named_tuple_factory(colnames, rows2)
        # Same Row class should be reused
        assert type(result1[0]) is type(result2[0])

    def test_different_schemas_get_different_classes(self):
        _named_tuple_cache.clear()
        result1 = named_tuple_factory(("a", "b"), [(1, 2)])
        result2 = named_tuple_factory(("x", "y"), [(3, 4)])
        assert type(result1[0]) is not type(result2[0])
        assert result1[0]._fields == ("a", "b")
        assert result2[0]._fields == ("x", "y")


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------


class TestNamedTupleFactoryBenchmark:
    """Benchmark cached vs uncached named_tuple_factory."""

    # --- 5 columns, 100 rows ---

    @pytest.mark.benchmark(group="ntf_5cols_100rows")
    def test_uncached_5cols_100rows(self, benchmark):
        colnames = make_colnames(5)
        rows = make_rows(5, 100)
        benchmark(named_tuple_factory_uncached, colnames, rows)

    @pytest.mark.benchmark(group="ntf_5cols_100rows")
    def test_cached_5cols_100rows(self, benchmark):
        colnames = make_colnames(5)
        rows = make_rows(5, 100)
        _named_tuple_cache.clear()
        # Warm the cache with one call
        named_tuple_factory(colnames, rows)
        benchmark(named_tuple_factory, colnames, rows)

    # --- 10 columns, 100 rows ---

    @pytest.mark.benchmark(group="ntf_10cols_100rows")
    def test_uncached_10cols_100rows(self, benchmark):
        colnames = make_colnames(10)
        rows = make_rows(10, 100)
        benchmark(named_tuple_factory_uncached, colnames, rows)

    @pytest.mark.benchmark(group="ntf_10cols_100rows")
    def test_cached_10cols_100rows(self, benchmark):
        colnames = make_colnames(10)
        rows = make_rows(10, 100)
        _named_tuple_cache.clear()
        named_tuple_factory(colnames, rows)
        benchmark(named_tuple_factory, colnames, rows)

    # --- 20 columns, 100 rows ---

    @pytest.mark.benchmark(group="ntf_20cols_100rows")
    def test_uncached_20cols_100rows(self, benchmark):
        colnames = make_colnames(20)
        rows = make_rows(20, 100)
        benchmark(named_tuple_factory_uncached, colnames, rows)

    @pytest.mark.benchmark(group="ntf_20cols_100rows")
    def test_cached_20cols_100rows(self, benchmark):
        colnames = make_colnames(20)
        rows = make_rows(20, 100)
        _named_tuple_cache.clear()
        named_tuple_factory(colnames, rows)
        benchmark(named_tuple_factory, colnames, rows)

    # --- 5 columns, 1000 rows ---

    @pytest.mark.benchmark(group="ntf_5cols_1000rows")
    def test_uncached_5cols_1000rows(self, benchmark):
        colnames = make_colnames(5)
        rows = make_rows(5, 1000)
        benchmark(named_tuple_factory_uncached, colnames, rows)

    @pytest.mark.benchmark(group="ntf_5cols_1000rows")
    def test_cached_5cols_1000rows(self, benchmark):
        colnames = make_colnames(5)
        rows = make_rows(5, 1000)
        _named_tuple_cache.clear()
        named_tuple_factory(colnames, rows)
        benchmark(named_tuple_factory, colnames, rows)

    # --- 10 columns, 1 row (measures class creation overhead most clearly) ---

    @pytest.mark.benchmark(group="ntf_10cols_1row")
    def test_uncached_10cols_1row(self, benchmark):
        colnames = make_colnames(10)
        rows = make_rows(10, 1)
        benchmark(named_tuple_factory_uncached, colnames, rows)

    @pytest.mark.benchmark(group="ntf_10cols_1row")
    def test_cached_10cols_1row(self, benchmark):
        colnames = make_colnames(10)
        rows = make_rows(10, 1)
        _named_tuple_cache.clear()
        named_tuple_factory(colnames, rows)
        benchmark(named_tuple_factory, colnames, rows)
