# Copyright DataStax, Inc.
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


import cassandra.query as query_module
from cassandra.query import named_tuple_factory, _named_tuple_cache

import logging
import re
import threading
import warnings
from collections import namedtuple

import sys

from unittest import TestCase

import pytest

from cassandra.util import _sanitize_identifiers


log = logging.getLogger(__name__)


NAMEDTUPLE_CREATION_BUG = sys.version_info >= (3,) and sys.version_info < (3, 7)

class TestNamedTupleFactory(TestCase):

    long_colnames, long_rows = (
        ['col{}'.format(x) for x in range(300)],
        [
            ['value{}'.format(x) for x in range(300)]
            for _ in range(100)
        ]
    )
    short_colnames, short_rows = (
        ['col{}'.format(x) for x in range(200)],
        [
            ['value{}'.format(x) for x in range(200)]
            for _ in range(100)
        ]
    )

    def test_creation_warning_on_long_column_list(self):
        """
        Reproduces the failure described in PYTHON-893

        @since 3.15
        @jira_ticket PYTHON-893
        @expected_result creation fails on Python > 3 and < 3.7

        @test_category row_factory
        """
        if not NAMEDTUPLE_CREATION_BUG:
            named_tuple_factory(self.long_colnames, self.long_rows)
            return

        with warnings.catch_warnings(record=True) as w:
            rows = named_tuple_factory(self.long_colnames, self.long_rows)
        assert len(w) == 1
        warning = w[0]
        assert 'pseudo_namedtuple_factory' in str(warning)
        assert '3.7' in str(warning)

        for r in rows:
            assert r.col0 == self.long_rows[0][0]

    def test_creation_no_warning_on_short_column_list(self):
        """
        Tests that normal namedtuple row creation still works after PYTHON-893 fix

        @since 3.15
        @jira_ticket PYTHON-893
        @expected_result creates namedtuple-based Rows

        @test_category row_factory
        """
        with warnings.catch_warnings(record=True) as w:
            rows = named_tuple_factory(self.short_colnames, self.short_rows)
        assert len(w) == 0
        # check that this is a real namedtuple
        assert hasattr(rows[0], '_fields')
        assert isinstance(rows[0], tuple)


# ---------------------------------------------------------------------------
# Correctness tests for the namedtuple-class cache in named_tuple_factory.
#
# These were originally added under benchmarks/test_named_tuple_factory_benchmark.py
# alongside the timing benchmarks for the same cache, but the project's
# wheel test commands only run `pytest tests/unit`, so none of this
# cache-hit/cache-key/eviction correctness coverage was exercised in CI.
# Moved here so regressions in the production cache are actually caught;
# only genuine timing/benchmark code remains under benchmarks/.
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
    """Reference implementation without caching, used to verify the cached
    implementation in cassandra.query.named_tuple_factory returns
    equivalent results."""
    clean_column_names = map(_clean_column_name_old, colnames)
    try:
        Row = namedtuple("Row", clean_column_names)
    except SyntaxError:
        raise
    except Exception:
        clean_column_names = list(map(_clean_column_name_old, colnames))
        Row = namedtuple("Row", _sanitize_identifiers(clean_column_names))
    return [Row(*row) for row in rows]


def make_colnames(n):
    return tuple(f"col_{i}" for i in range(n))


def make_rows(ncols, nrows):
    return [tuple(range(ncols)) for _ in range(nrows)]


class TestNamedTupleFactoryCache:
    """Verify the cached implementation matches the uncached one, and that
    the cache's keying and eviction behavior are correct."""

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

    def test_case_difference_does_not_collide(self):
        # Same names modulo case must not share a cached Row class: the raw
        # (uncleaned) column names differ, so the cache key differs too.
        _named_tuple_cache.clear()
        result1 = named_tuple_factory(("Name", "Age"), [("Alice", 30)])
        result2 = named_tuple_factory(("name", "age"), [("bob", 25)])
        assert type(result1[0]) is not type(result2[0])
        assert result1[0]._fields == ("Name", "Age")
        assert result2[0]._fields == ("name", "age")

    def test_column_order_does_not_collide(self):
        # Same names in a different order must not share a cached Row class.
        _named_tuple_cache.clear()
        result1 = named_tuple_factory(("a", "b"), [(1, 2)])
        result2 = named_tuple_factory(("b", "a"), [(2, 1)])
        assert type(result1[0]) is not type(result2[0])
        assert result1[0]._fields == ("a", "b")
        assert result2[0]._fields == ("b", "a")

    def test_cache_is_bounded_and_evicts_oldest(self):
        # Guard against unbounded growth for applications executing many
        # distinct ad hoc queries against highly variable/generated schemas.
        _named_tuple_cache.clear()
        original_max_size = query_module._NAMED_TUPLE_CACHE_MAX_SIZE
        query_module._NAMED_TUPLE_CACHE_MAX_SIZE = 3
        try:
            for i in range(5):
                named_tuple_factory((f"col_{i}",), [(i,)])
            assert len(_named_tuple_cache) == 3
            # Oldest entries (col_0, col_1) should have been evicted first.
            assert ("col_0",) not in _named_tuple_cache
            assert ("col_1",) not in _named_tuple_cache
            assert ("col_4",) in _named_tuple_cache
        finally:
            query_module._NAMED_TUPLE_CACHE_MAX_SIZE = original_max_size
            _named_tuple_cache.clear()


class TestNamedTupleFactoryCacheThreadSafety:
    """
    Regression test for a race in the namedtuple-class cache's eviction
    path: the check-evict-insert sequence on a cache miss was not
    synchronized, so one thread's `next(iter(_named_tuple_cache))` (picking
    an eviction victim) could race with another thread mutating the same
    dict, raising `RuntimeError: dictionary changed size during iteration`;
    separately, two threads that both observed the cache under its size
    bound before either inserted could together push it past that bound.
    This driver advertises free-threaded Python support (see the
    ``Free Threading`` classifier in pyproject.toml and the ``3.14t`` CI
    jobs), where such races are far more likely to manifest than under a
    GIL-enabled interpreter.

    This test hammers the cache from many threads, each using many distinct
    column-name sets (so almost every call is a miss, forcing constant
    eviction once the bound -- lowered here for a fast, reliable repro --
    is reached), and asserts that no thread ever observes an exception and
    that the cache never exceeds its stated bound.
    """

    def test_concurrent_misses_do_not_raise_and_respect_bound(self):
        _named_tuple_cache.clear()
        original_max_size = query_module._NAMED_TUPLE_CACHE_MAX_SIZE
        max_size = 50
        query_module._NAMED_TUPLE_CACHE_MAX_SIZE = max_size

        n_threads = 32
        n_iters_per_thread = 100
        errors = []
        errors_lock = threading.Lock()
        start_barrier = threading.Barrier(n_threads)

        def worker(tid):
            # Synchronize thread start as tightly as possible to maximize
            # the chance of many threads racing into the cache-miss path
            # (and the eviction it triggers) together.
            start_barrier.wait()
            for i in range(n_iters_per_thread):
                colnames = (f"t{tid}_col_{i}_a", f"t{tid}_col_{i}_b", f"t{tid}_col_{i}_c")
                try:
                    named_tuple_factory(colnames, [(1, 2, 3)])
                except Exception as e:
                    with errors_lock:
                        errors.append((tid, i, e))

        try:
            threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

            assert errors == [], (
                "named_tuple_factory raised under concurrent cache misses "
                "(expected no exceptions): %r" % (errors,)
            )
            assert len(_named_tuple_cache) <= max_size, (
                "cache grew past its stated bound (%d) under concurrent "
                "misses: size=%d" % (max_size, len(_named_tuple_cache))
            )
        finally:
            query_module._NAMED_TUPLE_CACHE_MAX_SIZE = original_max_size
            _named_tuple_cache.clear()
