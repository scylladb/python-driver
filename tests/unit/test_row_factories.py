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


from cassandra.query import (
    named_tuple_factory,
    _build_named_tuple_row_class,
    _named_tuple_row_class,
    _NAMED_TUPLE_ROW_CLASS_CACHE_SIZE,
)

import logging
import threading
import warnings

from unittest import TestCase

import pytest


log = logging.getLogger(__name__)


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
        @expected_result creates namedtuple-based Rows (no 255-field limit since Python 3.7)

        @test_category row_factory
        """
        rows = named_tuple_factory(self.long_colnames, self.long_rows)
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


class TestNamedTupleFactoryCache:

    def test_results_match_uncached(self):
        colnames = tuple("col_%d" % i for i in range(10))
        rows = [tuple(range(10)) for _ in range(5)]
        _named_tuple_row_class.cache_clear()
        Row = _build_named_tuple_row_class(colnames)
        result = named_tuple_factory(colnames, rows)
        assert [tuple(r) for r in result] == [tuple(Row(*r)) for r in rows]
        assert result[0]._fields == Row._fields

    def test_cache_hit_returns_same_class(self):
        colnames = ("name", "age", "email")
        rows1 = [("Alice", 30, "a@b.com")]
        rows2 = [("Bob", 25, "b@c.com")]
        _named_tuple_row_class.cache_clear()
        result1 = named_tuple_factory(colnames, rows1)
        result2 = named_tuple_factory(colnames, rows2)
        # Same Row class should be reused
        assert type(result1[0]) is type(result2[0])

    def test_different_schemas_get_different_classes(self):
        _named_tuple_row_class.cache_clear()
        result1 = named_tuple_factory(("a", "b"), [(1, 2)])
        result2 = named_tuple_factory(("x", "y"), [(3, 4)])
        assert type(result1[0]) is not type(result2[0])
        assert result1[0]._fields == ("a", "b")
        assert result2[0]._fields == ("x", "y")

    def test_case_and_order_do_not_collide(self):
        # The key is the raw column-name tuple, so case/order variants get separate classes.
        _named_tuple_row_class.cache_clear()
        schemas = [("Name", "Age"), ("name", "age"), ("age", "name")]
        classes = [type(named_tuple_factory(c, [(1, 2)])[0]) for c in schemas]
        assert len(set(classes)) == 3
        assert [c._fields for c in classes] == schemas

    def test_cache_eviction_is_bounded(self):
        # Inserting more schemas than maxsize must not grow the cache past its
        # bound: the oldest generated classes are evicted.
        _named_tuple_row_class.cache_clear()
        maxsize = _NAMED_TUPLE_ROW_CLASS_CACHE_SIZE
        for i in range(maxsize + 1):
            _named_tuple_row_class(("col_%d" % i,))
        assert len(_named_tuple_row_class._cache) == maxsize
        # FIFO: the oldest schema was evicted, the newest is retained.
        assert ("col_0",) not in _named_tuple_row_class._cache
        assert ("col_%d" % maxsize,) in _named_tuple_row_class._cache

    def test_concurrent_misses_share_one_class(self):
        # Concurrent first use of a cold schema must yield a single Row class,
        # not one per thread.
        _named_tuple_row_class.cache_clear()
        colnames = ("name", "age")
        nthreads = 16
        barrier = threading.Barrier(nthreads)
        results = [None] * nthreads

        def worker(idx):
            barrier.wait()
            results[idx] = _named_tuple_row_class(colnames)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(nthreads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(set(results)) == 1
        assert results[0]._fields == colnames


class TestNamedTupleFactoryFallback:
    """Cover the invalid-identifier sanitization path and its caching."""

    def test_invalid_identifier_is_sanitized(self):
        _named_tuple_row_class.cache_clear()
        rows = named_tuple_factory(("col", "1bad"), [(1, 2)])
        assert rows[0]._fields == ("col", "field_1_")
        assert tuple(rows[0]) == (1, 2)

    def test_invalid_identifier_warning_logged_once_per_schema(self, caplog):
        _named_tuple_row_class.cache_clear()
        with caplog.at_level(logging.WARNING, logger="cassandra.query"):
            named_tuple_factory(("1bad",), [(1,)])
            named_tuple_factory(("1bad",), [(2,)])
        warnings = [r for r in caplog.records
                    if "Failed creating named tuple" in r.getMessage()]
        assert len(warnings) == 1

    def test_invalid_identifier_warning_per_distinct_schema(self, caplog):
        _named_tuple_row_class.cache_clear()
        with caplog.at_level(logging.WARNING, logger="cassandra.query"):
            named_tuple_factory(("1bad",), [(1,)])
            named_tuple_factory(("2bad",), [(1,)])
            named_tuple_factory(("2bad",), [(2,)])
        warnings = [r for r in caplog.records
                    if "Failed creating named tuple" in r.getMessage()]
        assert len(warnings) == 2

    def test_duplicate_and_keyword_columns_are_sanitized(self):
        _named_tuple_row_class.cache_clear()
        rows = named_tuple_factory(("a", "a", "class"), [(1, 2, 3)])
        assert tuple(rows[0]) == (1, 2, 3)
        assert len(set(rows[0]._fields)) == 3
