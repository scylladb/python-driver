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


from cassandra.query import named_tuple_factory, _named_tuple_row_class

import logging
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


def named_tuple_factory_uncached(colnames, rows):
    Row = _named_tuple_row_class.__wrapped__(tuple(colnames))
    return [Row(*row) for row in rows]


def make_colnames(n):
    return tuple(f"col_{i}" for i in range(n))


def make_rows(ncols, nrows):
    return [tuple(range(ncols)) for _ in range(nrows)]


class TestNamedTupleFactoryCache:
    """Verify the cached implementation matches the uncached one and is keyed correctly."""

    @pytest.mark.parametrize("ncols", [1, 5, 10, 20])
    @pytest.mark.parametrize("nrows", [1, 10, 100])
    def test_results_match(self, ncols, nrows):
        colnames = make_colnames(ncols)
        rows = make_rows(ncols, nrows)
        _named_tuple_row_class.cache_clear()
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

    def test_case_difference_does_not_collide(self):
        # Same names modulo case must not share a cached Row class: the raw
        # (uncleaned) column names differ, so the cache key differs too.
        _named_tuple_row_class.cache_clear()
        result1 = named_tuple_factory(("Name", "Age"), [("Alice", 30)])
        result2 = named_tuple_factory(("name", "age"), [("bob", 25)])
        assert type(result1[0]) is not type(result2[0])
        assert result1[0]._fields == ("Name", "Age")
        assert result2[0]._fields == ("name", "age")

    def test_column_order_does_not_collide(self):
        # Same names in a different order must not share a cached Row class.
        _named_tuple_row_class.cache_clear()
        result1 = named_tuple_factory(("a", "b"), [(1, 2)])
        result2 = named_tuple_factory(("b", "a"), [(2, 1)])
        assert type(result1[0]) is not type(result2[0])
        assert result1[0]._fields == ("a", "b")
        assert result2[0]._fields == ("b", "a")
