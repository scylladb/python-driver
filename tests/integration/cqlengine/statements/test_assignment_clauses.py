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
import unittest

from cassandra.cqlengine.statements import AssignmentClause, SetUpdateClause, ListUpdateClause, MapUpdateClause, MapDeleteClause, FieldDeleteClause, CounterUpdateClause


class AssignmentClauseTests(unittest.TestCase):

    def test_rendering(self):
        pass

    def test_insert_tuple(self):
        ac = AssignmentClause('a', 'b')
        ac.set_context_id(10)
        assert ac.insert_tuple() == ('a', 10)


class SetUpdateClauseTests(unittest.TestCase):

    def test_update_from_none(self):
        c = SetUpdateClause('s', set((1, 2)), previous=None)
        c._analyze()
        c.set_context_id(0)

        assert c._assignments == set((1, 2))
        assert c._additions is None
        assert c._removals is None

        assert c.get_context_size() == 1
        assert str(c) == '"s" = %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': set((1, 2))}

    def test_null_update(self):
        """ tests setting a set to None creates an empty update statement """
        c = SetUpdateClause('s', None, previous=set((1, 2)))
        c._analyze()
        c.set_context_id(0)

        assert c._assignments is None
        assert c._additions is None
        assert c._removals is None

        assert c.get_context_size() == 0
        assert str(c) == ''

        ctx = {}
        c.update_context(ctx)
        assert ctx == {}

    def test_no_update(self):
        """ tests an unchanged value creates an empty update statement """
        c = SetUpdateClause('s', set((1, 2)), previous=set((1, 2)))
        c._analyze()
        c.set_context_id(0)

        assert c._assignments is None
        assert c._additions is None
        assert c._removals is None

        assert c.get_context_size() == 0
        assert str(c) == ''

        ctx = {}
        c.update_context(ctx)
        assert ctx == {}

    def test_update_empty_set(self):
        """tests assigning a set to an empty set creates a nonempty
        update statement and nonzero context size."""
        c = SetUpdateClause(field='s', value=set())
        c._analyze()
        c.set_context_id(0)

        assert c._assignments == set()
        assert c._additions is None
        assert c._removals is None

        assert c.get_context_size() == 1
        assert str(c) == '"s" = %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': set()}

    def test_additions(self):
        c = SetUpdateClause('s', set((1, 2, 3)), previous=set((1, 2)))
        c._analyze()
        c.set_context_id(0)

        assert c._assignments is None
        assert c._additions == set((3,))
        assert c._removals is None

        assert c.get_context_size() == 1
        assert str(c) == '"s" = "s" + %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': set((3,))}

    def test_removals(self):
        c = SetUpdateClause('s', set((1, 2)), previous=set((1, 2, 3)))
        c._analyze()
        c.set_context_id(0)

        assert c._assignments is None
        assert c._additions is None
        assert c._removals == set((3,))

        assert c.get_context_size() == 1
        assert str(c) == '"s" = "s" - %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': set((3,))}

    def test_additions_and_removals(self):
        c = SetUpdateClause('s', set((2, 3)), previous=set((1, 2)))
        c._analyze()
        c.set_context_id(0)

        assert c._assignments is None
        assert c._additions == set((3,))
        assert c._removals == set((1,))

        assert c.get_context_size() == 2
        assert str(c) == '"s" = "s" + %(0)s, "s" = "s" - %(1)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': set((3,)), '1': set((1,))}


class ListUpdateClauseTests(unittest.TestCase):

    def test_update_from_none(self):
        c = ListUpdateClause('s', [1, 2, 3])
        c._analyze()
        c.set_context_id(0)

        assert c._assignments == [1, 2, 3]
        assert c._append is None
        assert c._prepend is None

        assert c.get_context_size() == 1
        assert str(c) == '"s" = %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': [1, 2, 3]}

    def test_update_from_empty(self):
        c = ListUpdateClause('s', [1, 2, 3], previous=[])
        c._analyze()
        c.set_context_id(0)

        assert c._assignments == [1, 2, 3]
        assert c._append is None
        assert c._prepend is None

        assert c.get_context_size() == 1
        assert str(c) == '"s" = %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': [1, 2, 3]}

    def test_update_from_different_list(self):
        c = ListUpdateClause('s', [1, 2, 3], previous=[3, 2, 1])
        c._analyze()
        c.set_context_id(0)

        assert c._assignments == [1, 2, 3]
        assert c._append is None
        assert c._prepend is None

        assert c.get_context_size() == 1
        assert str(c) == '"s" = %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': [1, 2, 3]}

    def test_append(self):
        c = ListUpdateClause('s', [1, 2, 3, 4], previous=[1, 2])
        c._analyze()
        c.set_context_id(0)

        assert c._assignments is None
        assert c._append == [3, 4]
        assert c._prepend is None

        assert c.get_context_size() == 1
        assert str(c) == '"s" = "s" + %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': [3, 4]}

    def test_prepend(self):
        c = ListUpdateClause('s', [1, 2, 3, 4], previous=[3, 4])
        c._analyze()
        c.set_context_id(0)

        assert c._assignments is None
        assert c._append is None
        assert c._prepend == [1, 2]

        assert c.get_context_size() == 1
        assert str(c) == '"s" = %(0)s + "s"'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': [1, 2]}

    def test_append_and_prepend(self):
        c = ListUpdateClause('s', [1, 2, 3, 4, 5, 6], previous=[3, 4])
        c._analyze()
        c.set_context_id(0)

        assert c._assignments is None
        assert c._append == [5, 6]
        assert c._prepend == [1, 2]

        assert c.get_context_size() == 2
        assert str(c) == '"s" = %(0)s + "s", "s" = "s" + %(1)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': [1, 2], '1': [5, 6]}

    def test_shrinking_list_update(self):
        """ tests that updating to a smaller list results in an insert statement """
        c = ListUpdateClause('s', [1, 2, 3], previous=[1, 2, 3, 4])
        c._analyze()
        c.set_context_id(0)

        assert c._assignments == [1, 2, 3]
        assert c._append is None
        assert c._prepend is None

        assert c.get_context_size() == 1
        assert str(c) == '"s" = %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': [1, 2, 3]}


class MapUpdateTests(unittest.TestCase):

    def test_update(self):
        c = MapUpdateClause('s', {3: 0, 5: 6}, previous={5: 0, 3: 4})
        c._analyze()
        c.set_context_id(0)

        assert c._updates == [3, 5]
        assert c.get_context_size() == 4
        assert str(c) == '"s"[%(0)s] = %(1)s, "s"[%(2)s] = %(3)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': 3, "1": 0, '2': 5, '3': 6}

    def test_update_from_null(self):
        c = MapUpdateClause('s', {3: 0, 5: 6})
        c._analyze()
        c.set_context_id(0)

        assert c._updates == [3, 5]
        assert c.get_context_size() == 4
        assert str(c) == '"s"[%(0)s] = %(1)s, "s"[%(2)s] = %(3)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': 3, "1": 0, '2': 5, '3': 6}

    def test_nulled_columns_arent_included(self):
        c = MapUpdateClause('s', {3: 0}, {1: 2, 3: 4})
        c._analyze()
        c.set_context_id(0)

        assert 1 not in c._updates


class CounterUpdateTests(unittest.TestCase):

    def test_positive_update(self):
        c = CounterUpdateClause('a', 5, 3)
        c.set_context_id(5)

        assert c.get_context_size() == 1
        assert str(c) == '"a" = "a" + %(5)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'5': 2}

    def test_negative_update(self):
        c = CounterUpdateClause('a', 4, 7)
        c.set_context_id(3)

        assert c.get_context_size() == 1
        assert str(c) == '"a" = "a" - %(3)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'3': 3}

    def noop_update(self):
        c = CounterUpdateClause('a', 5, 5)
        c.set_context_id(5)

        assert c.get_context_size() == 1
        assert str(c) == '"a" = "a" + %(0)s'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'5': 0}


class MapDeleteTests(unittest.TestCase):

    def test_update(self):
        c = MapDeleteClause('s', {3: 0}, {1: 2, 3: 4, 5: 6})
        c._analyze()
        c.set_context_id(0)

        assert c._removals == [1, 5]
        assert c.get_context_size() == 2
        assert str(c) == '"s"[%(0)s], "s"[%(1)s]'

        ctx = {}
        c.update_context(ctx)
        assert ctx == {'0': 1, '1': 5}


class FieldDeleteTests(unittest.TestCase):

    def test_str(self):
        f = FieldDeleteClause("blake")
        assert str(f) == '"blake"'
