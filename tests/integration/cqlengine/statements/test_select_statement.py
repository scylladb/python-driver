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

from cassandra.cqlengine.columns import Column
from cassandra.cqlengine.statements import SelectStatement, WhereClause
from cassandra.cqlengine.operators import *

class SelectStatementTests(unittest.TestCase):

    def test_single_field_is_listified(self):
        """ tests that passing a string field into the constructor puts it into a list """
        ss = SelectStatement('table', 'field')
        assert ss.fields == ['field']

    def test_field_rendering(self):
        """ tests that fields are properly added to the select statement """
        ss = SelectStatement('table', ['f1', 'f2'])
        assert str(ss).startswith('SELECT "f1", "f2"'), str(ss)
        assert str(ss).startswith('SELECT "f1", "f2"'), str(ss)

    def test_none_fields_rendering(self):
        """ tests that a '*' is added if no fields are passed in """
        ss = SelectStatement('table')
        assert str(ss).startswith('SELECT *'), str(ss)
        assert str(ss).startswith('SELECT *'), str(ss)

    def test_table_rendering(self):
        ss = SelectStatement('table')
        assert str(ss).startswith('SELECT * FROM table'), str(ss)
        assert str(ss).startswith('SELECT * FROM table'), str(ss)

    def test_where_clause_rendering(self):
        ss = SelectStatement('table')
        ss.add_where(Column(db_field='a'), EqualsOperator(), 'b')
        assert str(ss) == 'SELECT * FROM table WHERE "a" = %(0)s', str(ss)

    def test_count(self):
        ss = SelectStatement('table', count=True, limit=10, order_by='d')
        ss.add_where(Column(db_field='a'), EqualsOperator(), 'b')
        assert str(ss) == 'SELECT COUNT(*) FROM table WHERE "a" = %(0)s LIMIT 10', str(ss)
        assert 'LIMIT' in str(ss)
        assert 'ORDER' not in str(ss)

    def test_distinct(self):
        ss = SelectStatement('table', distinct_fields=['field2'])
        ss.add_where(Column(db_field='field1'), EqualsOperator(), 'b')
        assert str(ss) == 'SELECT DISTINCT "field2" FROM table WHERE "field1" = %(0)s', str(ss)

        ss = SelectStatement('table', distinct_fields=['field1', 'field2'])
        assert str(ss) == 'SELECT DISTINCT "field1", "field2" FROM table'

        ss = SelectStatement('table', distinct_fields=['field1'], count=True)
        assert str(ss) == 'SELECT DISTINCT COUNT("field1") FROM table'

    def test_context(self):
        ss = SelectStatement('table')
        ss.add_where(Column(db_field='a'), EqualsOperator(), 'b')
        assert ss.get_context() == {'0': 'b'}

    def test_context_id_update(self):
        """ tests that the right things happen the the context id """
        ss = SelectStatement('table')
        ss.add_where(Column(db_field='a'), EqualsOperator(), 'b')
        assert ss.get_context() == {'0': 'b'}
        assert str(ss) == 'SELECT * FROM table WHERE "a" = %(0)s'

        ss.update_context_id(5)
        assert ss.get_context() == {'5': 'b'}
        assert str(ss) == 'SELECT * FROM table WHERE "a" = %(5)s'

    def test_additional_rendering(self):
        ss = SelectStatement(
            'table',
            None,
            order_by=['x', 'y'],
            limit=15,
            allow_filtering=True
        )
        qstr = str(ss)
        assert 'LIMIT 15' in qstr
        assert 'ORDER BY x, y' in qstr
        assert 'ALLOW FILTERING' in qstr

    def test_limit_rendering(self):
        ss = SelectStatement('table', None, limit=10)
        qstr = str(ss)
        assert 'LIMIT 10' in qstr

        ss = SelectStatement('table', None, limit=0)
        qstr = str(ss)
        assert 'LIMIT' not in qstr

        ss = SelectStatement('table', None, limit=None)
        qstr = str(ss)
        assert 'LIMIT' not in qstr
