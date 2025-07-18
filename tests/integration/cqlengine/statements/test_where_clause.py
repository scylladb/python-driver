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

from cassandra.cqlengine.operators import EqualsOperator
from cassandra.cqlengine.statements import StatementException, WhereClause
import pytest


class TestWhereClause(unittest.TestCase):

    def test_operator_check(self):
        """ tests that creating a where statement with a non BaseWhereOperator object fails """
        with pytest.raises(StatementException):
            WhereClause('a', 'b', 'c')

    def test_where_clause_rendering(self):
        """ tests that where clauses are rendered properly """
        wc = WhereClause('a', EqualsOperator(), 'c')
        wc.set_context_id(5)

        assert '"a" = %(5)s' == str(wc), str(wc)
        assert '"a" = %(5)s' == str(wc), type(wc)

    def test_equality_method(self):
        """ tests that 2 identical where clauses evaluate as == """
        wc1 = WhereClause('a', EqualsOperator(), 'c')
        wc2 = WhereClause('a', EqualsOperator(), 'c')
        assert wc1 == wc2
