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

from unittest.mock import patch

from cassandra.cqlengine import columns, CQLEngineException
from cassandra.cqlengine.management import sync_table, drop_table, create_keyspace_simple, drop_keyspace
from cassandra.cqlengine import models
from cassandra.cqlengine.models import Model, ModelDefinitionException
from uuid import uuid1
from tests.integration import pypy
from tests.integration.cqlengine.base import TestQueryUpdateModel
import pytest

class TestModel(unittest.TestCase):
    """ Tests the non-io functionality of models """

    def test_instance_equality(self):
        """ tests the model equality functionality """
        class EqualityModel(Model):

            pk = columns.Integer(primary_key=True)

        m0 = EqualityModel(pk=0)
        m1 = EqualityModel(pk=1)

        assert m0 == m0
        assert m0 != m1

    def test_model_equality(self):
        """ tests the model equality functionality """
        class EqualityModel0(Model):

            pk = columns.Integer(primary_key=True)

        class EqualityModel1(Model):

            kk = columns.Integer(primary_key=True)

        m0 = EqualityModel0(pk=0)
        m1 = EqualityModel1(kk=1)

        assert m0 == m0
        assert m0 != m1

    def test_keywords_as_names(self):
        """
        Test for CQL keywords as names

        test_keywords_as_names tests that CQL keywords are properly and automatically quoted in cqlengine. It creates
        a keyspace, keyspace, which should be automatically quoted to "keyspace" in CQL. It then creates a table, table,
        which should also be automatically quoted to "table". It then verfies that operations can be done on the
        "keyspace"."table" which has been created. It also verifies that table alternations work and operations can be
        performed on the altered table.

        @since 2.6.0
        @jira_ticket PYTHON-244
        @expected_result Cqlengine should quote CQL keywords properly when creating keyspaces and tables.

        @test_category schema:generation
        """

        # If the keyspace exists, it will not be re-created
        create_keyspace_simple('keyspace', 1)

        class table(Model):
            __keyspace__ = 'keyspace'
            select = columns.Integer(primary_key=True)
            table = columns.Text()

        # In case the table already exists in keyspace
        drop_table(table)

        # Create should work
        sync_table(table)

        created = table.create(select=0, table='table')
        selected = table.objects(select=0)[0]
        assert created.select == selected.select
        assert created.table == selected.table

        # Alter should work
        class table(Model):
            __keyspace__ = 'keyspace'
            select = columns.Integer(primary_key=True)
            table = columns.Text()
            where = columns.Text()

        sync_table(table)

        created = table.create(select=1, table='table')
        selected = table.objects(select=1)[0]
        assert created.select == selected.select
        assert created.table == selected.table
        assert created.where == selected.where

        drop_keyspace('keyspace')

    def test_column_family(self):
        class TestModel(Model):
            k = columns.Integer(primary_key=True)

        # no model keyspace uses default
        assert TestModel.column_family_name() == "%s.test_model" % (models.DEFAULT_KEYSPACE,)

        # model keyspace overrides
        TestModel.__keyspace__ = "my_test_keyspace"
        assert TestModel.column_family_name() == "%s.test_model" % (TestModel.__keyspace__,)

        # neither set should raise CQLEngineException before failing or formatting an invalid name
        del TestModel.__keyspace__
        with patch('cassandra.cqlengine.models.DEFAULT_KEYSPACE', None):
            with pytest.raises(CQLEngineException):
                TestModel.column_family_name()
            # .. but we can still get the bare CF name
            assert TestModel.column_family_name(include_keyspace=False) == "test_model"

    def test_column_family_case_sensitive(self):
        """
        Test to ensure case sensitivity is honored when __table_name_case_sensitive__ flag is set

        @since 3.1
        @jira_ticket PYTHON-337
        @expected_result table_name case is respected

        @test_category object_mapper
        """
        class TestModel(Model):
            __table_name__ = 'TestModel'
            __table_name_case_sensitive__ = True

            k = columns.Integer(primary_key=True)

        assert TestModel.column_family_name() == '%s."TestModel"' % (models.DEFAULT_KEYSPACE,)

        TestModel.__keyspace__ = "my_test_keyspace"
        assert TestModel.column_family_name() == '%s."TestModel"' % (TestModel.__keyspace__,)

        del TestModel.__keyspace__
        with patch('cassandra.cqlengine.models.DEFAULT_KEYSPACE', None):
            with pytest.raises(CQLEngineException):
                TestModel.column_family_name()
            assert TestModel.column_family_name(include_keyspace=False) == '"TestModel"'


class BuiltInAttributeConflictTest(unittest.TestCase):
    """tests Model definitions that conflict with built-in attributes/methods"""

    def test_model_with_attribute_name_conflict(self):
        """should raise exception when model defines column that conflicts with built-in attribute"""
        with pytest.raises(ModelDefinitionException):
            class IllegalTimestampColumnModel(Model):

                my_primary_key = columns.Integer(primary_key=True)
                timestamp = columns.BigInt()

    def test_model_with_method_name_conflict(self):
        """should raise exception when model defines column that conflicts with built-in method"""
        with pytest.raises(ModelDefinitionException):
            class IllegalFilterColumnModel(Model):

                my_primary_key = columns.Integer(primary_key=True)
                filter = columns.Text()


class ModelOverWriteTest(unittest.TestCase):

    def test_model_over_write(self):
        """
        Test to ensure overwriting of primary keys in model inheritance is allowed

        This is currently only an issue in PyPy. When PYTHON-504 is introduced this should
        be updated error out and warn the user

        @since 3.6.0
        @jira_ticket PYTHON-576
        @expected_result primary keys can be overwritten via inheritance

        @test_category object_mapper
        """
        class TimeModelBase(Model):
            uuid = columns.TimeUUID(primary_key=True)

        class DerivedTimeModel(TimeModelBase):
            __table_name__ = 'derived_time'
            uuid = columns.TimeUUID(primary_key=True, partition_key=True)
            value = columns.Text(required=False)

        # In case the table already exists in keyspace
        drop_table(DerivedTimeModel)

        sync_table(DerivedTimeModel)
        uuid_value = uuid1()
        uuid_value2 = uuid1()
        DerivedTimeModel.create(uuid=uuid_value, value="first")
        DerivedTimeModel.create(uuid=uuid_value2, value="second")
        DerivedTimeModel.objects.filter(uuid=uuid_value)


class TestColumnComparison(unittest.TestCase):
    def test_comparison(self):
        l = [TestQueryUpdateModel.partition.column,
             TestQueryUpdateModel.cluster.column,
             TestQueryUpdateModel.count.column,
             TestQueryUpdateModel.text.column,
             TestQueryUpdateModel.text_set.column,
             TestQueryUpdateModel.text_list.column,
             TestQueryUpdateModel.text_map.column]

        assert l == sorted(l)
        assert TestQueryUpdateModel.partition.column != TestQueryUpdateModel.cluster.column
        assert TestQueryUpdateModel.partition.column <= TestQueryUpdateModel.cluster.column
        assert TestQueryUpdateModel.cluster.column > TestQueryUpdateModel.partition.column
        assert TestQueryUpdateModel.cluster.column >= TestQueryUpdateModel.partition.column


class TestDeprecationWarning(unittest.TestCase):
    def test_deprecation_warnings(self):
        """
        Test to some deprecation warning have been added. It tests warnings for
        negative index, negative index slicing and table sensitive removal

        This test should be removed in 4.0, that's why the imports are in
        this test, so it's easier to remove

        @since 3.13
        @jira_ticket PYTHON-877
        @expected_result the deprecation warnings are emitted

        @test_category logs
        """
        import warnings

        class SensitiveModel(Model):
            __table_name__ = 'SensitiveModel'
            __table_name_case_sensitive__ = True
            k = columns.Integer(primary_key=True)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sync_table(SensitiveModel)
            self.addCleanup(drop_table, SensitiveModel)

            SensitiveModel.create(k=0)

            rows = SensitiveModel.objects().all().allow_filtering()
            rows[-1]
            rows[-1:]

            # ignore DeprecationWarning('The loop argument is deprecated since Python 3.8, and scheduled for removal in Python 3.10.')
            relevant_warnings = [warn for warn in w if "The loop argument is deprecated" not in str(warn.message)]

            assert "__table_name_case_sensitive__ will be removed in 4.0." in str(relevant_warnings[0].message)
            assert "__table_name_case_sensitive__ will be removed in 4.0." in str(relevant_warnings[1].message)
            assert "ModelQuerySet indexing with negative indices support will be removed in 4.0." in str(relevant_warnings[2].message)
            assert "ModelQuerySet slicing with negative indices support will be removed in 4.0." in str(relevant_warnings[3].message)
