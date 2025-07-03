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

from packaging.version import Version

from tests.integration import (BasicExistingKeyspaceUnitTestCase, BasicSharedKeyspaceUnitTestCase,
                               BasicSharedKeyspaceUnitTestCaseRF1,
                               greaterthanorequaldse51,
                               use_single_node,
                               DSE_VERSION, requiredse, TestCluster)

import unittest

import logging
import time


log = logging.getLogger(__name__)


def setup_module():
    if DSE_VERSION:
        use_single_node()


@requiredse
class RLACMetadataTests(BasicSharedKeyspaceUnitTestCase):

    @classmethod
    def setUpClass(cls):
        if DSE_VERSION:
            super(RLACMetadataTests, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        if DSE_VERSION:
            super(RLACMetadataTests, cls).setUpClass()

    @greaterthanorequaldse51
    def test_rlac_on_table(self):
        """
        Checks to ensure that the RLAC table extension appends the proper cql on export to tables

        @since 3.20
        @jira_ticket PYTHON-638
        @expected_result Invalid hosts on the contact list should be excluded

        @test_category metadata
        """
        self.session.execute("CREATE TABLE {0}.reports ("
                             " report_user text, "
                             " report_number int, "
                             " report_month int, "
                             " report_year int, "
                             " report_text text,"
                             " PRIMARY KEY (report_user, report_number))".format(self.keyspace_name))
        restrict_cql = "RESTRICT ROWS ON {0}.reports USING report_user".format(self.keyspace_name)
        self.session.execute(restrict_cql)
        table_meta = self.cluster.metadata.keyspaces[self.keyspace_name].tables['reports']
        self.assertTrue(restrict_cql in table_meta.export_as_string())

    @unittest.skip("Dse 5.1 doesn't support MV and RLAC remove after update")
    @greaterthanorequaldse51
    def test_rlac_on_mv(self):
        """
        Checks to ensure that the RLAC table extension appends the proper cql to export on mV's

        @since 3.20
        @jira_ticket PYTHON-682
        @expected_result Invalid hosts on the contact list should be excluded

        @test_category metadata
        """
        self.session.execute("CREATE TABLE {0}.reports2 ("
                             " report_user text, "
                             " report_number int, "
                             " report_month int, "
                             " report_year int, "
                             " report_text text,"
                             " PRIMARY KEY (report_user, report_number))".format(self.keyspace_name))
        self.session.execute("CREATE MATERIALIZED VIEW {0}.reports_by_year AS "
                             " SELECT report_year, report_user, report_number, report_text FROM {0}.reports2 "
                             " WHERE report_user IS NOT NULL AND report_number IS NOT NULL AND report_year IS NOT NULL "
                             " PRIMARY KEY ((report_year, report_user), report_number)".format(self.keyspace_name))

        restrict_cql_table = "RESTRICT ROWS ON {0}.reports2 USING report_user".format(self.keyspace_name)
        self.session.execute(restrict_cql_table)
        restrict_cql_view = "RESTRICT ROWS ON {0}.reports_by_year USING report_user".format(self.keyspace_name)
        self.session.execute(restrict_cql_view)
        table_cql = self.cluster.metadata.keyspaces[self.keyspace_name].tables['reports2'].export_as_string()
        view_cql = self.cluster.metadata.keyspaces[self.keyspace_name].tables['reports2'].views["reports_by_year"].export_as_string()
        self.assertTrue(restrict_cql_table in table_cql)
        self.assertTrue(restrict_cql_view in table_cql)
        self.assertTrue(restrict_cql_view in view_cql)
        self.assertTrue(restrict_cql_table not in view_cql)

