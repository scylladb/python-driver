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

import os
import time

from cassandra.cqlengine import columns, connection, models
from cassandra.cqlengine.management import (CQLENG_ALLOW_SCHEMA_MANAGEMENT,
                                      create_keyspace_simple, drop_table,
                                      sync_table)
from cassandra.cqlengine.statements import IsNotNull
from tests.integration import DSE_VERSION, requiredse, CASSANDRA_IP, TestCluster
from tests.integration.advanced import use_single_node_with_graph_and_solr
from tests.integration.cqlengine import DEFAULT_KEYSPACE


class SimpleNullableModel(models.Model):
    __keyspace__ = DEFAULT_KEYSPACE
    partition = columns.Integer(primary_key=True)
    nullable = columns.Integer(required=False)
    # nullable = columns.Integer(required=False, custom_index=True)


def setup_module():
    if DSE_VERSION:
        os.environ[CQLENG_ALLOW_SCHEMA_MANAGEMENT] = '1'
        use_single_node_with_graph_and_solr()
        setup_connection(DEFAULT_KEYSPACE)
        create_keyspace_simple(DEFAULT_KEYSPACE, 1)
        sync_table(SimpleNullableModel)


def setup_connection(keyspace_name):
    connection.setup([CASSANDRA_IP],
                     # consistency=ConsistencyLevel.ONE,
                     # protocol_version=PROTOCOL_VERSION,
                     default_keyspace=keyspace_name)


def teardown_module():
    if DSE_VERSION:
        drop_table(SimpleNullableModel)

