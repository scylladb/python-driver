# Copyright 2026 ScyllaDB, Inc.
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
Unit coverage for the schema and topology paths of cassandra/metadata.py that
a ScyllaDB cluster exercises: the system_schema parsers (V3, which ScyllaDB
selects, and V4), schema refresh events, CQL export, and the token map.

The parsers are driven through a fake connection that answers each
system_schema query with canned rows, so the whole query -> aggregate -> build
pipeline runs without a server.
"""

import re
import struct
import unittest
import uuid
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from cassandra import InvalidRequest, OperationTimedOut, SignatureDescriptor
from cassandra import cqltypes
from cassandra.connection import DefaultEndPoint
from cassandra.metadata import (NO_VALID_REPLICA, Aggregate, BytesToken,
                                ColumnMetadata, Function, IndexMetadata,
                                KeyspaceMetadata, MaterializedViewMetadata,
                                MD5Token, Metadata, Murmur3Token,
                                NoMurmur3, RegisteredTableExtension,
                                SchemaParserV3, SchemaParserV4,
                                SimpleStrategy, TableMetadata,
                                TableMetadataV3, TriggerMetadata,
                                UserType, _ConsistencyMode,
                                _RegisteredExtensionType, _UnknownStrategy,
                                _cql_from_cass_type, group_keys_by_replica)
from cassandra.policies import HostDistance, SimpleConvictionPolicy
from cassandra.pool import Host
from cassandra.protocol import QueryMessage
from cassandra.tablets import Tablet


class _FakeResult(object):
    """The parts of a ResultMessage that _SchemaParser._handle_results reads."""

    def __init__(self, rows, paging_state=None):
        self.column_names = list(rows[0].keys()) if rows else []
        self.parsed_rows = [[row[c] for c in self.column_names] for row in rows]
        self.paging_state = paging_state


class _FakeConnection(object):
    """
    Answers system_schema queries with canned rows keyed by the table named in
    the FROM clause. ``failures`` maps a table to the exception it fails with.
    ``shard_id`` is what SchemaParserV3._is_not_scylla() looks at.
    """

    def __init__(self, rows, scylla=True, tablets_routing_v2=False, failures=None):
        self.rows = rows
        self.failures = failures or {}
        self.features = SimpleNamespace(shard_id=0 if scylla else None,
                                        tablets_routing_v2=tablets_routing_v2)
        self.queries = []

    @staticmethod
    def table_of(query):
        return re.search(r'from (\S+)', query, re.IGNORECASE).group(1)

    def _respond(self, message):
        self.queries.append(message.query)
        table = self.table_of(message.query)
        if table in self.failures:
            return False, self.failures[table]
        return True, _FakeResult(self.rows.get(table, []))

    def wait_for_responses(self, *messages, **kwargs):
        assert kwargs.get('fail_on_error') is False
        return [self._respond(m) for m in messages]

    def queried_tables(self):
        return [self.table_of(q) for q in self.queries]


def _column(table, name, kind, cql_type, position=-1, order='none'):
    return {'keyspace_name': 'ks', 'table_name': table, 'column_name': name,
            'kind': kind, 'position': position, 'type': cql_type,
            'clustering_order': order}


def _table(name, flags, comment=''):
    return {'keyspace_name': 'ks', 'table_name': name, 'flags': flags,
            'comment': comment,
            'compaction': {'class': 'SizeTieredCompactionStrategy'},
            'gc_grace_seconds': 864000, 'extensions': {}}


def _schema_rows():
    """
    A small but representative ScyllaDB schema: a regular table with a
    composite partition key, a secondary index and a materialized view, plus
    the two legacy compact storage layouts, a UDT, a UDF and a UDA.
    Rows are deliberately listed out of position order.
    """
    return {
        'system_schema.keyspaces': [
            {'keyspace_name': 'ks', 'durable_writes': True,
             'replication': {'class': 'org.apache.cassandra.locator.SimpleStrategy',
                             'replication_factor': '1'}},
        ],
        'system_schema.tables': [
            _table('users', {'compound'}, comment='user accounts'),
            _table('dense', {'dense'}),
            _table('static', set()),
        ],
        'system_schema.columns': [
            _column('users', 'bucket', 'partition_key', 'int', position=1),
            _column('users', 'id', 'partition_key', 'uuid', position=0),
            _column('users', 'seq', 'clustering', 'int', position=1),
            _column('users', 'ts', 'clustering', 'timeuuid', position=0, order='desc'),
            _column('users', 'name', 'regular', 'text'),
            _column('users', 'addr', 'regular', 'frozen<address>'),
            # COMPACT STORAGE with a clustering column: the unnamed value
            # column has the empty type and is not part of the logical table.
            _column('dense', 'k', 'partition_key', 'int', position=0),
            _column('dense', 'c', 'clustering', 'int', position=0, order='asc'),
            _column('dense', 'value', 'regular', 'empty'),
            # COMPACT STORAGE without clustering columns ("static" compact):
            # only the static columns are logical, and they lose 'static'.
            _column('static', 'k', 'partition_key', 'int', position=0),
            _column('static', 'column1', 'clustering', 'text', position=0, order='asc'),
            _column('static', 'value', 'regular', 'blob'),
            _column('static', 'v', 'static', 'text'),
            # the view's columns live in system_schema.columns too
            _column('users_by_name', 'name', 'partition_key', 'text', position=0),
            _column('users_by_name', 'id', 'clustering', 'uuid', position=0, order='asc'),
            _column('users_by_name', 'bucket', 'clustering', 'int', position=1, order='asc'),
            _column('users_by_name', 'ts', 'clustering', 'timeuuid', position=2, order='desc'),
            _column('users_by_name', 'seq', 'clustering', 'int', position=3, order='asc'),
        ],
        'system_schema.indexes': [
            {'keyspace_name': 'ks', 'table_name': 'users', 'index_name': 'users_name_idx',
             'kind': 'COMPOSITES', 'options': {'target': 'name'}},
        ],
        'system_schema.views': [
            {'keyspace_name': 'ks', 'view_name': 'users_by_name', 'base_table_name': 'users',
             'include_all_columns': False,
             'where_clause': 'name IS NOT NULL',
             'comment': '', 'extensions': {}},
        ],
        'system_schema.types': [
            {'keyspace_name': 'ks', 'type_name': 'address',
             'field_names': ['street', 'zip'], 'field_types': ['text', 'int']},
        ],
        'system_schema.functions': [
            {'keyspace_name': 'ks', 'function_name': 'plus',
             'argument_types': ['int', 'int'], 'argument_names': ['a', 'b'],
             'return_type': 'int', 'language': 'lua', 'body': 'return a + b',
             'called_on_null_input': False},
        ],
        'system_schema.aggregates': [
            {'keyspace_name': 'ks', 'aggregate_name': 'total', 'argument_types': ['int'],
             'state_func': 'plus', 'state_type': 'int', 'final_func': None,
             'initcond': '0', 'return_type': 'int'},
        ],
        'system_schema.triggers': [
            {'keyspace_name': 'ks', 'table_name': 'users', 'trigger_name': 'audit',
             'options': {'class': 'org.example.AuditTrigger'}},
        ],
        'system_schema.scylla_keyspaces': [
            {'keyspace_name': 'ks', 'consistency': 'global'},
        ],
    }


def _parser(parser_class, connection):
    return parser_class(connection, timeout=1.0, fetch_size=1000,
                        metadata_request_timeout=None)


class SchemaParserV3QueryAllTest(unittest.TestCase):
    """SchemaParserV3.get_all_keyspaces() over a full system_schema read."""

    def _keyspaces(self, connection, parser_class=SchemaParserV3):
        return {ks.name: ks for ks in _parser(parser_class, connection).get_all_keyspaces()}

    def test_scylla_builds_complete_keyspace(self):
        connection = _FakeConnection(_schema_rows(), scylla=True, tablets_routing_v2=True)
        keyspaces = self._keyspaces(connection)

        # ScyllaDB has no triggers table, and the consistency option rides in
        # the same batch once TABLETS_ROUTING_V2 is negotiated.
        assert connection.queried_tables() == [
            'system_schema.keyspaces', 'system_schema.tables', 'system_schema.columns',
            'system_schema.types', 'system_schema.functions', 'system_schema.aggregates',
            'system_schema.indexes', 'system_schema.views', 'system_schema.scylla_keyspaces']

        ks = keyspaces['ks']
        assert ks.durable_writes is True
        assert ks.replication_strategy == SimpleStrategy({'replication_factor': '1'})
        assert ks._consistency_mode == _ConsistencyMode.GLOBAL
        assert set(ks.tables) == {'users', 'dense', 'static'}
        assert ks.user_types['address'].field_types == ['text', 'int']
        assert list(ks.functions) == ['plus(int,int)']
        assert list(ks.aggregates) == ['total(int)']

        users = ks.tables['users']
        assert isinstance(users, TableMetadataV3)
        assert users.is_compact_storage is False
        # Keys are ordered by position, not by row order.
        assert [c.name for c in users.partition_key] == ['id', 'bucket']
        assert [c.name for c in users.clustering_key] == ['ts', 'seq']
        assert [c.is_reversed for c in users.clustering_key] == [True, False]
        assert list(users.columns) == ['id', 'bucket', 'ts', 'seq', 'name', 'addr']
        assert users.triggers == {}
        assert list(users.indexes) == ['users_name_idx']
        assert ks.indexes['users_name_idx'] is users.indexes['users_name_idx']
        assert users.options['comment'] == 'user accounts'
        assert users.options['compaction'] == {'class': 'SizeTieredCompactionStrategy'}

        view = ks.views['users_by_name']
        assert users.views == {'users_by_name': view}
        assert view.base_table_name == 'users'
        assert [c.name for c in view.partition_key] == ['name']
        assert [c.name for c in view.clustering_key] == ['id', 'bucket', 'ts', 'seq']

        dense = ks.tables['dense']
        assert dense.is_compact_storage is True
        assert list(dense.columns) == ['k', 'c']

        static = ks.tables['static']
        assert static.is_compact_storage is True
        assert list(static.columns) == ['k', 'v']
        assert static.clustering_key == []
        assert static.columns['v'].is_static is False

    def test_scylla_export_as_string(self):
        connection = _FakeConnection(_schema_rows(), scylla=True)
        users = self._keyspaces(connection)['ks'].tables['users']

        assert users.export_as_string() == (
            "CREATE TABLE ks.users (\n"
            "    id uuid,\n"
            "    bucket int,\n"
            "    ts timeuuid,\n"
            "    seq int,\n"
            "    name text,\n"
            "    addr frozen<address>,\n"
            "    PRIMARY KEY ((id, bucket), ts, seq)\n"
            ") WITH CLUSTERING ORDER BY (ts DESC, seq ASC)\n"
            "    AND comment = 'user accounts'\n"
            "    AND compaction = {'class': 'SizeTieredCompactionStrategy'}\n"
            "    AND gc_grace_seconds = 864000;\n"
            "CREATE INDEX users_name_idx ON ks.users (name);\n"
            "\n"
            "CREATE MATERIALIZED VIEW ks.users_by_name AS\n"
            "    SELECT name, id, bucket, ts, seq\n"
            "    FROM ks.users\n"
            "    WHERE name IS NOT NULL\n"
            "    PRIMARY KEY (name, id, bucket, ts, seq)\n"
            "    WITH CLUSTERING ORDER BY (id ASC, bucket ASC, ts DESC, seq ASC)\n"
            "    AND comment = '';")

    def test_without_tablets_routing_v2_keyspaces_are_eventual(self):
        connection = _FakeConnection(_schema_rows(), scylla=True, tablets_routing_v2=False)
        ks = self._keyspaces(connection)['ks']

        assert 'system_schema.scylla_keyspaces' not in connection.queried_tables()
        assert ks._consistency_mode == _ConsistencyMode.EVENTUAL

    def test_missing_scylla_keyspaces_table_is_eventual(self):
        # Older ScyllaDB answers InvalidRequest for the table or column.
        connection = _FakeConnection(
            _schema_rows(), scylla=True, tablets_routing_v2=True,
            failures={'system_schema.scylla_keyspaces': InvalidRequest('unconfigured table')})
        ks = self._keyspaces(connection)['ks']

        assert ks._consistency_mode == _ConsistencyMode.EVENTUAL
        assert set(ks.tables) == {'users', 'dense', 'static'}

    def test_transient_scylla_keyspaces_failure_aborts_refresh(self):
        connection = _FakeConnection(
            _schema_rows(), scylla=True, tablets_routing_v2=True,
            failures={'system_schema.scylla_keyspaces': OperationTimedOut('timed out')})

        with pytest.raises(OperationTimedOut):
            self._keyspaces(connection)

    def test_failed_core_query_aborts_refresh(self):
        connection = _FakeConnection(
            _schema_rows(), scylla=True,
            failures={'system_schema.tables': OperationTimedOut('timed out')})

        with pytest.raises(OperationTimedOut):
            self._keyspaces(connection)

    def test_cassandra_reads_triggers(self):
        connection = _FakeConnection(_schema_rows(), scylla=False)
        users = self._keyspaces(connection)['ks'].tables['users']

        assert connection.queried_tables()[-1] == 'system_schema.triggers'
        trigger = users.triggers['audit']
        assert isinstance(trigger, TriggerMetadata)
        assert trigger.table is users
        assert trigger.options == {'class': 'org.example.AuditTrigger'}
        assert ("\nCREATE TRIGGER audit ON ks.users USING 'org.example.AuditTrigger';"
                in users.export_as_string())

    def test_unparseable_table_is_kept_with_error(self):
        rows = _schema_rows()
        # A column row with a null clustering_order cannot be built.
        rows['system_schema.columns'][4]['clustering_order'] = None
        connection = _FakeConnection(rows, scylla=True)

        users = self._keyspaces(connection)['ks'].tables['users']

        assert users._exc_info is not None
        assert users._exc_info[0] is AttributeError
        # Whatever was built before the failure is still exported.
        assert [c.name for c in users.partition_key] == ['id', 'bucket']
        exported = users.export_as_string()
        assert exported.startswith(
            "/*\nWarning: Table ks.users is incomplete because of an error processing metadata.\n")
        assert "AttributeError: 'NoneType' object has no attribute 'upper'" in exported
        assert "Approximate structure, for reference:" in exported
        assert exported.endswith("*/")


    def test_unparseable_keyspace_row_is_kept_as_unknown(self):
        rows = _schema_rows()
        rows['system_schema.keyspaces'][0]['replication'] = {'replication_factor': '1'}
        connection = _FakeConnection(rows, scylla=True)

        ks = self._keyspaces(connection)['ks']

        assert ks._exc_info[0] is KeyError
        assert ks.durable_writes is False
        assert ks.replication_strategy == _UnknownStrategy('UNKNOWN', {})
        # Its tables are still attached.
        assert set(ks.tables) == {'users', 'dense', 'static'}

    def test_unparseable_aggregate_marks_keyspace_incomplete(self):
        rows = _schema_rows()
        del rows['system_schema.aggregates'][0]['return_type']
        connection = _FakeConnection(rows, scylla=True)

        ks = self._keyspaces(connection)['ks']

        assert ks._exc_info[0] is KeyError
        assert ks.aggregates == {}
        exported = ks.export_as_string()
        assert exported.startswith(
            "/*\nWarning: Keyspace ks is incomplete because of an error processing metadata.\n")
        assert "CREATE TABLE ks.users (" in exported
        assert exported.endswith("*/")


class SchemaParserV3SingleElementTest(unittest.TestCase):
    """The per-element reads behind keyspace, type, function and aggregate events."""

    def test_get_keyspace(self):
        connection = _FakeConnection(_schema_rows(), scylla=True, tablets_routing_v2=True)

        ks = _parser(SchemaParserV3, connection).get_keyspace({}, 'ks')

        assert connection.queried_tables() == [
            'system_schema.keyspaces', 'system_schema.scylla_keyspaces']
        assert all(q.endswith(" WHERE keyspace_name = 'ks'") for q in connection.queries)
        assert ks.name == 'ks'
        assert ks.replication_strategy == SimpleStrategy({'replication_factor': '1'})
        assert ks._consistency_mode == _ConsistencyMode.GLOBAL

    def test_get_keyspace_without_consistency_row_is_eventual(self):
        rows = _schema_rows()
        rows['system_schema.scylla_keyspaces'] = []
        connection = _FakeConnection(rows, scylla=True, tablets_routing_v2=True)

        ks = _parser(SchemaParserV3, connection).get_keyspace({}, 'ks')

        assert ks._consistency_mode == _ConsistencyMode.EVENTUAL

    def test_get_dropped_keyspace(self):
        rows = _schema_rows()
        rows['system_schema.keyspaces'] = []
        connection = _FakeConnection(rows, scylla=True, tablets_routing_v2=True)

        assert _parser(SchemaParserV3, connection).get_keyspace({}, 'ks') is None
        # No consistency read for a keyspace that is gone.
        assert connection.queried_tables() == ['system_schema.keyspaces']

    def test_get_type_and_types_map(self):
        connection = _FakeConnection(_schema_rows())
        parser = _parser(SchemaParserV3, connection)

        user_type = parser.get_type({}, 'ks', 'address')
        assert connection.queries[-1].endswith(
            " WHERE keyspace_name = 'ks' AND type_name = 'address'")
        assert user_type.as_cql_query() == 'CREATE TYPE ks.address (street text, zip int)'

        types_map = parser.get_types_map({}, 'ks')
        assert connection.queries[-1].endswith(" WHERE keyspace_name = 'ks'")
        assert list(types_map) == ['address']

    def test_get_function(self):
        connection = _FakeConnection(_schema_rows())

        function = _parser(SchemaParserV3, connection).get_function(
            {}, 'ks', SignatureDescriptor('plus', ['int', 'int']))

        assert connection.queries[-1].endswith(
            " WHERE keyspace_name = 'ks' AND function_name = 'plus'"
            " AND argument_types = ['int', 'int']")
        assert function.signature == 'plus(int,int)'
        assert function.argument_names == ['a', 'b']
        assert function.return_type == 'int'

    def test_get_aggregate(self):
        connection = _FakeConnection(_schema_rows())

        aggregate = _parser(SchemaParserV3, connection).get_aggregate(
            {}, 'ks', SignatureDescriptor('total', ['int']))

        assert connection.queries[-1].endswith(
            " WHERE keyspace_name = 'ks' AND aggregate_name = 'total'"
            " AND argument_types = ['int']")
        assert aggregate.signature == 'total(int)'
        assert aggregate.state_func == 'plus'
        assert aggregate.initial_condition == '0'

    def test_missing_system_table_reads_as_absent(self):
        connection = _FakeConnection(_schema_rows(), failures={
            'system_schema.types': InvalidRequest('unconfigured table')})

        assert _parser(SchemaParserV3, connection).get_type({}, 'ks', 'address') is None


class SchemaParserV3GetTableTest(unittest.TestCase):
    """SchemaParserV3.get_table(), used for single table/view change events."""

    @staticmethod
    def _rows_for(name):
        # The fake connection does not evaluate WHERE clauses, so narrow the
        # canned rows to what the server would return for this name.
        rows = _schema_rows()
        for table, table_rows in rows.items():
            key = 'view_name' if table == 'system_schema.views' else 'table_name'
            rows[table] = [r for r in table_rows if r.get(key, name) == name]
        return rows

    def test_scylla_table(self):
        connection = _FakeConnection(self._rows_for('users'), scylla=True)

        table = _parser(SchemaParserV3, connection).get_table({}, 'ks', 'users')

        assert connection.queried_tables() == [
            'system_schema.tables', 'system_schema.columns',
            'system_schema.indexes', 'system_schema.views']
        assert "WHERE keyspace_name = 'ks' AND table_name = 'users'" in connection.queries[0]
        assert "WHERE keyspace_name = 'ks' AND view_name = 'users'" in connection.queries[3]
        assert isinstance(table, TableMetadataV3)
        assert [c.name for c in table.partition_key] == ['id', 'bucket']
        assert list(table.indexes) == ['users_name_idx']
        assert table.triggers == {}

    def test_index_row_without_name_or_kind_is_skipped(self):
        rows = self._rows_for('users')
        rows['system_schema.indexes'].append(
            {'keyspace_name': 'ks', 'table_name': 'users', 'index_name': None,
             'kind': None, 'options': None})
        connection = _FakeConnection(rows, scylla=True)

        table = _parser(SchemaParserV3, connection).get_table({}, 'ks', 'users')

        assert list(table.indexes) == ['users_name_idx']

    def test_cassandra_table_includes_triggers(self):
        connection = _FakeConnection(self._rows_for('users'), scylla=False)

        table = _parser(SchemaParserV3, connection).get_table({}, 'ks', 'users')

        assert connection.queried_tables() == [
            'system_schema.tables', 'system_schema.columns',
            'system_schema.indexes', 'system_schema.triggers', 'system_schema.views']
        assert list(table.triggers) == ['audit']

    def test_view(self):
        # Protocol v4 events do not tell tables from views, so an empty table
        # result falls through to the views query.
        connection = _FakeConnection(self._rows_for('users_by_name'), scylla=True)

        view = _parser(SchemaParserV3, connection).get_table({}, 'ks', 'users_by_name')

        assert isinstance(view, MaterializedViewMetadata)
        assert view.base_table_name == 'users'
        assert [c.name for c in view.clustering_key] == ['id', 'bucket', 'ts', 'seq']

    def test_absent(self):
        connection = _FakeConnection(self._rows_for('nope'), scylla=True)

        assert _parser(SchemaParserV3, connection).get_table({}, 'ks', 'nope') is None


class SchemaParserV4Test(unittest.TestCase):
    """SchemaParserV4 adds the system_virtual_schema keyspaces and tables."""

    @staticmethod
    def _rows():
        rows = _schema_rows()
        rows['system_virtual_schema.keyspaces'] = [{'keyspace_name': 'system_views'}]
        rows['system_virtual_schema.tables'] = [
            {'keyspace_name': 'system_views', 'table_name': 'clients', 'comment': 'connected clients'}]
        rows['system_virtual_schema.columns'] = [
            {'keyspace_name': 'system_views', 'table_name': 'clients', 'column_name': c,
             'kind': kind, 'position': pos, 'type': t, 'clustering_order': order}
            for c, kind, pos, t, order in [
                ('port', 'clustering', 0, 'int', 'asc'),
                ('address', 'partition_key', 0, 'inet', 'none'),
                ('username', 'regular', -1, 'text', 'none')]]
        return rows

    def test_virtual_keyspaces(self):
        connection = _FakeConnection(self._rows(), scylla=False)

        keyspaces = list(_parser(SchemaParserV4, connection).get_all_keyspaces())

        assert connection.queried_tables()[8:] == [
            'system_virtual_schema.keyspaces', 'system_virtual_schema.tables',
            'system_virtual_schema.columns', 'system_schema.triggers']
        assert [ks.name for ks in keyspaces] == ['ks', 'system_views']
        assert list(keyspaces[0].tables['users'].triggers) == ['audit']

        virtual = keyspaces[1]
        assert virtual.virtual is True
        # Virtual keyspace rows carry neither replication nor durable_writes.
        assert virtual.durable_writes is None
        assert virtual.replication_strategy is None
        assert virtual._exc_info is None

        clients = virtual.tables['clients']
        assert clients.virtual is True
        assert clients.is_compact_storage is False
        assert [c.name for c in clients.partition_key] == ['address']
        assert [c.name for c in clients.clustering_key] == ['port']
        assert clients.export_as_string() == (
            "/*\nWarning: Table system_views.clients is a virtual table and cannot be recreated with CQL.\n"
            "Structure, for reference:\n"
            "VIRTUAL TABLE system_views.clients (\n"
            "    address inet,\n"
            "    port int,\n"
            "    username text,\n"
            "    PRIMARY KEY (address, port)\n"
            ") WITH CLUSTERING ORDER BY (port ASC)\n"
            "    AND comment = 'connected clients';\n*/")
        assert virtual.export_as_string().startswith(
            "/*\nWarning: Keyspace system_views is a virtual keyspace and cannot be recreated with CQL.\n"
            "Structure, for reference:*/\n// VIRTUAL KEYSPACE system_views;")

    def test_scylla_skips_triggers(self):
        connection = _FakeConnection(self._rows(), scylla=True)

        keyspaces = list(_parser(SchemaParserV4, connection).get_all_keyspaces())

        assert 'system_schema.triggers' not in connection.queried_tables()
        assert [ks.name for ks in keyspaces] == ['ks', 'system_views']
        assert keyspaces[0].tables['users'].triggers == {}

    def test_missing_virtual_tables_are_ignored(self):
        # Some servers report 4.x without having system_virtual_schema.
        missing = InvalidRequest('unconfigured table')
        connection = _FakeConnection(self._rows(), scylla=True, failures={
            'system_virtual_schema.keyspaces': missing,
            'system_virtual_schema.tables': missing,
            'system_virtual_schema.columns': missing})

        keyspaces = list(_parser(SchemaParserV4, connection).get_all_keyspaces())

        assert [ks.name for ks in keyspaces] == ['ks']


class HandleResultsPagingTest(unittest.TestCase):
    """_SchemaParser._handle_results follows paging_state to the last page."""

    def _parser(self, next_pages):
        connection = Mock()
        connection.wait_for_response.side_effect = next_pages
        return _parser(SchemaParserV3, connection), connection

    @staticmethod
    def _page(names, paging_state):
        return _FakeResult([{'keyspace_name': n} for n in names], paging_state=paging_state)

    def test_follows_pages(self):
        parser, connection = self._parser([
            (True, self._page(['b'], b'p2')),
            (True, self._page(['c'], None))])
        message = QueryMessage('SELECT * FROM system_schema.keyspaces', consistency_level=1)

        rows = parser._handle_results(True, self._page(['a'], b'p1'), query_msg=message)

        assert rows == [{'keyspace_name': 'a'}, {'keyspace_name': 'b'}, {'keyspace_name': 'c'}]
        assert connection.wait_for_response.call_count == 2
        # The last request carried the second page's paging state.
        assert message.paging_state == b'p2'

    def test_empty_last_page(self):
        parser, _ = self._parser([(True, self._page([], None))])
        message = QueryMessage('SELECT * FROM system_schema.keyspaces', consistency_level=1)

        rows = parser._handle_results(True, self._page(['a'], b'p1'), query_msg=message)

        assert rows == [{'keyspace_name': 'a'}]

    def test_failed_page_raises(self):
        parser, _ = self._parser([(False, OperationTimedOut('page timed out'))])
        message = QueryMessage('SELECT * FROM system_schema.keyspaces', consistency_level=1)

        with pytest.raises(OperationTimedOut):
            parser._handle_results(True, self._page(['a'], b'p1'), query_msg=message)

    def test_paging_state_ignored_without_query_message(self):
        parser, connection = self._parser([])

        rows = parser._handle_results(True, self._page(['a'], b'p1'))

        assert rows == [{'keyspace_name': 'a'}]
        connection.wait_for_response.assert_not_called()

    def test_failures(self):
        parser, _ = self._parser([])
        assert parser._handle_results(False, InvalidRequest('x'), expected_failures=(InvalidRequest,)) == []
        with pytest.raises(InvalidRequest):
            parser._handle_results(False, InvalidRequest('x'))


class MetadataRefreshTargetTest(unittest.TestCase):
    """Metadata.refresh() for a single schema element, as driven by schema change events."""

    def setUp(self):
        self.metadata = Metadata()
        self.metadata.keyspaces['ks'] = KeyspaceMetadata(
            'ks', True, 'SimpleStrategy', {'replication_factor': '1'})
        self.parser = Mock()
        patcher = patch('cassandra.metadata.get_schema_parser', return_value=self.parser)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _refresh(self, target_type, **kwargs):
        connection = Mock(spec=['original_endpoint'], original_endpoint=DefaultEndPoint('127.0.0.1'))
        self.metadata.refresh(connection, 1.0, target_type=target_type, **kwargs)

    def test_function_created_then_dropped(self):
        function = Function('ks', 'plus', ['int', 'int'], ['a', 'b'], 'int', 'lua',
                            'return a + b', False, False, False, ())
        signature = SignatureDescriptor('plus', ['int', 'int'])
        self.parser.get_function.return_value = function

        self._refresh('FUNCTION', keyspace='ks', function=signature)
        assert self.metadata.keyspaces['ks'].functions == {'plus(int,int)': function}
        self.parser.get_function.assert_called_once_with(
            self.metadata.keyspaces, keyspace='ks', function=signature)

        self.parser.get_function.return_value = None
        self._refresh('FUNCTION', keyspace='ks', function=signature)
        assert self.metadata.keyspaces['ks'].functions == {}

    def test_aggregate_created_then_dropped(self):
        aggregate = Aggregate('ks', 'total', ['int'], 'plus', 'int', None, '0', 'int', False)
        signature = SignatureDescriptor('total', ['int'])
        self.parser.get_aggregate.return_value = aggregate

        self._refresh('AGGREGATE', keyspace='ks', aggregate=signature)
        assert self.metadata.keyspaces['ks'].aggregates == {'total(int)': aggregate}

        self.parser.get_aggregate.return_value = None
        self._refresh('AGGREGATE', keyspace='ks', aggregate=signature)
        assert self.metadata.keyspaces['ks'].aggregates == {}

    def test_events_for_unknown_keyspace_are_ignored(self):
        # The keyspace can disappear while an event is being processed.
        function = Function('gone', 'f', [], [], 'int', 'lua', '', False, False, False, ())
        aggregate = Aggregate('gone', 'a', [], 'f', 'int', None, None, 'int', False)
        self.parser.get_function.return_value = function
        self.parser.get_aggregate.return_value = aggregate
        self._refresh('function', keyspace='gone', function=function)
        self._refresh('aggregate', keyspace='gone', aggregate=aggregate)

        self.parser.get_function.return_value = None
        self.parser.get_aggregate.return_value = None
        self._refresh('function', keyspace='gone', function=function)
        self._refresh('aggregate', keyspace='gone', aggregate=aggregate)

        assert list(self.metadata.keyspaces) == ['ks']
        assert self.metadata.keyspaces['ks'].functions == {}
        assert self.metadata.keyspaces['ks'].aggregates == {}

    def test_table_and_view_events(self):
        connection = _FakeConnection(SchemaParserV3GetTableTest._rows_for('users'))
        users = _parser(SchemaParserV3, connection).get_table({}, 'ks', 'users')
        connection = _FakeConnection(SchemaParserV3GetTableTest._rows_for('users_by_name'))
        view = _parser(SchemaParserV3, connection).get_table({}, 'ks', 'users_by_name')
        ks = self.metadata.keyspaces['ks']

        self.parser.get_table.return_value = users
        self._refresh('TABLE', keyspace='ks', table='users')
        assert ks.tables == {'users': users}
        assert set(ks.indexes) == {'users_name_idx'}

        self.parser.get_table.return_value = view
        self._refresh('TABLE', keyspace='ks', table='users_by_name')
        assert ks.views == {'users_by_name': view}
        assert users.views == {'users_by_name': view}

        # Dropping the view leaves the base table in place.
        self.parser.get_table.return_value = None
        self._refresh('TABLE', keyspace='ks', table='users_by_name')
        assert ks.views == {}
        assert users.views == {}
        assert list(ks.tables) == ['users']

        self._refresh('TABLE', keyspace='ks', table='users')
        assert ks.tables == {}
        assert ks.indexes == {}

    def test_keyspace_events(self):
        old_ks = self.metadata.keyspaces['ks']
        old_ks.tables['t'] = TableMetadataV3('ks', 't')
        old_ks.user_types['u'] = UserType('ks', 'u', ['f'], ['int'])
        self.metadata._tablets.add_tablet('ks', 't', Tablet(
            first_token=-100, last_token=100, replicas=[(uuid.uuid4(), 0)], tablet_version=1))

        # An ALTER KEYSPACE event carries no table data: the children move to
        # the new object, and a replication change invalidates tablets.
        new_ks = KeyspaceMetadata('ks', True, 'SimpleStrategy', {'replication_factor': '3'})
        self.parser.get_keyspace.return_value = new_ks
        self._refresh('KEYSPACE', keyspace='ks')
        assert self.metadata.keyspaces['ks'] is new_ks
        assert list(new_ks.tables) == ['t']
        assert list(new_ks.user_types) == ['u']
        assert not self.metadata._tablets.table_has_tablets('ks', 't')

        created = KeyspaceMetadata('other', True, 'SimpleStrategy', {'replication_factor': '1'})
        self.parser.get_keyspace.return_value = created
        self._refresh('KEYSPACE', keyspace='other')
        assert self.metadata.keyspaces['other'] is created

        self.parser.get_keyspace.return_value = None
        self._refresh('KEYSPACE', keyspace='other')
        self._refresh('KEYSPACE', keyspace='never_existed')
        assert list(self.metadata.keyspaces) == ['ks']

    def test_type_events(self):
        user_type = UserType('ks', 'address', ['street'], ['text'])
        self.parser.get_type.return_value = user_type
        self._refresh('TYPE', keyspace='ks', type='address')
        assert self.metadata.keyspaces['ks'].user_types == {'address': user_type}

        # Unknown keyspace is ignored on both create and drop.
        self.parser.get_type.return_value = UserType('gone', 'address', [], [])
        self._refresh('TYPE', keyspace='gone', type='address')
        self.parser.get_type.return_value = None
        self._refresh('TYPE', keyspace='gone', type='address')

        self._refresh('TYPE', keyspace='ks', type='address')
        assert self.metadata.keyspaces['ks'].user_types == {}

    def test_full_rebuild(self):
        metadata = self.metadata
        host = Host(DefaultEndPoint('127.0.0.1'), SimpleConvictionPolicy, host_id=uuid.uuid4())
        metadata.rebuild_token_map('RandomPartitioner', {host: ['0']})
        metadata.keyspaces['gone'] = KeyspaceMetadata(
            'gone', True, 'SimpleStrategy', {'replication_factor': '1'})
        metadata.keyspaces['ks'].tables['dropped'] = TableMetadataV3('ks', 'dropped')
        metadata.token_map.get_replicas('ks', MD5Token(1))
        metadata.token_map.get_replicas('gone', MD5Token(1))
        for ks_name in ('ks', 'gone'):
            metadata._tablets.add_tablet(ks_name, 't', Tablet(
                first_token=-100, last_token=100, replicas=[(host.host_id, 0)], tablet_version=1))

        ks = KeyspaceMetadata('ks', True, 'SimpleStrategy', {'replication_factor': '1'})
        fresh = KeyspaceMetadata('fresh', True, 'SimpleStrategy', {'replication_factor': '1'})
        self.parser.get_all_keyspaces.return_value = [ks, fresh]
        self._refresh(None)

        assert metadata.keyspaces == {'ks': ks, 'fresh': fresh}
        # Replica maps are rebuilt for keyspaces that had one and dropped for
        # removed keyspaces; new keyspaces are built lazily on first use.
        assert set(metadata.token_map.tokens_to_hosts_by_ks) == {'ks'}
        assert not metadata._tablets.table_has_tablets('ks', 't')
        assert not metadata._tablets.table_has_tablets('gone', 't')

    def test_table_replacement_keeps_views_and_drops_stale_indexes(self):
        ks = self.metadata.keyspaces['ks']
        connection = _FakeConnection(SchemaParserV3GetTableTest._rows_for('users'))
        users = _parser(SchemaParserV3, connection).get_table({}, 'ks', 'users')
        connection = _FakeConnection(SchemaParserV3GetTableTest._rows_for('users_by_name'))
        view = _parser(SchemaParserV3, connection).get_table({}, 'ks', 'users_by_name')
        self.parser.get_table.return_value = users
        self._refresh('table', keyspace='ks', table='users')
        self.parser.get_table.return_value = view
        self._refresh('table', keyspace='ks', table='users_by_name')

        # ALTER TABLE ... after DROP INDEX: table events do not carry views.
        altered = TableMetadataV3('ks', 'users')
        self.parser.get_table.return_value = altered
        self._refresh('table', keyspace='ks', table='users')

        assert ks.tables == {'users': altered}
        assert altered.views == {'users_by_name': view}
        assert ks.indexes == {}

    def test_orphan_view_events_are_ignored(self):
        ks = self.metadata.keyspaces['ks']
        view = MaterializedViewMetadata('ks', 'mv', 'missing_base', True, 'k IS NOT NULL', {})
        self.parser.get_table.return_value = view
        self._refresh('table', keyspace='ks', table='mv')
        assert ks.views == {}

        # A view whose base table is already gone is still removed.
        ks.views['mv'] = view
        self.parser.get_table.return_value = None
        self._refresh('table', keyspace='ks', table='mv')
        assert ks.views == {}

    def test_unknown_target_type(self):
        with pytest.raises(ValueError, match="Unknown schema target_type: 'trigger'"):
            self._refresh('trigger', keyspace='ks')


class TableMetadataExportTest(unittest.TestCase):

    @staticmethod
    def _table(cls=TableMetadataV3, **kwargs):
        table = cls('ks', 't', **kwargs)
        for name, cql_type in (('k', 'int'), ('c', 'int'), ('v', 'text')):
            table.columns[name] = ColumnMetadata(table, name, cql_type)
        table.partition_key = [table.columns['k']]
        table.clustering_key = [table.columns['c']]
        table.is_compact_storage = False
        return table

    def _register_extension(self):
        """Register a test-only table extension for the duration of a test."""
        class _TestExtension(RegisteredTableExtension):
            name = 'test_ext'

            @classmethod
            def after_table_cql(cls, table_meta, ext_key, ext_blob):
                if not ext_blob:
                    return None
                return "ALTER TABLE %s.%s WITH test_ext = '%s';" % (
                    table_meta.keyspace_name, table_meta.name, ext_blob.decode())

        self.addCleanup(_RegisteredExtensionType._extension_registry.pop, 'test_ext', None)

    def test_registered_extension_cql_is_appended(self):
        self._register_extension()
        table = self._table()
        table.extensions = {'test_ext': b'on', 'unregistered': b'ignored'}

        assert table.export_as_string().endswith(
            "\n\nALTER TABLE ks.t WITH test_ext = 'on';")

    def test_extension_without_cql_adds_nothing(self):
        self._register_extension()
        table = self._table()
        plain = table.export_as_string()
        table.extensions = {'test_ext': b''}

        assert table.export_as_string() == plain

    def test_view_extension_cql_is_appended(self):
        view = MaterializedViewMetadata('ks', 'mv', 't', True, 'c IS NOT NULL', {'comment': 'by c'})
        view.columns['c'] = ColumnMetadata(view, 'c', 'int')
        view.columns['k'] = ColumnMetadata(view, 'k', 'int')
        view.partition_key = [view.columns['c'], view.columns['k']]
        self._register_extension()
        view.extensions = {'test_ext': b'on'}

        assert view.as_cql_query() == (
            "CREATE MATERIALIZED VIEW ks.mv AS SELECT * FROM ks.t WHERE c IS NOT NULL "
            "PRIMARY KEY ((c, k)) WITH comment = 'by c'"
            "\n\nALTER TABLE ks.mv WITH test_ext = 'on';")
        assert view.export_as_string().startswith(
            "CREATE MATERIALIZED VIEW ks.mv AS\n"
            "    SELECT *\n"
            "    FROM ks.t\n"
            "    WHERE c IS NOT NULL\n"
            "    PRIMARY KEY ((c, k))\n"
            "    WITH comment = 'by c'\n"
            "\n"
            "ALTER TABLE ks.mv WITH test_ext = 'on';")

    def test_legacy_compact_table_is_not_cql_compatible(self):
        # Base TableMetadata (system.schema_columnfamilies layout): COMPACT
        # STORAGE with clustering and more than one value column cannot be
        # expressed in CQL.
        table = self._table(cls=TableMetadata)
        table.comparator = 'CompositeType(Int32Type)'
        assert table.is_cql_compatible

        table.is_compact_storage = True
        table.columns['w'] = ColumnMetadata(table, 'w', 'text')
        assert not table.is_cql_compatible

        exported = table.export_as_string()
        assert exported.startswith(
            "/*\nWarning: Table ks.t omitted because it has constructs not compatible with CQL "
            "(was created via legacy API).\n")
        assert "PRIMARY KEY (k, c)\n) WITH COMPACT STORAGE" in exported
        assert exported.endswith("*/")

        table.virtual = True
        assert not table.is_cql_compatible

    def test_legacy_option_strings(self):
        options = {
            'compaction_strategy_class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy',
            'compaction_strategy_options': '{"sstable_size_in_mb": "160"}',
            'min_compaction_threshold': 4,
            'max_compaction_threshold': 32,
            'compression_parameters': '{"sstable_compression": "LZ4Compressor"}',
            'comment': None,
            'gc_grace_seconds': 10,
            'caching': '{"keys":"ALL"}',
        }
        assert TableMetadata._make_option_strings(options) == [
            "caching = '{\"keys\":\"ALL\"}'",
            "compaction = {'sstable_size_in_mb': '160', "
            "'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}",
            "compression = {'sstable_compression': 'LZ4Compressor'}",
            "gc_grace_seconds = 10",
        ]

        # A 'compression' map (newer servers) wins over compression_parameters,
        # and an empty comment is still written out.
        options = {'compaction_strategy_class': 'SizeTieredCompactionStrategy',
                   'compression': "{'class': 'LZ4Compressor'}", 'comment': ''}
        assert TableMetadata._make_option_strings(options) == [
            "comment = ''",
            "compaction = {'class': 'SizeTieredCompactionStrategy'}",
            "compression = '{''class'': ''LZ4Compressor''}'",
        ]


    def test_option_strings_skip_null_values(self):
        assert TableMetadataV3._make_option_strings({
            'compaction': {'class': 'IncrementalCompactionStrategy'},
            'caching': None, 'comment': None, 'default_time_to_live': 0,
        }) == ["compaction = {'class': 'IncrementalCompactionStrategy'}",
               "default_time_to_live = 0"]


class TriggerMetadataTest(unittest.TestCase):

    def test_as_cql_query(self):
        table = TableMetadataV3('Ks', 'my table')
        trigger = TriggerMetadata(table, 'Audit', {'class': "org.example.It's"})

        assert trigger.as_cql_query() == (
            'CREATE TRIGGER "Audit" ON "Ks"."my table" USING \'org.example.It\'\'s\'')
        assert trigger.export_as_string() == trigger.as_cql_query() + ';'


class MetadataTopologyTest(unittest.TestCase):

    @staticmethod
    def _host(address, **kwargs):
        return Host(DefaultEndPoint(address), SimpleConvictionPolicy, host_id=uuid.uuid4(), **kwargs)

    def test_rebuild_token_map_byte_ordered(self):
        metadata = Metadata()
        a, b = self._host('127.0.0.1'), self._host('127.0.0.2')

        metadata.rebuild_token_map(
            'org.apache.cassandra.dht.ByteOrderedPartitioner', {a: ['6262'], b: ['61']})

        token_map = metadata.token_map
        assert token_map.token_class is BytesToken
        assert token_map.ring == [BytesToken(b'a'), BytesToken(b'bb')]
        assert token_map.token_to_host_owner == {BytesToken(b'a'): b, BytesToken(b'bb'): a}

    def test_rebuild_token_map_unknown_partitioner(self):
        metadata = Metadata()
        metadata.rebuild_token_map('org.apache.cassandra.dht.RandomPartitioner',
                                   {self._host('127.0.0.1'): ['1']})
        assert metadata.token_map is not None

        metadata.rebuild_token_map('org.example.CustomPartitioner', {self._host('127.0.0.1'): ['1']})

        assert metadata.partitioner == 'org.example.CustomPartitioner'
        assert metadata.token_map is None
        assert metadata.get_replicas('ks', b'key') == []

    def test_get_replicas_without_murmur3(self):
        metadata = Metadata()
        metadata.rebuild_token_map('Murmur3Partitioner', {self._host('127.0.0.1'): ['0']})

        with patch('cassandra.metadata.murmur3', None):
            with pytest.raises(NoMurmur3):
                Murmur3Token.hash_fn(b'key')
            assert metadata.get_replicas('ks', b'key') == []
            assert not metadata.can_support_partitioner()

    def test_remove_host_by_host_id(self):
        metadata = Metadata()
        a, b = self._host('127.0.0.1'), self._host('127.0.0.2')
        for host in (a, b):
            metadata.add_or_return_host(host)
            metadata._tablets.add_tablet('ks', 'tbl_%s' % host.address, Tablet(
                first_token=-100, last_token=100, replicas=[(host.host_id, 0)], tablet_version=1))

        assert not metadata.remove_host_by_host_id(uuid.uuid4())
        # A stale endpoint must not remove the host that now owns the id.
        assert not metadata.remove_host_by_host_id(a.host_id, DefaultEndPoint('127.0.0.9'))
        assert metadata.get_host_by_host_id(a.host_id) is a

        assert metadata.remove_host_by_host_id(a.host_id, a.endpoint)
        assert metadata.get_host_by_host_id(a.host_id) is None
        assert metadata.get_host(a.endpoint) is None
        assert not metadata._tablets.table_has_tablets('ks', 'tbl_127.0.0.1')
        assert metadata._tablets.table_has_tablets('ks', 'tbl_127.0.0.2')

        assert metadata.remove_host_by_host_id(b.host_id)
        assert metadata.all_hosts() == []

    def test_replace_host_endpoints_rejects_noop_and_repeated_source(self):
        metadata = Metadata()
        host = self._host('127.0.0.1')
        metadata.add_or_return_host(host)
        same = Host(host.endpoint, SimpleConvictionPolicy, host_id=host.host_id)
        moved = Host(DefaultEndPoint('127.0.0.2'), SimpleConvictionPolicy, host_id=host.host_id)
        moved_again = Host(DefaultEndPoint('127.0.0.3'), SimpleConvictionPolicy, host_id=host.host_id)

        with pytest.raises(ValueError, match="requires a new endpoint"):
            metadata.replace_host_endpoints([(host, same)])
        with pytest.raises(ValueError, match="Host appears more than once"):
            metadata.replace_host_endpoints([(host, moved), (host, moved_again)])
        assert metadata.get_host(host.endpoint) is host
        assert metadata.get_host(moved.endpoint) is None


class TokenMapTest(unittest.TestCase):

    def setUp(self):
        self.metadata = Metadata()
        self.hosts = [Host(DefaultEndPoint('127.0.0.%d' % i), SimpleConvictionPolicy,
                           host_id=uuid.uuid4()) for i in (1, 2)]
        self.metadata.rebuild_token_map('RandomPartitioner', {self.hosts[0]: ['10'], self.hosts[1]: ['20']})
        self.token_map = self.metadata.token_map

    def test_get_replicas_wraps_around_the_ring(self):
        self.metadata.keyspaces['ks'] = KeyspaceMetadata(
            'ks', True, 'SimpleStrategy', {'replication_factor': '1'})

        assert self.token_map.get_replicas('ks', MD5Token(15)) == [self.hosts[1]]
        assert self.token_map.get_replicas('ks', MD5Token(25)) == [self.hosts[0]]

    def test_rebuild_keyspace_skips_unknown_keyspace(self):
        self.token_map.rebuild_keyspace('missing', build_if_absent=True)
        assert 'missing' not in self.token_map.tokens_to_hosts_by_ks
        assert self.token_map.get_replicas('missing', MD5Token(15)) == []

    def test_keyspace_without_strategy_has_no_replica_map(self):
        self.metadata.keyspaces['ks'] = KeyspaceMetadata('ks', True, None, {})
        assert self.token_map.replica_map_for_keyspace(self.metadata.keyspaces['ks']) is None
        assert self.token_map.get_replicas('ks', MD5Token(15)) == []

    def test_rebuild_keyspace_failure_is_contained(self):
        ks = KeyspaceMetadata('ks', True, 'SimpleStrategy', {'replication_factor': '1'})
        ks.replication_strategy = Mock(**{'make_token_replica_map.side_effect': RuntimeError('boom')})
        self.metadata.keyspaces['ks'] = ks

        with self.assertLogs('cassandra.metadata', level='ERROR') as logs:
            self.token_map.rebuild_keyspace('ks', build_if_absent=True)

        assert self.token_map.tokens_to_hosts_by_ks['ks'] == {}
        assert "Failed creating a token map for keyspace 'ks'" in logs.output[0]

    def test_keyspace_events_rebuild_existing_maps_only(self):
        self.metadata.keyspaces['ks'] = KeyspaceMetadata(
            'ks', True, 'SimpleStrategy', {'replication_factor': '1'})
        # Not built yet: a keyspace update must not build it eagerly.
        self.metadata._keyspace_updated('ks')
        assert 'ks' not in self.token_map.tokens_to_hosts_by_ks

        assert self.token_map.get_replicas('ks', MD5Token(15)) == [self.hosts[1]]
        self.metadata.keyspaces['ks'] = KeyspaceMetadata(
            'ks', True, 'SimpleStrategy', {'replication_factor': '2'})
        self.metadata._keyspace_updated('ks')
        assert self.token_map.get_replicas('ks', MD5Token(15)) == [self.hosts[1], self.hosts[0]]

        self.metadata._keyspace_removed('ks')
        assert 'ks' not in self.token_map.tokens_to_hosts_by_ks


class GroupKeysByReplicaTest(unittest.TestCase):

    def setUp(self):
        self.local, self.remote, self.down = [
            Host(DefaultEndPoint('127.0.0.%d' % i), SimpleConvictionPolicy, host_id=uuid.uuid4())
            for i in (1, 2, 3)]
        self.local.set_up()
        self.remote.set_up()
        self.down.set_down()

        metadata = Metadata()
        metadata.rebuild_token_map('RandomPartitioner', {
            self.local: ['0'], self.remote: ['100'], self.down: ['200']})
        ks = KeyspaceMetadata('ks', True, 'SimpleStrategy', {'replication_factor': '3'})
        for name, partition_key in (('single', [('k', 'int')]),
                                    ('composite', [('k', 'int'), ('b', 'text')])):
            table = TableMetadataV3('ks', name)
            for col_name, cql_type in partition_key:
                table.columns[col_name] = ColumnMetadata(table, col_name, cql_type)
                table.partition_key.append(table.columns[col_name])
            ks.tables[name] = table
        metadata.keyspaces['ks'] = ks

        distances = {self.local: HostDistance.LOCAL, self.remote: HostDistance.REMOTE,
                     self.down: HostDistance.LOCAL}
        policy = Mock(**{'distance.side_effect': distances.get})
        self.cluster = SimpleNamespace(metadata=metadata, protocol_version=4,
                                       _default_load_balancing_policy=policy)
        self.session = SimpleNamespace(cluster=self.cluster)

    def test_prefers_local_up_replica(self):
        keys = [(1,), (2,), (3,)]
        assert group_keys_by_replica(self.session, 'ks', 'single', keys) == {self.local: keys}

    def test_falls_back_to_any_up_replica(self):
        self.local.set_down()
        keys = [(1,), (2,)]
        assert group_keys_by_replica(self.session, 'ks', 'single', keys) == {self.remote: keys}

    def test_no_up_replica(self):
        self.local.set_down()
        self.remote.set_down()
        keys = [(1,)]
        assert group_keys_by_replica(self.session, 'ks', 'single', keys) == {NO_VALID_REPLICA: keys}

    def test_composite_partition_key_routing_key(self):
        metadata = self.cluster.metadata
        with patch.object(metadata, 'get_replicas', wraps=metadata.get_replicas) as get_replicas:
            result = group_keys_by_replica(self.session, 'ks', 'composite', [(1, 'a')])

        assert result == {self.local: [(1, 'a')]}
        # Each component is length-prefixed and followed by a zero byte.
        expected = (struct.pack('>H', 4) + struct.pack('>i', 1) + b'\x00' +
                    struct.pack('>H', 1) + b'a' + b'\x00')
        get_replicas.assert_called_once_with('ks', expected)


class MiscMetadataTest(unittest.TestCase):

    def test_simple_strategy_equality(self):
        rf3 = SimpleStrategy({'replication_factor': '3'})
        assert rf3 == SimpleStrategy({'replication_factor': 3})
        assert rf3 != SimpleStrategy({'replication_factor': '2'})
        assert rf3 != 'SimpleStrategy'

    def test_cql_from_cass_type(self):
        assert _cql_from_cass_type(cqltypes.lookup_casstype(
            'org.apache.cassandra.db.marshal.MapType('
            'org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)')
        ) == 'map<text, int>'
        # Clustering order is not part of the CQL type.
        assert _cql_from_cass_type(cqltypes.lookup_casstype(
            'org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.TimeUUIDType)')
        ) == 'timeuuid'

    def test_md5_token_accepts_bytes_and_text(self):
        assert MD5Token.hash_fn(b'key') == MD5Token.hash_fn('key')

    def test_index_and_user_type_round_trip_through_keyspace_export(self):
        ks = KeyspaceMetadata('ks', True, 'SimpleStrategy', {'replication_factor': '1'})
        ks.user_types['t'] = UserType('ks', 't', ['f'], ['int'])
        table = TableMetadataExportTest._table()
        table.indexes['t_v_idx'] = IndexMetadata('ks', 't', 't_v_idx', 'COMPOSITES', {'target': 'v'})
        ks.tables['t'] = table
        metadata = Metadata()
        metadata.keyspaces['ks'] = ks

        assert metadata.export_schema_as_string() == (
            "CREATE KEYSPACE ks WITH replication = "
            "{'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;\n"
            "\n"
            "CREATE TYPE ks.t (\n    f int\n);\n"
            "\n"
            "CREATE TABLE ks.t (\n"
            "    k int,\n"
            "    c int,\n"
            "    v text,\n"
            "    PRIMARY KEY (k, c)\n"
            ") WITH CLUSTERING ORDER BY (c ASC);\n"
            "CREATE INDEX t_v_idx ON ks.t (v);")
