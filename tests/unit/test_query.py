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

from cassandra.query import BatchStatement, PreparedStatement, SimpleStatement


class BatchStatementTest(unittest.TestCase):
    # TODO: this suite could be expanded; for now just adding a test covering a PR

    def test_clear(self):
        keyspace = 'keyspace'
        routing_key = 'routing_key'
        custom_payload = {'key': b'value'}

        ss = SimpleStatement('whatever', keyspace=keyspace, routing_key=routing_key, custom_payload=custom_payload)

        batch = BatchStatement()
        batch.add(ss)

        assert batch._statements_and_parameters
        assert batch.keyspace == keyspace
        assert batch.routing_key == routing_key
        assert batch.custom_payload == custom_payload

        batch.clear()
        assert not batch._statements_and_parameters
        assert batch.keyspace is None
        assert batch.routing_key is None
        assert not batch.custom_payload

        batch.add(ss)

    def test_clear_empty(self):
        batch = BatchStatement()
        batch.clear()
        assert not batch._statements_and_parameters
        assert batch.keyspace is None
        assert batch.routing_key is None
        assert not batch.custom_payload

        batch.add('something')

    def test_add_all(self):
        batch = BatchStatement()
        statements = ['%s'] * 10
        parameters = [(i,) for i in range(10)]
        batch.add_all(statements, parameters)
        bound_statements = [t[1] for t in batch._statements_and_parameters]
        str_parameters = [str(i) for i in range(10)]
        assert bound_statements == str_parameters

    def test_len(self):
        for n in 0, 10, 100:
            batch = BatchStatement()
            batch.add_all(statements=['%s'] * n,
                          parameters=[(i,) for i in range(n)])
            assert len(batch) == n

    def _make_prepared_statement(self, is_lwt=False):
        return PreparedStatement(
            column_metadata=[],
            query_id=b"query-id",
            routing_key_indexes=[],
            query="INSERT INTO test.table (id) VALUES (1)",
            keyspace=None,
            protocol_version=4,
            result_metadata=[],
            result_metadata_id=None,
            is_lwt=is_lwt,
        )

    def test_is_lwt_false_for_non_lwt_statements(self):
        batch = BatchStatement()
        batch.add(self._make_prepared_statement(is_lwt=False))
        batch.add(self._make_prepared_statement(is_lwt=False).bind(()))
        batch.add(SimpleStatement("INSERT INTO test.table (id) VALUES (3)"))
        batch.add("INSERT INTO test.table (id) VALUES (4)")
        assert batch.is_lwt() is False

    def test_is_lwt_propagates_from_statements(self):
        batch = BatchStatement()
        batch.add(self._make_prepared_statement(is_lwt=False))
        assert batch.is_lwt() is False

        batch.add(self._make_prepared_statement(is_lwt=True))
        assert batch.is_lwt() is True

        bound_lwt = self._make_prepared_statement(is_lwt=True).bind(())
        batch_with_bound = BatchStatement()
        batch_with_bound.add(bound_lwt)
        assert batch_with_bound.is_lwt() is True

        # SimpleStatement now detects LWT from query string (no subclass needed)
        lwt_simple = SimpleStatement(
            "INSERT INTO test.table (id) VALUES (2) IF NOT EXISTS"
        )
        assert lwt_simple.is_lwt() is True

        batch_with_simple = BatchStatement()
        batch_with_simple.add(lwt_simple)
        assert batch_with_simple.is_lwt() is True

class SimpleStatementIsLwtTest(unittest.TestCase):
    """Tests for SimpleStatement.is_lwt() CQL-based LWT detection."""

    # --- INSERT IF NOT EXISTS ---

    def test_insert_if_not_exists(self):
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        assert s.is_lwt() is True

    def test_insert_if_not_exists_lowercase(self):
        s = SimpleStatement("insert into ks.t (a) values (1) if not exists")
        assert s.is_lwt() is True

    def test_insert_if_not_exists_mixed_case(self):
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1) If Not Exists")
        assert s.is_lwt() is True

    # --- UPDATE IF EXISTS ---

    def test_update_if_exists(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1 IF EXISTS")
        assert s.is_lwt() is True

    # --- DELETE IF EXISTS ---

    def test_delete_if_exists(self):
        s = SimpleStatement("DELETE FROM ks.t WHERE k=1 IF EXISTS")
        assert s.is_lwt() is True

    # --- Conditional UPDATE (IF <column> = <value>) ---

    def test_conditional_update_equals(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1 IF a = 2")
        assert s.is_lwt() is True

    def test_conditional_update_not_equals(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1 IF a != 2")
        assert s.is_lwt() is True

    def test_conditional_update_greater_than(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1 IF a > 2")
        assert s.is_lwt() is True

    def test_conditional_update_multiple_conditions(self):
        s = SimpleStatement(
            "UPDATE ks.t SET a=1 WHERE k=1 IF a = 2 AND b = 3")
        assert s.is_lwt() is True

    # --- Conditional DELETE ---

    def test_conditional_delete(self):
        s = SimpleStatement("DELETE FROM ks.t WHERE k=1 IF a = 2")
        assert s.is_lwt() is True

    # --- Non-LWT queries (should return False) ---

    def test_select_not_lwt(self):
        s = SimpleStatement("SELECT * FROM ks.t WHERE k=1")
        assert s.is_lwt() is False

    def test_insert_without_if(self):
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1)")
        assert s.is_lwt() is False

    def test_update_without_if(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1")
        assert s.is_lwt() is False

    def test_delete_without_if(self):
        s = SimpleStatement("DELETE FROM ks.t WHERE k=1")
        assert s.is_lwt() is False

    def test_create_table_with_if_not_exists(self):
        """DDL IF NOT EXISTS is correctly excluded — only DML can be LWT."""
        s = SimpleStatement("CREATE TABLE IF NOT EXISTS ks.t (a int PRIMARY KEY)")
        assert s.is_lwt() is False

    def test_create_index_if_not_exists(self):
        s = SimpleStatement("CREATE INDEX IF NOT EXISTS idx ON ks.t (a)")
        assert s.is_lwt() is False

    def test_create_keyspace_if_not_exists(self):
        s = SimpleStatement(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH replication = "
            "{'class': 'SimpleStrategy', 'replication_factor': 1}")
        assert s.is_lwt() is False

    def test_drop_table_if_exists(self):
        s = SimpleStatement("DROP TABLE IF EXISTS ks.t")
        assert s.is_lwt() is False

    def test_alter_table_not_lwt(self):
        s = SimpleStatement("ALTER TABLE ks.t ADD col int")
        assert s.is_lwt() is False

    # --- Caching ---

    def test_result_is_cached(self):
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        assert s.is_lwt() is True
        assert s.is_lwt() is True  # should use cache
        assert s._cached_is_lwt is True

    def test_non_lwt_result_is_cached(self):
        s = SimpleStatement("SELECT * FROM ks.t")
        assert s.is_lwt() is False
        assert s._cached_is_lwt is False

    # --- Edge cases ---

    def test_multiline_query(self):
        s = SimpleStatement("""
            INSERT INTO ks.t (a, b)
            VALUES (1, 2)
            IF NOT EXISTS
        """)
        assert s.is_lwt() is True

    def test_extra_whitespace(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1  IF   EXISTS")
        assert s.is_lwt() is True

    def test_tab_separated(self):
        s = SimpleStatement("DELETE FROM ks.t WHERE k=1\tIF\tEXISTS")
        assert s.is_lwt() is True

    # --- Quoted identifiers ---

    def test_conditional_with_quoted_identifier(self):
        s = SimpleStatement('UPDATE ks.t SET a=1 WHERE k=1 IF "my_col" = 2')
        assert s.is_lwt() is True

    def test_conditional_delete_quoted_identifier(self):
        s = SimpleStatement('DELETE FROM ks.t WHERE k=1 IF "Col" = 2')
        assert s.is_lwt() is True

    # --- BEGIN BATCH ---

    def test_begin_batch_with_lwt(self):
        s = SimpleStatement(
            "BEGIN BATCH "
            "INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS "
            "APPLY BATCH")
        assert s.is_lwt() is True

    def test_begin_batch_without_lwt(self):
        s = SimpleStatement(
            "BEGIN BATCH "
            "INSERT INTO ks.t (a) VALUES (1) "
            "APPLY BATCH")
        assert s.is_lwt() is False

    # --- Leading whitespace ---

    def test_leading_whitespace(self):
        s = SimpleStatement("  \n  INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        assert s.is_lwt() is True

    def test_leading_whitespace_ddl(self):
        s = SimpleStatement("  \n  CREATE TABLE IF NOT EXISTS ks.t (a int PRIMARY KEY)")
        assert s.is_lwt() is False
