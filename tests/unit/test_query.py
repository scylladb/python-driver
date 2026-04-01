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

import pytest

from cassandra import query as query_module
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
        self.assertIs(lwt_simple.is_lwt(), True)

        batch_with_simple = BatchStatement()
        batch_with_simple.add(lwt_simple)
        assert batch_with_simple.is_lwt() is True


class PreparedStatementMetadataPairTest(unittest.TestCase):
    """
    result_metadata and result_metadata_id are stored as one tuple replaced in a
    single attribute assignment: response callbacks update a statement while
    request threads read it, and a torn pair (fresh id + stale metadata) would
    make the server skip sending metadata while rows are decoded against the
    wrong columns.
    """

    @staticmethod
    def _make_statement(result_metadata, result_metadata_id):
        return PreparedStatement(
            column_metadata=[], query_id=b'qid', routing_key_indexes=None,
            query="SELECT * FROM foo", keyspace='ks', protocol_version=4,
            result_metadata=result_metadata, result_metadata_id=result_metadata_id)

    def test_constructor_sets_pair(self):
        meta = [('ks', 'tb', 'col', None)]
        ps = self._make_statement(meta, b'hash')
        assert ps.result_metadata is meta
        assert ps.result_metadata_id == b'hash'
        assert ps.result_metadata_and_id == (meta, b'hash')

    def test_update_replaces_pair_atomically(self):
        ps = self._make_statement([('ks', 'tb', 'old', None)], b'old')
        snapshot_before = ps.result_metadata_and_id

        new_meta = [('ks', 'tb', 'new', None)]
        ps.update_result_metadata(new_meta, b'new')

        # a snapshot taken before the update stays internally consistent
        assert snapshot_before == ([('ks', 'tb', 'old', None)], b'old')
        assert ps.result_metadata_and_id == (new_meta, b'new')

    def test_halves_of_the_pair_cannot_be_assigned_individually(self):
        # Assigning one half alone would leave the other stale, which is exactly
        # the torn state update_result_metadata() exists to prevent, so neither
        # attribute is writable.
        meta = [('ks', 'tb', 'col', None)]
        ps = self._make_statement(meta, b'hash')

        with pytest.raises(AttributeError):
            ps.result_metadata_id = b'other'

        with pytest.raises(AttributeError):
            ps.result_metadata = []

        assert ps.result_metadata_and_id == (meta, b'hash')


class SimpleStatementIsLwtTest(unittest.TestCase):
    """Tests for SimpleStatement.is_lwt() CQL-based LWT detection."""

    # --- INSERT IF NOT EXISTS ---

    def test_insert_if_not_exists(self):
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        self.assertIs(s.is_lwt(), True)

    def test_insert_if_not_exists_lowercase(self):
        s = SimpleStatement("insert into ks.t (a) values (1) if not exists")
        self.assertIs(s.is_lwt(), True)

    def test_insert_if_not_exists_mixed_case(self):
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1) If Not Exists")
        self.assertIs(s.is_lwt(), True)

    # --- UPDATE IF EXISTS ---

    def test_update_if_exists(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1 IF EXISTS")
        self.assertIs(s.is_lwt(), True)

    # --- DELETE IF EXISTS ---

    def test_delete_if_exists(self):
        s = SimpleStatement("DELETE FROM ks.t WHERE k=1 IF EXISTS")
        self.assertIs(s.is_lwt(), True)

    # --- Conditional UPDATE (IF <column> = <value>) ---

    def test_conditional_update_equals(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1 IF a = 2")
        self.assertIs(s.is_lwt(), True)

    def test_conditional_update_not_equals(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1 IF a != 2")
        self.assertIs(s.is_lwt(), True)

    def test_conditional_update_greater_than(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1 IF a > 2")
        self.assertIs(s.is_lwt(), True)

    def test_conditional_update_multiple_conditions(self):
        s = SimpleStatement(
            "UPDATE ks.t SET a=1 WHERE k=1 IF a = 2 AND b = 3")
        self.assertIs(s.is_lwt(), True)

    # --- Conditional DELETE ---

    def test_conditional_delete(self):
        s = SimpleStatement("DELETE FROM ks.t WHERE k=1 IF a = 2")
        self.assertIs(s.is_lwt(), True)

    # --- Non-LWT queries (should return False) ---

    def test_select_not_lwt(self):
        s = SimpleStatement("SELECT * FROM ks.t WHERE k=1")
        self.assertIs(s.is_lwt(), False)

    def test_insert_without_if(self):
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1)")
        self.assertIs(s.is_lwt(), False)

    def test_update_without_if(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1")
        self.assertIs(s.is_lwt(), False)

    def test_delete_without_if(self):
        s = SimpleStatement("DELETE FROM ks.t WHERE k=1")
        self.assertIs(s.is_lwt(), False)

    def test_create_table_with_if_not_exists(self):
        """DDL IF NOT EXISTS is correctly excluded — only DML can be LWT."""
        s = SimpleStatement("CREATE TABLE IF NOT EXISTS ks.t (a int PRIMARY KEY)")
        self.assertIs(s.is_lwt(), False)

    def test_create_index_if_not_exists(self):
        s = SimpleStatement("CREATE INDEX IF NOT EXISTS idx ON ks.t (a)")
        self.assertIs(s.is_lwt(), False)

    def test_create_keyspace_if_not_exists(self):
        s = SimpleStatement(
            "CREATE KEYSPACE IF NOT EXISTS ks WITH replication = "
            "{'class': 'SimpleStrategy', 'replication_factor': 1}")
        self.assertIs(s.is_lwt(), False)

    def test_drop_table_if_exists(self):
        s = SimpleStatement("DROP TABLE IF EXISTS ks.t")
        self.assertIs(s.is_lwt(), False)

    def test_alter_table_not_lwt(self):
        s = SimpleStatement("ALTER TABLE ks.t ADD col int")
        self.assertIs(s.is_lwt(), False)

    # --- Caching ---
    #
    # These validate caching via an observable effect (the LWT-detection
    # regexes are invoked at most once per statement instance, even across
    # repeated is_lwt() calls) rather than asserting on the private
    # _cached_is_lwt attribute, so the tests don't couple to that
    # implementation detail and keep passing across refactors of the cache
    # itself.

    def test_result_is_cached(self):
        """For a DML/LWT statement, is_lwt() takes the branch that calls
        _LWT_PATTERN.search(); confirm that regex is only invoked once
        across multiple is_lwt() calls, proving the result is cached."""
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        with patch.object(query_module, '_LWT_PATTERN',
                           wraps=query_module._LWT_PATTERN) as mock_pattern:
            self.assertIs(s.is_lwt(), True)
            self.assertIs(s.is_lwt(), True)  # should use cache, not re-match
            self.assertEqual(mock_pattern.search.call_count, 1)

    def test_non_lwt_result_is_cached(self):
        """For a non-DML statement, is_lwt() short-circuits before ever
        calling _LWT_PATTERN.search(), but it still runs the noise-stripping
        regex to decide; confirm that regex is only invoked once across
        multiple is_lwt() calls, proving the result is cached."""
        s = SimpleStatement("SELECT * FROM ks.t")
        with patch.object(query_module, '_LWT_NOISE_RE',
                           wraps=query_module._LWT_NOISE_RE) as mock_noise_re:
            self.assertIs(s.is_lwt(), False)
            self.assertIs(s.is_lwt(), False)  # should use cache, not recompute
            self.assertEqual(mock_noise_re.sub.call_count, 1)

    def test_cached_attribute_is_set_after_first_call(self):
        """Minimal direct check retained for debugging clarity: the tests
        above are the primary proof of caching behavior via observable
        effects, but a single check that the private cache attribute holds
        the computed value is useful when diagnosing a failure here."""
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        self.assertIs(s.is_lwt(), True)
        self.assertIs(s._cached_is_lwt, True)

    # --- Edge cases ---

    def test_multiline_query(self):
        s = SimpleStatement("""
            INSERT INTO ks.t (a, b)
            VALUES (1, 2)
            IF NOT EXISTS
        """)
        self.assertIs(s.is_lwt(), True)

    def test_extra_whitespace(self):
        s = SimpleStatement("UPDATE ks.t SET a=1 WHERE k=1  IF   EXISTS")
        self.assertIs(s.is_lwt(), True)

    def test_tab_separated(self):
        s = SimpleStatement("DELETE FROM ks.t WHERE k=1\tIF\tEXISTS")
        self.assertIs(s.is_lwt(), True)

    # --- Quoted identifiers ---

    def test_conditional_with_quoted_identifier(self):
        s = SimpleStatement('UPDATE ks.t SET a=1 WHERE k=1 IF "my_col" = 2')
        self.assertIs(s.is_lwt(), True)

    def test_conditional_delete_quoted_identifier(self):
        s = SimpleStatement('DELETE FROM ks.t WHERE k=1 IF "Col" = 2')
        self.assertIs(s.is_lwt(), True)

    # --- BEGIN BATCH ---

    def test_begin_batch_with_lwt(self):
        s = SimpleStatement(
            "BEGIN BATCH "
            "INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS "
            "APPLY BATCH")
        self.assertIs(s.is_lwt(), True)

    def test_begin_batch_without_lwt(self):
        s = SimpleStatement(
            "BEGIN BATCH "
            "INSERT INTO ks.t (a) VALUES (1) "
            "APPLY BATCH")
        self.assertIs(s.is_lwt(), False)

    # --- Leading whitespace ---

    def test_leading_whitespace(self):
        s = SimpleStatement("  \n  INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        self.assertIs(s.is_lwt(), True)

    def test_leading_whitespace_ddl(self):
        s = SimpleStatement("  \n  CREATE TABLE IF NOT EXISTS ks.t (a int PRIMARY KEY)")
        self.assertIs(s.is_lwt(), False)

    # --- False positives: IF/EXISTS-like text inside string literals ---
    # (must not be mistaken for a real LWT clause)

    def test_string_literal_containing_if_not_exists(self):
        s = SimpleStatement(
            "INSERT INTO ks.t (a, note) VALUES (1, 'IF NOT EXISTS')")
        self.assertIs(s.is_lwt(), False)

    def test_string_literal_containing_if_exists_sentence(self):
        s = SimpleStatement(
            "INSERT INTO ks.t (a, note) VALUES "
            "(1, 'please check IF EXISTS in cache')")
        self.assertIs(s.is_lwt(), False)

    def test_string_literal_looks_like_conditional(self):
        s = SimpleStatement("UPDATE ks.t SET note = 'IF a = 2' WHERE k=1")
        self.assertIs(s.is_lwt(), False)

    def test_string_literal_with_escaped_quote_containing_if_exists(self):
        # CQL escapes a literal quote inside a string as ''.
        s = SimpleStatement(
            "INSERT INTO ks.t (a, note) VALUES (1, 'it''s a test IF EXISTS')")
        self.assertIs(s.is_lwt(), False)

    def test_map_literal_value_containing_if_exists(self):
        s = SimpleStatement(
            "INSERT INTO ks.t (a, m) VALUES (1, {'k': 'IF EXISTS'})")
        self.assertIs(s.is_lwt(), False)

    # --- Column named similarly to "if"/"exists" is not itself a clause ---

    def test_column_named_if_deleted_no_clause(self):
        s = SimpleStatement("UPDATE ks.t SET if_deleted = true WHERE k=1")
        self.assertIs(s.is_lwt(), False)

    def test_real_if_clause_referencing_if_deleted_column(self):
        s = SimpleStatement(
            "UPDATE ks.t SET a=1 WHERE k=1 IF if_deleted = true")
        self.assertIs(s.is_lwt(), True)

    def test_quoted_identifier_literally_containing_if_prefix(self):
        # A quoted identifier can contain arbitrary text, including a
        # leading "IF "; it is not a real LWT clause.
        s = SimpleStatement('UPDATE ks.t SET "IF status" = 1 WHERE k=1')
        self.assertIs(s.is_lwt(), False)

    # --- Comments: must not hide a real clause or fabricate one ---

    def test_leading_line_comment_before_real_lwt(self):
        s = SimpleStatement(
            "-- trace comment\nINSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        self.assertIs(s.is_lwt(), True)

    def test_leading_slash_comment_before_real_lwt(self):
        s = SimpleStatement(
            "// trace comment\nINSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        self.assertIs(s.is_lwt(), True)

    def test_leading_block_comment_before_real_lwt(self):
        s = SimpleStatement(
            "/* trace comment */ INSERT INTO ks.t (a) VALUES (1) IF NOT EXISTS")
        self.assertIs(s.is_lwt(), True)

    def test_trailing_comment_mentioning_if_not_exists_is_not_lwt(self):
        s = SimpleStatement("INSERT INTO ks.t (a) VALUES (1) -- IF NOT EXISTS")
        self.assertIs(s.is_lwt(), False)

    def test_inline_block_comment_mentioning_if_exists_is_not_lwt(self):
        s = SimpleStatement(
            "INSERT INTO ks.t (a) /* IF NOT EXISTS, check later */ VALUES (1)")
        self.assertIs(s.is_lwt(), False)
