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

        class LwtSimpleStatement(SimpleStatement):
            def __init__(self):
                super(LwtSimpleStatement, self).__init__(
                    "INSERT INTO test.table (id) VALUES (2) IF NOT EXISTS"
                )

            def is_lwt(self):
                return True

        batch_with_simple = BatchStatement()
        batch_with_simple.add(LwtSimpleStatement())
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

    def test_individual_setters_keep_pair_consistent(self):
        # backwards-compatible attribute assignment still works and replaces
        # the whole pair underneath
        meta = [('ks', 'tb', 'col', None)]
        ps = self._make_statement(meta, b'hash')

        ps.result_metadata_id = b'other'
        assert ps.result_metadata_and_id == (meta, b'other')

        new_meta = []
        ps.result_metadata = new_meta
        assert ps.result_metadata_and_id == (new_meta, b'other')
