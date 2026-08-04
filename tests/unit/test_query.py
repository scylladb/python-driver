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

import pickle
import unittest

import pytest

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
        assert ps.result_metadata == meta
        assert isinstance(ps.result_metadata, list)
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

    def test_constructor_does_not_retain_mutable_metadata_sequence(self):
        meta = [
            ('ks', 'tb', 'first', None),
            ('ks', 'tb', 'second', None),
        ]
        ps = self._make_statement(meta, b'hash')

        meta.reverse()
        meta.append(('ks', 'tb', 'third', None))

        assert ps.result_metadata == [
            ('ks', 'tb', 'first', None),
            ('ks', 'tb', 'second', None),
        ]

    def test_update_does_not_retain_mutable_metadata_sequence(self):
        ps = self._make_statement([], None)
        meta = [('ks', 'tb', 'col', None)]

        ps.update_result_metadata(meta, b'hash')
        meta.clear()

        assert ps.result_metadata == [('ks', 'tb', 'col', None)]
        assert ps.result_metadata_id == b'hash'

    def test_public_metadata_is_a_defensive_copy(self):
        ps = self._make_statement(
            [('ks', 'tb', 'col', None)], b'hash')

        returned_metadata = ps.result_metadata
        returned_metadata.clear()

        assert ps.result_metadata == [('ks', 'tb', 'col', None)]
        assert ps.result_metadata_id == b'hash'

    def test_update_freezes_mutable_column_definition(self):
        ps = self._make_statement([], None)
        column = ['ks', 'tb', 'col', None]
        ps.update_result_metadata([column], b'hash')

        column[2] = 'changed'
        column[3] = object()

        assert ps.result_metadata == [('ks', 'tb', 'col', None)]

    def test_rejects_malformed_column_definition(self):
        ps = self._make_statement([], None)

        with pytest.raises(ValueError, match='exactly four'):
            ps.update_result_metadata([('ks', 'tb', 'col')], b'hash')

    def test_internal_update_tracks_decoder_context_atomically(self):
        ps = self._make_statement([], None)
        decoder_context = object()
        meta = [('ks', 'tb', 'col', None)]

        ps._update_result_metadata(meta, b'hash', decoder_context)
        snapshot = ps._result_metadata_snapshot

        assert snapshot == (tuple(meta), b'hash', decoder_context)

        ps._update_result_metadata([], b'new-hash', object())
        assert snapshot == (tuple(meta), b'hash', decoder_context)

    def test_public_update_clears_decoder_context_provenance(self):
        ps = self._make_statement([], None)
        ps._update_result_metadata([], b'old-hash', object())

        ps.update_result_metadata([], b'new-hash')

        assert ps._result_metadata_snapshot == ((), b'new-hash', None)

    def test_invalidate_id_preserves_metadata_and_decoder_context(self):
        ps = self._make_statement([], None)
        decoder_context = object()
        meta = [('ks', 'tb', 'col', None)]
        ps._update_result_metadata(meta, b'hash', decoder_context)

        ps._invalidate_result_metadata_id()

        assert ps._result_metadata_snapshot == (
            tuple(meta), None, decoder_context)

    def test_pickle_drops_runtime_decoder_context(self):
        ps = self._make_statement([], None)
        ps._update_result_metadata(
            [('ks', 'tb', 'col', None)], b'hash', lambda: None)

        restored = pickle.loads(pickle.dumps(ps))

        assert restored._result_metadata_snapshot == (
            (('ks', 'tb', 'col', None),), b'hash', None)

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

    def test_migrates_pickle_with_metadata_pair(self):
        ps = self._make_statement([], None)
        ps.__dict__.pop('_result_metadata_state')
        ps.__dict__['_result_metadata_and_id'] = (
            [('ks', 'tb', 'col', None)], b'hash')

        restored = pickle.loads(pickle.dumps(ps))

        assert restored._result_metadata_snapshot == (
            (('ks', 'tb', 'col', None),), b'hash', None)
        assert '_result_metadata_and_id' not in restored.__dict__

    def test_migrates_pickle_with_independent_metadata_attributes(self):
        ps = self._make_statement([], None)
        state = ps.__dict__.copy()
        state.pop('_result_metadata_state')
        state['result_metadata'] = [
            ('ks', 'tb', 'col', None)]
        state['result_metadata_id'] = b'hash'

        restored = object.__new__(PreparedStatement)
        restored.__setstate__(state)

        assert restored._result_metadata_snapshot == (
            (('ks', 'tb', 'col', None),), b'hash', None)
        assert 'result_metadata' not in restored.__dict__
        assert 'result_metadata_id' not in restored.__dict__
