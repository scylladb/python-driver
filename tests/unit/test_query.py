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

import pytest

from cassandra import metadata
from cassandra.cqltypes import Int32Type
from cassandra.protocol import ColumnMetadata, _NO_ROUTING_STATEMENT
from cassandra.query import (BatchStatement, BoundStatement, PreparedStatement, SimpleStatement, Statement,
                             UNSET_VALUE)


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

    @staticmethod
    def _make_keyed_prepared_statement(table):
        return PreparedStatement.from_message(
            query_id=b"query-id-" + table.encode(),
            column_metadata=[ColumnMetadata('ks', table, 'pk', Int32Type)],
            pk_indexes=[0],
            cluster_metadata=None,
            query="INSERT INTO ks.%s (pk) VALUES (?)" % table,
            prepared_keyspace='ks',
            protocol_version=4,
            result_metadata=[],
            result_metadata_id=None,
            is_lwt=False,
        )

    @staticmethod
    def _routing_statement_index(batch):
        routing_statement = batch._get_routing_statement()
        return _NO_ROUTING_STATEMENT if routing_statement is None else routing_statement.index

    def test_routing_attributes_come_from_the_first_keyed_statement(self):
        """
        The batch takes its keyspace, table and routing key from the first
        statement that has a routing key, so that tablet lookups use the table
        the batch is routed by.
        """
        batch = BatchStatement()
        batch.add("INSERT INTO ks.other (pk) VALUES (0)")
        batch.add(SimpleStatement("INSERT INTO ks.other (pk) VALUES (0)", keyspace='ks'))
        batch.add(self._make_keyed_prepared_statement('tbl'), (1,))
        batch.add(self._make_keyed_prepared_statement('other'), (2,))

        assert batch.keyspace == 'ks'
        assert batch.table == 'tbl'
        assert batch.routing_key == Int32Type.serialize(1, 4)

        batch.clear()
        assert batch.keyspace is None
        assert batch.table is None
        assert batch.routing_key is None

    def test_routing_statement_index_names_the_keyed_bound_statement(self):
        """
        The routing hint names the statement the batch takes its routing key
        from, even when statements without one come before it.
        """
        batch = BatchStatement()
        batch.add("INSERT INTO ks.other (pk) VALUES (0)")
        batch.add(SimpleStatement("INSERT INTO ks.other (pk) VALUES (0)", keyspace='ks'))
        batch.add(self._make_keyed_prepared_statement('tbl'), (1,))
        batch.add(self._make_keyed_prepared_statement('other').bind((2,)))

        assert self._routing_statement_index(batch) == 2
        assert batch._get_routing_statement().routing_key == Int32Type.serialize(1, 4)

    def test_routing_statement_index_of_a_bound_statement(self):
        batch = BatchStatement()
        batch.add(self._make_keyed_prepared_statement('tbl').bind((1,)))

        assert self._routing_statement_index(batch) == 0

    def test_no_routing_statement_without_a_keyed_bound_statement(self):
        """
        The batch is routed by the first statement that has a routing key. The
        driver only names it if it's a prepared statement with partition key
        bind indices, so a batch routed by a SimpleStatement names none, even if
        a statement that could be named comes after it.
        """
        batch = BatchStatement()
        batch.add(SimpleStatement("INSERT INTO ks.tbl (pk) VALUES (1)", keyspace='ks',
                                  routing_key=Int32Type.serialize(1, 4)))
        batch.add(self._make_keyed_prepared_statement('tbl'), (2,))
        assert batch.routing_key == Int32Type.serialize(1, 4)
        assert self._routing_statement_index(batch) == _NO_ROUTING_STATEMENT

        unkeyed = BatchStatement()
        unkeyed.add("INSERT INTO ks.tbl (pk) VALUES (1)")
        unkeyed.add(self._make_prepared_statement())
        assert self._routing_statement_index(unkeyed) == _NO_ROUTING_STATEMENT

        assert self._routing_statement_index(BatchStatement()) == _NO_ROUTING_STATEMENT

    def test_no_routing_statement_after_clear(self):
        batch = BatchStatement()
        batch.add(self._make_keyed_prepared_statement('tbl'), (1,))
        assert self._routing_statement_index(batch) == 0

        batch.clear()
        assert self._routing_statement_index(batch) == _NO_ROUTING_STATEMENT

        batch.add("INSERT INTO ks.tbl (pk) VALUES (1)")
        batch.add(self._make_keyed_prepared_statement('tbl'), (1,))
        assert self._routing_statement_index(batch) == 1

    def test_no_routing_statement_after_keyspace_or_table_change(self):
        """
        Once the user changes the keyspace or the table of the batch, the
        routing information returned for the statement would be filed under
        another table.
        """
        def keyed_batch():
            batch = BatchStatement()
            batch.add("INSERT INTO ks.tbl (pk) VALUES (0)")
            batch.add(self._make_keyed_prepared_statement('tbl'), (1,))
            assert self._routing_statement_index(batch) == 1
            return batch

        batch = keyed_batch()
        batch.keyspace = 'other_ks'
        assert self._routing_statement_index(batch) == _NO_ROUTING_STATEMENT

        batch = keyed_batch()
        batch.table = 'other'
        assert self._routing_statement_index(batch) == _NO_ROUTING_STATEMENT

    def test_routing_statement_kept_when_attributes_are_reassigned_equal(self):
        """Assigning equal values doesn't change the table."""
        batch = BatchStatement()
        batch.add(self._make_keyed_prepared_statement('tbl'), (1,))
        batch.keyspace = 'ks'
        batch.table = 'tbl'
        assert self._routing_statement_index(batch) == 0

    def test_routing_statement_kept_after_routing_key_change(self):
        """
        The user may route the batch by another routing key. The routing hint
        still describes the partition the statement writes.
        """
        batch = BatchStatement()
        batch.add(self._make_keyed_prepared_statement('tbl'), (1,))
        batch.routing_key = Int32Type.serialize(2, 4)

        routing_statement = batch._get_routing_statement()
        assert routing_statement.index == 0
        assert routing_statement.routing_key == Int32Type.serialize(1, 4)

    def test_no_routing_statement_for_routing_key_set_before_any_statement(self):
        """
        A batch whose routing key was set before any statement was added takes
        no routing attributes from its statements, so it names none of them,
        even if it was given the keyspace and the table of one.
        """
        routing_key = Int32Type.serialize(2, 4)
        for keyspace, table in ((None, None), ('ks', 'tbl')):
            for bind in (False, True):
                with self.subTest(keyspace=keyspace, table=table, bind=bind):
                    batch = BatchStatement()
                    batch.routing_key = routing_key
                    batch.keyspace = keyspace
                    batch.table = table
                    prepared = self._make_keyed_prepared_statement('tbl')
                    if bind:
                        batch.add(prepared.bind((1,)))
                    else:
                        batch.add(prepared, (1,))

                    assert batch.routing_key == routing_key
                    assert batch.keyspace == keyspace
                    assert batch.table == table
                    assert self._routing_statement_index(batch) == _NO_ROUTING_STATEMENT

    # The server accepts a routing statement only if it sent partition key bind
    # indices for it in the PREPARED metadata, and computes the token from the
    # values at those indices. The tests below prepare statements the way the
    # driver does on a PREPARED response, with the pk_indexes the server sends,
    # for ks.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)).

    @staticmethod
    def _prepare(query, bind_names, pk_indexes):
        table = metadata.TableMetadata('ks', 'tbl')
        table.partition_key = [metadata.ColumnMetadata(table, 'pk', 'int')]
        table.clustering_key = [metadata.ColumnMetadata(table, 'ck', 'int')]
        keyspace = metadata.KeyspaceMetadata('ks', True, 'NetworkTopologyStrategy', {'dc1': '1'})
        keyspace.tables['tbl'] = table
        cluster_metadata = metadata.Metadata()
        cluster_metadata.keyspaces['ks'] = keyspace
        column_metadata = [ColumnMetadata('ks', 'tbl', name, Int32Type) for name in bind_names]
        return PreparedStatement.from_message(
            query_id=b"query-id",
            column_metadata=column_metadata,
            pk_indexes=pk_indexes,
            cluster_metadata=cluster_metadata,
            query=query,
            prepared_keyspace='ks',
            protocol_version=4,
            result_metadata=[],
            result_metadata_id=None,
            is_lwt=False,
        )

    def test_routing_statement_with_partition_key_bind_indices(self):
        """The server sent partition key bind indices, so it accepts the statement."""
        prepared = self._prepare("INSERT INTO ks.tbl (pk, ck, v) VALUES (?, ?, ?)",
                                 ['pk', 'ck', 'v'], pk_indexes=[0])
        batch = BatchStatement()
        batch.add(prepared, (1, 2, 3))
        assert self._routing_statement_index(batch) == 0

    def test_no_routing_statement_for_named_marker_named_like_the_partition_key(self):
        """
        The marker is named pk but sets v, so the partition key is a literal and
        the server sends no partition key bind indices. The driver still guesses
        a routing key from the marker's name, but the server would ignore the
        hint of a BATCH naming the statement and send no routing information.
        """
        prepared = self._prepare("UPDATE ks.tbl SET v = :pk WHERE pk = 0 AND ck = 0",
                                 ['pk'], pk_indexes=[])
        assert prepared.routing_key_indexes == [0]
        batch = BatchStatement()
        batch.add(prepared, (5,))
        assert batch.routing_key == Int32Type.serialize(5, 4)
        assert self._routing_statement_index(batch) == _NO_ROUTING_STATEMENT

    def test_no_routing_statement_for_user_assigned_routing_key_indexes(self):
        """
        The partition key is a literal, so the server sends no partition key bind
        indices. The user may still make the driver route by a bound value, but
        the server would ignore the hint of a BATCH naming the statement and
        send no routing information.
        """
        prepared = self._prepare("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, ?, ?)",
                                 ['ck', 'v'], pk_indexes=[])
        prepared.routing_key_indexes = [0]
        batch = BatchStatement()
        batch.add(prepared, (1, 2))
        assert batch.routing_key is not None
        assert self._routing_statement_index(batch) == _NO_ROUTING_STATEMENT

    def test_routing_statement_for_routing_key_indexes_other_than_the_servers(self):
        """
        The user routes by ck, but the server computes the token from pk: the
        routing hint describes the partition of pk.
        """
        prepared = self._prepare("INSERT INTO ks.tbl (pk, ck, v) VALUES (?, ?, ?)",
                                 ['pk', 'ck', 'v'], pk_indexes=[0])
        prepared.routing_key_indexes = [1]
        batch = BatchStatement()
        batch.add(prepared, (1, 2, 3))

        assert batch.routing_key == Int32Type.serialize(2, 4)
        routing_statement = batch._get_routing_statement()
        assert routing_statement.index == 0
        assert routing_statement.routing_key == Int32Type.serialize(1, 4)

    def test_routing_statement_for_routing_key_indexes_changed_in_place(self):
        """
        The routing key indices start as the partition key bind indices the
        server sent. Changing them in place doesn't change the latter: the
        routing hint still describes the partition of pk.
        """
        prepared = self._prepare("INSERT INTO ks.tbl (pk, ck, v) VALUES (?, ?, ?)",
                                 ['pk', 'ck', 'v'], pk_indexes=[0])
        prepared.routing_key_indexes[0] = 1
        batch = BatchStatement()
        batch.add(prepared, (1, 2, 3))

        assert batch.routing_key == Int32Type.serialize(2, 4)
        routing_statement = batch._get_routing_statement()
        assert routing_statement.index == 0
        assert routing_statement.routing_key == Int32Type.serialize(1, 4)

    def test_routing_statement_for_bound_statement_with_explicit_routing_key(self):
        """
        The user routes by a routing key of their own, but the server computes
        the token from pk: the routing hint describes the partition of pk.
        """
        prepared = self._prepare("INSERT INTO ks.tbl (pk, ck, v) VALUES (?, ?, ?)",
                                 ['pk', 'ck', 'v'], pk_indexes=[0])
        bound = BoundStatement(prepared, routing_key=Int32Type.serialize(7, 4)).bind((1, 2, 3))
        batch = BatchStatement()
        batch.add(bound)

        assert batch.routing_key == Int32Type.serialize(7, 4)
        routing_statement = batch._get_routing_statement()
        assert routing_statement.index == 0
        assert routing_statement.routing_key == Int32Type.serialize(1, 4)

    def test_routing_statement_for_first_bound_statement_with_explicit_routing_key(self):
        """
        The batch is routed by the first statement that has a routing key, even
        if it's one of the user's own. The server sent partition key bind
        indices for that statement, so it's named, with the routing key of the
        partition it writes, and a later statement that could be named isn't.
        """
        prepared = self._prepare("INSERT INTO ks.tbl (pk, ck, v) VALUES (?, ?, ?)",
                                 ['pk', 'ck', 'v'], pk_indexes=[0])
        batch = BatchStatement()
        batch.add("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 0, 0)")
        batch.add(BoundStatement(prepared, routing_key=Int32Type.serialize(7, 4)).bind((1, 2, 3)))
        batch.add(self._make_keyed_prepared_statement('tbl'), (4,))

        assert batch.routing_key == Int32Type.serialize(7, 4)
        routing_statement = batch._get_routing_statement()
        assert routing_statement.index == 1
        assert routing_statement.routing_key == Int32Type.serialize(1, 4)

    def test_no_routing_statement_for_first_bound_statement_with_explicit_routing_key(self):
        """
        The batch is routed by the first statement that has a routing key, even
        if it's one of the user's own. The partition key of that statement is a
        literal, so the server sent no partition key bind indices for it, and
        the batch names no routing statement, even if a later statement could
        be named.

        A BoundStatement ignores an explicit routing key while its prepared
        statement has no routing key indices, so the user assigns those too.
        """
        prepared = self._prepare("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, ?, ?)",
                                 ['ck', 'v'], pk_indexes=[])
        prepared.routing_key_indexes = [0]
        batch = BatchStatement()
        batch.add("INSERT INTO ks.tbl (pk, ck, v) VALUES (0, 0, 0)")
        batch.add(BoundStatement(prepared, routing_key=Int32Type.serialize(7, 4)).bind((2, 3)))
        batch.add(self._make_keyed_prepared_statement('tbl'), (4,))

        assert batch.routing_key == Int32Type.serialize(7, 4)
        assert self._routing_statement_index(batch) == _NO_ROUTING_STATEMENT

    def test_no_routing_statement_for_null_partition_key(self):
        """
        A write with a null partition key is invalid. The batch is still routed
        by the explicit routing key, but there's no key to compute the block
        from, so the driver names no routing statement and leaves reporting the
        error to the server.
        """
        prepared = self._prepare("INSERT INTO ks.tbl (pk, ck, v) VALUES (?, ?, ?)",
                                 ['pk', 'ck', 'v'], pk_indexes=[0])
        bound = BoundStatement(prepared, routing_key=Int32Type.serialize(7, 4)).bind((None, 2, 3))
        batch = BatchStatement()
        batch.add(bound)
        assert batch.routing_key is not None
        assert self._routing_statement_index(batch) == _NO_ROUTING_STATEMENT

    @staticmethod
    def _prepare_composite():
        return PreparedStatement.from_message(
            query_id=b"query-id",
            column_metadata=[ColumnMetadata('ks', 'tbl', name, Int32Type) for name in ('pk1', 'v', 'pk2')],
            pk_indexes=[0, 2],
            cluster_metadata=None,
            query="INSERT INTO ks.tbl (pk1, v, pk2) VALUES (?, ?, ?)",
            prepared_keyspace='ks',
            protocol_version=4,
            result_metadata=[],
            result_metadata_id=None,
            is_lwt=False,
        )

    @staticmethod
    def _packed_routing_key(*values):
        return b"".join(Statement()._key_parts_packed([Int32Type.serialize(v, 4) for v in values]))

    def test_routing_statement_with_composite_partition_key(self):
        """
        The routing key of a composite partition key is packed as for routing.
        It's the routing key of the batch, so the batch doesn't keep a copy.
        """
        batch = BatchStatement()
        batch.add(self._prepare_composite(), (1, 2, 3))

        routing_statement = batch._get_routing_statement()
        assert routing_statement.index == 0
        assert routing_statement.routing_key is batch.routing_key
        assert routing_statement.routing_key == self._packed_routing_key(1, 3)

    def test_routing_statement_with_composite_partition_key_shared_by_batches(self):
        """Batches of the same bound statement share its routing key."""
        bound = self._prepare_composite().bind((1, 2, 3))
        batches = [BatchStatement() for _ in range(3)]
        for batch in batches:
            batch.add(bound)

        for batch in batches:
            assert batch._get_routing_statement().routing_key is bound.routing_key

    def test_routing_statement_with_composite_partition_key_and_explicit_routing_key(self):
        """
        The user routes by a routing key of their own, but the server computes
        the token from pk1 and pk2: the routing hint describes their partition.
        """
        explicit_routing_key = Int32Type.serialize(7, 4)
        batch = BatchStatement()
        batch.add(BoundStatement(self._prepare_composite(), routing_key=explicit_routing_key).bind((1, 2, 3)))

        assert batch.routing_key is explicit_routing_key
        assert batch._get_routing_statement().routing_key == self._packed_routing_key(1, 3)

    def test_routing_statement_with_composite_partition_key_and_routing_key_set_after_it_was_computed(self):
        """
        The routing key the statement computed is replaced with another one,
        but the server computes the token from pk1 and pk2: the routing hint
        still describes their partition.
        """
        bound = self._prepare_composite().bind((1, 2, 3))
        assert bound.routing_key == self._packed_routing_key(1, 3)
        explicit_routing_key = self._packed_routing_key(7, 8)
        bound._set_routing_key(explicit_routing_key)
        batch = BatchStatement()
        batch.add(bound)

        assert batch.routing_key is explicit_routing_key
        assert batch._get_routing_statement().routing_key == self._packed_routing_key(1, 3)

    def test_routing_statement_with_composite_partition_key_bound_again(self):
        """
        The statement keeps the routing key it computed before it was bound
        again, but the routing hint describes the partition the batch writes.
        """
        bound = self._prepare_composite().bind((1, 2, 3))
        assert bound.routing_key == self._packed_routing_key(1, 3)
        bound.bind((4, 5, 6))
        batch = BatchStatement()
        batch.add(bound)

        assert batch._get_routing_statement().routing_key == self._packed_routing_key(4, 6)

    def test_routing_statement_with_composite_partition_key_bound_again_before_routing(self):
        """
        A statement bound again before its routing key was computed still
        shares it with the batch.
        """
        bound = self._prepare_composite().bind((1, 2, 3))
        bound.bind((4, 5, 6))
        batch = BatchStatement()
        batch.add(bound)

        assert batch._get_routing_statement().routing_key is batch.routing_key
        assert batch.routing_key == self._packed_routing_key(4, 6)

    def test_routing_statement_with_composite_partition_key_bound_again_between_batches(self):
        """
        Binding a statement again doesn't change the routing hint of a batch
        it was added to before, and a batch it's added to afterwards describes
        the partition of the new values.
        """
        bound = self._prepare_composite().bind((1, 2, 3))
        first = BatchStatement()
        first.add(bound)
        bound.bind((4, 5, 6))
        second = BatchStatement()
        second.add(bound)

        assert first._get_routing_statement().routing_key == self._packed_routing_key(1, 3)
        assert second._get_routing_statement().routing_key == self._packed_routing_key(4, 6)

    def test_routing_statement_with_composite_partition_key_and_other_routing_key_indexes(self):
        """
        The user routes by the partition key columns in another order, but the
        server computes the token from pk1 and pk2 in order: the routing hint
        describes their partition.
        """
        prepared = self._prepare_composite()
        prepared.routing_key_indexes = [2, 0]
        batch = BatchStatement()
        batch.add(prepared, (1, 2, 3))

        assert batch.routing_key == self._packed_routing_key(3, 1)
        assert batch._get_routing_statement().routing_key == self._packed_routing_key(1, 3)

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


class BoundStatementServerRoutingKeyTest(unittest.TestCase):
    """
    BoundStatement._server_routing_key() returns the routing key the server
    computes the token from: the values at the partition key bind indices of
    the PREPARED metadata. It reuses routing_key when that's the same key.
    """

    _prepare_composite = staticmethod(BatchStatementTest._prepare_composite)
    _packed_routing_key = staticmethod(BatchStatementTest._packed_routing_key)

    def test_reuses_routing_key_computed_from_the_server_indices(self):
        bound = self._prepare_composite().bind((1, 2, 3))

        server_routing_key = bound._server_routing_key()
        assert server_routing_key is bound.routing_key
        assert server_routing_key == self._packed_routing_key(1, 3)

    def test_single_component_partition_key_is_the_bound_value(self):
        bound = BatchStatementTest._make_keyed_prepared_statement('tbl').bind((5,))

        assert bound._server_routing_key() is bound.values[0]

    def test_routing_key_from_other_indices(self):
        prepared = self._prepare_composite()
        prepared.routing_key_indexes = [2, 0]
        bound = prepared.bind((1, 2, 3))

        assert bound._server_routing_key() == self._packed_routing_key(1, 3)
        assert bound.routing_key == self._packed_routing_key(3, 1)

    def test_explicit_routing_key(self):
        explicit_routing_key = Int32Type.serialize(7, 4)
        bound = BoundStatement(self._prepare_composite(), routing_key=explicit_routing_key).bind((1, 2, 3))

        assert bound._server_routing_key() == self._packed_routing_key(1, 3)
        assert bound.routing_key is explicit_routing_key

    def test_reuses_routing_key_computed_again_after_it_was_deleted(self):
        bound = self._prepare_composite().bind((1, 2, 3))
        computed_routing_key = bound.routing_key
        bound._del_routing_key()

        server_routing_key = bound._server_routing_key()
        assert server_routing_key is bound.routing_key
        assert server_routing_key is not computed_routing_key
        assert server_routing_key == self._packed_routing_key(1, 3)

    def test_bound_again_after_routing_key_was_computed(self):
        bound = self._prepare_composite().bind((1, 2, 3))
        assert bound._server_routing_key() == self._packed_routing_key(1, 3)
        bound.bind((4, 5, 6))

        assert bound._server_routing_key() == self._packed_routing_key(4, 6)

    def test_bound_again_before_routing_key_was_computed(self):
        bound = self._prepare_composite().bind((1, 2, 3))
        bound.bind((4, 5, 6))

        server_routing_key = bound._server_routing_key()
        assert server_routing_key is bound.routing_key
        assert server_routing_key == self._packed_routing_key(4, 6)

    def test_doesnt_reuse_routing_key_of_another_partition(self):
        """
        When routing_key isn't the routing key the server computes the token
        from, the latter is packed from the values instead.
        """
        prepared = self._prepare_composite()
        reordered = self._prepare_composite()
        reordered.routing_key_indexes = [2, 0]
        no_indexes = self._prepare_composite()
        no_indexes.routing_key_indexes = None
        bound_again = prepared.bind((4, 5, 6))
        bound_again.routing_key
        bound_again.bind((1, 2, 3))
        set_after_computed = prepared.bind((1, 2, 3))
        set_after_computed.routing_key
        set_after_computed._set_routing_key(self._packed_routing_key(7, 8))
        cases = {
            'other routing key indexes': reordered.bind((1, 2, 3)),
            'no routing key indexes': no_indexes.bind((1, 2, 3)),
            'explicit routing key': BoundStatement(prepared, routing_key=Int32Type.serialize(7, 4)).bind((1, 2, 3)),
            'bound again after routing key was computed': bound_again,
            'routing key set after it was computed': set_after_computed,
        }
        for case, bound in cases.items():
            with self.subTest(case):
                server_routing_key = bound._server_routing_key()

                assert server_routing_key == self._packed_routing_key(1, 3)
                assert server_routing_key != bound.routing_key

    def test_doesnt_reuse_equal_routing_key_computed_elsewhere(self):
        """
        Whether routing_key is reused depends on where it comes from, not on
        its bytes: a routing key that equals the one the server computes the
        token from, but comes from elsewhere, isn't reused.
        """
        prepared = self._prepare_composite()
        reordered = self._prepare_composite()
        reordered.routing_key_indexes = [2, 0]
        bound_again = prepared.bind((1, 2, 3))
        bound_again.routing_key
        bound_again.bind((1, 5, 3))
        bound_again_with_same_values = prepared.bind((1, 2, 3))
        bound_again_with_same_values.routing_key
        bound_again_with_same_values.bind((1, 2, 3))
        set_after_computed = prepared.bind((1, 2, 3))
        set_after_computed.routing_key
        set_after_computed._set_routing_key(self._packed_routing_key(1, 3))
        cases = {
            # pk1 == pk2, so the reordered routing key has the same bytes.
            'other routing key indexes': (reordered.bind((1, 2, 1)), self._packed_routing_key(1, 1)),
            'explicit routing key': (
                BoundStatement(prepared, routing_key=self._packed_routing_key(1, 3)).bind((1, 2, 3)),
                self._packed_routing_key(1, 3)),
            # Only the column outside the partition key changes.
            'bound again after routing key was computed': (bound_again, self._packed_routing_key(1, 3)),
            'bound again with the same values': (bound_again_with_same_values, self._packed_routing_key(1, 3)),
            'routing key set after it was computed': (set_after_computed, self._packed_routing_key(1, 3)),
        }
        for case, (bound, expected) in cases.items():
            with self.subTest(case):
                server_routing_key = bound._server_routing_key()

                assert server_routing_key == expected
                assert server_routing_key == bound.routing_key
                assert server_routing_key is not bound.routing_key

    def test_none_without_a_partition_key_value(self):
        bound = self._prepare_composite().bind((None, 2, 3))

        assert bound._server_routing_key() is None

    def test_none_with_an_unset_partition_key_value(self):
        prepared = self._prepare_composite()
        # UNSET_VALUE can't be bound at a routing key index.
        prepared.routing_key_indexes = [1]
        bound = prepared.bind((UNSET_VALUE, 2, 3))

        assert bound._server_routing_key() is None

    def test_none_without_server_indices(self):
        prepared = self._prepare_composite()
        prepared._pk_indexes = None

        assert prepared.bind((1, 2, 3))._server_routing_key() is None


class KeyPartsPackedTest(unittest.TestCase):
    """
    _key_parts_packed() builds the packed segments used to assemble a
    composite routing key. It must accept any buffer-protocol object
    (bytes, bytearray, memoryview -- including non-contiguous slices) for
    each key component, since routing key parts may come from encoders/
    serializers that hand back something other than a plain bytes object
    (e.g. a memoryview used to avoid a copy).
    """

    @staticmethod
    def _pack(parts):
        return list(Statement()._key_parts_packed(parts))

    def test_bytes_parts_packed(self):
        # Plain bytes is the common case; the packed format is
        # [2-byte big-endian length][value][0x00] per part.
        result = self._pack([b'abc', b'de'])
        assert result == [b'\x00\x03abc\x00', b'\x00\x02de\x00']

    def test_bytearray_part_packed_matches_bytes(self):
        result = self._pack([bytearray(b'abc')])
        assert result == [b'\x00\x03abc\x00']

    def test_contiguous_memoryview_part_packed_matches_bytes(self):
        result = self._pack([memoryview(b'abc')])
        assert result == [b'\x00\x03abc\x00']

    def test_non_contiguous_memoryview_part_does_not_raise(self):
        # A strided (non-contiguous) memoryview is not directly usable with
        # bytes concatenation (`b'' + memoryview` raises TypeError for
        # non-contiguous views), so it must be normalized first.
        mv = memoryview(b'abcdefgh')[::2]
        assert not mv.contiguous
        assert bytes(mv) == b'aceg'

        result = self._pack([mv])
        assert result == [b'\x00\x04aceg\x00']

    def test_mixed_buffer_types_in_composite_key(self):
        parts = [b'abc', bytearray(b'de'), memoryview(b'fghi')]
        result = self._pack(parts)
        assert result == [b'\x00\x03abc\x00', b'\x00\x02de\x00', b'\x00\x04fghi\x00']


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


class PreparedStatementPkIndexesTest(unittest.TestCase):
    """
    PreparedStatement._pk_indexes keeps the partition key bind indices the
    server sent in the PREPARED metadata, apart from routing_key_indexes,
    which the driver may guess and the user may set.
    """

    @staticmethod
    def _prepare(bind_names, pk_indexes):
        # Cluster metadata of ks.tbl, PRIMARY KEY ((pk1, pk2)), for the driver
        # to guess routing_key_indexes from when the server sent no indices.
        from cassandra import metadata
        table = metadata.TableMetadata('ks', 'tbl')
        table.partition_key = [metadata.ColumnMetadata(table, name, 'int') for name in ('pk1', 'pk2')]
        keyspace = metadata.KeyspaceMetadata('ks', True, 'NetworkTopologyStrategy', {'dc1': '1'})
        keyspace.tables['tbl'] = table
        cluster_metadata = metadata.Metadata()
        cluster_metadata.keyspaces['ks'] = keyspace
        return PreparedStatement.from_message(
            query_id=b"query-id",
            column_metadata=[ColumnMetadata('ks', 'tbl', name, Int32Type) for name in bind_names],
            pk_indexes=pk_indexes,
            cluster_metadata=cluster_metadata,
            query="",
            prepared_keyspace='ks',
            protocol_version=4,
            result_metadata=[],
            result_metadata_id=None,
            is_lwt=False,
        )

    def test_keeps_the_indices_the_server_sent(self):
        prepared = self._prepare(['v', 'pk2', 'pk1'], pk_indexes=[2, 1])

        assert prepared._pk_indexes == [2, 1]
        assert prepared.routing_key_indexes == [2, 1]

    def test_changing_routing_key_indexes_in_place_keeps_them(self):
        prepared = self._prepare(['pk1', 'pk2'], pk_indexes=[0, 1])
        prepared.routing_key_indexes[0] = 1

        assert prepared._pk_indexes == [0, 1]

    def test_none_without_indices_from_the_server(self):
        """
        The driver guesses routing_key_indexes from the names of the bind
        markers, but the server sent no indices.
        """
        for pk_indexes in (None, []):
            with self.subTest(pk_indexes=pk_indexes):
                prepared = self._prepare(['pk1', 'pk2'], pk_indexes=pk_indexes)

                assert prepared.routing_key_indexes == [0, 1]
                assert prepared._pk_indexes is None
