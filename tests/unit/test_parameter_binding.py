# Copyright ScyllaDB, Inc.
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
import struct
import pytest

from cassandra.encoder import Encoder
from cassandra.protocol import ColumnMetadata
from cassandra.query import (bind_params, ValueSequence, PreparedStatement,
                             BoundStatement, UNSET_VALUE)
from cassandra.cqltypes import Int32Type
from cassandra.util import OrderedDict

from tests.util import assertListEqual


class ParamBindingTest(unittest.TestCase):

    def test_bind_sequence(self):
        result = bind_params("%s %s %s", (1, "a", 2.0), Encoder())
        assert result == "1 'a' 2.0"

    def test_bind_map(self):
        result = bind_params("%(a)s %(b)s %(c)s", dict(a=1, b="a", c=2.0), Encoder())
        assert result == "1 'a' 2.0"

    def test_sequence_param(self):
        result = bind_params("%s", (ValueSequence((1, "a", 2.0)),), Encoder())
        assert result == "(1, 'a', 2.0)"

    def test_generator_param(self):
        result = bind_params("%s", ((i for i in range(3)),), Encoder())
        assert result == "[0, 1, 2]"

    def test_none_param(self):
        result = bind_params("%s", (None,), Encoder())
        assert result == "NULL"

    def test_list_collection(self):
        result = bind_params("%s", (['a', 'b', 'c'],), Encoder())
        assert result == "['a', 'b', 'c']"

    def test_set_collection(self):
        result = bind_params("%s", (set(['a', 'b']),), Encoder())
        assert result in ("{'a', 'b'}", "{'b', 'a'}")

    def test_map_collection(self):
        vals = OrderedDict()
        vals['a'] = 'a'
        vals['b'] = 'b'
        vals['c'] = 'c'
        result = bind_params("%s", (vals,), Encoder())
        assert result == "{'a': 'a', 'b': 'b', 'c': 'c'}"

    def test_quote_escaping(self):
        result = bind_params("%s", ("""'ef''ef"ef""ef'""",), Encoder())
        assert result == """'''ef''''ef"ef""ef'''"""

    def test_float_precision(self):
        f = 3.4028234663852886e+38
        assert float(bind_params("%s", (f,), Encoder())) == f

class BoundStatementTestV3(unittest.TestCase):

    protocol_version = 3

    @classmethod
    def setUpClass(cls):
        column_metadata = [ColumnMetadata('keyspace', 'cf', 'rk0', Int32Type),
                           ColumnMetadata('keyspace', 'cf', 'rk1', Int32Type),
                           ColumnMetadata('keyspace', 'cf', 'ck0', Int32Type),
                           ColumnMetadata('keyspace', 'cf', 'v0', Int32Type)]
        cls.prepared = PreparedStatement(column_metadata=column_metadata,
                                         query_id=None,
                                         routing_key_indexes=[1, 0],
                                         query=None,
                                         keyspace='keyspace',
                                         protocol_version=cls.protocol_version, result_metadata=None,
                                         result_metadata_id=None)
        cls.bound = BoundStatement(prepared_statement=cls.prepared)

    def test_invalid_argument_type(self):
        values = (0, 0, 0, 'string not int')
        with pytest.raises(TypeError) as exc:
            self.bound.bind(values)
        e = exc.value
        assert 'v0' in str(e)
        assert 'Int32Type' in str(e)
        assert 'str' in str(e)

        values = (['1', '2'], 0, 0, 0)

        with pytest.raises(TypeError) as exc:
            self.bound.bind(values)
        e = exc.value
        assert 'rk0' in str(e)
        assert 'Int32Type' in str(e)
        assert 'list' in str(e)

    def test_inherit_fetch_size(self):
        keyspace = 'keyspace1'
        column_family = 'cf1'

        column_metadata = [
            ColumnMetadata(keyspace, column_family, 'foo1', Int32Type),
            ColumnMetadata(keyspace, column_family, 'foo2', Int32Type)
        ]

        prepared_statement = PreparedStatement(column_metadata=column_metadata,
                                               query_id=None,
                                               routing_key_indexes=[],
                                               query=None,
                                               keyspace=keyspace,
                                               protocol_version=self.protocol_version,
                                               result_metadata=None,
                                               result_metadata_id=None)
        prepared_statement.fetch_size = 1234
        bound_statement = BoundStatement(prepared_statement=prepared_statement)
        assert 1234 == bound_statement.fetch_size

    def test_too_few_parameters_for_routing_key(self):
        with pytest.raises(ValueError):
            self.prepared.bind((1,))

        bound = self.prepared.bind((1, 2))
        assert bound.keyspace == 'keyspace'

    def test_dict_missing_routing_key(self):
        with pytest.raises(KeyError):
            self.bound.bind({'rk0': 0, 'ck0': 0, 'v0': 0})
        with pytest.raises(KeyError):
            self.bound.bind({'rk1': 0, 'ck0': 0, 'v0': 0})

    def test_missing_value(self):
        with pytest.raises(KeyError):
            self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0})

    def test_extra_value(self):
        self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0, 'v0': 0, 'should_not_be_here': 123})  # okay to have extra keys in dict
        assert self.bound.values == [b'\x00' * 4] * 4  # four encoded zeros
        with pytest.raises(ValueError):
            self.bound.bind((0, 0, 0, 0, 123))

    def test_values_none(self):
        # should have values
        with pytest.raises(ValueError):
            self.bound.bind(None)

        # prepared statement with no values
        prepared_statement = PreparedStatement(column_metadata=[],
                                               query_id=None,
                                               routing_key_indexes=[],
                                               query=None,
                                               keyspace='whatever',
                                               protocol_version=self.protocol_version,
                                               result_metadata=None,
                                               result_metadata_id=None)
        bound = prepared_statement.bind(None)
        assertListEqual(bound.values, [])

    def test_bind_none(self):
        self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0, 'v0': None})
        assert self.bound.values[-1] == None

        old_values = self.bound.values
        self.bound.bind((0, 0, 0, None))
        assert self.bound.values is not old_values
        assert self.bound.values[-1] == None

    def test_unset_value(self):
        with pytest.raises(ValueError):
            self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0, 'v0': UNSET_VALUE})
        with pytest.raises(ValueError):
            self.bound.bind((0, 0, 0, UNSET_VALUE))


class BoundStatementTestV4(BoundStatementTestV3):
    protocol_version = 4

    def test_dict_missing_routing_key(self):
        # in v4 it implicitly binds UNSET_VALUE for missing items,
        # UNSET_VALUE is ValueError for routing keys
        with pytest.raises(ValueError):
            self.bound.bind({'rk0': 0, 'ck0': 0, 'v0': 0})
        with pytest.raises(ValueError):
            self.bound.bind({'rk1': 0, 'ck0': 0, 'v0': 0})

    def test_missing_value(self):
        # in v4 missing values are UNSET_VALUE
        self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0})
        assert self.bound.values[-1] == UNSET_VALUE

        old_values = self.bound.values
        self.bound.bind((0, 0, 0))
        assert self.bound.values is not old_values
        assert self.bound.values[-1] == UNSET_VALUE

    def test_unset_value(self):
        self.bound.bind({'rk0': 0, 'rk1': 0, 'ck0': 0, 'v0': UNSET_VALUE})
        assert self.bound.values[-1] == UNSET_VALUE

        self.bound.bind((0, 0, 0, UNSET_VALUE))
        assert self.bound.values[-1] == UNSET_VALUE


class BoundStatementTestV5(BoundStatementTestV4):
    protocol_version = 5


class StubSerializer:
    """Stub that mimics a Cython Serializer object for testing the fast path."""

    def __init__(self, cqltype):
        self.cqltype = cqltype

    def serialize(self, value, protocol_version):
        return self.cqltype.serialize(value, protocol_version)


class OverflowSerializer:
    """Stub that raises struct.error, mimicking Cython int32 range check."""

    def __init__(self, cqltype):
        self.cqltype = cqltype

    def serialize(self, value, protocol_version):
        raise struct.error("'i' format requires -2147483648 <= number <= 2147483647")


class CythonBindPathTest(unittest.TestCase):
    """Tests for the Cython serializer fast path in BoundStatement.bind().

    These tests inject stub serializers via the PreparedStatement's cached
    _cached_serializers attribute to exercise the Cython bind branch without
    requiring compiled Cython.
    """

    protocol_version = 4

    def _make_prepared(self, column_metadata, serializers=None):
        """Create a PreparedStatement and inject serializers into its cache."""
        prepared = PreparedStatement(column_metadata=column_metadata,
                                     query_id=None,
                                     routing_key_indexes=[],
                                     query=None,
                                     keyspace='keyspace',
                                     protocol_version=self.protocol_version,
                                     result_metadata=None,
                                     result_metadata_id=None)
        # Inject directly into the cache attribute used by the _serializers
        # property, bypassing the lazy initialization.
        prepared._cached_serializers = serializers
        return prepared

    def test_cython_path_normal_serialization(self):
        """Cython fast path produces the same result as the plain Python path."""
        column_metadata = [ColumnMetadata('keyspace', 'cf', 'c0', Int32Type),
                           ColumnMetadata('keyspace', 'cf', 'c1', Int32Type)]
        serializers = [StubSerializer(Int32Type), StubSerializer(Int32Type)]
        prepared = self._make_prepared(column_metadata, serializers)

        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((42, -1))
        assert bound.values == [Int32Type.serialize(42, self.protocol_version),
                                Int32Type.serialize(-1, self.protocol_version)]

    def test_cython_path_none_value(self):
        """None values pass through the Cython path without serialization."""
        column_metadata = [ColumnMetadata('keyspace', 'cf', 'c0', Int32Type)]
        serializers = [StubSerializer(Int32Type)]
        prepared = self._make_prepared(column_metadata, serializers)

        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((None,))
        assert bound.values == [None]

    def test_cython_path_unset_value(self):
        """UNSET_VALUE is handled correctly in the Cython fast path (v4+)."""
        column_metadata = [ColumnMetadata('keyspace', 'cf', 'c0', Int32Type),
                           ColumnMetadata('keyspace', 'cf', 'c1', Int32Type)]
        serializers = [StubSerializer(Int32Type), StubSerializer(Int32Type)]
        prepared = self._make_prepared(column_metadata, serializers)

        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((42, UNSET_VALUE))
        assert bound.values[0] == Int32Type.serialize(42, self.protocol_version)
        assert bound.values[1] == UNSET_VALUE

    def test_cython_path_overflow_error_wrapped(self):
        """struct.error from Cython int32 range check is caught and wrapped with column context."""
        column_metadata = [ColumnMetadata('keyspace', 'cf', 'v0', Int32Type)]
        serializers = [OverflowSerializer(Int32Type)]
        prepared = self._make_prepared(column_metadata, serializers)

        bound = BoundStatement(prepared_statement=prepared)
        with pytest.raises(TypeError) as exc:
            bound.bind((2**31,))
        msg = str(exc.value)
        assert 'v0' in msg
        assert 'Int32Type' in msg
        assert 'int' in msg

    def test_cython_path_type_error_wrapped(self):
        """TypeError from serializer is caught and wrapped with column context."""
        column_metadata = [ColumnMetadata('keyspace', 'cf', 'v0', Int32Type)]
        serializers = [StubSerializer(Int32Type)]
        prepared = self._make_prepared(column_metadata, serializers)

        bound = BoundStatement(prepared_statement=prepared)
        with pytest.raises(TypeError) as exc:
            bound.bind(('not_an_int',))
        msg = str(exc.value)
        assert 'v0' in msg
        assert 'Int32Type' in msg

    def test_plain_path_overflow_error_wrapped(self):
        """Out-of-range int in the plain Python path raises OverflowError (from
        the Cython serializer) or struct.error (from the pure-Python fallback)
        and is wrapped with column context."""
        column_metadata = [ColumnMetadata('keyspace', 'cf', 'v0', Int32Type)]
        # Force the plain Python path (no Cython serializers)
        prepared = self._make_prepared(column_metadata, serializers=None)

        bound = BoundStatement(prepared_statement=prepared)
        with pytest.raises(TypeError) as exc:
            bound.bind((2**31,))
        msg = str(exc.value)
        assert 'v0' in msg
        assert 'Int32Type' in msg


class UnsetValueBindingTest(unittest.TestCase):
    """Tests for UNSET_VALUE handling in all bind paths.

    These specifically test UNSET_VALUE in non-trailing positions to catch
    index-management bugs in the pre-allocated values list.
    """

    protocol_version = 4

    def _make_prepared(self, column_metadata, serializers=None, routing_key_indexes=None):
        prepared = PreparedStatement(column_metadata=column_metadata,
                                     query_id=None,
                                     routing_key_indexes=routing_key_indexes or [],
                                     query=None,
                                     keyspace='keyspace',
                                     protocol_version=self.protocol_version,
                                     result_metadata=None,
                                     result_metadata_id=None)
        prepared._cached_serializers = serializers
        return prepared

    def _three_column_metadata(self):
        return [ColumnMetadata('keyspace', 'cf', 'c0', Int32Type),
                ColumnMetadata('keyspace', 'cf', 'c1', Int32Type),
                ColumnMetadata('keyspace', 'cf', 'c2', Int32Type)]

    # --- Plain Python path (no serializers) ---

    def test_plain_unset_mid_list(self):
        """UNSET_VALUE in the middle of a value list does not corrupt indices."""
        col_meta = self._three_column_metadata()
        prepared = self._make_prepared(col_meta, serializers=None)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((0, UNSET_VALUE, 2))
        assert bound.values == [
            Int32Type.serialize(0, self.protocol_version),
            UNSET_VALUE,
            Int32Type.serialize(2, self.protocol_version),
        ]

    def test_plain_unset_first(self):
        """UNSET_VALUE as the first value does not corrupt indices."""
        col_meta = self._three_column_metadata()
        prepared = self._make_prepared(col_meta, serializers=None)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((UNSET_VALUE, 1, 2))
        assert bound.values == [
            UNSET_VALUE,
            Int32Type.serialize(1, self.protocol_version),
            Int32Type.serialize(2, self.protocol_version),
        ]

    def test_plain_all_unset(self):
        """All values are UNSET_VALUE."""
        col_meta = self._three_column_metadata()
        prepared = self._make_prepared(col_meta, serializers=None)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((UNSET_VALUE, UNSET_VALUE, UNSET_VALUE))
        assert bound.values == [UNSET_VALUE, UNSET_VALUE, UNSET_VALUE]

    def test_plain_unset_trailing(self):
        """UNSET_VALUE as the last explicit value."""
        col_meta = self._three_column_metadata()
        prepared = self._make_prepared(col_meta, serializers=None)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((0, 1, UNSET_VALUE))
        assert bound.values == [
            Int32Type.serialize(0, self.protocol_version),
            Int32Type.serialize(1, self.protocol_version),
            UNSET_VALUE,
        ]

    def test_plain_implicit_unset_fill(self):
        """Fewer values than columns fills remaining with implicit UNSET_VALUE."""
        col_meta = self._three_column_metadata()
        prepared = self._make_prepared(col_meta, serializers=None)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((0,))
        assert bound.values == [
            Int32Type.serialize(0, self.protocol_version),
            UNSET_VALUE,
            UNSET_VALUE,
        ]

    def test_plain_mixed_none_and_unset(self):
        """Mix of None and UNSET_VALUE in the same bind."""
        col_meta = self._three_column_metadata()
        prepared = self._make_prepared(col_meta, serializers=None)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((None, UNSET_VALUE, 2))
        assert bound.values == [
            None,
            UNSET_VALUE,
            Int32Type.serialize(2, self.protocol_version),
        ]

    # --- Cython serializer path (with stub serializers) ---

    def test_cython_unset_mid_list(self):
        """UNSET_VALUE in the middle with Cython serializers does not corrupt indices."""
        col_meta = self._three_column_metadata()
        serializers = [StubSerializer(Int32Type)] * 3
        prepared = self._make_prepared(col_meta, serializers=serializers)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((0, UNSET_VALUE, 2))
        assert bound.values == [
            Int32Type.serialize(0, self.protocol_version),
            UNSET_VALUE,
            Int32Type.serialize(2, self.protocol_version),
        ]

    def test_cython_unset_first(self):
        """UNSET_VALUE as the first value with Cython serializers."""
        col_meta = self._three_column_metadata()
        serializers = [StubSerializer(Int32Type)] * 3
        prepared = self._make_prepared(col_meta, serializers=serializers)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((UNSET_VALUE, 1, 2))
        assert bound.values == [
            UNSET_VALUE,
            Int32Type.serialize(1, self.protocol_version),
            Int32Type.serialize(2, self.protocol_version),
        ]

    def test_cython_all_unset(self):
        """All values are UNSET_VALUE with Cython serializers."""
        col_meta = self._three_column_metadata()
        serializers = [StubSerializer(Int32Type)] * 3
        prepared = self._make_prepared(col_meta, serializers=serializers)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((UNSET_VALUE, UNSET_VALUE, UNSET_VALUE))
        assert bound.values == [UNSET_VALUE, UNSET_VALUE, UNSET_VALUE]

    def test_cython_mixed_none_and_unset(self):
        """Mix of None and UNSET_VALUE with Cython serializers."""
        col_meta = self._three_column_metadata()
        serializers = [StubSerializer(Int32Type)] * 3
        prepared = self._make_prepared(col_meta, serializers=serializers)
        bound = BoundStatement(prepared_statement=prepared)
        bound.bind((None, UNSET_VALUE, 2))
        assert bound.values == [
            None,
            UNSET_VALUE,
            Int32Type.serialize(2, self.protocol_version),
        ]

