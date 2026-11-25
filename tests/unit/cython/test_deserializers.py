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

import io
import unittest

from cassandra import cqltypes
from cassandra.cqltypes import int32_pack
from cassandra.protocol import ProtocolHandler, ResultMessage, RESULT_KIND_ROWS
from tests.unit.cython.utils import cythontest

try:
    from cassandra import deserializers
except ImportError:
    deserializers = None

BlobList = cqltypes.ListType.apply_parameters([cqltypes.BytesType])
IntTextMap = cqltypes.MapType.apply_parameters([cqltypes.Int32Type, cqltypes.UTF8Type])
IntBlobTuple = cqltypes.TupleType.apply_parameters([cqltypes.Int32Type, cqltypes.BytesType])


def _value(raw):
    return int32_pack(-1) if raw is None else int32_pack(len(raw)) + raw


def decode_row(coltypes, encoded_values):
    """Decode one row through the Cython result-set path."""
    body = (int32_pack(ResultMessage._NO_METADATA_FLAG)
            + int32_pack(len(coltypes))
            + int32_pack(1)
            + b"".join(_value(v) for v in encoded_values))
    result = ProtocolHandler.message_types_by_opcode[ResultMessage.opcode](kind=RESULT_KIND_ROWS)
    metadata = [("ks", "t", "c%d" % i, ct) for i, ct in enumerate(coltypes)]
    result.recv_results_rows(io.BytesIO(body), 4, {}, metadata, None)
    return result.parsed_rows[0]


def encode_blob_list(items):
    return int32_pack(len(items)) + b"".join(_value(i) for i in items)


class FindDeserializerTest(unittest.TestCase):

    @cythontest
    def test_returns_fresh_instance_of_matching_class(self):
        d1 = deserializers.find_deserializer(cqltypes.Int32Type)
        d2 = deserializers.find_deserializer(cqltypes.Int32Type)
        self.assertIsNot(d1, d2)
        self.assertIs(type(d1), deserializers.DesInt32Type)
        self.assertIs(type(d2), deserializers.DesInt32Type)

    @cythontest
    def test_parameterized_types(self):
        self.assertIs(type(deserializers.find_deserializer(BlobList)), deserializers.DesListType)
        self.assertIs(type(deserializers.find_deserializer(IntTextMap)), deserializers.DesMapType)
        self.assertIs(type(deserializers.find_deserializer(IntBlobTuple)), deserializers.DesTupleType)


class MakeDeserializersTest(unittest.TestCase):

    TYPES = [cqltypes.Int32Type, cqltypes.UTF8Type, BlobList]

    @cythontest
    def test_returns_fresh_immutable_tuple(self):
        r1 = deserializers.make_deserializers(self.TYPES)
        r2 = deserializers.make_deserializers(self.TYPES)
        self.assertIs(type(r1), tuple)
        self.assertEqual(len(r1), 3)
        self.assertIsNot(r1, r2)
        for a, b in zip(r1, r2):
            self.assertIsNot(a, b)
        self.assertEqual([type(d) for d in r1],
                         [deserializers.DesInt32Type, deserializers.DesUTF8Type,
                          deserializers.DesListType])
        with self.assertRaises(TypeError):
            r1[0] = r2[0]

    @cythontest
    def test_consumes_iterable_once(self):
        consumed = []

        def gen():
            for ct in self.TYPES:
                consumed.append(ct)
                yield ct

        result = deserializers.make_deserializers(gen())
        self.assertEqual(consumed, self.TYPES)
        self.assertEqual([type(d) for d in result],
                         [deserializers.DesInt32Type, deserializers.DesUTF8Type,
                          deserializers.DesListType])

    @cythontest
    def test_empty(self):
        self.assertEqual(deserializers.make_deserializers([]), ())

    @cythontest
    def test_obj_array_still_available(self):
        arr = deserializers.obj_array([1, "a"])
        self.assertEqual((arr[0], arr[1]), (1, "a"))


class DecodeTest(unittest.TestCase):

    @cythontest
    def test_decodes_mixed_row(self):
        coltypes = [cqltypes.Int32Type, cqltypes.UTF8Type, cqltypes.BytesType,
                    BlobList, IntTextMap, IntBlobTuple]
        values = [
            int32_pack(7),
            "héllo".encode("utf8"),
            b"\x00\x01",
            encode_blob_list([b"a", None, b"bc"]),
            int32_pack(1) + _value(int32_pack(5)) + _value(b"five"),
            _value(int32_pack(9)) + _value(b"\xff"),
            ]
        row = decode_row(coltypes, values)
        self.assertEqual(row[:4], (7, "héllo", b"\x00\x01", [b"a", None, b"bc"]))
        self.assertEqual(dict(row[4]), {5: "five"})
        self.assertEqual(row[5], (9, b"\xff"))
        self.assertEqual(decode_row([cqltypes.Int32Type], [None]), (None,))


class DesBytesTypeReplacementTest(unittest.TestCase):
    """cqlsh replaces DesBytesType at runtime; later lookups must honour it."""

    def setUp(self):
        if deserializers is not None:
            self.original = deserializers.DesBytesType
            self.addCleanup(setattr, deserializers, "DesBytesType", self.original)

    @cythontest
    def test_lookups_after_replacement_use_new_class(self):
        coltypes = [cqltypes.BytesType, BlobList]
        values = [b"xy", encode_blob_list([b"z"])]

        before = deserializers.find_deserializer(cqltypes.BytesType)
        nested_before = deserializers.make_deserializers([BlobList])
        self.assertIs(type(before), self.original)
        row = decode_row(coltypes, values)
        self.assertIs(type(row[0]), bytes)
        self.assertIs(type(row[1][0]), bytes)

        deserializers.DesBytesType = deserializers.DesBytesTypeByteArray

        self.assertIs(type(deserializers.find_deserializer(cqltypes.BytesType)),
                      deserializers.DesBytesTypeByteArray)
        row = decode_row(coltypes, values)
        self.assertEqual(row, (bytearray(b"xy"), [bytearray(b"z")]))
        self.assertIs(type(row[0]), bytearray)
        self.assertIs(type(row[1][0]), bytearray)

        # Descriptors built earlier keep their original choice.
        self.assertIs(type(before), self.original)
        self.assertIs(type(nested_before[0]), deserializers.DesListType)
