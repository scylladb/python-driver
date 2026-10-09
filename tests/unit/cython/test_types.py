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

import io
import unittest

import pytest

from cassandra.cqltypes import Int32Type, ListType, MapType, SetType, int32_pack
from cassandra.cython_deps import HAVE_CYTHON
from cassandra.protocol import ProtocolHandler, ResultMessage, RESULT_KIND_ROWS
from tests.unit.cython.utils import cyimport, cythontest

types_testhelper = cyimport('tests.unit.cython.types_testhelper')


class TypesTest(unittest.TestCase):

    @cythontest
    def test_datetype(self):
        types_testhelper.test_datetype()

    @cythontest
    def test_date_side_by_side(self):
        types_testhelper.test_date_side_by_side()


def decode_cython_collection(collection, encoded_collection):
    assert HAVE_CYTHON, "VERIFY_CYTHON requires built Cython extensions"
    rows = io.BytesIO(
        int32_pack(ResultMessage._NO_METADATA_FLAG)
        + int32_pack(1)  # one column
        + int32_pack(1)  # one row
        + int32_pack(len(encoded_collection)) + encoded_collection
    )
    result_type = ProtocolHandler.message_types_by_opcode[ResultMessage.opcode]
    result = result_type(kind=RESULT_KIND_ROWS)
    result.recv_results_rows(rows, 4, {}, [("ks", "t", "values", collection)], None)
    return result.parsed_rows[0][0]


@cythontest
@pytest.mark.parametrize("collection_type", [ListType, SetType])
@pytest.mark.parametrize("items", [[None, 42], [7, None, 42]])
def test_cython_collection_with_null_element(collection_type, items):
    collection = collection_type.apply_parameters([Int32Type])
    encoded_items = b"".join(
        int32_pack(-1) if item is None else int32_pack(4) + int32_pack(item)
        for item in items
    )
    encoded_collection = int32_pack(len(items)) + encoded_items
    decoded = decode_cython_collection(collection, encoded_collection)
    assert decoded == collection.from_binary(encoded_collection, 4)
    if collection_type is SetType:
        assert len(decoded) == len(items)
        assert set(decoded) == set(items)
    else:
        assert decoded == items


@cythontest
@pytest.mark.parametrize("entries", [
    [(None, 42)],
    [(42, None)],
    [(None, 1), (2, None), (3, 4)],
])
def test_cython_map_with_null_entry(entries):
    collection = MapType.apply_parameters([Int32Type, Int32Type])
    encoded_entries = b"".join(
        (int32_pack(-1) if key is None else int32_pack(4) + int32_pack(key))
        + (int32_pack(-1) if value is None else int32_pack(4) + int32_pack(value))
        for key, value in entries
    )
    encoded_collection = int32_pack(len(entries)) + encoded_entries
    decoded = decode_cython_collection(collection, encoded_collection)
    assert decoded == collection.from_binary(encoded_collection, 4)
    assert decoded._items == entries
    assert decoded._index == {
        None if key is None else int32_pack(key): index
        for index, (key, _) in enumerate(entries)
    }
