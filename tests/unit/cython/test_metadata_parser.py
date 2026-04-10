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

"""
Tests for cassandra.metadata_parser, the Cython BytesIOReader-based
implementation of ResultMessage.recv_results_metadata.
"""

import io
import unittest

from tests.unit.cython.utils import cythontest

try:
    from cassandra.metadata_parser import make_recv_results_metadata
    from cassandra.bytesio import BytesIOReader
    from cassandra.protocol import ResultMessage, CUSTOM_TYPE, RESULT_KIND_ROWS, NotSupportedError
    from cassandra.cqltypes import (ListType, SetType, MapType, TupleType,
                                     UserType, lookup_casstype)
    from cassandra.marshal import int32_pack, uint16_pack
except ImportError:
    make_recv_results_metadata = None


# Type code for UserType (UDT), per the CQL binary protocol spec (0x0030).
_USER_TYPE_CODE = 0x0030


def _string(s):
    b = s.encode('utf8')
    return uint16_pack(len(b)) + b


class MetadataParserTest(unittest.TestCase):
    """Test the Cython metadata parser (cassandra.metadata_parser)."""

    def _make_recv(self, not_supported_error=None):
        return make_recv_results_metadata(
            ResultMessage.type_codes, CUSTOM_TYPE,
            ListType, SetType, MapType, TupleType, UserType, lookup_casstype,
            not_supported_error if not_supported_error is not None else NotSupportedError)

    @cythontest
    def test_negative_paging_state_length_rejected(self):
        """
        A malformed/corrupted frame carrying a negative length for the
        paging_state [bytes] value must raise a clean, catchable ValueError
        instead of an opaque SystemError from PyBytes_FromStringAndSize (or
        worse -- undefined behavior from slicing a char* with a negative
        bound). See read_binary_longstring_br() in metadata_parser.pyx.
        """
        recv = self._make_recv()
        flags = 0x0002 | 0x0004  # _HAS_MORE_PAGES_FLAG | _NO_METADATA_FLAG
        for bad_len in (-1, -2, -1000, -(2 ** 31)):
            buf = int32_pack(flags) + int32_pack(0) + int32_pack(bad_len)
            reader = BytesIOReader(buf)
            msg = ResultMessage(RESULT_KIND_ROWS)
            with self.assertRaises(ValueError):
                recv(msg, reader, {})

    @cythontest
    def test_valid_paging_state_roundtrip(self):
        recv = self._make_recv()
        flags = 0x0002 | 0x0004  # _HAS_MORE_PAGES_FLAG | _NO_METADATA_FLAG
        buf = int32_pack(flags) + int32_pack(0) + int32_pack(3) + b'abc'
        reader = BytesIOReader(buf)
        msg = ResultMessage(RESULT_KIND_ROWS)
        recv(msg, reader, {})
        self.assertEqual(msg.paging_state, b'abc')

    @cythontest
    def test_no_metadata_flag_skips_column_parsing(self):
        recv = self._make_recv()
        flags = 0x0004  # _NO_METADATA_FLAG
        buf = int32_pack(flags) + int32_pack(0)
        reader = BytesIOReader(buf)
        msg = ResultMessage(RESULT_KIND_ROWS)
        recv(msg, reader, {})
        self.assertIsNone(msg.column_metadata)

    @cythontest
    def test_column_metadata_matches_pure_python(self):
        """
        The Cython recv_results_metadata (BytesIOReader-based) must produce
        the same column_metadata as the pure-Python
        ResultMessage.recv_results_metadata for identical wire bytes, for
        both the global-tables-spec and the per-column keyspace/table-name
        wire layouts.
        """
        for global_tables_spec in (True, False):
            flags = 0x0001 if global_tables_spec else 0x0000
            body = int32_pack(flags) + int32_pack(2)
            if global_tables_spec:
                body += _string('ks') + _string('table')
                body += _string('col1') + uint16_pack(0x0009)
                body += _string('col2') + uint16_pack(0x0009)
            else:
                body += _string('ks') + _string('table') + _string('col1') + uint16_pack(0x0009)
                body += _string('ks') + _string('table') + _string('col2') + uint16_pack(0x0009)

            recv = self._make_recv()
            reader = BytesIOReader(body)
            cy_msg = ResultMessage(RESULT_KIND_ROWS)
            recv(cy_msg, reader, {})

            py_msg = ResultMessage(RESULT_KIND_ROWS)
            py_msg.recv_results_metadata(io.BytesIO(body), {})

            self.assertEqual(cy_msg.column_metadata, py_msg.column_metadata)

    @cythontest
    def test_unknown_type_code_raises_injected_exception(self):
        """
        _read_type_br() must raise the NotSupportedError class *injected*
        into make_recv_results_metadata() by the caller, not import it from
        cassandra.protocol at call time. Passing in a stand-in exception
        class (instead of the real cassandra.protocol.NotSupportedError)
        and asserting *that* class is what gets raised proves the raise
        goes through the injected closure variable rather than a hidden
        "from cassandra.protocol import NotSupportedError" inside the hot
        parsing path -- which would defeat the whole point of injecting
        CUSTOM_TYPE/ListType/etc. instead of importing them, as documented
        in make_recv_results_metadata()'s own docstring.
        """
        class StandInNotSupportedError(Exception):
            pass

        recv = self._make_recv(not_supported_error=StandInNotSupportedError)
        flags = 0x0001  # _FLAGS_GLOBAL_TABLES_SPEC
        unknown_optid = 0x00FF  # not a key in ResultMessage.type_codes
        body = (int32_pack(flags) + int32_pack(1)
                + _string('ks') + _string('table')
                + _string('col1') + uint16_pack(unknown_optid))
        reader = BytesIOReader(body)
        msg = ResultMessage(RESULT_KIND_ROWS)
        with self.assertRaises(StandInNotSupportedError):
            recv(msg, reader, {})

    @cythontest
    def test_zero_field_user_type_matches_pure_python(self):
        """
        A UserType (UDT) with num_fields == 0 is not something a real
        CREATE TYPE can ever produce (a UDT always has at least one field),
        so it only ever shows up on the wire as malformed/corrupted data.
        ResultMessage.read_type() (the pure-Python path) already rejects it
        -- zip(*()) unpacked into two variables raises ValueError -- so the
        Cython path must raise the same way instead of silently returning
        an empty-fields UDT, to avoid a behavioral divergence between the
        two implementations for the same malformed input.
        """
        user_type_bytes = (uint16_pack(_USER_TYPE_CODE)
                            + _string('ks') + _string('mytype') + uint16_pack(0))

        with self.assertRaises(ValueError):
            ResultMessage.read_type(io.BytesIO(user_type_bytes), {})

        flags = 0x0001  # _FLAGS_GLOBAL_TABLES_SPEC
        body = (int32_pack(flags) + int32_pack(1)
                + _string('ks') + _string('table')
                + _string('col1') + user_type_bytes)
        recv = self._make_recv()
        reader = BytesIOReader(body)
        msg = ResultMessage(RESULT_KIND_ROWS)
        with self.assertRaises(ValueError):
            recv(msg, reader, {})
