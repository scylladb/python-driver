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
Correctness tests for the Cython UTF-8 and ASCII deserializers.

These verify that the optimized PyUnicode_DecodeUTF8/DecodeASCII code path
in cassandra/deserializers.pyx produces correct results for edge cases.
"""

import struct
import unittest

from tests.unit.cython.utils import cythontest

from cassandra.cython_deps import HAVE_CYTHON

if HAVE_CYTHON:
    from cassandra.obj_parser import ListParser
    from cassandra.bytesio import BytesIOReader
    from cassandra.parsing import ParseDesc
    from cassandra.deserializers import make_deserializers
    from cassandra.cqltypes import UTF8Type, AsciiType
    from cassandra.policies import ColDesc

from cassandra import DriverException


def _build_text_rows_buffer(num_rows, num_cols, text_data):
    """Build a binary buffer representing num_rows x num_cols of text data.

    Format: [int32 row_count] [row1] [row2] ...
    Each row: [cell1] [cell2] ...
    Each cell: [int32 length] [data bytes]
    """
    parts = [struct.pack(">i", num_rows)]
    cell = struct.pack(">i", len(text_data)) + text_data
    row = cell * num_cols
    parts.append(row * num_rows)
    return b"".join(parts)


def _make_text_desc(num_cols, protocol_version=4):
    """Create a ParseDesc for num_cols text columns."""
    coltypes = [UTF8Type] * num_cols
    colnames = [f"col{i}" for i in range(num_cols)]
    coldescs = [ColDesc("ks", "tbl", f"col{i}") for i in range(num_cols)]
    desers = make_deserializers(coltypes)
    return ParseDesc(colnames, coltypes, None, coldescs, desers, protocol_version)


def _make_ascii_desc(num_cols, protocol_version=4):
    """Create a ParseDesc for num_cols ASCII columns."""
    coltypes = [AsciiType] * num_cols
    colnames = [f"col{i}" for i in range(num_cols)]
    coldescs = [ColDesc("ks", "tbl", f"col{i}") for i in range(num_cols)]
    desers = make_deserializers(coltypes)
    return ParseDesc(colnames, coltypes, None, coldescs, desers, protocol_version)


class TestCythonDeserializerCorrectness(unittest.TestCase):
    """Verify that the optimized Cython decode produces correct results."""

    @cythontest
    def test_utf8_empty_string(self):
        """Empty string should return empty string."""
        buf = _build_text_rows_buffer(1, 1, b"")
        desc = _make_text_desc(1)
        parser = ListParser()
        reader = BytesIOReader(buf)
        rows = parser.parse_rows(reader, desc)
        self.assertEqual(rows[0][0], "")

    @cythontest
    def test_utf8_ascii_only(self):
        """Pure ASCII content."""
        text = b"Hello, World! 12345"
        buf = _build_text_rows_buffer(1, 1, text)
        desc = _make_text_desc(1)
        parser = ListParser()
        reader = BytesIOReader(buf)
        rows = parser.parse_rows(reader, desc)
        self.assertEqual(rows[0][0], "Hello, World! 12345")

    @cythontest
    def test_utf8_multibyte(self):
        """Multibyte UTF-8 characters."""
        text = "Héllo wörld! こんにちは 🌍".encode("utf-8")
        buf = _build_text_rows_buffer(1, 1, text)
        desc = _make_text_desc(1)
        parser = ListParser()
        reader = BytesIOReader(buf)
        rows = parser.parse_rows(reader, desc)
        self.assertEqual(rows[0][0], "Héllo wörld! こんにちは 🌍")

    @cythontest
    def test_utf8_long_string(self):
        """Long string (10KB)."""
        text = ("x" * 10000).encode("utf-8")
        buf = _build_text_rows_buffer(1, 1, text)
        desc = _make_text_desc(1)
        parser = ListParser()
        reader = BytesIOReader(buf)
        rows = parser.parse_rows(reader, desc)
        self.assertEqual(rows[0][0], "x" * 10000)

    @cythontest
    def test_ascii_basic(self):
        """Basic ASCII decode."""
        text = b"Simple ASCII text 12345 !@#"
        buf = _build_text_rows_buffer(1, 1, text)
        desc = _make_ascii_desc(1)
        parser = ListParser()
        reader = BytesIOReader(buf)
        rows = parser.parse_rows(reader, desc)
        self.assertEqual(rows[0][0], "Simple ASCII text 12345 !@#")

    @cythontest
    def test_utf8_null_value(self):
        """NULL value (negative length) should return None."""
        # Build buffer: 1 row, 1 column with length = -1 (NULL)
        buf = struct.pack(">i", 1) + struct.pack(">i", -1)
        desc = _make_text_desc(1)
        parser = ListParser()
        reader = BytesIOReader(buf)
        rows = parser.parse_rows(reader, desc)
        self.assertIsNone(rows[0][0])

    @cythontest
    def test_utf8_multiple_rows_columns(self):
        """Multiple rows and columns."""
        texts = [b"alpha", b"beta", b"gamma"]
        # Build buffer with 3 rows x 1 col, different values
        parts = [struct.pack(">i", 3)]
        for t in texts:
            parts.append(struct.pack(">i", len(t)) + t)
        buf = b"".join(parts)
        desc = _make_text_desc(1)
        parser = ListParser()
        reader = BytesIOReader(buf)
        rows = parser.parse_rows(reader, desc)
        self.assertEqual([r[0] for r in rows], ["alpha", "beta", "gamma"])

    @cythontest
    def test_utf8_invalid_bytes(self):
        """Invalid UTF-8 bytes should raise an error (DriverException wrapping UnicodeDecodeError)."""
        # 0xFF 0xFE is not valid UTF-8
        buf = _build_text_rows_buffer(1, 1, b"\xff\xfe\x80\x81")
        desc = _make_text_desc(1)
        parser = ListParser()
        reader = BytesIOReader(buf)
        with self.assertRaises(DriverException) as ctx:
            parser.parse_rows(reader, desc)
        self.assertIn("utf-8", str(ctx.exception).lower())

    @cythontest
    def test_ascii_invalid_bytes(self):
        """Non-ASCII bytes in an ASCII column should raise an error (DriverException wrapping UnicodeDecodeError)."""
        buf = _build_text_rows_buffer(1, 1, b"\x80\x81\x82")
        desc = _make_ascii_desc(1)
        parser = ListParser()
        reader = BytesIOReader(buf)
        with self.assertRaises(DriverException) as ctx:
            parser.parse_rows(reader, desc)
        self.assertIn("ascii", str(ctx.exception).lower())
