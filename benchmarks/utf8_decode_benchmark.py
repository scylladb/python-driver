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
Benchmarks for UTF-8 and ASCII deserialization in the Cython row parser.

This optimization replaces the two-step to_bytes(buf).decode('utf8') with
a direct PyUnicode_DecodeUTF8(buf.ptr, buf.size, NULL) call, eliminating
an intermediate bytes object allocation per text cell.

Requires: pip install pytest-benchmark

Run with: pytest benchmarks/utf8_decode_benchmark.py -v --benchmark-sort=name
Compare before/after by running on master vs this branch.

Correctness tests live in tests/unit/cython/test_deserializers.py.
"""

import struct
import pytest

from cassandra.obj_parser import ListParser
from cassandra.bytesio import BytesIOReader
from cassandra.parsing import ParseDesc
from cassandra.deserializers import make_deserializers
from cassandra.cqltypes import UTF8Type, AsciiType, Int32Type
from cassandra.policies import ColDesc


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


def _build_mixed_rows_buffer(num_rows, text_data, int_value=42):
    """Build a buffer with mixed columns: 3 text + 2 int32."""
    parts = [struct.pack(">i", num_rows)]
    text_cell = struct.pack(">i", len(text_data)) + text_data
    int_cell = struct.pack(">i", 4) + struct.pack(">i", int_value)
    row = text_cell + text_cell + text_cell + int_cell + int_cell
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


def _make_mixed_desc(protocol_version=4):
    """Create a ParseDesc for 3 text + 2 int32 columns."""
    coltypes = [UTF8Type, UTF8Type, UTF8Type, Int32Type, Int32Type]
    colnames = ["text0", "text1", "text2", "int0", "int1"]
    coldescs = [ColDesc("ks", "tbl", n) for n in colnames]
    desers = make_deserializers(coltypes)
    return ParseDesc(colnames, coltypes, None, coldescs, desers, protocol_version)


# ---------------------------------------------------------------------------
# Cython pipeline benchmarks — UTF-8
# ---------------------------------------------------------------------------


class TestUTF8CythonPipeline:
    """Benchmark the full Cython row parsing pipeline with UTF-8 text columns.

    These benchmarks measure the end-to-end cost of parsing result sets
    through the optimized Cython path. The optimization replaces
    to_bytes(buf).decode('utf8') with PyUnicode_DecodeUTF8(buf.ptr, buf.size, NULL),
    eliminating one intermediate bytes allocation per text cell.
    """

    def test_bench_utf8_1row_1col_short(self, benchmark):
        """1 row x 1 col, short string (11 bytes) — isolates per-call overhead."""
        text = b"hello world"
        buf = _build_text_rows_buffer(1, 1, text)
        desc = _make_text_desc(1)
        parser = ListParser()

        def parse():
            reader = BytesIOReader(buf)
            return parser.parse_rows(reader, desc)

        result = benchmark(parse)
        assert len(result) == 1
        assert result[0][0] == "hello world"

    def test_bench_utf8_1row_10col_short(self, benchmark):
        """1 row x 10 cols, short strings — measures per-column overhead."""
        text = b"hello world"
        buf = _build_text_rows_buffer(1, 10, text)
        desc = _make_text_desc(10)
        parser = ListParser()

        def parse():
            reader = BytesIOReader(buf)
            return parser.parse_rows(reader, desc)

        result = benchmark(parse)
        assert len(result) == 1
        assert len(result[0]) == 10

    def test_bench_utf8_100rows_5col_medium(self, benchmark):
        """100 rows x 5 cols, medium string (46 bytes) — typical workload."""
        text = b"Hello, this is a test string for benchmarking!"
        buf = _build_text_rows_buffer(100, 5, text)
        desc = _make_text_desc(5)
        parser = ListParser()

        def parse():
            reader = BytesIOReader(buf)
            return parser.parse_rows(reader, desc)

        result = benchmark(parse)
        assert len(result) == 100
        assert result[0][0] == text.decode("utf8")

    def test_bench_utf8_1000rows_5col_medium(self, benchmark):
        """1000 rows x 5 cols, medium string — high-throughput scenario."""
        text = b"Hello, this is a test string for benchmarking!"
        buf = _build_text_rows_buffer(1000, 5, text)
        desc = _make_text_desc(5)
        parser = ListParser()

        def parse():
            reader = BytesIOReader(buf)
            return parser.parse_rows(reader, desc)

        result = benchmark(parse)
        assert len(result) == 1000

    def test_bench_utf8_100rows_5col_long(self, benchmark):
        """100 rows x 5 cols, long string (200 bytes) — larger values."""
        text = b"A" * 200
        buf = _build_text_rows_buffer(100, 5, text)
        desc = _make_text_desc(5)
        parser = ListParser()

        def parse():
            reader = BytesIOReader(buf)
            return parser.parse_rows(reader, desc)

        result = benchmark(parse)
        assert len(result) == 100
        assert result[0][0] == "A" * 200

    def test_bench_utf8_100rows_5col_multibyte(self, benchmark):
        """100 rows x 5 cols, multibyte UTF-8 string — tests non-ASCII."""
        text = "Héllo wörld! こんにちは 🌍".encode("utf-8")
        buf = _build_text_rows_buffer(100, 5, text)
        desc = _make_text_desc(5)
        parser = ListParser()

        def parse():
            reader = BytesIOReader(buf)
            return parser.parse_rows(reader, desc)

        result = benchmark(parse)
        assert len(result) == 100
        assert result[0][0] == text.decode("utf-8")


# ---------------------------------------------------------------------------
# Cython pipeline benchmarks — ASCII
# ---------------------------------------------------------------------------


class TestASCIICythonPipeline:
    """Benchmark the Cython row parsing pipeline with ASCII text columns."""

    def test_bench_ascii_100rows_5col_medium(self, benchmark):
        """100 rows x 5 cols, medium ASCII string."""
        text = b"Hello, this is a test ASCII string for benchmarking!"
        buf = _build_text_rows_buffer(100, 5, text)
        desc = _make_ascii_desc(5)
        parser = ListParser()

        def parse():
            reader = BytesIOReader(buf)
            return parser.parse_rows(reader, desc)

        result = benchmark(parse)
        assert len(result) == 100
        assert result[0][0] == text.decode("ascii")

    def test_bench_ascii_1000rows_5col_medium(self, benchmark):
        """1000 rows x 5 cols, medium ASCII string."""
        text = b"Hello, this is a test ASCII string for benchmarking!"
        buf = _build_text_rows_buffer(1000, 5, text)
        desc = _make_ascii_desc(5)
        parser = ListParser()

        def parse():
            reader = BytesIOReader(buf)
            return parser.parse_rows(reader, desc)

        result = benchmark(parse)
        assert len(result) == 1000


# ---------------------------------------------------------------------------
# Mixed columns benchmark
# ---------------------------------------------------------------------------


class TestMixedColumnsPipeline:
    """Benchmark with mixed column types (text + int) for realism."""

    def test_bench_mixed_100rows_3text_2int(self, benchmark):
        """100 rows x (3 text + 2 int) — realistic mixed schema."""
        text = b"Hello, this is a test string for benchmarking!"
        buf = _build_mixed_rows_buffer(100, text)
        desc = _make_mixed_desc()
        parser = ListParser()

        def parse():
            reader = BytesIOReader(buf)
            return parser.parse_rows(reader, desc)

        result = benchmark(parse)
        assert len(result) == 100
        assert result[0][0] == text.decode("utf8")
        assert result[0][3] == 42


# ---------------------------------------------------------------------------
# Python-level reference (bytes.decode) for comparison
# ---------------------------------------------------------------------------


class TestPythonDecodeReference:
    """Python-level microbenchmark showing the overhead of creating
    intermediate bytes objects before decode, which is what the
    original Cython code did (to_bytes(buf).decode('utf8')).

    These benchmarks isolate the bytes-creation overhead that the
    PyUnicode_DecodeUTF8 optimization eliminates.
    """

    def test_bench_python_bytes_decode_short(self, benchmark):
        """Python reference: bytes.decode('utf8') for 500 short strings."""
        data = b"hello world"

        def decode_loop():
            result = None
            for _ in range(500):
                result = data.decode("utf8")
            return result

        result = benchmark(decode_loop)
        assert result == "hello world"

    def test_bench_python_copy_then_decode_short(self, benchmark):
        """Python reference: bytes(data).decode('utf8') for 500 short strings.
        This simulates the old to_bytes(buf).decode() pattern, where
        to_bytes() creates a new bytes object from the C buffer."""
        data = b"hello world"
        mv = memoryview(data)

        def decode_loop():
            result = None
            for _ in range(500):
                copied = bytes(mv)  # simulates to_bytes(buf)
                result = copied.decode("utf8")
            return result

        result = benchmark(decode_loop)
        assert result == "hello world"

    def test_bench_python_bytes_decode_medium(self, benchmark):
        """Python reference: bytes.decode('utf8') for 500 medium strings."""
        data = b"Hello, this is a test string for benchmarking!"

        def decode_loop():
            result = None
            for _ in range(500):
                result = data.decode("utf8")
            return result

        result = benchmark(decode_loop)

    def test_bench_python_copy_then_decode_medium(self, benchmark):
        """Python reference: bytes(memoryview).decode('utf8') for 500 medium strings."""
        data = b"Hello, this is a test string for benchmarking!"
        mv = memoryview(data)

        def decode_loop():
            result = None
            for _ in range(500):
                copied = bytes(mv)  # simulates to_bytes(buf)
                result = copied.decode("utf8")
            return result

        result = benchmark(decode_loop)
