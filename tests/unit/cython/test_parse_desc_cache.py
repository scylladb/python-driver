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
Unit tests for the ParseDesc cache in row_parser.pyx.

Validates cache hit/miss behavior, protocol_version invalidation, cache
clearing, and bounded eviction — all exercised through the actual Cython
_get_or_build_parse_desc function via make_recv_results_rows().
"""

import io
import struct
import unittest

from tests.unit.cython.utils import cythontest

try:
    from cassandra.row_parser import (
        clear_parse_desc_cache,
        get_parse_desc_cache_size,
        make_recv_results_rows,
    )
    from cassandra.obj_parser import ListParser

    _HAS_ROW_PARSER = True
    _recv_results_rows = make_recv_results_rows(ListParser())
except ImportError:
    _HAS_ROW_PARSER = False
    _recv_results_rows = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_column_metadata(ncols):
    """Build a column_metadata list like the driver produces."""
    from cassandra import cqltypes

    return [("ks", "tbl", "col_%d" % i, cqltypes.UTF8Type) for i in range(ncols)]


# NO_METADATA_FLAG as defined in ResultMessage
_NO_METADATA_FLAG = 0x0004


class _MockResultMessage:
    """Minimal mock of ResultMessage for the prepared-statement path."""

    column_metadata = None
    column_names = None
    column_types = None
    parsed_rows = None
    paging_state = None
    continuous_paging_seq = None
    continuous_paging_last = None
    result_metadata_id = None

    def recv_results_metadata(self, f, user_type_map):
        """Simulate the prepared-statement path (NO_METADATA_FLAG is set)."""
        _flags = struct.unpack(">i", f.read(4))[0]
        _colcount = struct.unpack(">i", f.read(4))[0]


def _build_binary_buf(nrows, ncols, col_value=b"hello world"):
    """Build a full binary buffer for the prepared-statement path."""
    parts = []
    parts.append(struct.pack(">i", _NO_METADATA_FLAG))
    parts.append(struct.pack(">i", ncols))
    parts.append(struct.pack(">i", nrows))
    col_cell = struct.pack(">i", len(col_value)) + col_value
    row_data = col_cell * ncols
    for _ in range(nrows):
        parts.append(row_data)
    return b"".join(parts)


def _recv(binary_buf, col_meta, protocol_version=4, ce_policy=None):
    """Run recv_results_rows and return the MockResultMessage."""
    msg = _MockResultMessage()
    _recv_results_rows(
        msg, io.BytesIO(binary_buf), protocol_version, {}, col_meta, ce_policy
    )
    return msg


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class ParseDescCacheTest(unittest.TestCase):
    """Tests for the Cython ParseDesc cache in row_parser.pyx."""

    def setUp(self):
        if _HAS_ROW_PARSER:
            clear_parse_desc_cache()

    def tearDown(self):
        if _HAS_ROW_PARSER:
            clear_parse_desc_cache()

    @cythontest
    def test_cache_hit_returns_same_objects(self):
        """Repeated calls with the same col_meta object should return
        identical column_names and column_types objects (cache hit)."""
        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)

        msg1 = _recv(buf, col_meta)
        msg2 = _recv(buf, col_meta)

        self.assertIs(msg1.column_names, msg2.column_names)
        self.assertIs(msg1.column_types, msg2.column_types)

    @cythontest
    def test_cache_miss_different_metadata(self):
        """Different metadata list objects should produce cache misses."""
        buf = _build_binary_buf(1, 5)
        col_meta_a = _build_column_metadata(5)
        col_meta_b = _build_column_metadata(5)

        msg_a = _recv(buf, col_meta_a)
        msg_b = _recv(buf, col_meta_b)

        self.assertIsNot(msg_a.column_names, msg_b.column_names)
        self.assertEqual(msg_a.column_names, msg_b.column_names)

    @cythontest
    def test_protocol_version_invalidates_cache(self):
        """Changed protocol_version should invalidate the cache entry."""
        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)

        msg_v4 = _recv(buf, col_meta, protocol_version=4)
        msg_v5 = _recv(buf, col_meta, protocol_version=5)

        self.assertIsNot(msg_v4.column_names, msg_v5.column_names)

    @cythontest
    def test_clear_cache_invalidates_entries(self):
        """clear_parse_desc_cache() should invalidate cached entries."""
        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)

        msg1 = _recv(buf, col_meta)
        clear_parse_desc_cache()
        msg2 = _recv(buf, col_meta)

        self.assertIsNot(msg1.column_names, msg2.column_names)
        self.assertEqual(msg1.column_names, msg2.column_names)

    @cythontest
    def test_cache_bounded_size(self):
        """Cache should evict entries when exceeding the max size (256)."""
        buf = _build_binary_buf(1, 5)
        meta_lists = [_build_column_metadata(5) for _ in range(300)]

        for meta in meta_lists:
            _recv(buf, meta)

        cache_size = get_parse_desc_cache_size()
        self.assertLessEqual(
            cache_size,
            256,
            "Cache should be bounded to 256 entries, got %d" % cache_size,
        )

    @cythontest
    def test_parsed_rows_correctness(self):
        """Verify parsed row data is correct through the cached path."""
        ncols, nrows = 5, 3
        col_meta = _build_column_metadata(ncols)
        buf = _build_binary_buf(nrows, ncols, col_value=b"test_val")

        msg = _recv(buf, col_meta)

        self.assertEqual(len(msg.parsed_rows), nrows)
        for row in msg.parsed_rows:
            self.assertEqual(len(row), ncols)
            for val in row:
                self.assertEqual(val, "test_val")
        self.assertEqual(msg.column_names, ["col_%d" % i for i in range(ncols)])

    @cythontest
    def test_get_cache_size(self):
        """get_parse_desc_cache_size() reports correct count."""
        self.assertEqual(get_parse_desc_cache_size(), 0)

        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)
        _recv(buf, col_meta)

        self.assertEqual(get_parse_desc_cache_size(), 1)
