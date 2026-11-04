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

try:
    from cassandra.row_parser import (
        clear_parse_desc_cache,
        get_parse_desc_cache_size,
        make_recv_results_rows,
    )
    from cassandra.obj_parser import ListParser

    class _RecordingParser(ListParser):
        """Remembers the ParseDesc it was handed, to assert cache identity."""

        def parse_rows(self, reader, desc):
            self.desc = desc
            return ListParser.parse_rows(self, reader, desc)

    _HAS_ROW_PARSER = True
    _recording_parser = _RecordingParser()
    _recv_results_rows = make_recv_results_rows(_recording_parser)
except ImportError:
    _HAS_ROW_PARSER = False
    _recv_results_rows = None


# The Cython row_parser extension must be importable for these tests to run.
# The shared ``cythontest`` decorator only checks that Cython is installed at
# all, which is not enough: the extension may be missing (source checkout
# without a build), in which case clear_parse_desc_cache()/
# get_parse_desc_cache_size() would be undefined and the tests would fail
# with NameError instead of skipping.
require_row_parser = unittest.skipUnless(
    _HAS_ROW_PARSER, "cassandra.row_parser Cython extension is not available")


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


def _desc(binary_buf, col_meta, protocol_version=4):
    """Run recv_results_rows and return the ParseDesc it parsed with."""
    _recv(binary_buf, col_meta, protocol_version)
    return _recording_parser.desc


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

    @require_row_parser
    def test_cache_hit_returns_independent_lists(self):
        """Repeated calls with the same col_meta object hit the cache, but each
        response gets its own column_names/column_types lists.

        The lists are public, mutable attributes (ResultSet.column_names and
        the list handed to the user's row factory), so they must not be shared
        between responses.
        """
        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)

        msg1 = _recv(buf, col_meta)
        msg2 = _recv(buf, col_meta)

        # Same values, but distinct list objects.
        self.assertIsNot(msg1.column_names, msg2.column_names)
        self.assertIsNot(msg1.column_types, msg2.column_types)
        self.assertEqual(msg1.column_names, msg2.column_names)
        self.assertEqual(msg1.column_types, msg2.column_types)

    @require_row_parser
    def test_cache_hit_reuses_same_parse_desc(self):
        """The expensive ParseDesc object itself is still reused across
        executions of the same prepared statement."""
        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)

        desc1 = _desc(buf, col_meta)
        desc2 = _desc(buf, col_meta)
        desc3 = _desc(buf, col_meta)

        self.assertIs(desc1, desc2)
        self.assertIs(desc2, desc3)
        self.assertEqual(get_parse_desc_cache_size(), 1)

    @require_row_parser
    def test_mutating_one_response_does_not_affect_the_next(self):
        """A consumer mutating a response's column_names/column_types must not
        relabel later responses of the same prepared statement."""
        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)

        msg1 = _recv(buf, col_meta)
        original_names = list(msg1.column_names)
        original_types = list(msg1.column_types)

        # Simulate user code (or a custom row factory) mutating the metadata
        # it received.
        msg1.column_names[0] = 'CLOBBERED'
        msg1.column_types[0] = 'CLOBBERED_TYPE'

        msg2 = _recv(buf, col_meta)

        self.assertEqual(list(msg2.column_names), original_names)
        self.assertEqual(list(msg2.column_types), original_types)
        self.assertNotIn('CLOBBERED', msg2.column_names)
        self.assertNotIn('CLOBBERED_TYPE', msg2.column_types)

    @require_row_parser
    def test_mutation_on_cache_miss_does_not_affect_the_next(self):
        """The same guarantee holds for the first (cache-miss) execution."""
        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)

        msg1 = _recv(buf, col_meta)
        original_names = list(msg1.column_names)

        msg1.column_names[0] = 'CLOBBERED'

        msg2 = _recv(buf, col_meta)
        self.assertEqual(list(msg2.column_names), original_names)

    @require_row_parser
    def test_cache_miss_different_metadata(self):
        """Different metadata list objects should produce cache misses."""
        buf = _build_binary_buf(1, 5)
        col_meta_a = _build_column_metadata(5)
        col_meta_b = _build_column_metadata(5)

        msg_a = _recv(buf, col_meta_a)
        msg_b = _recv(buf, col_meta_b)

        self.assertIsNot(msg_a.column_names, msg_b.column_names)
        self.assertEqual(msg_a.column_names, msg_b.column_names)

    @require_row_parser
    def test_protocol_version_invalidates_cache(self):
        """Changed protocol_version should invalidate the cache entry."""
        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)

        desc_v4 = _desc(buf, col_meta, protocol_version=4)
        # Same metadata, different protocol version -> the cached entry does
        # not match (the protocol version is part of the cache key check), so
        # a new ParseDesc is built and stored under the same metadata id.
        desc_v5 = _desc(buf, col_meta, protocol_version=5)

        self.assertIsNot(desc_v4, desc_v5)
        self.assertEqual(desc_v5.protocol_version, 5)

        # The cache still holds a single entry (keyed by the metadata id),
        # but it was rebuilt for the new protocol version: decoding with v4
        # again must not reuse the v5 entry.
        self.assertEqual(get_parse_desc_cache_size(), 1)

        # Going back to v4 must produce a correct (rebuilt) decode as well.
        desc_v4_again = _desc(buf, col_meta, protocol_version=4)
        self.assertIsNot(desc_v4_again, desc_v4)
        self.assertIsNot(desc_v4_again, desc_v5)
        self.assertEqual(desc_v4_again.protocol_version, 4)

    @require_row_parser
    def test_clear_cache_invalidates_entries(self):
        """clear_parse_desc_cache() should invalidate cached entries."""
        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)

        _recv(buf, col_meta)
        self.assertEqual(get_parse_desc_cache_size(), 1)

        clear_parse_desc_cache()
        self.assertEqual(get_parse_desc_cache_size(), 0)

        msg2 = _recv(buf, col_meta)
        self.assertEqual(list(msg2.column_names),
                         list(_build_column_metadata(5)[i][2]
                              for i in range(5)))
        # The entry was rebuilt after the clear.
        self.assertEqual(get_parse_desc_cache_size(), 1)

    @require_row_parser
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

    @require_row_parser
    def test_cache_evicts_one_entry_at_a_time(self):
        """Overflowing the cache evicts a single (oldest) entry rather than
        clearing the whole cache.

        Clearing everything would make an application that legitimately uses
        slightly more than the maximum number of prepared statements rebuild
        every ParseDesc on every execution.
        """
        buf = _build_binary_buf(1, 5)

        # Fill the cache exactly to its limit.
        metas = [_build_column_metadata(5) for _ in range(256)]
        for meta in metas:
            _recv(buf, meta)
        self.assertEqual(get_parse_desc_cache_size(), 256)

        # One more entry: exactly one entry is evicted to make room, so the
        # cache stays at its limit instead of being wiped.
        oldest_desc = _desc(buf, metas[0])
        extra = _build_column_metadata(5)
        extra_desc = _desc(buf, extra)
        self.assertEqual(get_parse_desc_cache_size(), 256)

        # The most recently inserted metadata is still cached: asking for it
        # again is a hit and must not change the cache size.
        self.assertIs(_desc(buf, extra), extra_desc)
        self.assertEqual(get_parse_desc_cache_size(), 256)

        # The oldest entry was the one evicted, so re-requesting it is a miss
        # that re-inserts it -- again evicting one entry to stay bounded.
        self.assertIsNot(_desc(buf, metas[0]), oldest_desc)
        self.assertEqual(get_parse_desc_cache_size(), 256)

    @require_row_parser
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

    @require_row_parser
    def test_parse_desc_state_is_immutable(self):
        """A ParseDesc must not carry mutable state.

        A ParseDesc is cached and shared between the executions of a
        prepared statement, so a consumer able to mutate it would corrupt
        it -- and every later execution -- for everyone else.
        """
        from cassandra import cqltypes
        from cassandra.parsing import ParseDesc
        from cassandra.policies import ColDesc

        desc = ParseDesc(
            ["a", "b"],
            [cqltypes.UTF8Type, cqltypes.Int32Type],
            None,
            [ColDesc("ks", "tbl", "a"), ColDesc("ks", "tbl", "b")],
            [],
            5,
        )
        with self.assertRaises(TypeError):
            desc.colnames[0] = "clobbered"
        with self.assertRaises(TypeError):
            desc.coltypes[0] = cqltypes.BooleanType
        with self.assertRaises(TypeError):
            desc.coldescs[0] = ColDesc("ks", "tbl", "z")

    @require_row_parser
    def test_get_cache_size(self):
        """get_parse_desc_cache_size() reports correct count."""
        self.assertEqual(get_parse_desc_cache_size(), 0)

        col_meta = _build_column_metadata(5)
        buf = _build_binary_buf(1, 5)
        _recv(buf, col_meta)

        self.assertEqual(get_parse_desc_cache_size(), 1)
