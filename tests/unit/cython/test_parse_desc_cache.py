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

"""Tests for the prepared-statement ParseDesc cache in row_parser.pyx."""

import io
import struct
import threading
import unittest
from unittest import mock

from cassandra import cqltypes
from cassandra.policies import ColDesc, ColumnEncryptionPolicy

try:
    from cassandra import row_parser
    from cassandra.bytesio import BytesIOReader
    from cassandra.obj_parser import ListParser
    from cassandra.parsing import ParseDesc
    from cassandra.row_parser import clear_parse_desc_cache, get_parse_desc_cache_size
    _HAS_ROW_PARSER = True
except ImportError:
    _HAS_ROW_PARSER = False
    ListParser = object

require_row_parser = unittest.skipUnless(_HAS_ROW_PARSER, "cassandra.row_parser is not built")

_NO_METADATA_FLAG = 0x0004
_MAX = 256
# Bounds only a hang in a broken implementation; passing runs never wait on it.
_TIMEOUT = 10


class _RecordingParser(ListParser):
    """Records the ParseDesc it decodes with; optionally runs a hook first."""

    on_parse = None

    def parse_rows(self, reader, desc):
        self.desc = desc
        if self.on_parse is not None:
            self.on_parse()
        return ListParser.parse_rows(self, reader, desc)


class _Msg:
    column_metadata = None
    column_names = None
    column_types = None
    parsed_rows = None

    def recv_results_metadata(self, f, user_type_map):
        f.read(8)  # flags (NO_METADATA) + colcount


class _NoEncryptedColumnsPolicy(ColumnEncryptionPolicy):
    def contains_column(self, coldesc):
        return False


def _metadata(ncols=3, factory=list):
    return factory(("ks", "tbl", "col_%d" % i, cqltypes.UTF8Type) for i in range(ncols))


def _rows(nrows, ncols, value=b"v"):
    cell = struct.pack(">i", len(value)) + value
    return struct.pack(">i", nrows) + cell * ncols * nrows


def _body(nrows, ncols, value=b"v"):
    return struct.pack(">ii", _NO_METADATA_FLAG, ncols) + _rows(nrows, ncols, value)


@require_row_parser
class ParseDescCacheTest(unittest.TestCase):

    def setUp(self):
        clear_parse_desc_cache()
        # A cleanup, not tearDown: it must run after the cleanups that unblock test threads.
        self.addCleanup(clear_parse_desc_cache)
        self.parser = _RecordingParser()
        self.recv = row_parser.make_recv_results_rows(self.parser)

    def decode(self, meta, protocol_version=4, policy=None, nrows=1, value=b"v", inline=False):
        msg = _Msg()
        if inline:
            msg.column_metadata = meta
        self.recv(msg, io.BytesIO(_body(nrows, len(meta), value)), protocol_version, {}, meta, policy)
        self.assertEqual(msg.parsed_rows, [(value.decode(),) * len(meta)] * nrows)
        self.assertEqual(msg.column_names, [md[2] for md in meta])
        return msg, self.parser.desc

    def test_hit_reuses_desc_with_independent_response_lists(self):
        meta = _metadata()
        msg1, desc1 = self.decode(meta)
        msg1.column_names[0] = "CLOBBERED"
        msg1.column_types[0] = None
        msg2, desc2 = self.decode(meta)
        self.assertIs(desc1, desc2)
        self.assertIsNot(msg1.column_names, msg2.column_names)
        self.assertEqual(msg2.column_types, [cqltypes.UTF8Type] * 3)
        self.assertEqual(get_parse_desc_cache_size(), 1)

    def test_inline_metadata_is_not_cached(self):
        meta = _metadata()
        _, desc1 = self.decode(meta, inline=True)
        _, desc2 = self.decode(meta, inline=True)
        self.assertIsNot(desc1, desc2)
        self.assertEqual(get_parse_desc_cache_size(), 0)

    def test_metadata_policy_and_protocol_version_are_checked(self):
        meta = _metadata()
        policy = _NoEncryptedColumnsPolicy()
        _, desc = self.decode(meta)
        self.assertIsNot(self.decode(_metadata())[1], desc)
        _, desc_v5 = self.decode(meta, protocol_version=5)
        self.assertIsNot(desc_v5, desc)
        self.assertEqual(desc_v5.protocol_version, 5)
        _, desc_policy = self.decode(meta, protocol_version=5, policy=policy)
        self.assertIsNot(desc_policy, desc_v5)
        self.assertIs(desc_policy.column_encryption_policy, policy)
        self.assertEqual(desc_policy.coldescs, tuple(ColDesc("ks", "tbl", "col_%d" % i) for i in range(3)))
        self.assertIs(self.decode(meta, protocol_version=5, policy=policy)[1], desc_policy)
        self.assertEqual(get_parse_desc_cache_size(), 2)

    def test_policy_turning_truthy_after_caching_still_decodes(self):
        class TogglingPolicy(_NoEncryptedColumnsPolicy):
            enabled = False

            def __bool__(self):
                return self.enabled

        meta = _metadata()
        policy = TogglingPolicy()
        _, desc = self.decode(meta, policy=policy)
        policy.enabled = True
        self.assertIs(self.decode(meta, policy=policy)[1], desc)
        self.assertEqual(desc.coldescs, tuple(ColDesc("ks", "tbl", "col_%d" % i) for i in range(3)))

    def test_clear(self):
        meta = _metadata()
        _, desc = self.decode(meta)
        clear_parse_desc_cache()
        self.assertEqual(get_parse_desc_cache_size(), 0)
        self.assertIsNot(self.decode(meta)[1], desc)

    def test_overflow_evicts_only_the_oldest_entry(self):
        metas = [_metadata() for _ in range(_MAX)]
        descs = [self.decode(m)[1] for m in metas]
        self.decode(_metadata())
        self.assertEqual(get_parse_desc_cache_size(), _MAX)
        self.assertIs(self.decode(metas[1])[1], descs[1])
        self.assertIsNot(self.decode(metas[0])[1], descs[0])
        self.assertEqual(get_parse_desc_cache_size(), _MAX)

    def test_parse_desc_fields_are_read_only(self):
        meta = _metadata()
        _, desc = self.decode(meta, policy=_NoEncryptedColumnsPolicy())
        for field, value in [("colnames", ("x",) * 3), ("coltypes", (cqltypes.Int32Type,) * 3),
                             ("column_encryption_policy", None), ("coldescs", None),
                             ("protocol_version", 3)]:
            with self.assertRaises(AttributeError, msg=field):
                setattr(desc, field, value)
        for field in ("colnames", "coltypes", "coldescs"):
            with self.assertRaises(TypeError, msg=field):
                getattr(desc, field)[0] = None
        msg, desc2 = self.decode(meta, policy=desc.column_encryption_policy, value=b"next")
        self.assertIs(desc2, desc)
        self.assertEqual(desc.protocol_version, 4)
        self.assertEqual(msg.column_types, [cqltypes.UTF8Type] * 3)

    def _start_blocked_miss(self, meta):
        """Decode meta in a thread whose ParseDesc build blocks until released."""
        building, release = threading.Event(), threading.Event()
        real = row_parser.make_deserializers

        def blocking_make_deserializers(types):
            if threading.current_thread() is thread:
                building.set()
                release.wait()
            return real(types)

        patcher = mock.patch.object(row_parser, "make_deserializers", blocking_make_deserializers)
        patcher.start()
        self.addCleanup(patcher.stop)
        result = {}
        # Its own parser: self.parser records the other decodes' descs.
        parser = _RecordingParser()
        recv = row_parser.make_recv_results_rows(parser)

        def run():
            msg = _Msg()
            recv(msg, io.BytesIO(_body(1, len(meta))), 4, {}, meta, None)
            result["rows"], result["desc"] = msg.parsed_rows, parser.desc

        thread = threading.Thread(target=run)
        thread.start()
        self.addCleanup(release.set)
        self.assertTrue(building.wait(_TIMEOUT))
        return thread, release, result

    def _in_thread(self, fn):
        """Run fn in another thread; fails instead of hanging if fn blocks on the cache lock."""
        thread = threading.Thread(target=fn)
        thread.start()
        thread.join(_TIMEOUT)
        self.assertFalse(thread.is_alive())

    def _finish(self, thread, release):
        release.set()
        thread.join(_TIMEOUT)
        self.assertFalse(thread.is_alive())

    def test_concurrent_miss_reuses_published_entry(self):
        meta = _metadata()
        thread, release, result = self._start_blocked_miss(meta)
        self._in_thread(lambda: self.decode(meta))
        published = self.parser.desc
        self._finish(thread, release)
        self.assertIs(result["desc"], published)
        self.assertEqual(result["rows"], [("v",) * 3])
        self.assertEqual(get_parse_desc_cache_size(), 1)

    def test_clear_does_not_wait_for_or_cancel_a_miss_being_built(self):
        meta = _metadata()
        self.decode(_metadata())
        thread, release, result = self._start_blocked_miss(meta)
        self._in_thread(clear_parse_desc_cache)
        self.assertEqual(get_parse_desc_cache_size(), 0)
        self._finish(thread, release)
        # The in-flight miss still publishes after the clear.
        self.assertEqual(get_parse_desc_cache_size(), 1)
        self.assertIs(self.decode(meta)[1], result["desc"])

    def test_retained_reader_decodes_after_clear_and_eviction(self):
        meta = _metadata()
        _, desc = self.decode(meta)
        reading, release = threading.Event(), threading.Event()
        parser = _RecordingParser()
        recv = row_parser.make_recv_results_rows(parser)
        parser.on_parse = lambda: (reading.set(), release.wait())
        result = {}

        def run():
            msg = _Msg()
            recv(msg, io.BytesIO(_body(2, 3, b"kept")), 4, {}, meta, None)
            result["rows"] = msg.parsed_rows

        thread = threading.Thread(target=run)
        thread.start()
        self.addCleanup(release.set)
        self.assertTrue(reading.wait(_TIMEOUT))
        self.assertIs(parser.desc, desc)
        clear_parse_desc_cache()
        for _ in range(_MAX + 1):
            self.decode(_metadata())
        del desc
        self._finish(thread, release)
        self.assertEqual(result["rows"], [("kept",) * 3] * 2)

    def _assert_finalized_outside_lock(self, drop):
        """drop() must release the last reference to a cached entry's metadata."""
        seen = []

        class FinalizingList(list):
            def __del__(self):
                seen.append(row_parser._parse_desc_lock.locked())

        meta = _metadata(factory=FinalizingList)
        self.decode(meta)
        del meta
        self.assertEqual(seen, [])
        drop()
        self.assertEqual(seen, [False])

    def test_replaced_entry_is_finalized_outside_lock(self):
        seen = []

        class FinalizingPolicy(_NoEncryptedColumnsPolicy):
            def __del__(self):
                seen.append(row_parser._parse_desc_lock.locked())

        meta = _metadata()
        self.decode(meta, policy=FinalizingPolicy())
        self.parser.desc = None  # leave the cache entry as the policy's only owner
        self.assertEqual(seen, [])
        self.decode(meta, policy=_NoEncryptedColumnsPolicy())
        self.assertEqual(seen, [False])

    def test_evicted_entry_is_finalized_outside_lock(self):
        def overflow():
            for _ in range(_MAX):
                self.decode(_metadata())
        self._assert_finalized_outside_lock(overflow)

    def test_cleared_entry_is_finalized_outside_lock(self):
        self._assert_finalized_outside_lock(clear_parse_desc_cache)


@require_row_parser
class ParseDescTest(unittest.TestCase):

    def test_stores_tuples(self):
        des = row_parser.make_deserializers([cqltypes.UTF8Type])
        desc = ParseDesc(["a"], [cqltypes.UTF8Type], None, None, des, 4)
        self.assertEqual((desc.colnames, desc.coltypes, desc.coldescs), (("a",), (cqltypes.UTF8Type,), None))
        reader = BytesIOReader(_rows(1, 1, b"x"))
        self.assertEqual(ListParser().parse_rows(reader, desc), [("x",)])
