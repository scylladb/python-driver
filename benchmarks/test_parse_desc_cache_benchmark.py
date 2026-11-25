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
Benchmarks for the prepared-statement ParseDesc cache in row_parser.pyx.

test_recv_* time the real Cython recv_results_rows() on a skip_meta response
(cache hit) and on a response carrying inline metadata (no cache).
test_reference_* time a pure-Python replica of the cache, to split lookup cost
from row decoding; the replica follows the Cython cache step for step.

Run with:
    pytest benchmarks/test_parse_desc_cache_benchmark.py
"""

import io
import struct
import threading

import pytest

pytest.importorskip("pytest_benchmark")

try:
    from cassandra import cqltypes
    from cassandra.policies import ColDesc
    from cassandra.parsing import ParseDesc
    from cassandra.deserializers import make_deserializers
    from cassandra.obj_parser import ListParser
    from cassandra.row_parser import clear_parse_desc_cache, make_recv_results_rows
except ImportError as e:  # pragma: no cover
    pytest.skip("Cython extensions are not built: %s" % e, allow_module_level=True)

_NO_METADATA_FLAG = 0x0004
_recv_results_rows = make_recv_results_rows(ListParser())


class _Msg:
    column_metadata = None
    column_names = None
    column_types = None
    parsed_rows = None

    def recv_results_metadata(self, f, user_type_map):
        f.read(8)  # flags (NO_METADATA) + colcount


def _metadata(ncols):
    return [("ks", "tbl", "col_%d" % i, cqltypes.UTF8Type) for i in range(ncols)]


def _body(nrows, ncols, value=b"hello world"):
    cell = struct.pack(">i", len(value)) + value
    return struct.pack(">iii", _NO_METADATA_FLAG, ncols, nrows) + cell * ncols * nrows


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_parse_desc_cache()
    yield
    clear_parse_desc_cache()


# Pure-Python replica of row_parser._get_or_build_parse_desc().
_REF_MAX_SIZE = 256


class _ReferenceCache:
    def __init__(self):
        self.entries = {}
        self.lock = threading.Lock()

    @staticmethod
    def build(column_metadata, policy, protocol_version):
        column_types = [md[3] for md in column_metadata]
        coldescs = [ColDesc(md[0], md[1], md[2]) for md in column_metadata] if policy is not None else None
        return ParseDesc([md[2] for md in column_metadata], column_types, policy, coldescs,
                         make_deserializers(column_types), protocol_version)

    @staticmethod
    def matches(entry, column_metadata, policy, protocol_version):
        return entry[0] is column_metadata and entry[1] is policy and entry[2] == protocol_version

    def get(self, column_metadata, policy, protocol_version):
        key = id(column_metadata)
        cached = self.entries.get(key)
        if cached is not None and self.matches(cached, column_metadata, policy, protocol_version):
            return cached[3]
        desc = self.build(column_metadata, policy, protocol_version)
        entry = (column_metadata, policy, protocol_version, desc)
        evicted = None
        with self.lock:
            replaced = self.entries.get(key)
            if replaced is not None and self.matches(replaced, column_metadata, policy, protocol_version):
                return replaced[3]
            if replaced is None and len(self.entries) >= _REF_MAX_SIZE:
                evicted = self.entries.pop(next(iter(self.entries)))
            self.entries[key] = entry
        del evicted, replaced  # released outside the lock
        return desc


@pytest.mark.parametrize("nrows,ncols", [(1, 10), (100, 5), (1000, 5)])
@pytest.mark.parametrize("cached", [True, False], ids=["cached", "inline_metadata"])
def test_recv(benchmark, nrows, ncols, cached):
    meta = _metadata(ncols)
    binary = _body(nrows, ncols)

    def run():
        msg = _Msg()
        if not cached:
            msg.column_metadata = meta  # inline metadata bypasses the cache
        _recv_results_rows(msg, io.BytesIO(binary), 4, {}, meta, None)
        return msg.parsed_rows

    run()
    rows = benchmark(run)
    assert rows == [("hello world",) * ncols] * nrows


@pytest.mark.parametrize("ncols", [5, 10, 20, 50])
@pytest.mark.parametrize("cached", [True, False], ids=["cached", "uncached"])
def test_reference_parse_desc(benchmark, ncols, cached):
    meta = _metadata(ncols)
    cache = _ReferenceCache()
    if cached:
        cache.get(meta, None, 4)
        desc = benchmark(cache.get, meta, None, 4)
        assert cache.get(meta, None, 4) is desc
    else:
        desc = benchmark(_ReferenceCache.build, meta, None, 4)
    assert desc.colnames == tuple(md[2] for md in meta)


def test_reference_matches_cython_cache_behaviour():
    cache = _ReferenceCache()
    metas = [_metadata(3) for _ in range(_REF_MAX_SIZE)]
    descs = [cache.get(m, None, 4) for m in metas]
    assert cache.get(metas[0], None, 4) is descs[0]
    cache.get(_metadata(3), None, 4)
    assert len(cache.entries) == _REF_MAX_SIZE
    assert cache.get(metas[1], None, 4) is descs[1]
    assert cache.get(metas[0], None, 4) is not descs[0]  # oldest was evicted
