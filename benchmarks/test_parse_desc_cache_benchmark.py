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

"""
Benchmarks for ParseDesc construction with and without caching.

The ParseDesc is built on every response in recv_results_rows(). For prepared
statements the column_metadata list is the same object every time, so caching
the ParseDesc (keyed by id(column_metadata)) avoids repeated list
comprehensions, ColDesc construction, and make_deserializers() calls.

There are two benchmark tiers:

1. **Integration benchmarks** (test_integration_*): Exercise the actual Cython
   _get_or_build_parse_desc function through the real recv_results_rows closure
   returned by make_recv_results_rows(). These use a mock ResultMessage with a
   binary buffer simulating the prepared statement path (NO_METADATA_FLAG set).

2. **Isolated benchmarks** (test_parse_desc_*, test_full_pipeline_*): Measure
   ParseDesc construction and row parsing using a pure-Python cache replica.
   Useful for understanding the breakdown of costs but do not exercise the
   actual Cython cache code path.

Run with:
    pytest benchmarks/test_parse_desc_cache_benchmark.py -v
"""

import io
import struct
import pytest

from cassandra import cqltypes
from cassandra.policies import ColDesc
from cassandra.parsing import ParseDesc
from cassandra.deserializers import make_deserializers
from cassandra.bytesio import BytesIOReader
from cassandra.obj_parser import ListParser
from cassandra.row_parser import (
    clear_parse_desc_cache,
    get_parse_desc_cache_size,
    make_recv_results_rows,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_column_metadata(ncols, cql_type=cqltypes.UTF8Type):
    """Build a column_metadata list like the driver produces."""
    return [("ks", "tbl", "col_%d" % i, cql_type) for i in range(ncols)]


def _build_uncached_parse_desc(
    column_metadata, column_encryption_policy, protocol_version
):
    """Original uncached ParseDesc construction (baseline)."""
    column_names = [md[2] for md in column_metadata]
    column_types = [md[3] for md in column_metadata]
    desc = ParseDesc(
        column_names,
        column_types,
        column_encryption_policy,
        [ColDesc(md[0], md[1], md[2]) for md in column_metadata],
        make_deserializers(column_types),
        protocol_version,
    )
    return desc, column_names, column_types


def _build_binary_rows(nrows, ncols, col_value=b"hello world"):
    """
    Build a binary buffer matching the Cassandra row format:
      int32(rowcount)
      for each row:
        for each col: int32(len) + bytes
    """
    parts = [struct.pack(">i", nrows)]
    col_cell = struct.pack(">i", len(col_value)) + col_value
    row_data = col_cell * ncols
    for _ in range(nrows):
        parts.append(row_data)
    return b"".join(parts)


# ---------------------------------------------------------------------------
# Integration helpers — exercise the actual Cython recv_results_rows
# ---------------------------------------------------------------------------

# NO_METADATA_FLAG as defined in ResultMessage
_NO_METADATA_FLAG = 0x0004


class _MockResultMessage:
    """
    Minimal mock of ResultMessage for the prepared-statement path.

    When NO_METADATA_FLAG is set in the binary stream, recv_results_metadata
    reads just the flags + colcount and returns, leaving column_metadata as
    None.  The closure then falls through to result_metadata (the prepared
    statement's stored metadata).
    """

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
        # Read flags + colcount just like the real recv_results_metadata does
        _flags = struct.unpack(">i", f.read(4))[0]
        _colcount = struct.unpack(">i", f.read(4))[0]
        # NO_METADATA_FLAG is set, so return immediately — column_metadata stays None


def _build_integration_binary_buf(nrows, ncols, col_value=b"hello world"):
    """
    Build a full binary buffer for the integration benchmark.

    Format for the prepared-statement path:
      int32(flags=NO_METADATA_FLAG)  -- read by recv_results_metadata
      int32(colcount)                -- read by recv_results_metadata
      int32(rowcount)                -- read by BytesIOReader in parse_rows
      for each row:
        for each col: int32(len) + bytes
    """
    parts = []
    parts.append(struct.pack(">i", _NO_METADATA_FLAG))  # flags
    parts.append(struct.pack(">i", ncols))  # colcount
    parts.append(struct.pack(">i", nrows))  # rowcount
    col_cell = struct.pack(">i", len(col_value)) + col_value
    row_data = col_cell * ncols
    for _ in range(nrows):
        parts.append(row_data)
    return b"".join(parts)


# The actual Cython recv_results_rows closure — this calls _get_or_build_parse_desc internally
_cython_recv_results_rows = make_recv_results_rows(ListParser())


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clear_cython_cache():
    """Ensure the Cython ParseDesc cache is empty before and after each test."""
    clear_parse_desc_cache()
    yield
    clear_parse_desc_cache()


@pytest.fixture()
def py_cache():
    """Provide a fresh pure-Python cache dict for each test that needs it."""
    return {}


# ---------------------------------------------------------------------------
# Pure-Python cache replica (reference implementation)
# Useful for understanding cost breakdown but does NOT exercise the actual
# Cython cdef inline _get_or_build_parse_desc function.
# ---------------------------------------------------------------------------

_PY_CACHE_MAX_SIZE = 256


def _cached_parse_desc_py(
    column_metadata, column_encryption_policy, protocol_version, cache
):
    """Pure-Python replica of the Cython cache for reference comparison."""
    cache_key = id(column_metadata)
    cached = cache.get(cache_key)
    if cached is not None:
        if (
            cached[0] is column_metadata
            and cached[1] is column_encryption_policy
            and cached[2] == protocol_version
        ):
            return cached[3], cached[4], cached[5]

    column_names = [md[2] for md in column_metadata]
    column_types = [md[3] for md in column_metadata]
    desc = ParseDesc(
        column_names,
        column_types,
        column_encryption_policy,
        [ColDesc(md[0], md[1], md[2]) for md in column_metadata],
        make_deserializers(column_types),
        protocol_version,
    )

    if len(cache) >= _PY_CACHE_MAX_SIZE:
        cache.clear()

    cache[cache_key] = (
        column_metadata,
        column_encryption_policy,
        protocol_version,
        desc,
        column_names,
        column_types,
    )
    return desc, column_names, column_types


# ---------------------------------------------------------------------------
# Integration benchmarks: actual Cython recv_results_rows
# These exercise the real _get_or_build_parse_desc cdef inline function.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("nrows,ncols", [(1, 10), (100, 5), (1000, 5)])
def test_integration_cython_cached(benchmark, nrows, ncols):
    """Integration: Cython recv_results_rows with ParseDesc cache hit (prepared stmt)."""
    col_meta = _build_column_metadata(ncols)
    binary_buf = _build_integration_binary_buf(nrows, ncols)

    # Warm the Cython cache with the same col_meta object
    warmup = _MockResultMessage()
    _cython_recv_results_rows(warmup, io.BytesIO(binary_buf), 4, {}, col_meta, None)

    def run():
        msg = _MockResultMessage()
        _cython_recv_results_rows(msg, io.BytesIO(binary_buf), 4, {}, col_meta, None)
        return msg.parsed_rows

    rows = benchmark(run)
    assert len(rows) == nrows
    assert len(rows[0]) == ncols


@pytest.mark.parametrize("nrows,ncols", [(1, 10), (100, 5), (1000, 5)])
def test_integration_cython_uncached(benchmark, nrows, ncols):
    """Integration: Cython recv_results_rows with cache miss (fresh metadata each call)."""
    binary_buf = _build_integration_binary_buf(nrows, ncols)

    def run():
        # Fresh metadata list each call forces a cache miss
        fresh_meta = _build_column_metadata(ncols)
        msg = _MockResultMessage()
        _cython_recv_results_rows(msg, io.BytesIO(binary_buf), 4, {}, fresh_meta, None)
        return msg.parsed_rows

    rows = benchmark(run)
    assert len(rows) == nrows
    assert len(rows[0]) == ncols


# ---------------------------------------------------------------------------
# Integration correctness tests: verify the actual Cython cache behavior
# ---------------------------------------------------------------------------


def test_integration_cython_cache_hit():
    """Cython cache returns same column_names/column_types on repeated calls."""
    col_meta = _build_column_metadata(5)
    binary_buf = _build_integration_binary_buf(1, 5)

    msg1 = _MockResultMessage()
    _cython_recv_results_rows(msg1, io.BytesIO(binary_buf), 4, {}, col_meta, None)

    msg2 = _MockResultMessage()
    _cython_recv_results_rows(msg2, io.BytesIO(binary_buf), 4, {}, col_meta, None)

    # Same col_meta object -> cache hit -> same column_names/types objects
    assert msg1.column_names is msg2.column_names
    assert msg1.column_types is msg2.column_types


def test_integration_cython_cache_miss_different_metadata():
    """Different metadata list objects produce cache misses."""
    binary_buf = _build_integration_binary_buf(1, 5)

    col_meta_a = _build_column_metadata(5)
    col_meta_b = _build_column_metadata(5)  # same shape but different list object

    msg_a = _MockResultMessage()
    _cython_recv_results_rows(msg_a, io.BytesIO(binary_buf), 4, {}, col_meta_a, None)

    msg_b = _MockResultMessage()
    _cython_recv_results_rows(msg_b, io.BytesIO(binary_buf), 4, {}, col_meta_b, None)

    # Different list objects -> different id() -> cache miss
    assert msg_a.column_names is not msg_b.column_names
    # But values are equivalent
    assert msg_a.column_names == msg_b.column_names


def test_integration_cython_cache_invalidation_protocol_version():
    """Changed protocol_version invalidates the Cython cache entry."""
    col_meta = _build_column_metadata(5)
    binary_buf = _build_integration_binary_buf(1, 5)

    msg_v4 = _MockResultMessage()
    _cython_recv_results_rows(msg_v4, io.BytesIO(binary_buf), 4, {}, col_meta, None)

    msg_v5 = _MockResultMessage()
    _cython_recv_results_rows(msg_v5, io.BytesIO(binary_buf), 5, {}, col_meta, None)

    # Same col_meta but different protocol_version -> cache miss -> different objects
    assert msg_v4.column_names is not msg_v5.column_names


def test_integration_cython_clear_cache():
    """clear_parse_desc_cache() invalidates cached entries."""
    col_meta = _build_column_metadata(5)
    binary_buf = _build_integration_binary_buf(1, 5)

    msg1 = _MockResultMessage()
    _cython_recv_results_rows(msg1, io.BytesIO(binary_buf), 4, {}, col_meta, None)

    clear_parse_desc_cache()

    msg2 = _MockResultMessage()
    _cython_recv_results_rows(msg2, io.BytesIO(binary_buf), 4, {}, col_meta, None)

    # After cache clear, new ParseDesc is built -> different column_names object
    assert msg1.column_names is not msg2.column_names
    assert msg1.column_names == msg2.column_names


def test_integration_cython_parsed_rows_correctness():
    """Integration: verify parsed row data is correct through the Cython path."""
    ncols = 5
    nrows = 3
    col_meta = _build_column_metadata(ncols)
    binary_buf = _build_integration_binary_buf(nrows, ncols, col_value=b"test_val")

    msg = _MockResultMessage()
    _cython_recv_results_rows(msg, io.BytesIO(binary_buf), 4, {}, col_meta, None)

    assert len(msg.parsed_rows) == nrows
    for row in msg.parsed_rows:
        assert len(row) == ncols
        for val in row:
            assert val == "test_val"
    assert msg.column_names == ["col_%d" % i for i in range(ncols)]


def test_integration_cython_cache_bounded_size():
    """Cython cache evicts entries when exceeding max size."""
    # Fill the cache with many distinct metadata lists
    binary_buf = _build_integration_binary_buf(1, 5)
    meta_lists = [_build_column_metadata(5) for _ in range(300)]

    for meta in meta_lists:
        msg = _MockResultMessage()
        _cython_recv_results_rows(msg, io.BytesIO(binary_buf), 4, {}, meta, None)

    # Cache should have been evicted at least once
    cache_size = get_parse_desc_cache_size()
    assert cache_size <= 256, (
        "Cache should be bounded to 256 entries, got %d" % cache_size
    )


# ---------------------------------------------------------------------------
# Isolated benchmarks: ParseDesc construction (reference, pure-Python)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ncols", [5, 10, 20])
def test_parse_desc_build_uncached(benchmark, ncols):
    """Reference: build ParseDesc from scratch every time (original code path)."""
    col_meta = _build_column_metadata(ncols)

    def run():
        return _build_uncached_parse_desc(col_meta, None, 4)

    result = benchmark(run)
    desc, names, types = result
    assert len(names) == ncols
    assert len(desc.colnames) == ncols


@pytest.mark.parametrize("ncols", [5, 10, 20])
def test_parse_desc_build_cached(benchmark, ncols, py_cache):
    """Reference: cached second calls return cached ParseDesc (pure-Python replica)."""
    col_meta = _build_column_metadata(ncols)

    # Warm the cache
    _cached_parse_desc_py(col_meta, None, 4, py_cache)

    def run():
        return _cached_parse_desc_py(col_meta, None, 4, py_cache)

    result = benchmark(run)
    desc, names, types = result
    assert len(names) == ncols
    assert len(desc.colnames) == ncols


# ---------------------------------------------------------------------------
# Isolated benchmarks: Full parse_rows pipeline (reference, pure-Python)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("nrows,ncols", [(1, 10), (100, 5), (1000, 5)])
def test_full_pipeline_uncached(benchmark, nrows, ncols):
    """Reference: build ParseDesc from scratch + parse rows (pure-Python desc)."""
    col_meta = _build_column_metadata(ncols)
    binary_buf = _build_binary_rows(nrows, ncols)
    parser = ListParser()

    def run():
        desc, names, types = _build_uncached_parse_desc(col_meta, None, 4)
        reader = BytesIOReader(binary_buf)
        return parser.parse_rows(reader, desc)

    rows = benchmark(run)
    assert len(rows) == nrows
    assert len(rows[0]) == ncols


@pytest.mark.parametrize("nrows,ncols", [(1, 10), (100, 5), (1000, 5)])
def test_full_pipeline_cached(benchmark, nrows, ncols, py_cache):
    """Reference: cached ParseDesc + parse rows (pure-Python cache replica)."""
    col_meta = _build_column_metadata(ncols)
    binary_buf = _build_binary_rows(nrows, ncols)
    parser = ListParser()

    # Warm cache
    _cached_parse_desc_py(col_meta, None, 4, py_cache)

    def run():
        desc, names, types = _cached_parse_desc_py(col_meta, None, 4, py_cache)
        reader = BytesIOReader(binary_buf)
        return parser.parse_rows(reader, desc)

    rows = benchmark(run)
    assert len(rows) == nrows
    assert len(rows[0]) == ncols


# ---------------------------------------------------------------------------
# Isolated benchmarks: ParseDesc only (reference, varying column counts)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("ncols", [5, 10, 20, 50])
def test_parse_desc_only_uncached(benchmark, ncols):
    """Reference: isolated ParseDesc construction — uncached."""
    col_meta = _build_column_metadata(ncols)

    benchmark(_build_uncached_parse_desc, col_meta, None, 4)


@pytest.mark.parametrize("ncols", [5, 10, 20, 50])
def test_parse_desc_only_cached(benchmark, ncols, py_cache):
    """Reference: isolated ParseDesc construction — cached (pure-Python replica)."""
    col_meta = _build_column_metadata(ncols)
    _cached_parse_desc_py(col_meta, None, 4, py_cache)  # warm

    benchmark(_cached_parse_desc_py, col_meta, None, 4, py_cache)


# ---------------------------------------------------------------------------
# Reference correctness tests (pure-Python replica)
# ---------------------------------------------------------------------------


def test_cached_same_result_as_uncached(py_cache):
    """Verify pure-Python cached path produces identical results to uncached."""
    col_meta = _build_column_metadata(10)

    desc_u, names_u, types_u = _build_uncached_parse_desc(col_meta, None, 4)
    desc_c, names_c, types_c = _cached_parse_desc_py(col_meta, None, 4, py_cache)

    assert names_u == names_c
    assert types_u == types_c
    assert len(desc_u.colnames) == len(desc_c.colnames)
    assert desc_u.protocol_version == desc_c.protocol_version

    # Second call should be cache hit and return the same desc object
    desc_c2, names_c2, types_c2 = _cached_parse_desc_py(col_meta, None, 4, py_cache)
    assert desc_c2 is desc_c  # same object from cache


def test_cache_invalidation_on_different_metadata(py_cache):
    """Different column_metadata list should produce a new ParseDesc (pure-Python)."""
    col_meta_a = _build_column_metadata(5)
    col_meta_b = _build_column_metadata(10)

    desc_a, _, _ = _cached_parse_desc_py(col_meta_a, None, 4, py_cache)
    desc_b, _, _ = _cached_parse_desc_py(col_meta_b, None, 4, py_cache)

    assert desc_a is not desc_b
    assert len(desc_a.colnames) == 5
    assert len(desc_b.colnames) == 10


def test_cache_invalidation_on_protocol_version_change(py_cache):
    """Changed protocol_version should miss the cache (pure-Python)."""
    col_meta = _build_column_metadata(5)
    desc_v4, _, _ = _cached_parse_desc_py(col_meta, None, 4, py_cache)
    desc_v5, _, _ = _cached_parse_desc_py(col_meta, None, 5, py_cache)

    assert desc_v4 is not desc_v5


def test_clear_parse_desc_cache():
    """Verify the Cython cache can be cleared."""
    clear_parse_desc_cache()  # should not raise


def test_full_pipeline_correctness(py_cache):
    """End-to-end: parse rows with cached ParseDesc produces correct data (pure-Python)."""
    ncols = 5
    nrows = 3
    col_meta = _build_column_metadata(ncols)
    binary_buf = _build_binary_rows(nrows, ncols, col_value=b"test_val")
    parser = ListParser()

    desc, names, types = _cached_parse_desc_py(col_meta, None, 4, py_cache)
    reader = BytesIOReader(binary_buf)
    rows = parser.parse_rows(reader, desc)

    assert len(rows) == nrows
    for row in rows:
        assert len(row) == ncols
        for val in row:
            assert val == "test_val"
