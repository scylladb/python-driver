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

from cassandra.parsing cimport ParseDesc, ColumnParser
from cassandra.policies import ColDesc
from cassandra.obj_parser import TupleRowParser
from cassandra.deserializers import make_deserializers

include "ioutils.pyx"

# Maximum number of entries in the ParseDesc cache.  Each entry corresponds
# to a distinct PreparedStatement's result_metadata.  256 should be more
# than enough for most applications; when exceeded the oldest entry is
# evicted (single-entry eviction keeps the cache useful for applications
# that legitimately use slightly more than this many prepared statements).
cdef int _PARSE_DESC_CACHE_MAX_SIZE = 256

# Cache for ParseDesc objects keyed by id(column_metadata).
# For prepared statements, result_metadata is stored on PreparedStatement
# and reused across executions, so id() is stable.  The cache is only
# populated on the prepared-statement path (where column_metadata comes from
# result_metadata); inline metadata from non-prepared queries is always fresh
# and must not be cached to avoid unbounded growth.
#
# Cache value: (column_metadata_ref, column_encryption_policy_ref,
#               protocol_version, desc, column_names, column_types)
#
# The cached column_names/column_types are the *source* lists used to build
# the ParseDesc.  They are never handed out to a response directly: each
# response receives its own copy (see _get_or_build_parse_desc), because
# ResultSet.column_names/column_types are public, mutable attributes and
# user-supplied row factories receive the same list objects.  Handing out
# the cached lists would let one execution's mutation relabel every later
# execution of the same prepared statement.
#
# Thread safety: individual dict get/set operations are atomic under
# CPython's GIL and under free-threaded builds (PEP 703, which uses
# per-object locks on dicts).  The get-miss-build-set sequence is NOT
# transactionally atomic: two threads may both miss and both build a
# ParseDesc for the same key, with the last write winning.  This is
# benign -- no data corruption occurs, only redundant construction work
# on the first concurrent miss.  No additional locking is needed.
#
# Correctness on schema change / re-preparation: this cache's safety depends
# on PreparedStatement.result_metadata (cassandra/query.py) only ever being
# *replaced*, never mutated in place. PreparedStatement.update_result_metadata()
# -- called both on METADATA_CHANGED responses and after an UNPREPARED
# re-prepare -- always swaps in a brand-new list built fresh from the
# server's response, so a schema change necessarily produces a new id() and
# this cache misses and rebuilds a ParseDesc with the new column types,
# instead of returning stale ones. The old entry's list is kept alive only by
# this cache's own strong reference to it (the tuple stored below holds
# column_metadata itself), which also guarantees an evicted id() can never be
# reused by an unrelated object while still cached -- so an id() collision
# can never alias two different schemas. If a future change to query.py ever
# mutates result_metadata in place instead of replacing it, this cache would
# start returning stale ParseDesc/column_types for the new schema; any such
# change must also invalidate (or bypass) this cache.
cdef dict _parse_desc_cache = {}

cdef inline tuple _construct_parse_desc(object column_metadata, object column_encryption_policy, int protocol_version):
    """Build a ParseDesc and its column name/type lists from column_metadata.

    Returns (desc, column_names, column_types) where column_names and
    column_types are freshly-allocated lists owned by the caller.
    """
    cdef list column_names = [md[2] for md in column_metadata]
    cdef list column_types = [md[3] for md in column_metadata]
    cdef object desc = ParseDesc(
        column_names, column_types, column_encryption_policy,
        [ColDesc(md[0], md[1], md[2]) for md in column_metadata],
        make_deserializers(column_types), protocol_version)
    return (desc, column_names, column_types)


cdef inline tuple _get_or_build_parse_desc(object column_metadata, object column_encryption_policy, int protocol_version):
    """Look up or build a ParseDesc for the given column_metadata (cached path).

    Returns (desc, column_names, column_types).  The returned column_names
    and column_types are fresh lists owned by this response; the cached
    originals are never handed out, so a consumer that mutates them cannot
    affect any other response.
    """
    cdef object cache_key = id(column_metadata)
    cdef object cached_or_none = _parse_desc_cache.get(cache_key)

    if cached_or_none is not None:
        # Verify identity -- the object at this id must be the same list
        # and session-level settings must match.
        cached = <tuple>cached_or_none
        if (cached[0] is column_metadata and
            cached[1] is column_encryption_policy and
            cached[2] == protocol_version):
            # Cache hit: reuse the ParseDesc (the expensive part) but hand
            # this response its own copies of the column name/type lists.
            # ResultSet.column_names/column_types are public mutable
            # attributes, and the row factory is user code that receives the
            # very same list objects -- sharing them across responses would
            # let one execution's mutation relabel every later execution of
            # the same prepared statement.
            return (cached[3], list(cached[4]), list(cached[5]))

    # Cache miss -- build everything
    cdef tuple built = _construct_parse_desc(
        column_metadata, column_encryption_policy, protocol_version)
    cdef object desc = built[0]
    cdef list column_names = built[1]
    cdef list column_types = built[2]

    # Simple bounded eviction: if the cache is too large, evict a single
    # entry (the oldest inserted one) rather than clearing the whole cache.
    # Clearing everything would cause repeated full rebuilds once an
    # application legitimately uses slightly more than the maximum number of
    # prepared statements.  dict preserves insertion order, so the first key
    # is the oldest.  Eviction only happens on a miss, so it never affects
    # the hot path.
    if len(_parse_desc_cache) >= _PARSE_DESC_CACHE_MAX_SIZE:
        _parse_desc_cache.pop(next(iter(_parse_desc_cache)), None)

    # The cache keeps the source lists; every response gets a copy of them.
    _parse_desc_cache[cache_key] = (column_metadata, column_encryption_policy,
                                    protocol_version, desc, column_names, column_types)
    return (desc, list(column_names), list(column_types))


cdef inline tuple _build_parse_desc(object column_metadata, object column_encryption_policy, int protocol_version):
    """Build a ParseDesc without caching (for non-prepared inline metadata).

    Returns (desc, column_names, column_types).
    """
    return _construct_parse_desc(column_metadata, column_encryption_policy, protocol_version)


def clear_parse_desc_cache():
    """Clear the ParseDesc cache.

    This is a testing/debugging hook, not a stable public API: it may be
    removed or changed in a future release.  It is not tied to any
    Cluster's lifetime -- the cache is process-global -- so calling it
    affects every session in the process.
    """
    _parse_desc_cache.clear()


def get_parse_desc_cache_size():
    """Return the current number of entries in the ParseDesc cache.

    This is a testing/debugging hook, not a stable public API: it may be
    removed or changed in a future release.
    """
    return len(_parse_desc_cache)


def make_recv_results_rows(ColumnParser colparser):
    def recv_results_rows(self, f, int protocol_version, user_type_map, result_metadata, column_encryption_policy):
        """
        Parse protocol data given as a BytesIO f into a set of columns (e.g. list of tuples)
        This is used as the recv_results_rows method of (Fast)ResultMessage
        """
        self.recv_results_metadata(f, user_type_map)

        column_metadata = self.column_metadata or result_metadata

        # Only use the cache for prepared statements (self.column_metadata is
        # None, so column_metadata comes from result_metadata which is a
        # stable list stored on PreparedStatement).  Inline metadata from
        # non-prepared queries creates a fresh list every time and would
        # cause unbounded cache growth.
        if self.column_metadata is None and result_metadata is not None:
            desc, self.column_names, self.column_types = _get_or_build_parse_desc(
                column_metadata, column_encryption_policy, protocol_version)
        else:
            desc, self.column_names, self.column_types = _build_parse_desc(
                column_metadata, column_encryption_policy, protocol_version)

        reader = BytesIOReader(f.read())
        try:
            self.parsed_rows = colparser.parse_rows(reader, desc)
        except Exception as e:
            # Use explicitly the TupleRowParser to display better error messages for column decoding failures
            rowparser = TupleRowParser()
            reader.buf_ptr = reader.buf
            reader.pos = 0
            rowcount = read_int(reader)
            for i in range(rowcount):
                rowparser.unpack_row(reader, desc)

    return recv_results_rows
