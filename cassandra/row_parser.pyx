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
# Thread safety: individual dict operations are atomic under CPython's GIL
# and under free-threaded builds (PEP 703).  This cache relies on that
# guarantee; no additional locking is needed.
cdef dict _parse_desc_cache = {}

cdef inline object _get_or_build_parse_desc(object column_metadata, object column_encryption_policy, int protocol_version):
    """Look up or build a ParseDesc for the given column_metadata (cached path)."""
    cdef object cache_key = id(column_metadata)
    cdef object cached_or_none = _parse_desc_cache.get(cache_key)

    if cached_or_none is not None:
        # Verify identity -- the object at this id must be the same list
        # and session-level settings must match
        cached = <tuple>cached_or_none
        if (cached[0] is column_metadata and
            cached[1] is column_encryption_policy and
            cached[2] == protocol_version):
            return cached  # hit

    # Cache miss -- build everything
    cdef list column_names = [md[2] for md in column_metadata]
    cdef list column_types = [md[3] for md in column_metadata]
    cdef object desc = ParseDesc(
        column_names, column_types, column_encryption_policy,
        [ColDesc(md[0], md[1], md[2]) for md in column_metadata],
        make_deserializers(column_types), protocol_version)

    cdef tuple cached_entry = (column_metadata, column_encryption_policy,
                               protocol_version, desc, column_names, column_types)
    _parse_desc_cache[cache_key] = cached_entry
    return cached_entry


cdef inline object _build_parse_desc(object column_metadata, object column_encryption_policy, int protocol_version):
    """Build a ParseDesc without caching (for non-prepared inline metadata)."""
    cdef list column_names = [md[2] for md in column_metadata]
    cdef list column_types = [md[3] for md in column_metadata]
    cdef object desc = ParseDesc(
        column_names, column_types, column_encryption_policy,
        [ColDesc(md[0], md[1], md[2]) for md in column_metadata],
        make_deserializers(column_types), protocol_version)
    return (column_metadata, column_encryption_policy, protocol_version,
            desc, column_names, column_types)


def clear_parse_desc_cache():
    """Clear the ParseDesc cache. Exposed for testing and Cluster.shutdown()."""
    _parse_desc_cache.clear()


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
            cached = _get_or_build_parse_desc(column_metadata, column_encryption_policy, protocol_version)
        else:
            cached = _build_parse_desc(column_metadata, column_encryption_policy, protocol_version)
        self.column_names = cached[4]
        self.column_types = cached[5]
        desc = cached[3]

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
