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

from cpython.dict cimport PyDict_Next
from cpython.ref cimport PyObject
from cassandra.parsing cimport ParseDesc, ColumnParser
from cassandra.policies import ColDesc
from cassandra.obj_parser import TupleRowParser
from cassandra.deserializers import make_deserializers

include "ioutils.pyx"

import threading

# Bound on cached prepared-statement descriptors; the oldest entry is evicted on overflow.
cdef Py_ssize_t _PARSE_DESC_CACHE_MAX_SIZE = 256

# id(result_metadata) -> (result_metadata, column_encryption_policy, protocol_version, ParseDesc).
# Holding result_metadata keeps its id() from being reused; PreparedStatement replaces
# (never mutates) result_metadata on schema change, so a new schema always misses.
cdef dict _parse_desc_cache = {}
# Serializes cache mutations and clearing; hits read the dict without it.
_parse_desc_lock = threading.Lock()


cdef ParseDesc _build_parse_desc(column_metadata, list column_names, list column_types,
                                 column_encryption_policy, int protocol_version):
    coldescs = [ColDesc(md[0], md[1], md[2]) for md in column_metadata] if column_encryption_policy is not None else None
    return ParseDesc(column_names, column_types, column_encryption_policy, coldescs,
                     make_deserializers(column_types), protocol_version)


cdef inline bint _entry_matches(tuple entry, column_metadata, column_encryption_policy, int protocol_version):
    return entry[0] is column_metadata and entry[1] is column_encryption_policy and entry[2] == protocol_version


cdef ParseDesc _get_or_build_parse_desc(column_metadata, column_encryption_policy, int protocol_version):
    cdef Py_ssize_t pos
    cdef PyObject* oldest
    key = id(column_metadata)
    cached = _parse_desc_cache.get(key)
    if cached is not None and _entry_matches(cached, column_metadata, column_encryption_policy, protocol_version):
        return cached[3]

    desc = _build_parse_desc(column_metadata, [md[2] for md in column_metadata],
                             [md[3] for md in column_metadata], column_encryption_policy, protocol_version)
    entry = (column_metadata, column_encryption_policy, protocol_version, desc)
    # replaced/evicted outlive the lock, so dropping user-owned references never runs finalizers under it.
    evicted = None
    with _parse_desc_lock:
        replaced = _parse_desc_cache.get(key)
        if replaced is not None and _entry_matches(replaced, column_metadata, column_encryption_policy, protocol_version):
            return replaced[3]  # another miss published it first
        if replaced is None and len(_parse_desc_cache) >= _PARSE_DESC_CACHE_MAX_SIZE:
            # PyDict_Next allocates nothing, so GC (and user finalizers) can't run under the lock.
            pos = 0
            PyDict_Next(_parse_desc_cache, &pos, &oldest, NULL)
            evicted = _parse_desc_cache.pop(<object>oldest)
        _parse_desc_cache[key] = entry
    return desc


def clear_parse_desc_cache():
    """Drop all cached ParseDescs. Testing/debugging hook, not a stable API."""
    global _parse_desc_cache
    fresh = {}  # allocated before the lock: GC must not run finalizers under it
    with _parse_desc_lock:
        old = _parse_desc_cache
        _parse_desc_cache = fresh
    del old  # released outside the lock


def get_parse_desc_cache_size():
    """Number of cached ParseDescs. Testing/debugging hook, not a stable API."""
    return len(_parse_desc_cache)


def make_recv_results_rows(ColumnParser colparser):
    def recv_results_rows(self, f, int protocol_version, user_type_map, result_metadata, column_encryption_policy):
        """
        Parse protocol data given as a BytesIO f into a set of columns (e.g. list of tuples)
        This is used as the recv_results_rows method of (Fast)ResultMessage
        """
        self.recv_results_metadata(f, user_type_map)

        column_metadata = self.column_metadata or result_metadata

        # Only skip_meta responses decode against the PreparedStatement's long-lived result_metadata;
        # inline metadata is a fresh list per response and would only churn the cache.
        if self.column_metadata is None and result_metadata is not None:
            desc = _get_or_build_parse_desc(column_metadata, column_encryption_policy, protocol_version)
            # Fresh lists per response: the cached desc is shared and these are handed to user row factories.
            self.column_names = list(desc.colnames)
            self.column_types = list(desc.coltypes)
        else:
            self.column_names = [md[2] for md in column_metadata]
            self.column_types = [md[3] for md in column_metadata]
            desc = _build_parse_desc(column_metadata, self.column_names, self.column_types,
                                     column_encryption_policy, protocol_version)
        reader = BytesIOReader(f.read())
        try:
            self.parsed_rows = colparser.parse_rows(reader, desc)
        except Exception:
            # Use explicitly the TupleRowParser to display better error messages for column decoding failures
            rowparser = TupleRowParser()
            reader.buf_ptr = reader.buf
            reader.pos = 0
            rowcount = read_int(reader)
            for i in range(rowcount):
                rowparser.unpack_row(reader, desc)
            # The re-parse succeeded (e.g. NumpyParser-only failure); don't leave parsed_rows unset.
            raise

    return recv_results_rows
