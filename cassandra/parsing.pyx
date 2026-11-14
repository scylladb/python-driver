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
Module containing the definitions and declarations (parsing.pxd) for parsers.
"""

from cassandra.tuple cimport tuple_new, tuple_set

cdef class ParseDesc:
    """Description of what structure to parse"""

    def __init__(self, colnames, coltypes, column_encryption_policy, coldescs, deserializers, protocol_version):
        # Store tuples rather than the lists passed in: a ParseDesc may be
        # cached and shared between executions, so it must not carry mutable
        # state that a consumer could alter.  tuple() on an existing tuple
        # returns it unchanged, so this costs nothing when the caller already
        # has tuples.
        self.colnames = tuple(colnames)
        self.coltypes = tuple(coltypes)
        self.column_encryption_policy = column_encryption_policy
        self.coldescs = tuple(coldescs)
        self.deserializers = _as_deserializer_tuple(deserializers)
        self.protocol_version = protocol_version
        self.rowsize = len(colnames)


cdef tuple _as_deserializer_tuple(deserializers):
    """Return *deserializers* as an immutable tuple.

    Accepts a tuple (already immutable), a Cython memoryview of
    Deserializers, or any other sequence.  A memoryview is read through a
    typed view and copied at C level: iterating a memoryview from Python is
    several times slower than copying it, and this runs on every result set.
    """
    cdef Deserializer[::1] d
    cdef Py_ssize_t i, n
    cdef tuple result

    if type(deserializers) is tuple:
        return deserializers

    try:
        d = deserializers
    except (TypeError, ValueError):
        # Not a buffer of Deserializers -- e.g. a plain Python list.
        return tuple(deserializers)

    n = len(d)
    result = tuple_new(n)
    for i in range(n):
        tuple_set(result, i, d[i])
    return result


cdef class ColumnParser:
    """Decode a ResultMessage into a set of columns"""

    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc):
        raise NotImplementedError


cdef class RowParser:
    """Parser for a single row"""

    cpdef unpack_row(self, BytesIOReader reader, ParseDesc desc):
        """
        Unpack a single row of data in a ResultMessage.
        """
        raise NotImplementedError
