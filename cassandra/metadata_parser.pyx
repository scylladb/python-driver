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
Cython-optimized metadata parsing for CQL protocol ResultMessage.

Uses BytesIOReader for zero-copy reads, eliminating per-read bytes allocation
that dominates recv_results_metadata cost.
"""

include "ioutils.pyx"


# ---------- low-level readers on BytesIOReader ----------
# read_int(BytesIOReader) and read_short(BytesIOReader) are provided by ioutils.pyx

cdef inline str read_string_br(BytesIOReader reader):
    """Read a [string]: a [short] n, followed by n bytes of UTF-8."""
    cdef uint16_t size = read_short(reader)
    cdef char *ptr = reader.read(size)
    return ptr[:size].decode('utf8')

cdef inline bytes read_binary_string_br(BytesIOReader reader):
    """Read a [short bytes]: a [short] n, followed by n raw bytes."""
    cdef uint16_t size = read_short(reader)
    cdef char *ptr = reader.read(size)
    return ptr[:size]

cdef inline bytes read_binary_longstring_br(BytesIOReader reader):
    """Read a [bytes]: an [int] n, followed by n raw bytes.

    Unlike row values (where a negative length is a valid NULL/"not set"
    sentinel handled by get_buf() in ioutils.pyx), the [bytes] values read
    here (paging_state, result_metadata_id) are only read when the protocol
    says they are present, so a negative length here means a malformed or
    corrupted frame. Reject it explicitly instead of passing it through:
    BytesIOReader.read() treats any negative n as "read to the end of the
    buffer" rather than raising, and slicing a char* with a negative stop
    bound is not a validated length -- on CPython that currently surfaces
    as an opaque SystemError from PyBytes_FromStringAndSize instead of a
    clean, catchable protocol error.
    """
    cdef int32_t size = read_int(reader)
    if size < 0:
        raise ValueError(
            "Invalid negative length %d for [bytes] value" % size)
    cdef char *ptr = reader.read(size)
    return ptr[:size]


# ---------- flag constants (mirrored from ResultMessage) ----------
# These MUST stay in sync with the class attributes in ResultMessage (protocol.py).
# They are duplicated here as compile-time DEF constants for Cython performance.

DEF _FLAGS_GLOBAL_TABLES_SPEC = 0x0001
DEF _HAS_MORE_PAGES_FLAG      = 0x0002
DEF _NO_METADATA_FLAG          = 0x0004
DEF _METADATA_ID_FLAG          = 0x0008
DEF _CONTINUOUS_PAGING_FLAG      = 0x40000000
DEF _CONTINUOUS_PAGING_LAST_FLAG = 0x80000000


# ---------- read_type using BytesIOReader ----------

cdef object _read_type_br(BytesIOReader reader, dict type_codes_map, object user_type_map,
                           object ListType, object SetType, object MapType,
                           object TupleType, object UserType, object CUSTOM_TYPE,
                           object lookup_casstype, object NotSupportedError):
    """
    Cython version of ResultMessage.read_type() operating on BytesIOReader.

    Parameters are passed in to avoid module-level imports from protocol.py
    (which would create circular dependencies). They are captured once in the
    closure created by make_recv_results_metadata().
    """
    cdef uint16_t optid = read_short(reader)

    typeclass = type_codes_map.get(optid)
    if typeclass is None:
        raise NotSupportedError(
            "Unknown data type code 0x%04x. Have to skip entire result set." % (optid,))

    if typeclass is ListType or typeclass is SetType:
        subtype = _read_type_br(reader, type_codes_map, user_type_map,
                                ListType, SetType, MapType, TupleType, UserType,
                                CUSTOM_TYPE, lookup_casstype, NotSupportedError)
        typeclass = typeclass.apply_parameters((subtype,))
    elif typeclass is MapType:
        keysubtype = _read_type_br(reader, type_codes_map, user_type_map,
                                   ListType, SetType, MapType, TupleType, UserType,
                                   CUSTOM_TYPE, lookup_casstype, NotSupportedError)
        valsubtype = _read_type_br(reader, type_codes_map, user_type_map,
                                   ListType, SetType, MapType, TupleType, UserType,
                                   CUSTOM_TYPE, lookup_casstype, NotSupportedError)
        typeclass = typeclass.apply_parameters((keysubtype, valsubtype))
    elif typeclass is TupleType:
        num_items = read_short(reader)
        types = tuple(_read_type_br(reader, type_codes_map, user_type_map,
                                    ListType, SetType, MapType, TupleType, UserType,
                                    CUSTOM_TYPE, lookup_casstype, NotSupportedError)
                      for _ in range(num_items))
        typeclass = typeclass.apply_parameters(types)
    elif typeclass is UserType:
        ks = read_string_br(reader)
        udt_name = read_string_br(reader)
        num_fields = read_short(reader)
        names_and_types = tuple(
            (read_string_br(reader),
             _read_type_br(reader, type_codes_map, user_type_map,
                           ListType, SetType, MapType, TupleType, UserType,
                           CUSTOM_TYPE, lookup_casstype, NotSupportedError))
            for _ in range(num_fields))
        # zip(*()) with num_fields == 0 raises ValueError (not enough values
        # to unpack), matching ResultMessage.read_type()'s pure-Python
        # behavior for the same malformed/degenerate case: a real UDT
        # always has at least one field, so a UserType with num_fields == 0
        # on the wire indicates a corrupted or malformed frame and should
        # raise rather than silently producing an empty-field UDT.
        names, types = zip(*names_and_types)
        specialized_type = typeclass.make_udt_class(ks, udt_name, names, types)
        specialized_type.mapped_class = user_type_map.get(ks, {}).get(udt_name)
        typeclass = specialized_type
    elif typeclass is CUSTOM_TYPE:
        classname = read_string_br(reader)
        typeclass = lookup_casstype(classname)

    return typeclass


# ---------- public factory: creates closures that capture type objects ----------

def make_recv_results_metadata(dict type_codes_map, object CUSTOM_TYPE,
                                object ListType, object SetType, object MapType,
                                object TupleType, object UserType,
                                object lookup_casstype, object NotSupportedError):
    """
    Factory that returns a recv_results_metadata function suitable for use
    as an unbound method replacement on FastResultMessage.

    The closure captures the type-code map and type objects once, so they
    don't have to be looked up on every call.

    All type objects -- including the NotSupportedError exception class
    raised for an unrecognized type code -- are passed in by the caller
    (cython_protocol_handler() in protocol.py, which already has them as
    module-level names) rather than imported here. cassandra.metadata_parser
    is itself imported from inside cassandra.protocol's own module body
    (cython_protocol_handler() runs while "import cassandra.protocol" is
    still executing), so a module-level
    "from cassandra.protocol import ResultMessage, CUSTOM_TYPE" here would be a
    self-referential import back into the partially-initialized protocol
    module -- the same class of import-order fragility that used to make
    cassandra.cython_deps.HAVE_CYTHON detection depend on which cassandra
    module a process happened to import first. It happens to work today only
    because ResultMessage/CUSTOM_TYPE are defined earlier in protocol.py than
    cython_protocol_handler() is called; passing them in avoids relying on
    that ordering at all.
    """
    def read_type_br_closure(BytesIOReader reader, user_type_map):
        return _read_type_br(reader, type_codes_map, user_type_map,
                             ListType, SetType, MapType, TupleType, UserType,
                             CUSTOM_TYPE, lookup_casstype, NotSupportedError)

    def recv_results_metadata(self, BytesIOReader reader, user_type_map):
        """
        Cython-optimized recv_results_metadata operating on BytesIOReader.
        Replaces ResultMessage.recv_results_metadata.
        """
        cdef int32_t flags = read_int(reader)
        cdef int32_t colcount = read_int(reader)

        if flags & _HAS_MORE_PAGES_FLAG:
            self.paging_state = read_binary_longstring_br(reader)

        if flags & _NO_METADATA_FLAG:
            return

        if flags & _CONTINUOUS_PAGING_FLAG:
            self.continuous_paging_seq = read_int(reader)
            self.continuous_paging_last = flags & _CONTINUOUS_PAGING_LAST_FLAG

        if flags & _METADATA_ID_FLAG:
            self.result_metadata_id = read_binary_string_br(reader)

        cdef str ksname, cfname, colname
        cdef object coltype
        cdef int i
        cdef list column_metadata = [None] * colcount

        if flags & _FLAGS_GLOBAL_TABLES_SPEC:
            ksname = read_string_br(reader)
            cfname = read_string_br(reader)
            for i in range(colcount):
                colname = read_string_br(reader)
                coltype = read_type_br_closure(reader, user_type_map)
                column_metadata[i] = (ksname, cfname, colname, coltype)
        else:
            for i in range(colcount):
                ksname = read_string_br(reader)
                cfname = read_string_br(reader)
                colname = read_string_br(reader)
                coltype = read_type_br_closure(reader, user_type_map)
                column_metadata[i] = (ksname, cfname, colname, coltype)

        self.column_metadata = column_metadata

    return recv_results_metadata
