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
Cython-optimized serializers for CQL types.

Mirrors the architecture of deserializers.pyx. Currently implements
optimized serialization for:
- FloatType (4-byte big-endian float)
- DoubleType (8-byte big-endian double)
- Int32Type (4-byte big-endian signed int)
- VectorType (type-specialized for float/double/int32, generic fallback)

For all other types, GenericSerializer delegates to the Python-level
cqltype.serialize() classmethod.
"""

from libc.stdint cimport int32_t, uint32_t
from libc.string cimport memcpy
from libc.stdlib cimport malloc, free
from cpython.bytes cimport PyBytes_FromStringAndSize

from cassandra import cqltypes

cdef bint is_little_endian
from cassandra.util import is_little_endian


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

cdef class Serializer:
    """Cython-based serializer class for a cqltype"""

    def __init__(self, cqltype):
        self.cqltype = cqltype

    cpdef bytes serialize(self, object value, int protocol_version):
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Scalar serializers
# ---------------------------------------------------------------------------

cdef class SerFloatType(Serializer):
    """Serialize a Python float to 4-byte big-endian IEEE 754."""

    cpdef bytes serialize(self, object value, int protocol_version):
        cdef float val = <float>value
        cdef char out[4]
        cdef char *src = <char *>&val

        if is_little_endian:
            out[0] = src[3]
            out[1] = src[2]
            out[2] = src[1]
            out[3] = src[0]
        else:
            memcpy(out, src, 4)

        return PyBytes_FromStringAndSize(out, 4)


cdef class SerDoubleType(Serializer):
    """Serialize a Python float to 8-byte big-endian IEEE 754."""

    cpdef bytes serialize(self, object value, int protocol_version):
        cdef double val = <double>value
        cdef char out[8]
        cdef char *src = <char *>&val

        if is_little_endian:
            out[0] = src[7]
            out[1] = src[6]
            out[2] = src[5]
            out[3] = src[4]
            out[4] = src[3]
            out[5] = src[2]
            out[6] = src[1]
            out[7] = src[0]
        else:
            memcpy(out, src, 8)

        return PyBytes_FromStringAndSize(out, 8)


cdef class SerInt32Type(Serializer):
    """Serialize a Python int to 4-byte big-endian signed int32."""

    cpdef bytes serialize(self, object value, int protocol_version):
        cdef int32_t val = <int32_t>value
        cdef char out[4]
        cdef char *src = <char *>&val

        if is_little_endian:
            out[0] = src[3]
            out[1] = src[2]
            out[2] = src[1]
            out[3] = src[0]
        else:
            memcpy(out, src, 4)

        return PyBytes_FromStringAndSize(out, 4)


# ---------------------------------------------------------------------------
# Type detection helpers
# ---------------------------------------------------------------------------

cdef inline bint _is_float_type(object subtype):
    return subtype is cqltypes.FloatType or issubclass(subtype, cqltypes.FloatType)

cdef inline bint _is_double_type(object subtype):
    return subtype is cqltypes.DoubleType or issubclass(subtype, cqltypes.DoubleType)

cdef inline bint _is_int32_type(object subtype):
    return subtype is cqltypes.Int32Type or issubclass(subtype, cqltypes.Int32Type)


# ---------------------------------------------------------------------------
# VectorType serializer
# ---------------------------------------------------------------------------

cdef class SerVectorType(Serializer):
    """
    Optimized Cython serializer for VectorType.

    For float, double, and int32 vectors, pre-allocates a contiguous buffer
    and uses C-level byte swapping. For other subtypes, falls back to
    per-element Python serialization.
    """

    cdef int vector_size
    cdef object subtype
    # 0 = generic, 1 = float, 2 = double, 3 = int32
    cdef int type_code

    def __init__(self, cqltype):
        super().__init__(cqltype)
        self.vector_size = cqltype.vector_size
        self.subtype = cqltype.subtype

        if _is_float_type(self.subtype):
            self.type_code = 1
        elif _is_double_type(self.subtype):
            self.type_code = 2
        elif _is_int32_type(self.subtype):
            self.type_code = 3
        else:
            self.type_code = 0

    cpdef bytes serialize(self, object value, int protocol_version):
        cdef int v_length = len(value)
        if v_length != self.vector_size:
            raise ValueError(
                "Expected sequence of size %d for vector of type %s and "
                "dimension %d, observed sequence of length %d" % (
                    self.vector_size, self.subtype.typename,
                    self.vector_size, v_length))

        if self.type_code == 1:
            return self._serialize_float(value)
        elif self.type_code == 2:
            return self._serialize_double(value)
        elif self.type_code == 3:
            return self._serialize_int32(value)
        else:
            return self._serialize_generic(value, protocol_version)

    cdef inline bytes _serialize_float(self, object values):
        """Serialize a list of floats into a contiguous big-endian buffer."""
        cdef Py_ssize_t i
        cdef Py_ssize_t buf_size = self.vector_size * 4
        cdef char *buf = <char *>malloc(buf_size)
        if buf == NULL:
            raise MemoryError("Failed to allocate %d bytes for vector serialization" % buf_size)

        cdef float val
        cdef char *src
        cdef char *dst

        try:
            for i in range(self.vector_size):
                val = <float>values[i]
                src = <char *>&val
                dst = buf + i * 4

                if is_little_endian:
                    dst[0] = src[3]
                    dst[1] = src[2]
                    dst[2] = src[1]
                    dst[3] = src[0]
                else:
                    memcpy(dst, src, 4)

            return PyBytes_FromStringAndSize(buf, buf_size)
        finally:
            free(buf)

    cdef inline bytes _serialize_double(self, object values):
        """Serialize a list of doubles into a contiguous big-endian buffer."""
        cdef Py_ssize_t i
        cdef Py_ssize_t buf_size = self.vector_size * 8
        cdef char *buf = <char *>malloc(buf_size)
        if buf == NULL:
            raise MemoryError("Failed to allocate %d bytes for vector serialization" % buf_size)

        cdef double val
        cdef char *src
        cdef char *dst

        try:
            for i in range(self.vector_size):
                val = <double>values[i]
                src = <char *>&val
                dst = buf + i * 8

                if is_little_endian:
                    dst[0] = src[7]
                    dst[1] = src[6]
                    dst[2] = src[5]
                    dst[3] = src[4]
                    dst[4] = src[3]
                    dst[5] = src[2]
                    dst[6] = src[1]
                    dst[7] = src[0]
                else:
                    memcpy(dst, src, 8)

            return PyBytes_FromStringAndSize(buf, buf_size)
        finally:
            free(buf)

    cdef inline bytes _serialize_int32(self, object values):
        """Serialize a list of int32 values into a contiguous big-endian buffer."""
        cdef Py_ssize_t i
        cdef Py_ssize_t buf_size = self.vector_size * 4
        cdef char *buf = <char *>malloc(buf_size)
        if buf == NULL:
            raise MemoryError("Failed to allocate %d bytes for vector serialization" % buf_size)

        cdef int32_t val
        cdef char *src
        cdef char *dst

        try:
            for i in range(self.vector_size):
                val = <int32_t>values[i]
                src = <char *>&val
                dst = buf + i * 4

                if is_little_endian:
                    dst[0] = src[3]
                    dst[1] = src[2]
                    dst[2] = src[1]
                    dst[3] = src[0]
                else:
                    memcpy(dst, src, 4)

            return PyBytes_FromStringAndSize(buf, buf_size)
        finally:
            free(buf)

    cdef inline bytes _serialize_generic(self, object values, int protocol_version):
        """Fallback: element-by-element Python serialization for non-optimized types."""
        import io
        from cassandra.marshal import uvint_pack

        serialized_size = self.subtype.serial_size()
        buf = io.BytesIO()
        for item in values:
            item_bytes = self.subtype.serialize(item, protocol_version)
            if serialized_size is None:
                buf.write(uvint_pack(len(item_bytes)))
            buf.write(item_bytes)
        return buf.getvalue()


# ---------------------------------------------------------------------------
# Generic serializer (fallback for all other types)
# ---------------------------------------------------------------------------

cdef class GenericSerializer(Serializer):
    """
    Wraps a generic cqltype for serialization, delegating to the Python-level
    cqltype.serialize() classmethod.
    """

    cpdef bytes serialize(self, object value, int protocol_version):
        return self.cqltype.serialize(value, protocol_version)

    def __repr__(self):
        return "GenericSerializer(%s)" % (self.cqltype,)


# ---------------------------------------------------------------------------
# Lookup and factory
# ---------------------------------------------------------------------------

cdef dict _ser_classes = {}

cpdef Serializer find_serializer(cqltype):
    """Find a serializer for a cqltype."""

    # For VectorType, always use SerVectorType (it handles generic subtypes internally)
    if issubclass(cqltype, cqltypes.VectorType):
        return SerVectorType(cqltype)

    # For scalar types with dedicated serializers, look up by name
    name = 'Ser' + cqltype.__name__
    cls = _ser_classes.get(name)
    if cls is not None:
        return cls(cqltype)

    # Fallback to generic
    return GenericSerializer(cqltype)


def make_serializers(cqltypes_list):
    """Create a list of Serializer objects for each given cqltype."""
    return [find_serializer(ct) for ct in cqltypes_list]


# Build the lookup dict for scalar serializers at module load time
_ser_classes['SerFloatType'] = SerFloatType
_ser_classes['SerDoubleType'] = SerDoubleType
_ser_classes['SerInt32Type'] = SerInt32Type
