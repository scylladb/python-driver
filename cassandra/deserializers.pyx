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


from libc.stdint cimport int32_t, int64_t, int16_t, uint16_t, uint32_t
from libc.string cimport memcpy

include 'cython_marshal.pyx'
from cassandra.buffer cimport Buffer, to_bytes, from_ptr_and_size
from cassandra.cython_utils cimport datetime_from_timestamp

from cython.view cimport array as cython_array
from cassandra.tuple cimport tuple_new, tuple_set

import socket
from decimal import Decimal
from uuid import UUID

from cassandra import cqltypes
from cassandra import util

# Import numpy availability flag and conditionally import numpy
from cassandra.cython_deps import HAVE_NUMPY
if HAVE_NUMPY:
    import numpy as np

cdef class Deserializer:
    """Cython-based deserializer class for a cqltype"""

    def __init__(self, cqltype):
        self.cqltype = cqltype
        self.empty_binary_ok = cqltype.empty_binary_ok

    cdef deserialize(self, Buffer *buf, int protocol_version):
        raise NotImplementedError


cdef class DesBytesType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if buf.size == 0:
            return b""
        return to_bytes(buf)

# this is to facilitate cqlsh integration, which requires bytearrays for BytesType
# It is switched in by simply overwriting DesBytesType:
# deserializers.DesBytesType = deserializers.DesBytesTypeByteArray
cdef class DesBytesTypeByteArray(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if buf.size == 0:
            return bytearray()
        return bytearray(buf.ptr[:buf.size])

# TODO: Use libmpdec: http://www.bytereef.org/mpdecimal/index.html
cdef class DesDecimalType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef int32_t scale = unpack_num[int32_t](buf)

        # Create a view of the remaining bytes (after the 4-byte scale)
        cdef Buffer varint_buf
        from_ptr_and_size(buf.ptr + 4, buf.size - 4, &varint_buf)
        unscaled = varint_unpack(&varint_buf)

        return Decimal('%de%d' % (unscaled, -scale))


cdef class DesUUIDType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return UUID(bytes=to_bytes(buf))


cdef class DesBooleanType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if unpack_num[int8_t](buf):
            return True
        return False


cdef class DesByteType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[int8_t](buf)


cdef class DesAsciiType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if buf.size == 0:
            return ""
        return to_bytes(buf).decode('ascii')


cdef class DesFloatType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[float](buf)


cdef class DesDoubleType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[double](buf)


cdef class DesLongType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[int64_t](buf)


cdef class DesInt32Type(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[int32_t](buf)


cdef class DesIntegerType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return varint_unpack(buf)


cdef class DesInetAddressType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef bytes byts = to_bytes(buf)

        # TODO: optimize inet_ntop, inet_ntoa
        if buf.size == 16:
            return util.inet_ntop(socket.AF_INET6, byts)
        else:
            # util.inet_pton could also handle, but this is faster
            # since we've already determined the AF
            return socket.inet_ntoa(byts)


cdef class DesCounterColumnType(DesLongType):
    pass


cdef class DesDateType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef double timestamp = unpack_num[int64_t](buf) / 1000.0
        return datetime_from_timestamp(timestamp)


cdef class TimestampType(DesDateType):
    pass


cdef class TimeUUIDType(DesDateType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return UUID(bytes=to_bytes(buf))


# Values of the 'date'` type are encoded as 32-bit unsigned integers
# representing a number of days with epoch (January 1st, 1970) at the center of the
# range (2^31).
EPOCH_OFFSET_DAYS = 2 ** 31

cdef class DesSimpleDateType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        days = unpack_num[uint32_t](buf) - EPOCH_OFFSET_DAYS
        return util.Date(days)


cdef class DesShortType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return unpack_num[int16_t](buf)


cdef class DesTimeType(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return util.Time(unpack_num[int64_t](buf))


cdef class DesUTF8Type(Deserializer):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        if buf.size == 0:
            return ""
        cdef val = to_bytes(buf)
        return val.decode('utf8')


cdef class DesVarcharType(DesUTF8Type):
    pass


#--------------------------------------------------------------------------
# Vector deserialization

cdef inline bint _is_float_type(object subtype):
    return subtype is cqltypes.FloatType or issubclass(subtype, cqltypes.FloatType)

cdef inline bint _is_double_type(object subtype):
    return subtype is cqltypes.DoubleType or issubclass(subtype, cqltypes.DoubleType)

cdef inline bint _is_int32_type(object subtype):
    return subtype is cqltypes.Int32Type or issubclass(subtype, cqltypes.Int32Type)

cdef inline bint _is_int64_type(object subtype):
    return subtype is cqltypes.LongType or issubclass(subtype, cqltypes.LongType)

cdef inline bint _is_int16_type(object subtype):
    return subtype is cqltypes.ShortType or issubclass(subtype, cqltypes.ShortType)

cdef inline list _deserialize_numpy_vector(Buffer *buf, int vector_size, str dtype):
    """Unified numpy deserialization for large vectors"""
    return np.frombuffer(buf.ptr[:buf.size], dtype=dtype, count=vector_size).tolist()

cdef class DesVectorType(Deserializer):
    """
    Optimized Cython deserializer for VectorType.

    For float and double vectors, uses direct memory access with C-level casting
    for significantly better performance than Python-level deserialization.
    """

    cdef int vector_size
    cdef object subtype

    def __init__(self, cqltype):
        super().__init__(cqltype)
        self.vector_size = cqltype.vector_size
        self.subtype = cqltype.subtype

    def deserialize_bytes(self, bytes data, int protocol_version):
        """Python-callable wrapper for deserialize that takes bytes."""
        cdef Buffer buf
        buf.ptr = <char*>data
        buf.size = len(data)
        return self.deserialize(&buf, protocol_version)

    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef int expected_size
        cdef int elem_size
        cdef bint use_numpy = HAVE_NUMPY and self.vector_size >= 32

        # Determine element type, size, and dispatch appropriately
        if _is_float_type(self.subtype):
            elem_size = 4
            expected_size = self.vector_size * elem_size
            if buf.size == expected_size:
                if use_numpy:
                    return _deserialize_numpy_vector(buf, self.vector_size, '>f4')
                return self._deserialize_float(buf)
            raise ValueError(
                f"Expected vector of type {self.subtype.typename} and dimension {self.vector_size} "
                f"to have serialized size {expected_size}; observed serialized size of {buf.size} instead")
        elif _is_double_type(self.subtype):
            elem_size = 8
            expected_size = self.vector_size * elem_size
            if buf.size == expected_size:
                if use_numpy:
                    return _deserialize_numpy_vector(buf, self.vector_size, '>f8')
                return self._deserialize_double(buf)
            raise ValueError(
                f"Expected vector of type {self.subtype.typename} and dimension {self.vector_size} "
                f"to have serialized size {expected_size}; observed serialized size of {buf.size} instead")
        elif _is_int32_type(self.subtype):
            elem_size = 4
            expected_size = self.vector_size * elem_size
            if buf.size == expected_size:
                if use_numpy:
                    return _deserialize_numpy_vector(buf, self.vector_size, '>i4')
                return self._deserialize_int32(buf)
            raise ValueError(
                f"Expected vector of type {self.subtype.typename} and dimension {self.vector_size} "
                f"to have serialized size {expected_size}; observed serialized size of {buf.size} instead")
        elif _is_int64_type(self.subtype):
            elem_size = 8
            expected_size = self.vector_size * elem_size
            if buf.size == expected_size:
                if use_numpy:
                    return _deserialize_numpy_vector(buf, self.vector_size, '>i8')
                return self._deserialize_int64(buf)
            raise ValueError(
                f"Expected vector of type {self.subtype.typename} and dimension {self.vector_size} "
                f"to have serialized size {expected_size}; observed serialized size of {buf.size} instead")
        elif _is_int16_type(self.subtype):
            elem_size = 2
            expected_size = self.vector_size * elem_size
            if buf.size == expected_size:
                if use_numpy:
                    return _deserialize_numpy_vector(buf, self.vector_size, '>i2')
                return self._deserialize_int16(buf)
            raise ValueError(
                f"Expected vector of type {self.subtype.typename} and dimension {self.vector_size} "
                f"to have serialized size {expected_size}; observed serialized size of {buf.size} instead")
        else:
            # Unsupported type, use generic deserialization
            return self._deserialize_generic(buf, protocol_version)

    cdef inline list _deserialize_float(self, Buffer *buf):
        """Deserialize float vector using direct C-level access with byte swapping"""
        cdef Py_ssize_t i
        cdef list result
        cdef float temp
        cdef uint32_t temp32

        result = [None] * self.vector_size
        for i in range(self.vector_size):
            # Read 4 bytes and convert from big-endian
            temp32 = ntohl((<uint32_t*>(buf.ptr + i * 4))[0])
            temp = (<float*>&temp32)[0]
            result[i] = temp

        return result

    cdef inline list _deserialize_double(self, Buffer *buf):
        """Deserialize double vector using direct C-level access with byte swapping"""
        cdef Py_ssize_t i
        cdef list result
        cdef double temp
        cdef char *src_bytes
        cdef char *out_bytes
        cdef int j

        result = [None] * self.vector_size
        for i in range(self.vector_size):
            src_bytes = buf.ptr + i * 8
            out_bytes = <char*>&temp

            # Swap bytes for big-endian to native conversion
            if is_little_endian:
                for j in range(8):
                    out_bytes[7 - j] = src_bytes[j]
            else:
                memcpy(&temp, src_bytes, 8)

            result[i] = temp

        return result

    cdef inline list _deserialize_int32(self, Buffer *buf):
        """Deserialize int32 vector using direct C-level access with ntohl"""
        cdef Py_ssize_t i
        cdef list result
        cdef int32_t temp

        result = [None] * self.vector_size
        for i in range(self.vector_size):
            temp = <int32_t>ntohl((<uint32_t*>(buf.ptr + i * 4))[0])
            result[i] = temp

        return result

    cdef inline list _deserialize_int64(self, Buffer *buf):
        """Deserialize int64/long vector using direct C-level access with byte swapping"""
        cdef Py_ssize_t i
        cdef list result
        cdef int64_t temp
        cdef char *src_bytes
        cdef char *out_bytes
        cdef int j

        result = [None] * self.vector_size
        for i in range(self.vector_size):
            src_bytes = buf.ptr + i * 8
            out_bytes = <char*>&temp

            # Swap bytes for big-endian to native conversion
            if is_little_endian:
                for j in range(8):
                    out_bytes[7 - j] = src_bytes[j]
            else:
                memcpy(&temp, src_bytes, 8)

            result[i] = temp

        return result

    cdef inline list _deserialize_int16(self, Buffer *buf):
        """Deserialize int16/short vector using direct C-level access with ntohs"""
        cdef Py_ssize_t i
        cdef list result
        cdef int16_t temp

        result = [None] * self.vector_size
        for i in range(self.vector_size):
            temp = <int16_t>ntohs((<uint16_t*>(buf.ptr + i * 2))[0])
            result[i] = temp

        return result

    cdef inline list _deserialize_generic(self, Buffer *buf, int protocol_version):
        """Fallback: element-by-element deserialization for non-optimized types"""
        cdef Py_ssize_t i
        cdef Buffer elem_buf
        cdef int offset = 0
        cdef int serialized_size = self.subtype.serial_size()
        cdef list result = [None] * self.vector_size

        if serialized_size is None:
            raise ValueError(
                f"VectorType with variable-size subtype {self.subtype.typename} "
                "is not supported in Cython deserializer")

        for i in range(self.vector_size):
            from_ptr_and_size(buf.ptr + offset, serialized_size, &elem_buf)
            result[i] = self.subtype.deserialize(to_bytes(&elem_buf), protocol_version)
            offset += serialized_size

        return result


cdef class _DesParameterizedType(Deserializer):


    cdef object subtypes
    cdef Deserializer[::1] deserializers
    cdef Py_ssize_t subtypes_len

    def __init__(self, cqltype):
        super().__init__(cqltype)
        self.subtypes = cqltype.subtypes
        self.deserializers = make_deserializers(cqltype.subtypes)
        self.subtypes_len = len(self.subtypes)


cdef class _DesSingleParamType(_DesParameterizedType):
    cdef Deserializer deserializer

    def __init__(self, cqltype):
        assert cqltype.subtypes and len(cqltype.subtypes) == 1, cqltype.subtypes
        super().__init__(cqltype)
        self.deserializer = self.deserializers[0]


#--------------------------------------------------------------------------
# List and set deserialization

cdef class DesListType(_DesSingleParamType):
    cdef deserialize(self, Buffer *buf, int protocol_version):

        result = _deserialize_list_or_set(
            buf, protocol_version, self.deserializer)

        return result

cdef class DesSetType(DesListType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return util.sortedset(DesListType.deserialize(self, buf, protocol_version))


cdef list _deserialize_list_or_set(Buffer *buf, int protocol_version,
                                   Deserializer deserializer):
    """
    Deserialize a list or set.
    """
    cdef Buffer itemlen_buf
    cdef Buffer elem_buf

    cdef int32_t numelements
    cdef int offset
    cdef list result = []

    _unpack_len(buf, 0, &numelements)
    offset = sizeof(int32_t)
    protocol_version = max(3, protocol_version)
    for _ in range(numelements):
        subelem(buf, &elem_buf, &offset)
        result.append(from_binary(deserializer, &elem_buf, protocol_version))

    return result


cdef inline int subelem(
        Buffer *buf, Buffer *elem_buf, int* offset) except -1:
    """
    Read the next element from the buffer: first read the size (in bytes) of the
    element, then fill elem_buf with a newly sliced buffer of this size (and the
    right offset).
    """
    cdef int32_t elemlen

    _unpack_len(buf, offset[0], &elemlen)
    offset[0] += sizeof(int32_t)
    from_ptr_and_size(buf.ptr + offset[0], elemlen, elem_buf)
    offset[0] += elemlen
    return 0


cdef inline int _unpack_len(Buffer *buf, int offset, int32_t *output) except -1:
    """Read a big-endian int32 at the given offset using direct pointer access."""
    cdef uint32_t *src = <uint32_t*>(buf.ptr + offset)
    output[0] = <int32_t>ntohl(src[0])
    return 0

#--------------------------------------------------------------------------
# Map deserialization

cdef class DesMapType(_DesParameterizedType):

    cdef Deserializer key_deserializer, val_deserializer

    def __init__(self, cqltype):
        super().__init__(cqltype)
        self.key_deserializer = self.deserializers[0]
        self.val_deserializer = self.deserializers[1]

    cdef deserialize(self, Buffer *buf, int protocol_version):
        key_type, val_type = self.cqltype.subtypes

        result = _deserialize_map(
            buf, protocol_version,
            self.key_deserializer, self.val_deserializer,
            key_type, val_type)

        return result


cdef _deserialize_map(Buffer *buf, int protocol_version,
                      Deserializer key_deserializer, Deserializer val_deserializer,
                      key_type, val_type):
    cdef Buffer key_buf, val_buf
    cdef Buffer itemlen_buf

    cdef int32_t numelements
    cdef int offset
    cdef list result = []

    _unpack_len(buf, 0, &numelements)
    offset = sizeof(int32_t)
    themap = util.OrderedMapSerializedKey(key_type, protocol_version)
    protocol_version = max(3, protocol_version)
    for _ in range(numelements):
        subelem(buf, &key_buf, &offset)
        subelem(buf, &val_buf, &offset)
        key = from_binary(key_deserializer, &key_buf, protocol_version)
        val = from_binary(val_deserializer, &val_buf, protocol_version)
        themap._insert_unchecked(key, to_bytes(&key_buf), val)

    return themap

#--------------------------------------------------------------------------

cdef class DesTupleType(_DesParameterizedType):

    # TODO: Use TupleRowParser to parse these tuples

    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef Py_ssize_t i, p
        cdef int32_t itemlen
        cdef tuple res = tuple_new(self.subtypes_len)
        cdef Buffer item_buf
        cdef Deserializer deserializer

        # collections inside UDTs are always encoded with at least the
        # version 3 format
        protocol_version = max(3, protocol_version)

        p = 0
        values = []
        for i in range(self.subtypes_len):
            item = None
            if p < buf.size:
                # Read itemlen directly using ntohl instead of slice_buffer
                itemlen = <int32_t>ntohl((<uint32_t*>(buf.ptr + p))[0])
                p += 4
                if itemlen >= 0:
                    from_ptr_and_size(buf.ptr + p, itemlen, &item_buf)
                    p += itemlen

                    deserializer = self.deserializers[i]
                    item = from_binary(deserializer, &item_buf, protocol_version)

            tuple_set(res, i, item)

        return res


cdef class DesUserType(DesTupleType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        typ = self.cqltype
        values = DesTupleType.deserialize(self, buf, protocol_version)
        if typ.mapped_class:
            return typ.mapped_class(**dict(zip(typ.fieldnames, values)))
        elif typ.tuple_type:
            return typ.tuple_type(*values)
        else:
            return tuple(values)


cdef class DesCompositeType(_DesParameterizedType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        cdef Py_ssize_t i, idx, start
        cdef Buffer elem_buf
        cdef int16_t element_length
        cdef Deserializer deserializer
        cdef tuple res = tuple_new(self.subtypes_len)

        idx = 0
        for i in range(self.subtypes_len):
            if not buf.size:
                # CompositeType can have missing elements at the end

                # Fill the tuple with None values and slice it
                #
                # (I'm not sure a tuple needs to be fully initialized before
                #  it can be destroyed, so play it safe)
                for j in range(i, self.subtypes_len):
                    tuple_set(res, j, None)
                res = res[:i]
                break

            element_length = unpack_num[uint16_t](buf)
            from_ptr_and_size(buf.ptr + 2, element_length, &elem_buf)

            deserializer = self.deserializers[i]
            item = from_binary(deserializer, &elem_buf, protocol_version)
            tuple_set(res, i, item)

            # skip element length, element, and the EOC (one byte)
            # Advance buffer in-place with direct assignment
            start = 2 + element_length + 1
            buf.ptr = buf.ptr + start
            buf.size = buf.size - start

        return res


DesDynamicCompositeType = DesCompositeType


cdef class DesReversedType(_DesSingleParamType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return from_binary(self.deserializer, buf, protocol_version)


cdef class DesFrozenType(_DesSingleParamType):
    cdef deserialize(self, Buffer *buf, int protocol_version):
        return from_binary(self.deserializer, buf, protocol_version)

#--------------------------------------------------------------------------

cdef _ret_empty(Deserializer deserializer, Py_ssize_t buf_size):
    """
    Decide whether to return None or EMPTY when a buffer size is
    zero or negative. This is used by from_binary in deserializers.pxd.
    """
    if buf_size < 0:
        return None
    elif deserializer.cqltype.support_empty_values:
        return cqltypes.EMPTY
    else:
        return None

#--------------------------------------------------------------------------
# Generic deserialization

cdef class GenericDeserializer(Deserializer):
    """
    Wrap a generic datatype for deserialization
    """

    cdef deserialize(self, Buffer *buf, int protocol_version):
        return self.cqltype.deserialize(to_bytes(buf), protocol_version)

    def __repr__(self):
        return "GenericDeserializer(%s)" % (self.cqltype,)

#--------------------------------------------------------------------------
# Helper utilities

def make_deserializers(cqltypes):
    """Create an array of Deserializers for each given cqltype in cqltypes"""
    cdef Deserializer[::1] deserializers
    return obj_array([find_deserializer(ct) for ct in cqltypes])


cdef dict classes = globals()

cpdef Deserializer find_deserializer(cqltype):
    """Find a deserializer for a cqltype"""
    name = 'Des' + cqltype.__name__

    if name in globals():
        cls = classes[name]
    elif issubclass(cqltype, cqltypes.ListType):
        cls = DesListType
    elif issubclass(cqltype, cqltypes.SetType):
        cls = DesSetType
    elif issubclass(cqltype, cqltypes.MapType):
        cls = DesMapType
    elif issubclass(cqltype, cqltypes.UserType):
        # UserType is a subclass of TupleType, so should precede it
        cls = DesUserType
    elif issubclass(cqltype, cqltypes.TupleType):
        cls = DesTupleType
    elif issubclass(cqltype, cqltypes.DynamicCompositeType):
        # DynamicCompositeType is a subclass of CompositeType, so should precede it
        cls = DesDynamicCompositeType
    elif issubclass(cqltype, cqltypes.CompositeType):
        cls = DesCompositeType
    elif issubclass(cqltype, cqltypes.ReversedType):
        cls = DesReversedType
    elif issubclass(cqltype, cqltypes.FrozenType):
        cls = DesFrozenType
    elif issubclass(cqltype, cqltypes.VectorType):
        cls = DesVectorType
    else:
        cls = GenericDeserializer

    return cls(cqltype)


def obj_array(list objs):
    """Create a (Cython) array of objects given a list of objects"""
    cdef object[:] arr
    cdef Py_ssize_t i
    arr = cython_array(shape=(len(objs),), itemsize=sizeof(void *), format="O")
    # arr[:] = objs # This does not work (segmentation faults)
    for i, obj in enumerate(objs):
        arr[i] = obj
    return arr
