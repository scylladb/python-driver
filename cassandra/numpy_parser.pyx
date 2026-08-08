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
This module provides an optional protocol parser that returns
NumPy arrays.

=============================================================================
This module should not be imported by any of the main python-driver modules,
as numpy is an optional dependency.
=============================================================================
"""

include "ioutils.pyx"

cimport cython
from libc.stdint cimport uint64_t
from libc.string cimport memset
from cpython.ref cimport Py_INCREF, PyObject

from cassandra.bytesio cimport BytesIOReader
from cassandra.deserializers cimport Deserializer, from_binary
from cassandra.parsing cimport ParseDesc, ColumnParser, RowParser
from cassandra import cqltypes
from cassandra.util import is_little_endian

import numpy as np

cdef extern from "numpyFlags.h":
    # Include 'numpyFlags.h' into the generated C code to disable the
    # deprecated NumPy API
    pass

cdef extern from "Python.h":
    # An integer type large enough to hold a pointer
    ctypedef uint64_t Py_uintptr_t


# Simple array descriptor, useful to parse rows into a NumPy array
ctypedef struct ArrDesc:
    Py_uintptr_t buf_ptr
    int stride # should be large enough as we allocate contiguous arrays
    int is_object
    Py_uintptr_t mask_ptr
    int mask_stride

arrDescDtype = np.dtype(
    [ ('buf_ptr', np.uintp)
    , ('stride', np.dtype('i'))
    , ('is_object', np.dtype('i'))
    , ('mask_ptr', np.uintp)
    , ('mask_stride', np.dtype('i'))
    ], align=True)

_cqltype_to_numpy = {
    cqltypes.LongType:          np.dtype('>i8'),
    cqltypes.CounterColumnType: np.dtype('>i8'),
    cqltypes.Int32Type:         np.dtype('>i4'),
    cqltypes.ShortType:         np.dtype('>i2'),
    cqltypes.FloatType:         np.dtype('>f4'),
    cqltypes.DoubleType:        np.dtype('>f8'),
}
# This table is consulted from two different call sites in make_array()
# below, and the two call sites do NOT mean the same thing by "fixed width":
#
#   - Scalar top-level columns (`_cqltype_to_numpy[coltype]`): here ShortType
#     (smallint) genuinely is fixed-width on the wire (a plain 2-byte
#     big-endian field, no length prefix), so it belongs in this table and
#     gets the fast masked-array path, exactly like Int32Type/LongType/etc.
#
#   - VectorType.subtype dispatch (`_cqltype_to_numpy[subtype]` in the
#     VectorType branch below): Cassandra/Scylla vint-prefixes each element
#     *inside* a vector unless the subtype overrides serial_size() with a
#     fixed value (see cqltypes.VectorType.serialize/deserialize, which
#     branch on exactly this). ShortType (and ByteType/tinyint) do not
#     override serial_size() -- it returns None from the _CassandraType
#     base -- so inside a vector they are NOT fixed-width, even though
#     ShortType *is* fixed-width as a scalar column. Using this table
#     directly for a vector subtype without checking serial_size() would
#     make the vector fast-path assume a fixed 2-byte stride that does not
#     match the actual wire format, causing a crash (see unpack_row's
#     stride check) instead of falling back to the safe, slower
#     object-array path used for other subtypes without a fixed width
#     (e.g. UTF8Type).
#
# Rather than removing ShortType from this shared table (which would also
# silently break the scalar smallint fast path, since both call sites read
# from the same dict), the VectorType call site below guards with
# `subtype.serial_size() is not None` before consulting this table at all.
# That mirrors the check VectorType itself uses to decide its own wire
# encoding, so it is the general, authoritative test -- and it also
# protects any subtype added to this table in the future without requiring
# every caller to separately remember the vector-vs-scalar distinction.

obj_dtype = np.dtype('O')

cdef class NumpyParser(ColumnParser):
    """Decode a ResultMessage into a bunch of NumPy arrays"""

    cpdef parse_rows(self, BytesIOReader reader, ParseDesc desc):
        cdef Py_ssize_t rowcount
        cdef ArrDesc[::1] array_descs
        cdef ArrDesc *arrs

        rowcount = read_int(reader)
        array_descs, arrays = make_arrays(desc, rowcount)
        arrs = &array_descs[0]

        _parse_rows(reader, desc, arrs, rowcount)

        arrays = [make_native_byteorder(arr) for arr in arrays]
        result = dict(zip(desc.colnames, arrays))
        return result


cdef _parse_rows(BytesIOReader reader, ParseDesc desc,
                 ArrDesc *arrs, Py_ssize_t rowcount):
    cdef Py_ssize_t i

    for i in range(rowcount):
        unpack_row(reader, desc, arrs)


### Helper functions to create NumPy arrays and array descriptors

def make_arrays(ParseDesc desc, array_size):
    """
    Allocate arrays for each result column.

    returns a tuple of (array_descs, arrays), where
        'array_descs' describe the arrays for NativeRowParser and
        'arrays' is a dict mapping column names to arrays
            (e.g. this can be fed into pandas.DataFrame)
    """
    array_descs = np.empty((desc.rowsize,), arrDescDtype)
    arrays = []

    for i, coltype in enumerate(desc.coltypes):
        arr = make_array(coltype, array_size)
        array_descs[i]['buf_ptr'] = arr.ctypes.data
        array_descs[i]['stride'] = arr.strides[0]
        array_descs[i]['is_object'] = arr.dtype is obj_dtype
        try:
            array_descs[i]['mask_ptr'] = arr.mask.ctypes.data
            array_descs[i]['mask_stride'] = arr.mask.strides[0]
        except AttributeError:
            array_descs[i]['mask_ptr'] = 0
            array_descs[i]['mask_stride'] = 1
        arrays.append(arr)

    return array_descs, arrays


def make_array(coltype, array_size):
    """
    Allocate a new NumPy array of the given column type and size.
    For VectorType, creates a 2D array (array_size x vector_dimension).
    """
    # Check if this is a VectorType
    if issubclass(coltype, cqltypes.VectorType):
        # VectorType - create 2D array (rows x vector_dimension)
        vector_size = coltype.vector_size
        subtype = coltype.subtype
        # Only use the fixed-width fast path if the subtype actually has a
        # fixed per-element wire size *inside a vector*. subtype.serial_size()
        # is exactly the check VectorType.serialize()/deserialize() use to
        # decide whether elements are plain fixed-width fields or
        # vint-length-prefixed (e.g. ShortType/ByteType return None here,
        # even though ShortType has its own, unrelated entry in
        # _cqltype_to_numpy for the scalar-column case below). Checking this
        # first -- rather than only catching KeyError on _cqltype_to_numpy --
        # also protects any subtype added to that table in the future.
        if subtype.serial_size() is not None:
            try:
                dtype = _cqltype_to_numpy[subtype]
                a = np.ma.empty((array_size, vector_size), dtype=dtype)
                a.mask = np.zeros((array_size, vector_size), dtype=bool)
                return a
            except KeyError:
                pass
        # Unsupported vector subtype, or one without a fixed per-element
        # wire width - fall back to object array.
        a = np.empty((array_size,), dtype=obj_dtype)
        return a

    # Scalar types
    try:
        a = np.ma.empty((array_size,), dtype=_cqltype_to_numpy[coltype])
        a.mask = np.zeros((array_size,), dtype=bool)
    except KeyError:
        a = np.empty((array_size,), dtype=obj_dtype)
    return a


#### Parse rows into NumPy arrays

@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline int unpack_row(
        BytesIOReader reader, ParseDesc desc, ArrDesc *arrays) except -1:
    cdef Buffer buf
    cdef Py_ssize_t i, rowsize = desc.rowsize
    cdef ArrDesc arr
    cdef Deserializer deserializer
    for i in range(rowsize):
        get_buf(reader, &buf)
        arr = arrays[i]

        if arr.is_object:
            deserializer = desc.deserializers[i]
            val = from_binary(deserializer, &buf, desc.protocol_version)
            Py_INCREF(val)
            (<PyObject **> arr.buf_ptr)[0] = <PyObject *> val
        elif buf.size >= 0:
            if buf.size != arr.stride:
                raise ValueError(
                    "Column %d (%r): received %d bytes but array stride is %d "
                    "(payload must exactly match the expected element size)" %
                    (i, desc.colnames[i], buf.size, arr.stride))
            memcpy(<char *> arr.buf_ptr, buf.ptr, buf.size)
        else:
            memset(<char *>arr.mask_ptr, 1, arr.mask_stride)
            # The row's data bytes were never written for a NULL value, so
            # they still hold uninitialized heap memory from the array's
            # allocation (np.ma.empty/np.empty do not zero-fill). That memory
            # can be observed directly via `.data` or via `.filled()`-adjacent
            # operations, so zero it out explicitly. For a VectorType column
            # this is `arr.stride` bytes (the whole vector row, e.g. 3072
            # bytes for a float[768] embedding) rather than a single scalar
            # value, which makes the amount of exposed uninitialized memory
            # much larger if left unfixed.
            memset(<char *> arr.buf_ptr, 0, arr.stride)

        # Update the pointer into the array for the next time
        arrays[i].buf_ptr += arr.stride
        arrays[i].mask_ptr += arr.mask_stride

    return 0


def make_native_byteorder(arr):
    """
    Make sure all values have a native endian in the NumPy arrays.
    Handles both 1D (scalar types) and 2D (VectorType) arrays.
    """
    if is_little_endian and not arr.dtype.kind == 'O':
        # We have arrays in big-endian order. First swap the bytes
        # into little endian order, and then update the numpy dtype
        # accordingly (e.g. from '>i8' to '<i8')
        #
        # Ignore any object arrays of dtype('O')
        # Note: arr.newbyteorder() was removed in NumPy 2.0, use view() instead
        #
        # 'arr' was just freshly allocated in make_array() and is not
        # referenced anywhere else at this point (array_descs only holds a
        # raw pointer to its buffer, not a Python reference), so it is safe
        # to byteswap it in place instead of allocating a second full-size
        # copy of the array. This matters more as vector dimensionality
        # grows (e.g. a 768-float embedding is 3072 bytes per row).
        return arr.byteswap(inplace=True).view(arr.dtype.newbyteorder())
    return arr
