# Copyright 2026 ScyllaDB, Inc.
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
Cython-optimized LZ4 compression/decompression wrappers that call the LZ4 C
library directly, bypassing the Python lz4 module's overhead.

These functions produce output that is wire-compatible with the CQL binary
protocol v4 LZ4 compression format:

    [4 bytes big-endian uncompressed length] [LZ4 compressed data]

The Cassandra/ScyllaDB protocol requires big-endian byte order for the
uncompressed length prefix, while the Python lz4 library uses little-endian.
The pure-Python wrappers in connection.py work around this mismatch by
byte-swapping and slicing Python bytes objects, which allocates intermediate
objects on every call.  By calling LZ4_compress_default() and
LZ4_decompress_safe() through Cython's C interface we avoid all intermediate
Python object allocations and perform the byte-order conversion with simple
C pointer operations.
"""

from cpython.bytes cimport (PyBytes_AS_STRING, PyBytes_GET_SIZE,
                             PyBytes_FromStringAndSize)
from libc.stdint cimport uint32_t, INT32_MAX
from libc.stdlib cimport malloc, free
from libc.string cimport memcpy

cdef extern from "alloca.h":
    void *alloca(size_t size) noexcept nogil

# Use htonl/ntohl for big-endian ↔ native conversion (single bswap
# instruction on x86).  Same cross-platform pattern used in
# cython_marshal.pyx (see PR #732).
cdef extern from *:
    """
    #ifdef _WIN32
        #include <winsock2.h>
    #else
        #include <arpa/inet.h>
    #endif
    """
    uint32_t htonl(uint32_t hostlong) nogil
    uint32_t ntohl(uint32_t netlong) nogil

# CQL native protocol v4 frames have a 32-bit body length, so the
# theoretical maximum is ~2 GiB.  We use 256 MiB as a practical upper
# bound (matching the server's default frame size limit) to avoid
# accidentally allocating multi-GiB buffers on corrupt headers.
DEF MAX_DECOMPRESSED_LENGTH = 268435456  # 256 MiB

# LZ4_MAX_INPUT_SIZE from lz4.h — the LZ4 C API uses C int (32-bit
# signed) for sizes, so we must reject Python bytes objects that
# exceed this before casting Py_ssize_t down to int.
DEF LZ4_MAX_INPUT_SIZE = 0x7E000000  # 2 113 929 216 bytes

# Maximum LZ4_compressBound value for which we use alloca (stack
# allocation) instead of malloc.  128 KiB is well within the default
# 8 MiB thread stack size and covers CQL frames up to ~127 KiB
# uncompressed — the vast majority of real traffic.  Larger frames
# fall back to heap allocation.
DEF STACK_ALLOC_THRESHOLD = 131072  # 128 KiB


cdef extern from "lz4.h":
    int LZ4_compress_default(const char *src, char *dst,
                             int srcSize, int dstCapacity) nogil
    int LZ4_decompress_safe(const char *src, char *dst,
                            int compressedSize, int dstCapacity) nogil
    int LZ4_compressBound(int inputSize) nogil


cdef inline void _write_be32(char *dst, uint32_t value) noexcept nogil:
    """Write a 32-bit unsigned integer in big-endian byte order."""
    cdef uint32_t tmp = htonl(value)
    memcpy(dst, &tmp, 4)


cdef inline uint32_t _read_be32(const char *src) noexcept nogil:
    """Read a 32-bit unsigned integer in big-endian byte order."""
    cdef uint32_t tmp
    memcpy(&tmp, src, 4)
    return ntohl(tmp)


def lz4_compress(bytes data not None):
    """Compress *data* using LZ4 for the CQL binary protocol.

    Returns a bytes object containing a 4-byte big-endian uncompressed-length
    header followed by the raw LZ4-compressed payload.

    Raises ``RuntimeError`` if LZ4 compression fails (returns 0).  This should
    only happen if *data* exceeds ``LZ4_MAX_INPUT_SIZE`` (~1.9 GiB).
    """
    cdef Py_ssize_t src_len = PyBytes_GET_SIZE(data)

    if src_len > LZ4_MAX_INPUT_SIZE:
        raise OverflowError(
            "Input size %d exceeds LZ4_MAX_INPUT_SIZE (%d)" %
            (src_len, LZ4_MAX_INPUT_SIZE))

    cdef const char *src = PyBytes_AS_STRING(data)
    cdef int src_size = <int>src_len

    cdef int bound = LZ4_compressBound(src_size)
    if bound <= 0:
        raise RuntimeError(
            "LZ4_compressBound() returned non-positive value for input "
            "size %d; input may exceed LZ4_MAX_INPUT_SIZE" % src_size)

    # Compress into a temporary buffer to learn the exact output size,
    # then copy into an exact-size Python bytes object.  For typical CQL
    # frames (bound <= 128 KiB) we use alloca to avoid heap malloc/free
    # overhead entirely — stack allocation is just a pointer decrement.
    # Rare oversized frames fall back to heap allocation.
    cdef char *tmp
    cdef bint heap_allocated = bound > STACK_ALLOC_THRESHOLD
    if heap_allocated:
        tmp = <char *>malloc(bound)
        if tmp == NULL:
            raise MemoryError(
                "Failed to allocate %d-byte LZ4 compression buffer" % bound)
    else:
        tmp = <char *>alloca(bound)

    cdef int compressed_size
    with nogil:
        compressed_size = LZ4_compress_default(src, tmp, src_size, bound)
    if compressed_size <= 0:
        if heap_allocated:
            free(tmp)
        raise RuntimeError(
            "LZ4_compress_default() failed for input size %d" % src_size)

    # Build the final bytes: [4-byte BE header][compressed data].
    cdef Py_ssize_t final_size = 4 + <Py_ssize_t>compressed_size
    cdef bytes result = PyBytes_FromStringAndSize(NULL, final_size)
    cdef char *out_ptr = PyBytes_AS_STRING(result)
    _write_be32(out_ptr, <uint32_t>src_size)
    memcpy(out_ptr + 4, tmp, <size_t>compressed_size)
    if heap_allocated:
        free(tmp)

    return result


def lz4_decompress(bytes data not None):
    """Decompress a CQL-protocol LZ4 frame.

    Expects *data* to start with a 4-byte big-endian uncompressed-length header
    followed by raw LZ4-compressed payload (the format produced by
    :func:`lz4_compress` and by the Cassandra/ScyllaDB server).

    Raises ``ValueError`` if *data* is too short or the declared size is
    non-positive.  Raises ``RuntimeError`` if decompression fails (malformed
    payload).
    """
    cdef const char *src = PyBytes_AS_STRING(data)
    cdef Py_ssize_t src_len = PyBytes_GET_SIZE(data)

    if src_len < 4:
        raise ValueError(
            "LZ4-compressed frame too short: need at least 4 bytes for the "
            "length header, got %d" % src_len)

    cdef uint32_t uncompressed_size = _read_be32(src)

    if uncompressed_size == 0:
        return b""

    if uncompressed_size > MAX_DECOMPRESSED_LENGTH:
        raise ValueError(
            "Declared uncompressed size %d exceeds safety limit of %d bytes; "
            "frame header may be corrupt" % (uncompressed_size,
                                             MAX_DECOMPRESSED_LENGTH))

    cdef Py_ssize_t compressed_len = src_len - 4
    if compressed_len > <Py_ssize_t>INT32_MAX:
        raise ValueError(
            "Compressed payload size %d exceeds maximum supported size" %
            compressed_len)
    cdef int compressed_size = <int>compressed_len
    cdef bytes out = PyBytes_FromStringAndSize(NULL, <Py_ssize_t>uncompressed_size)
    cdef char *out_ptr = PyBytes_AS_STRING(out)

    cdef int result
    with nogil:
        result = LZ4_decompress_safe(src + 4, out_ptr,
                                     compressed_size,
                                     <int>uncompressed_size)
    if result < 0:
        raise RuntimeError(
            "LZ4_decompress_safe() failed with error code %d; "
            "compressed payload may be malformed" % result)

    if <uint32_t>result != uncompressed_size:
        raise RuntimeError(
            "LZ4_decompress_safe() produced %d bytes but header declared %d" %
            (result, uncompressed_size))

    return out
