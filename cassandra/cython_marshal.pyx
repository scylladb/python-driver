# -- cython: profile=True
#
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

from libc.stdint cimport (int8_t, int16_t, int32_t, int64_t,
                          uint8_t, uint16_t, uint32_t, uint64_t)
from libc.string cimport memcpy
from cassandra.buffer cimport Buffer, buf_read, to_bytes

# Use ntohs/ntohl for efficient big-endian to native conversion (single bswap instruction on x86)
# Platform-specific header: arpa/inet.h on POSIX, winsock2.h on Windows
cdef extern from *:
    """
    #ifdef _WIN32
        #include <winsock2.h>
    #else
        #include <arpa/inet.h>
    #endif
    """
    uint16_t ntohs(uint16_t netshort) nogil
    uint32_t ntohl(uint32_t netlong) nogil

cdef bint is_little_endian
from cassandra.util import is_little_endian

ctypedef fused num_t:
    int64_t
    int32_t
    int16_t
    int8_t
    uint64_t
    uint32_t
    uint16_t
    uint8_t
    double
    float

cdef inline num_t unpack_num(Buffer *buf, num_t *dummy=NULL): # dummy pointer because cython wants the fused type as an arg
    """
    Copy to aligned destination, conditionally swapping to native byte order.
    Uses ntohs/ntohl for 16/32-bit types (compiles to single bswap instruction).
    """
    cdef Py_ssize_t i
    cdef char *src = buf_read(buf, sizeof(num_t))
    cdef num_t ret
    cdef char *out = <char*> &ret
    cdef uint32_t temp32  # For float byte-swapping

    # Copy to aligned location first
    memcpy(&ret, src, sizeof(num_t))

    if not is_little_endian:
        return ret

    # Use optimized byte-swap intrinsics for 16-bit and 32-bit types
    if num_t is int16_t or num_t is uint16_t:
        return <num_t>ntohs(<uint16_t>ret)
    elif num_t is int32_t or num_t is uint32_t:
        return <num_t>ntohl(<uint32_t>ret)
    elif num_t is float:
        # For float, reinterpret bits as uint32, swap, then reinterpret back
        temp32 = (<uint32_t*>&ret)[0]
        temp32 = ntohl(temp32)
        return (<float*>&temp32)[0]
    else:
        # 64-bit, double, or 8-bit: use byte-swap loop (8-bit loop is no-op)
        for i in range(sizeof(num_t)):
            out[sizeof(num_t) - i - 1] = src[i]
        return ret

cdef varint_unpack(Buffer *term):
    """Unpack a variable-sized integer"""
    return varint_unpack_py3(to_bytes(term))

cdef varint_unpack_py3(bytes term):
    return int.from_bytes(term, byteorder='big', signed=True)
