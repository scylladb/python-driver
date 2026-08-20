# Copyright 2020 ScyllaDB, Inc.
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

from libc.stdint cimport uint64_t, int64_t

cdef extern from *:
    """
    #include <stdint.h>

    /* Portable multiply-high: (biased_token * shards_count) >> 64 using only
       64-bit arithmetic. Always compiled so it stays testable everywhere, and
       it is the only path MSVC can take (no 128-bit integer type there). */
    static int cassandra_shard_id_portable(uint64_t biased_token, uint64_t shards_count) {
        uint64_t low_product = (biased_token & UINT32_MAX) * shards_count;
        uint64_t carry = low_product >> 32;
        uint64_t mid = (biased_token >> 32) * shards_count + carry;
        return (int)(mid >> 32);
    }

    #if defined(__SIZEOF_INT128__)
    static int cassandra_shard_id_native(uint64_t biased_token, uint64_t shards_count) {
        return (int)(((unsigned __int128)biased_token * shards_count) >> 64);
    }
    #define CASSANDRA_HAVE_INT128 1
    #define cassandra_shard_id_from_token cassandra_shard_id_native
    #else
    /* unsigned __int128 is a GCC/Clang extension; MSVC has no 128-bit integer
       type, so the portable decomposition is the only implementation. */
    static int cassandra_shard_id_native(uint64_t biased_token, uint64_t shards_count) {
        return cassandra_shard_id_portable(biased_token, shards_count);
    }
    #define CASSANDRA_HAVE_INT128 0
    #define cassandra_shard_id_from_token cassandra_shard_id_portable
    #endif
    """
    int cassandra_shard_id_from_token(uint64_t biased_token, uint64_t shards_count)
    int cassandra_shard_id_native(uint64_t biased_token, uint64_t shards_count)
    int cassandra_shard_id_portable(uint64_t biased_token, uint64_t shards_count)
    int CASSANDRA_HAVE_INT128

HAVE_INT128 = bool(CASSANDRA_HAVE_INT128)


def _shard_id_from_token_impl(int64_t token_input, int shards_count, int sharding_ignore_msb,
                              bint portable):
    """Test hook: run one shard-id computation through a chosen implementation."""
    cdef uint64_t biased_token = token_input + (<uint64_t>1 << 63)
    biased_token <<= sharding_ignore_msb
    if portable:
        return cassandra_shard_id_portable(biased_token, <uint64_t>shards_count)
    return cassandra_shard_id_native(biased_token, <uint64_t>shards_count)

cdef class ShardingInfo():
    cdef readonly int shards_count
    cdef readonly unicode partitioner
    cdef readonly unicode sharding_algorithm
    cdef readonly int sharding_ignore_msb
    cdef readonly int shard_aware_port
    cdef readonly int shard_aware_port_ssl

    cdef object __weakref__

    def __init__(self, shard_id, shards_count, partitioner, sharding_algorithm, sharding_ignore_msb, shard_aware_port,
                 shard_aware_port_ssl):
        self.shards_count = int(shards_count)
        self.partitioner = partitioner
        self.sharding_algorithm = sharding_algorithm
        self.sharding_ignore_msb = int(sharding_ignore_msb)
        self.shard_aware_port = int(shard_aware_port) if shard_aware_port else 0
        self.shard_aware_port_ssl = int(shard_aware_port_ssl) if shard_aware_port_ssl else 0

    def shard_id_from_token(self, int64_t token_input):
        cdef uint64_t biased_token = token_input + (<uint64_t>1 << 63);
        biased_token <<= self.sharding_ignore_msb;
        cdef uint64_t shards_count = <uint64_t>self.shards_count
        return cassandra_shard_id_from_token(biased_token, shards_count)
