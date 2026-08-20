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

    static int cassandra_shard_id_from_token(uint64_t biased_token, uint64_t shards_count) {
    #if defined(__SIZEOF_INT128__)
        return (int)(((unsigned __int128)biased_token * shards_count) >> 64);
    #else
        uint64_t low_product = (biased_token & UINT32_MAX) * shards_count;
        uint64_t carry = low_product >> 32;
        uint64_t mid = (biased_token >> 32) * shards_count + carry;
        return (int)(mid >> 32);
    #endif
    }
    """
    int cassandra_shard_id_from_token(uint64_t biased_token, uint64_t shards_count)

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
