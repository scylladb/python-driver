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
Micro-benchmark: inline checksumming check vs classmethod call.

Measures the overhead of ProtocolVersion.has_checksumming_support()
classmethod call versus an inline integer comparison on the
encode/decode hot path.

Run:
    python benchmarks/bench_checksumming_inline.py
"""

import sys
import timeit

from cassandra import ProtocolVersion
from cassandra.protocol import _CHECKSUMMING_MIN_VERSION, _CHECKSUMMING_MAX_VERSION


def bench():
    protocol_version = ProtocolVersion.V4

    def via_classmethod():
        return ProtocolVersion.has_checksumming_support(protocol_version)

    def via_inline():
        return _CHECKSUMMING_MIN_VERSION <= protocol_version < _CHECKSUMMING_MAX_VERSION

    n = 5_000_000
    t_classmethod = timeit.timeit(via_classmethod, number=n)
    t_inline = timeit.timeit(via_inline, number=n)

    saving_ns = (t_classmethod - t_inline) / n * 1e9
    speedup = t_classmethod / t_inline if t_inline > 0 else float('inf')

    print(f"=== has_checksumming_support ({n:,} iters) ===")
    print(f"  classmethod call: {t_classmethod / n * 1e9:.1f} ns")
    print(f"  inline compare:   {t_inline / n * 1e9:.1f} ns")
    print(f"  saving: {saving_ns:.1f} ns/call ({speedup:.1f}x)")


if __name__ == "__main__":
    print(f"Python {sys.version}")
    bench()
