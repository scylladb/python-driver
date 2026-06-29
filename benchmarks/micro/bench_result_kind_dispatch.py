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
Micro-benchmark: RESULT_KIND dispatch ordering and getattr vs direct access.

Measures the cost difference between:
1. Checking RESULT_KIND_ROWS first vs third in the if/elif chain
2. getattr(msg, 'continuous_paging_options', None) vs msg.continuous_paging_options

Run:
    python benchmarks/bench_result_kind_dispatch.py
"""

import sys
import timeit


def bench():
    n = 2_000_000

    # Simulate the result kind values
    RESULT_KIND_SET_KEYSPACE = 0x0003
    RESULT_KIND_SCHEMA_CHANGE = 0x0005
    RESULT_KIND_ROWS = 0x0002
    RESULT_KIND_VOID = 0x0001

    kind = RESULT_KIND_ROWS  # the common case

    # Old order: SET_KEYSPACE, SCHEMA_CHANGE, ROWS, VOID
    def old_dispatch():
        if kind == RESULT_KIND_SET_KEYSPACE:
            return 'set_keyspace'
        elif kind == RESULT_KIND_SCHEMA_CHANGE:
            return 'schema_change'
        elif kind == RESULT_KIND_ROWS:
            return 'rows'
        elif kind == RESULT_KIND_VOID:
            return 'void'

    # New order: ROWS, VOID, SET_KEYSPACE, SCHEMA_CHANGE
    def new_dispatch():
        if kind == RESULT_KIND_ROWS:
            return 'rows'
        elif kind == RESULT_KIND_VOID:
            return 'void'
        elif kind == RESULT_KIND_SET_KEYSPACE:
            return 'set_keyspace'
        elif kind == RESULT_KIND_SCHEMA_CHANGE:
            return 'schema_change'

    print(f"=== RESULT_KIND dispatch order ({n:,} iters) ===\n")

    # Warmup
    for _ in range(10000):
        old_dispatch()
        new_dispatch()

    t_old = timeit.timeit(old_dispatch, number=n)
    t_new = timeit.timeit(new_dispatch, number=n)
    ns_old = t_old / n * 1e9
    ns_new = t_new / n * 1e9
    saving = ns_old - ns_new
    speedup = ns_old / ns_new if ns_new > 0 else float('inf')
    print(f"  Old (ROWS=3rd):  {ns_old:.1f} ns")
    print(f"  New (ROWS=1st):  {ns_new:.1f} ns")
    print(f"  Saving: {saving:.1f} ns ({speedup:.2f}x)")

    # getattr vs direct attribute access
    print(f"\n=== getattr vs direct attribute access ({n:,} iters) ===\n")

    class OldMsg:
        pass

    class NewMsg:
        continuous_paging_options = None

    old_msg = OldMsg()
    new_msg = NewMsg()

    def old_getattr():
        return getattr(old_msg, 'continuous_paging_options', None)

    def new_direct():
        return new_msg.continuous_paging_options

    for _ in range(10000):
        old_getattr()
        new_direct()

    t_old = timeit.timeit(old_getattr, number=n)
    t_new = timeit.timeit(new_direct, number=n)
    ns_old = t_old / n * 1e9
    ns_new = t_new / n * 1e9
    saving = ns_old - ns_new
    speedup = ns_old / ns_new if ns_new > 0 else float('inf')
    print(f"  getattr(msg, 'continuous_paging_options', None): {ns_old:.1f} ns")
    print(f"  msg.continuous_paging_options:                    {ns_new:.1f} ns")
    print(f"  Saving: {saving:.1f} ns ({speedup:.2f}x)")


if __name__ == "__main__":
    print(f"Python {sys.version}\n")
    bench()
