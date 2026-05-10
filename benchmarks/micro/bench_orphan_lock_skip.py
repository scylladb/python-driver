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
Micro-benchmark: orphaned request lock skip in process_msg.

Measures the cost of always acquiring a lock vs checking the set first.

Run:
    python benchmarks/bench_orphan_lock_skip.py
"""

import sys
import timeit
from threading import Lock


def bench():
    n = 2_000_000
    lock = Lock()
    orphaned_set = set()  # empty — the common case
    stream_id = 42
    in_flight = 100

    # Old: always acquire lock
    def old_check():
        nonlocal in_flight
        with lock:
            if stream_id in orphaned_set:
                in_flight -= 1
                orphaned_set.remove(stream_id)

    # New: check set first, skip lock if empty
    def new_check():
        nonlocal in_flight
        if orphaned_set:
            with lock:
                if stream_id in orphaned_set:
                    in_flight -= 1
                    orphaned_set.remove(stream_id)

    print(f"=== orphaned request lock skip ({n:,} iters) ===\n")

    # Warmup
    for _ in range(10000):
        old_check()
        new_check()

    t_old = timeit.timeit(old_check, number=n)
    t_new = timeit.timeit(new_check, number=n)
    ns_old = t_old / n * 1e9
    ns_new = t_new / n * 1e9
    saving = ns_old - ns_new
    speedup = ns_old / ns_new if ns_new > 0 else float('inf')
    print(f"  Empty orphaned set (common case):")
    print(f"    Old (always lock): {ns_old:.1f} ns")
    print(f"    New (check first): {ns_new:.1f} ns")
    print(f"    Saving: {saving:.1f} ns ({speedup:.2f}x)")

    # Non-empty set (rare case) — should still work
    orphaned_set_full = {99, 100, 101}
    def old_check_full():
        nonlocal in_flight
        with lock:
            if stream_id in orphaned_set_full:
                in_flight -= 1
                orphaned_set_full.remove(stream_id)

    def new_check_full():
        nonlocal in_flight
        if orphaned_set_full:
            with lock:
                if stream_id in orphaned_set_full:
                    in_flight -= 1
                    orphaned_set_full.remove(stream_id)

    for _ in range(10000):
        old_check_full()
        new_check_full()

    t_old = timeit.timeit(old_check_full, number=n)
    t_new = timeit.timeit(new_check_full, number=n)
    ns_old = t_old / n * 1e9
    ns_new = t_new / n * 1e9
    diff = ns_new - ns_old
    print(f"\n  Non-empty orphaned set (rare case):")
    print(f"    Old (always lock): {ns_old:.1f} ns")
    print(f"    New (check first): {ns_new:.1f} ns")
    print(f"    Overhead: {diff:.1f} ns (extra truthiness check)")


if __name__ == "__main__":
    print(f"Python {sys.version}\n")
    bench()
