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
Micro-benchmark: lazy initialization of _callbacks/_errbacks.

Measures the allocation savings from deferring list creation in
ResponseFuture.__init__() for the common case where no callbacks
are registered (synchronous execute path).

Run:
    python benchmarks/bench_lazy_init_callbacks.py
"""
import timeit
import sys


def bench_lazy_init():
    """Compare allocation cost of [] vs None initialization."""
    n = 1_000_000

    # Simulate the __init__ allocation pattern
    def init_with_lists():
        callbacks = []
        errbacks = []
        return callbacks, errbacks

    def init_with_none():
        callbacks = None
        errbacks = None
        return callbacks, errbacks

    t_lists = timeit.timeit(init_with_lists, number=n)
    t_none = timeit.timeit(init_with_none, number=n)

    print(f"Init with [] x2 ({n} iters): {t_lists / n * 1e9:.1f} ns/call")
    print(f"Init with None x2 ({n} iters): {t_none / n * 1e9:.1f} ns/call")
    print(f"Speedup: {t_lists / t_none:.1f}x")
    print(f"Memory per empty list: {sys.getsizeof([])} bytes")
    print(f"Saved per request (no callbacks): {sys.getsizeof([]) * 2} bytes")

    # Benchmark the happy path: _set_final_result with no callbacks
    # This is the hot path - iterating None vs empty list
    def iter_empty_list():
        callbacks = []
        for fn, args, kwargs in callbacks:
            pass

    def iter_none_with_guard():
        callbacks = None
        for fn, args, kwargs in callbacks or ():
            pass

    t_list_iter = timeit.timeit(iter_empty_list, number=n)
    t_none_iter = timeit.timeit(iter_none_with_guard, number=n)

    print(f"\nHappy-path iteration (no callbacks):")
    print(f"  Iterate empty []: {t_list_iter / n * 1e9:.1f} ns/call")
    print(f"  Guard None or (): {t_none_iter / n * 1e9:.1f} ns/call")
    print(f"  Speedup: {t_list_iter / t_none_iter:.2f}x")


def main():
    bench_lazy_init()


if __name__ == '__main__':
    main()
