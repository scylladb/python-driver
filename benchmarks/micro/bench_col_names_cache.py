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
Micro-benchmark: column_names / column_types extraction from metadata.

Measures the cost of building [c[2] for c in metadata] and [c[3] for c in metadata]
vs using pre-cached lists (as done for prepared statements with result_metadata).

Run:
    python benchmarks/bench_col_names_cache.py
"""

import sys
import timeit


def make_column_metadata(ncols):
    """Create fake column_metadata tuples like recv_results_metadata produces."""
    class FakeType:
        pass
    return [(f"ks_{i}", f"tbl_{i}", f"col_{i}", FakeType) for i in range(ncols)]


def bench():
    for ncols in (5, 10, 20, 50):
        metadata = make_column_metadata(ncols)

        # Pre-cached (done once at prepare time)
        cached_names = [c[2] for c in metadata]
        cached_types = [c[3] for c in metadata]

        def extract_uncached():
            names = [c[2] for c in metadata]
            types = [c[3] for c in metadata]
            return names, types

        def extract_cached():
            return cached_names, cached_types

        n = 500_000
        t_uncached = timeit.timeit(extract_uncached, number=n)
        t_cached = timeit.timeit(extract_cached, number=n)

        saving_ns = (t_uncached - t_cached) / n * 1e9
        speedup = t_uncached / t_cached if t_cached > 0 else float('inf')
        print(f"  {ncols} cols: uncached={t_uncached / n * 1e9:.1f} ns, "
              f"cached={t_cached / n * 1e9:.1f} ns, "
              f"saving={saving_ns:.1f} ns ({speedup:.1f}x)")


if __name__ == "__main__":
    print(f"Python {sys.version}")
    print("\n=== column_names / column_types extraction ===")
    bench()
