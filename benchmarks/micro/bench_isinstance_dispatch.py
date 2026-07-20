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
Micro-benchmark: isinstance dispatch order in _create_response_future.

Measures the cost of checking BoundStatement first vs SimpleStatement first
in the isinstance chain that dispatches query types to message constructors.

For prepared-statement workloads (the perf-critical case), BoundStatement is
the most common type. Checking it first saves one wasted isinstance call.

Run:
    python benchmarks/bench_isinstance_dispatch.py
"""

import sys
import timeit

from cassandra.query import SimpleStatement, BoundStatement, BatchStatement, Statement


class _FakeGraphStatement(Statement):
    """Stand-in for GraphStatement to avoid importing DSE dependencies."""
    pass


def make_bound_statement():
    """Create a minimal BoundStatement-like object for benchmarking."""
    # We only need isinstance() to work; no actual prepared statement needed.
    bs = object.__new__(BoundStatement)
    return bs


def make_simple_statement():
    return SimpleStatement("SELECT * FROM t")


def make_batch_statement():
    return BatchStatement()


def bench():
    bound = make_bound_statement()
    simple = make_simple_statement()
    batch = make_batch_statement()
    graph = _FakeGraphStatement()

    # Simulate typical workload mix: ~80% BoundStatement, ~15% SimpleStatement,
    # ~4% BatchStatement, ~1% GraphStatement
    queries = ([bound] * 80 + [simple] * 15 + [batch] * 4 + [graph] * 1)

    def dispatch_simple_first():
        """Original order: SimpleStatement checked first."""
        for q in queries:
            if isinstance(q, SimpleStatement):
                pass
            elif isinstance(q, BoundStatement):
                pass
            elif isinstance(q, BatchStatement):
                pass
            elif isinstance(q, _FakeGraphStatement):
                pass

    def dispatch_bound_first():
        """Optimized order: BoundStatement checked first."""
        for q in queries:
            if isinstance(q, BoundStatement):
                pass
            elif isinstance(q, SimpleStatement):
                pass
            elif isinstance(q, BatchStatement):
                pass
            elif isinstance(q, _FakeGraphStatement):
                pass

    n = 200_000
    t_simple_first = timeit.timeit(dispatch_simple_first, number=n)
    t_bound_first = timeit.timeit(dispatch_bound_first, number=n)

    total_calls = n * len(queries)
    print(f"=== isinstance dispatch order (100 queries x {n} iters = {total_calls:,} dispatches) ===")
    print(f"SimpleStatement first: {t_simple_first:.3f}s  ({t_simple_first / total_calls * 1e9:.1f} ns/dispatch)")
    print(f"BoundStatement first:  {t_bound_first:.3f}s  ({t_bound_first / total_calls * 1e9:.1f} ns/dispatch)")

    if t_bound_first < t_simple_first:
        speedup = t_simple_first / t_bound_first
        saving_ns = (t_simple_first - t_bound_first) / total_calls * 1e9
        print(f"Speedup: {speedup:.2f}x ({saving_ns:.1f} ns/dispatch saved)")
    else:
        print(f"No improvement (ratio: {t_simple_first / t_bound_first:.2f}x)")


if __name__ == "__main__":
    print(f"Python {sys.version}")
    bench()
