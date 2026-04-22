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
Micro-benchmark: cql_parameterized_type memoization.

Measures the cost of building the CQL type string representation
with and without memoization for various type complexities.

Run:
    python benchmarks/bench_cql_parameterized_type.py
"""

import sys
import timeit

from cassandra.cqltypes import (
    MapType, SetType, ListType, TupleType,
    Int32Type, UTF8Type, FloatType, DoubleType, BooleanType,
    _CassandraType,
)


def bench():
    # Create parameterized types
    map_type = MapType.apply_parameters([UTF8Type, Int32Type])
    set_type = SetType.apply_parameters([FloatType])
    list_type = ListType.apply_parameters([DoubleType])
    tuple_type = TupleType.apply_parameters([Int32Type, UTF8Type, BooleanType])
    nested_type = MapType.apply_parameters([
        UTF8Type,
        ListType.apply_parameters([
            TupleType.apply_parameters([Int32Type, FloatType, DoubleType])
        ])
    ])

    test_types = [
        ("Int32Type (simple)", Int32Type),
        ("MapType<text, int>", map_type),
        ("SetType<float>", set_type),
        ("ListType<double>", list_type),
        ("TupleType<int, text, bool>", tuple_type),
        ("MapType<text, list<tuple<int, float, double>>>", nested_type),
    ]

    n = 500_000
    print(f"=== cql_parameterized_type ({n:,} iters) ===\n")

    for label, typ in test_types:
        # Clear cache to measure uncached
        typ._cql_type_str = None
        # One call to populate cache
        result = typ.cql_parameterized_type()

        # Measure cached (warm)
        t_cached = timeit.timeit(typ.cql_parameterized_type, number=n)

        # Measure uncached (cold)
        def uncached():
            typ._cql_type_str = None
            return typ.cql_parameterized_type()
        t_uncached = timeit.timeit(uncached, number=n)

        saving_ns = (t_uncached - t_cached) / n * 1e9
        speedup = t_uncached / t_cached if t_cached > 0 else float('inf')
        print(f"  {label}:")
        print(f"    result: {result}")
        print(f"    uncached: {t_uncached / n * 1e9:.1f} ns, "
              f"cached: {t_cached / n * 1e9:.1f} ns, "
              f"saving: {saving_ns:.1f} ns ({speedup:.1f}x)")


if __name__ == "__main__":
    print(f"Python {sys.version}\n")
    bench()
