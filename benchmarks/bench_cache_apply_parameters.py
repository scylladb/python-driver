"""
Micro-benchmark: apply_parameters caching.

Measures the speedup from caching parameterized type creation
in _CassandraType.apply_parameters().

Run:
    python benchmarks/bench_cache_apply_parameters.py
"""
import timeit
from cassandra.cqltypes import (
    MapType, SetType, ListType, TupleType,
    Int32Type, UTF8Type, FloatType, DoubleType, BooleanType,
    _CassandraType,
)


def bench_apply_parameters():
    """Benchmark apply_parameters with cache (repeated calls)."""
    cache = _CassandraType._apply_parameters_cache

    # Warm up the cache
    MapType.apply_parameters([UTF8Type, Int32Type])
    SetType.apply_parameters([FloatType])
    ListType.apply_parameters([DoubleType])
    TupleType.apply_parameters([Int32Type, UTF8Type, BooleanType])

    calls = [
        (MapType, [UTF8Type, Int32Type]),
        (SetType, [FloatType]),
        (ListType, [DoubleType]),
        (TupleType, [Int32Type, UTF8Type, BooleanType]),
    ]

    def run_cached():
        for cls, subtypes in calls:
            cls.apply_parameters(subtypes)

    # Benchmark cached path
    n = 100_000
    t_cached = timeit.timeit(run_cached, number=n)
    print(f"Cached apply_parameters ({len(calls)} types x {n} iters): "
          f"{t_cached:.3f}s  ({t_cached / (n * len(calls)) * 1e6:.2f} us/call)")

    # Benchmark uncached path (clear cache each iteration)
    def run_uncached():
        for cls, subtypes in calls:
            cache.clear()
            cls.apply_parameters(subtypes)

    t_uncached = timeit.timeit(run_uncached, number=n)
    print(f"Uncached apply_parameters ({len(calls)} types x {n} iters): "
          f"{t_uncached:.3f}s  ({t_uncached / (n * len(calls)) * 1e6:.2f} us/call)")

    speedup = t_uncached / t_cached
    print(f"Speedup: {speedup:.1f}x")


if __name__ == '__main__':
    bench_apply_parameters()
