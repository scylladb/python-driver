"""
Micro-benchmark: was_applied fast path for known LWT statements.

Measures the speedup from skipping regex batch detection when the
query already knows it's an LWT statement (is_lwt() returns True).

Run:
    python benchmarks/bench_was_applied.py
"""
import re
import timeit
from unittest.mock import Mock

from cassandra.query import named_tuple_factory, SimpleStatement, BatchStatement


def bench_was_applied():
    """Benchmark was_applied fast path vs slow path."""
    batch_regex = re.compile(r'\s*BEGIN', re.IGNORECASE)

    # Fast path: known LWT statement (BoundStatement-like, is_lwt=True)
    lwt_query = Mock()
    lwt_query.is_lwt.return_value = True

    def fast_path():
        query = lwt_query
        if query.is_lwt() and not isinstance(query, BatchStatement):
            # Fast path - known single LWT, skip batch detection
            pass

    # Slow path: non-LWT SimpleStatement (must check regex)
    non_lwt_query = Mock(spec=SimpleStatement)
    non_lwt_query.is_lwt.return_value = False
    non_lwt_query.query_string = "INSERT INTO t (k, v) VALUES (1, 2) IF NOT EXISTS"

    def slow_path():
        query = non_lwt_query
        if query.is_lwt() and not isinstance(query, BatchStatement):
            pass
        else:
            isinstance(query, BatchStatement) or \
                (isinstance(query, SimpleStatement) and batch_regex.match(query.query_string))

    n = 500_000
    t_fast = timeit.timeit(fast_path, number=n)
    t_slow = timeit.timeit(slow_path, number=n)

    print(f"Fast path (known LWT, {n} iters): {t_fast:.3f}s  ({t_fast / n * 1e6:.2f} us/call)")
    print(f"Slow path (regex check, {n} iters): {t_slow:.3f}s  ({t_slow / n * 1e6:.2f} us/call)")
    print(f"Speedup: {t_slow / t_fast:.1f}x")


if __name__ == '__main__':
    bench_was_applied()
