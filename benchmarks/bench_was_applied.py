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
Micro-benchmark: was_applied fast path for known LWT statements.

Measures the speedup from skipping regex batch detection when the
query already knows it's an LWT statement (is_lwt() returns True).

This benchmarks the real `cassandra.cluster.ResultSet.was_applied` property
(not a simplified stand-in for it): a minimal `ResultSet` is constructed with
a mocked `response_future` -- was_applied only reads
`response_future.row_factory`/`response_future.query` -- and real
`cassandra.query` statement objects, so both the fast-path `is_lwt()` check
and the slow-path `ResultSet.batch_regex` match run exactly as they do in
production.

Run:
    python benchmarks/bench_was_applied.py
"""
import timeit
from unittest.mock import Mock

from cassandra.cluster import ResultSet
from cassandra.query import named_tuple_factory, SimpleStatement, PreparedStatement, BoundStatement


def _make_result_set(query, row):
    """Build a minimal ResultSet with a mocked response_future, mirroring what
    Session.execute()/ResponseFuture.result() construct in production."""
    response_future = Mock(row_factory=named_tuple_factory, query=query,
                            _col_names=None, _col_types=None)
    return ResultSet(response_future, [row])


def bench_was_applied():
    """Benchmark ResultSet.was_applied: fast path vs slow path."""
    # Fast path: a BoundStatement bound from a PreparedStatement whose LWT
    # status was already resolved from the server's PREPARE response, so
    # was_applied can skip batch/regex detection entirely.
    prepared = PreparedStatement(
        column_metadata=None, query_id=b'\x00', routing_key_indexes=None,
        query="UPDATE t SET v=1 WHERE k=1 IF v=0", keyspace=None,
        protocol_version=4, result_metadata=None, result_metadata_id=None,
        is_lwt=True)
    lwt_query = BoundStatement(prepared)
    fast_rs = _make_result_set(lwt_query, (True,))

    # Slow path: a plain SimpleStatement with unknown LWT status, so
    # was_applied must match the query string against the real
    # ResultSet.batch_regex to rule out a BEGIN BATCH.
    non_lwt_query = SimpleStatement("INSERT INTO t (k, v) VALUES (1, 2) IF NOT EXISTS")
    slow_rs = _make_result_set(non_lwt_query, (True,))

    def fast_path():
        _ = fast_rs.was_applied

    def slow_path():
        _ = slow_rs.was_applied

    n = 500_000
    t_fast = timeit.timeit(fast_path, number=n)
    t_slow = timeit.timeit(slow_path, number=n)

    print(f"Fast path (known LWT, {n} iters): {t_fast:.3f}s  ({t_fast / n * 1e6:.2f} us/call)")
    print(f"Slow path (regex check, {n} iters): {t_slow:.3f}s  ({t_slow / n * 1e6:.2f} us/call)")
    print(f"Speedup: {t_slow / t_fast:.1f}x")


def main():
    bench_was_applied()


if __name__ == '__main__':
    main()
