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
Micro-benchmark: BoundStatement.bind() fast path without column encryption.

Measures the improvement from skipping ColDesc namedtuple creation and
ce_policy checks when column_encryption_policy is None (the common case).

Run:
    python benchmarks/bench_bind_no_encryption.py
"""

import datetime
import sys
import timeit
from unittest.mock import MagicMock

from cassandra.query import BoundStatement, PreparedStatement
from cassandra.cqltypes import (
    DateType, Int32Type, DoubleType, FloatType, UTF8Type,
    BooleanType, LongType,
)


def make_prepared_statement(col_names, col_types):
    """Build a mock PreparedStatement with the given columns."""
    col_meta = []
    for name, ctype in zip(col_names, col_types):
        cm = MagicMock()
        cm.name = name
        cm.keyspace_name = 'ks'
        cm.table_name = 'metrics'
        cm.type = ctype
        col_meta.append(cm)

    ps = MagicMock(spec=PreparedStatement)
    ps.column_metadata = col_meta
    ps.routing_key_indexes = None
    ps.protocol_version = 4
    ps.column_encryption_policy = None
    ps.serial_consistency_level = None
    ps.retry_policy = None
    ps.consistency_level = None
    ps.fetch_size = None
    ps.custom_payload = None
    ps.is_idempotent = False
    return ps


def bench():
    schemas = [
        (
            "3-col (int, double, text)",
            ['id', 'value', 'tag'],
            [Int32Type, DoubleType, UTF8Type],
            [42, 3.14159, 'sensor-001'],
        ),
        (
            "5-col time-series",
            ['ts', 'sensor_id', 'value', 'quality', 'tag'],
            [DateType, Int32Type, DoubleType, FloatType, UTF8Type],
            [datetime.datetime(2025, 4, 5, 12, 0, 0, 123456), 42, 3.14, 0.95, 'alpha'],
        ),
        (
            "8-col wide row",
            ['ts', 'id', 'v1', 'v2', 'v3', 'v4', 'flag', 'name'],
            [DateType, LongType, DoubleType, DoubleType, FloatType, FloatType, BooleanType, UTF8Type],
            [datetime.datetime(2025, 1, 1), 12345678, 1.1, 2.2, 3.3, 4.4, True, 'test-row'],
        ),
    ]

    n = 200_000
    print(f"=== BoundStatement.bind() no-encryption fast path ({n:,} iters) ===\n")

    for label, col_names, col_types, row in schemas:
        ps = make_prepared_statement(col_names, col_types)

        def do_bind():
            bs = BoundStatement(ps)
            bs.bind(row)

        # Warmup
        for _ in range(1000):
            do_bind()

        t = timeit.timeit(do_bind, number=n)
        ns_per = t / n * 1e9
        print(f"  {label}:")
        print(f"    {ns_per:.1f} ns/call ({n:,} iters)")


if __name__ == "__main__":
    print(f"Python {sys.version}\n")
    bench()
