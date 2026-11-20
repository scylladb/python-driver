#!/usr/bin/env python3
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
Microbenchmarks for time-series write and read hot paths.

Covers:
  - DateType.serialize / deserialize
  - varint_pack / varint_unpack
  - BoundStatement.bind() for a typical time-series schema

All results in nanoseconds per call.  Run with:
    python benchmarks/micro/bench_timeseries.py
"""

import datetime
import sys
import timeit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

WARMUP = 50_000
ITERATIONS = 500_000


def bench(label, stmt, setup="pass", number=ITERATIONS, warmup=WARMUP):
    """Run *stmt* under *setup*, return ns/call and print a line."""
    globs = {}
    exec(setup, globs)
    # warmup
    t_code = compile(stmt, "<bench>", "exec")
    for _ in range(warmup):
        exec(t_code, globs)
    # measure
    timer = timeit.Timer(stmt, setup, globals=globs)
    raw = timer.timeit(number=number)
    ns = raw / number * 1e9
    print(f"  {label:.<60s} {ns:>9.1f} ns/call")
    return ns


# ---------------------------------------------------------------------------
# DateType.serialize / deserialize
# ---------------------------------------------------------------------------


def bench_datetype():
    print("\n=== DateType.serialize ===")
    setup = """\
from cassandra.cqltypes import DateType
import datetime
dt_now   = datetime.datetime(2025, 4, 5, 12, 0, 0, 123456)
dt_epoch = datetime.datetime(1970, 1, 1, 0, 0, 1, 0)
dt_far   = datetime.datetime(2300, 1, 1, 0, 0, 0, 1000)
d_only   = datetime.date(2025, 4, 5)
ts_int   = 1712318400000
"""
    bench("serialize  datetime (2025)", "DateType.serialize(dt_now, 4)", setup)
    bench("serialize  datetime (epoch)", "DateType.serialize(dt_epoch, 4)", setup)
    bench("serialize  datetime (2300)", "DateType.serialize(dt_far, 4)", setup)
    bench("serialize  date object", "DateType.serialize(d_only, 4)", setup)
    bench("serialize  raw int timestamp", "DateType.serialize(ts_int, 4)", setup)

    print("\n=== DateType.deserialize ===")
    setup_deser = (
        setup
        + """\
packed_now = DateType.serialize(dt_now, 4)
packed_far = DateType.serialize(dt_far, 4)
"""
    )
    bench("deserialize (2025)", "DateType.deserialize(packed_now, 4)", setup_deser)
    bench("deserialize (2300)", "DateType.deserialize(packed_far, 4)", setup_deser)


# ---------------------------------------------------------------------------
# varint_pack / varint_unpack
# ---------------------------------------------------------------------------


def bench_varint():
    print("\n=== varint_pack ===")
    setup = """\
from cassandra.marshal import varint_pack, varint_unpack
small    = 42
medium   = 2**62
large    = 2**127
negative = -(2**62)
zero     = 0
"""
    bench("varint_pack  zero", "varint_pack(zero)", setup)
    bench("varint_pack  small", "varint_pack(small)", setup)
    bench("varint_pack  medium", "varint_pack(medium)", setup)
    bench("varint_pack  large", "varint_pack(large)", setup)
    bench("varint_pack  negative", "varint_pack(negative)", setup)

    print("\n=== varint_unpack ===")
    setup_u = (
        setup
        + """\
packed_small    = varint_pack(small)
packed_medium   = varint_pack(medium)
packed_large    = varint_pack(large)
packed_negative = varint_pack(negative)
packed_zero     = varint_pack(zero)
"""
    )
    bench("varint_unpack  zero", "varint_unpack(packed_zero)", setup_u)
    bench("varint_unpack  small", "varint_unpack(packed_small)", setup_u)
    bench("varint_unpack  medium", "varint_unpack(packed_medium)", setup_u)
    bench("varint_unpack  large", "varint_unpack(packed_large)", setup_u)
    bench("varint_unpack  negative", "varint_unpack(packed_negative)", setup_u)


# ---------------------------------------------------------------------------
# BoundStatement.bind() — typical time-series schema
# ---------------------------------------------------------------------------


def bench_bind():
    print("\n=== BoundStatement.bind (time-series schema) ===")
    setup = """\
import datetime
from cassandra.query import BoundStatement, PreparedStatement
from cassandra.cqltypes import (
    DateType, Int32Type, DoubleType, FloatType, UTF8Type,
)
from cassandra.protocol import ColumnMetadata

# Real PreparedStatement (no mocks) with 5 columns:
# (ts timestamp, sensor_id int, value double, quality float, tag text)
col_types = [DateType, Int32Type, DoubleType, FloatType, UTF8Type]
col_names = ['ts', 'sensor_id', 'value', 'quality', 'tag']
col_meta = [ColumnMetadata('ks', 'metrics', name, ctype)
            for name, ctype in zip(col_names, col_types)]
ps = PreparedStatement(
    column_metadata=col_meta, query_id=None, routing_key_indexes=[],
    query=None, keyspace='ks', protocol_version=4,
    result_metadata=None, result_metadata_id=None)

dt = datetime.datetime(2025, 4, 5, 12, 0, 0, 123456)
row = [dt, 42, 3.14159, 0.95, 'sensor-alpha-001']
"""
    bench(
        "bind 5-col time-series row",
        """\
bs = BoundStatement(ps)
bs.bind(row)
""",
        setup,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Python {sys.version}")
    print(f"Iterations per benchmark: {ITERATIONS:,}")

    bench_datetype()
    bench_varint()
    bench_bind()

    print("\nDone.")
