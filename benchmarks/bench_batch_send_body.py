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
Benchmark: BatchMessage.send_body() for vector and scalar workloads.

Measures the actual loaded module's BatchMessage.send_body() method.
Run this before and after optimization to compare.

Usage:
  # Build baseline .so, then:
  python benchmarks/bench_batch_send_body.py
  # Apply optimization, rebuild .so, then:
  python benchmarks/bench_batch_send_body.py
"""

import io
import struct
import time
import timeit
import sys
import os

# Ensure the repo root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import cassandra.protocol
from cassandra.protocol import BatchMessage
from cassandra.query import BatchType
from cassandra.marshal import int32_pack


# ---------------------------------------------------------------------------
# Scenario builders
# ---------------------------------------------------------------------------


def make_batch_vector_queries(num_queries, dim):
    """Batch of prepared INSERT with (int32_key, float_vector) params."""
    vector_bytes = struct.pack(f">{dim}f", *([0.1] * dim))
    key_bytes = int32_pack(42)
    return [
        (True, b"\x01\x02\x03\x04\x05\x06\x07\x08", [key_bytes, vector_bytes])
        for _ in range(num_queries)
    ]


def make_batch_scalar_queries(num_queries, num_params, param_size=20):
    """Batch of prepared INSERT with N text columns of param_size bytes."""
    params = [b"\x41" * param_size for _ in range(num_params)]
    return [
        (True, b"\x01\x02\x03\x04\x05\x06\x07\x08", list(params))
        for _ in range(num_queries)
    ]


def make_batch_unprepared_queries(num_queries, num_params, param_size=20):
    """Batch of unprepared INSERT statements."""
    stmt = "INSERT INTO ks.tbl (k, v) VALUES (?, ?)"
    params = [b"\x41" * param_size for _ in range(num_params)]
    return [(False, stmt, list(params)) for _ in range(num_queries)]


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROTO_VERSION = 4
ITERATIONS = 50_000
REPEATS = 3

SCENARIOS = [
    ("10 queries x 2 params (128D vec)", make_batch_vector_queries(10, 128)),
    ("10 queries x 2 params (768D vec)", make_batch_vector_queries(10, 768)),
    ("50 queries x 2 params (128D vec)", make_batch_vector_queries(50, 128)),
    ("10 queries x 10 text params", make_batch_scalar_queries(10, 10, 20)),
    ("50 queries x 10 text params", make_batch_scalar_queries(50, 10, 20)),
    ("10 unprepared x 2 params", make_batch_unprepared_queries(10, 2, 20)),
]


# ---------------------------------------------------------------------------
# Benchmark
# ---------------------------------------------------------------------------


def bench_batch(queries, iterations, repeats):
    """Benchmark BatchMessage.send_body(), return best ns/call."""
    msg = BatchMessage(
        batch_type=BatchType.LOGGED,
        queries=queries,
        consistency_level=1,
        timestamp=1234567890123456,
    )
    f = io.BytesIO()

    def run():
        f.seek(0)
        f.truncate()
        msg.send_body(f, PROTO_VERSION)

    t = timeit.repeat(run, number=iterations, repeat=repeats, timer=time.process_time)
    return min(t) / iterations * 1e9


def main():
    is_cython = cassandra.protocol.__file__.endswith(".so")
    print(f"Python:  {sys.version.split()[0]}")
    print(f"Module:  {cassandra.protocol.__file__}")
    print(f"Cython:  {'YES (.so loaded)' if is_cython else 'NO (pure Python .py)'}")
    print(f"Config:  proto v{PROTO_VERSION}, {ITERATIONS:,} iters, best of {REPEATS}")
    print()
    print(f"{'Scenario':45s}  {'ns/call':>10s}  {'bytes':>8s}")
    print(f"{'-' * 45}  {'-' * 10}  {'-' * 8}")

    for label, queries in SCENARIOS:
        # Measure output size
        msg = BatchMessage(
            batch_type=BatchType.LOGGED,
            queries=queries,
            consistency_level=1,
            timestamp=1234567890123456,
        )
        f = io.BytesIO()
        msg.send_body(f, PROTO_VERSION)
        nbytes = len(f.getvalue())

        ns = bench_batch(queries, ITERATIONS, REPEATS)
        print(f"{label:45s}  {ns:8.1f}    {nbytes:>6d}")

    print()


if __name__ == "__main__":
    main()
