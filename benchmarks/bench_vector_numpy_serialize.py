# Copyright 2025 ScyllaDB, Inc.
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
Benchmark: VectorType serialization – list vs NumPy ndarray vs bulk.

Compares three approaches for serializing float vectors into the CQL
binary protocol wire format:

  1. list path      – original element-by-element serialize (struct.pack per element)
  2. numpy path     – single ndarray passed to serialize(), uses tobytes()
  3. bulk path      – serialize_numpy_bulk() on a 2-D array (one byte-swap for
                      the entire batch, then bytes slicing)

Each scenario is tested at realistic vector dimensions (128, 768, 1536) and
batch sizes (1, 100, 10_000 rows).

Usage:
  python benchmarks/bench_vector_numpy_serialize.py
"""

import os
import sys
import time
import timeit

# Ensure the repo root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import numpy as np

from cassandra.cqltypes import parse_casstype_args

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DIMENSIONS = [128, 768, 1536]
BATCH_SIZES = [1, 100, 10_000]
REPEATS = 3
# Number of iterations is scaled per scenario to keep total time reasonable
MIN_ITERS = 5
TARGET_TIME_S = 0.2  # aim for ~0.2s per measurement


def auto_iters(fn, target_s=TARGET_TIME_S, min_iters=MIN_ITERS):
    """Estimate a good iteration count for the given function."""
    # Warm up
    fn()
    # Time a single call
    t0 = time.perf_counter()
    fn()
    t1 = time.perf_counter()
    elapsed = t1 - t0
    if elapsed <= 0:
        return 100_000
    n = max(min_iters, int(target_s / elapsed))
    return n


# ---------------------------------------------------------------------------
# Benchmark functions
# ---------------------------------------------------------------------------


def bench_list_serialize(ctype, vectors_list, protocol_version=4):
    """Serialize each row as a Python list (original path)."""
    for row in vectors_list:
        ctype.serialize(row, protocol_version)


def bench_numpy_serialize(ctype, vectors_np_rows, protocol_version=4):
    """Serialize each row as an individual 1-D ndarray (numpy fast path)."""
    for row in vectors_np_rows:
        ctype.serialize(row, protocol_version)


def bench_bulk_serialize(ctype, vectors_2d, protocol_version=4):
    """Serialize all rows at once with serialize_numpy_bulk()."""
    ctype.serialize_numpy_bulk(vectors_2d)


def bench_bytes_passthrough(ctype, pre_serialized, protocol_version=4):
    """Simulate bind() calling serialize() on pre-serialized bytes."""
    for row_bytes in pre_serialized:
        ctype.serialize(row_bytes, protocol_version)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    print(f"Python:     {sys.version.split()[0]}")
    print(f"NumPy:      {np.__version__}")
    print(f"Repeats:    {REPEATS} (best of)")
    print()

    for dim in DIMENSIONS:
        ctype_str = (
            f"org.apache.cassandra.db.marshal.VectorType("
            f"org.apache.cassandra.db.marshal.FloatType, {dim})"
        )
        ctype = parse_casstype_args(ctype_str)

        print(f"{'=' * 72}")
        print(f"  Vector dimension: {dim}  (float32, {dim * 4} bytes/vector)")
        print(f"{'=' * 72}")
        print()

        for batch_size in BATCH_SIZES:
            # Prepare data
            vectors_2d = np.random.rand(batch_size, dim).astype(np.float32)
            vectors_list = [vectors_2d[i].tolist() for i in range(batch_size)]
            vectors_np_rows = [vectors_2d[i] for i in range(batch_size)]

            print(f"  --- batch_size = {batch_size:,} ---")
            print(
                f"    {'method':30s}  {'iters':>8s}  {'ns/call':>12s}  {'total_ms':>10s}  {'us/row':>10s}  {'vs list':>8s}"
            )
            print(
                f"    {'------':30s}  {'-----':>8s}  {'-------':>12s}  {'--------':>10s}  {'------':>10s}  {'-------':>8s}"
            )

            results = {}

            # 1. List path
            def run_list():
                bench_list_serialize(ctype, vectors_list)

            n_iters = auto_iters(run_list)
            t = timeit.repeat(
                run_list, number=n_iters, repeat=REPEATS, timer=time.perf_counter
            )
            best_s = min(t) / n_iters
            us_per_row = best_s / batch_size * 1e6
            total_ms = best_s * 1e3
            ns_per_call = best_s * 1e9
            results["list"] = us_per_row
            print(
                f"    {'list (element-by-element)':30s}  {n_iters:8d}  {ns_per_call:12.1f}  {total_ms:10.3f}  {us_per_row:10.3f}  {'1.00x':>8s}"
            )

            # 2. NumPy per-row path
            def run_numpy():
                bench_numpy_serialize(ctype, vectors_np_rows)

            n_iters = auto_iters(run_numpy)
            t = timeit.repeat(
                run_numpy, number=n_iters, repeat=REPEATS, timer=time.perf_counter
            )
            best_s = min(t) / n_iters
            us_per_row = best_s / batch_size * 1e6
            total_ms = best_s * 1e3
            ns_per_call = best_s * 1e9
            speedup = results["list"] / us_per_row if us_per_row > 0 else float("inf")
            results["numpy"] = us_per_row
            print(
                f"    {'numpy (per-row ndarray)':30s}  {n_iters:8d}  {ns_per_call:12.1f}  {total_ms:10.3f}  {us_per_row:10.3f}  {speedup:7.2f}x"
            )

            # 3. Bulk path
            def run_bulk():
                bench_bulk_serialize(ctype, vectors_2d)

            n_iters = auto_iters(run_bulk)
            t = timeit.repeat(
                run_bulk, number=n_iters, repeat=REPEATS, timer=time.perf_counter
            )
            best_s = min(t) / n_iters
            us_per_row = best_s / batch_size * 1e6
            total_ms = best_s * 1e3
            ns_per_call = best_s * 1e9
            speedup = results["list"] / us_per_row if us_per_row > 0 else float("inf")
            results["bulk"] = us_per_row
            print(
                f"    {'bulk (serialize_numpy_bulk)':30s}  {n_iters:8d}  {ns_per_call:12.1f}  {total_ms:10.3f}  {us_per_row:10.3f}  {speedup:7.2f}x"
            )

            # 4. Bytes passthrough (simulates bind() on pre-serialized bulk output)
            pre_serialized = ctype.serialize_numpy_bulk(vectors_2d)

            def run_passthrough():
                bench_bytes_passthrough(ctype, pre_serialized)

            n_iters = auto_iters(run_passthrough)
            t = timeit.repeat(
                run_passthrough, number=n_iters, repeat=REPEATS, timer=time.perf_counter
            )
            best_s = min(t) / n_iters
            us_per_row = best_s / batch_size * 1e6
            total_ms = best_s * 1e3
            ns_per_call = best_s * 1e9
            speedup = results["list"] / us_per_row if us_per_row > 0 else float("inf")
            print(
                f"    {'bytes passthrough (bind)':30s}  {n_iters:8d}  {ns_per_call:12.1f}  {total_ms:10.3f}  {us_per_row:10.3f}  {speedup:7.2f}x"
            )

            # 5. Bulk + passthrough combined (realistic end-to-end)
            def run_bulk_e2e():
                serialized = ctype.serialize_numpy_bulk(vectors_2d)
                for row_bytes in serialized:
                    ctype.serialize(row_bytes, 4)

            n_iters = auto_iters(run_bulk_e2e)
            t = timeit.repeat(
                run_bulk_e2e, number=n_iters, repeat=REPEATS, timer=time.perf_counter
            )
            best_s = min(t) / n_iters
            us_per_row = best_s / batch_size * 1e6
            total_ms = best_s * 1e3
            ns_per_call = best_s * 1e9
            speedup = results["list"] / us_per_row if us_per_row > 0 else float("inf")
            print(
                f"    {'bulk+passthrough (end-to-end)':30s}  {n_iters:8d}  {ns_per_call:12.1f}  {total_ms:10.3f}  {us_per_row:10.3f}  {speedup:7.2f}x"
            )

            print()

    # --- Correctness verification ---
    print("Correctness verification:")
    for dim in DIMENSIONS:
        ctype_str = (
            f"org.apache.cassandra.db.marshal.VectorType("
            f"org.apache.cassandra.db.marshal.FloatType, {dim})"
        )
        ctype = parse_casstype_args(ctype_str)
        vectors = np.random.rand(10, dim).astype(np.float32)
        bulk = ctype.serialize_numpy_bulk(vectors)
        for i in range(10):
            list_result = ctype.serialize(vectors[i].tolist(), 4)
            numpy_result = ctype.serialize(vectors[i], 4)
            assert bulk[i] == list_result == numpy_result, (
                f"Mismatch at dim={dim}, row={i}"
            )
            # Bytes passthrough
            assert ctype.serialize(bulk[i], 4) == bulk[i]
        print(f"  dim={dim}: OK (list == numpy == bulk == passthrough)")

    print()
    print("Done.")


if __name__ == "__main__":
    main()
