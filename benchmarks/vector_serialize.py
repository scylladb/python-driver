#!/usr/bin/env python
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
Benchmark for VectorType serialization performance.

Tests different optimization strategies:
1. Current implementation (Python io.BytesIO loop)
2. Python struct.pack batch format string
3. Cython SerVectorType serializer (when available)
4. BoundStatement.bind() end-to-end with 1 vector column (when available)

Run with: python benchmarks/vector_serialize.py
"""

import os
import sys
import time
import struct

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from cassandra.cqltypes import FloatType, DoubleType, Int32Type, lookup_casstype
from cassandra.marshal import float_pack, double_pack, int32_pack


def create_test_values(vector_size, element_type):
    """Create test values for serialization benchmarks."""
    if element_type == FloatType:
        return [float(i * 0.1) for i in range(vector_size)]
    elif element_type == DoubleType:
        return [float(i * 0.1) for i in range(vector_size)]
    elif element_type == Int32Type:
        return list(range(vector_size))
    else:
        raise ValueError(f"Unsupported element type: {element_type}")


def benchmark_current_implementation(vector_type, values, iterations=10000):
    """Benchmark the current VectorType.serialize implementation (io.BytesIO loop)."""
    protocol_version = 4

    start = time.perf_counter()
    for _ in range(iterations):
        result = vector_type.serialize(values, protocol_version)
    end = time.perf_counter()

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, result


def benchmark_struct_pack(vector_type, values, iterations=10000):
    """Benchmark struct.pack batch format string optimization."""
    vector_size = vector_type.vector_size
    subtype = vector_type.subtype

    # Determine format string
    if subtype is FloatType or (
        isinstance(subtype, type) and issubclass(subtype, FloatType)
    ):
        format_str = f">{vector_size}f"
    elif subtype is DoubleType or (
        isinstance(subtype, type) and issubclass(subtype, DoubleType)
    ):
        format_str = f">{vector_size}d"
    elif subtype is Int32Type or (
        isinstance(subtype, type) and issubclass(subtype, Int32Type)
    ):
        format_str = f">{vector_size}i"
    else:
        return None, None, None

    # Pre-compile the struct for fair comparison
    packer = struct.Struct(format_str)

    start = time.perf_counter()
    for _ in range(iterations):
        result = packer.pack(*values)
    end = time.perf_counter()

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, result


def benchmark_cython_serializer(vector_type, values, iterations=10000):
    """Benchmark Cython SerVectorType serializer (when available).

    The Cython serializer API is optional and may be absent, incomplete, or
    incompatible (e.g. `find_serializer()` missing/changed, or the returned
    object lacking a working `serialize()`). Any of those cases are treated
    as "this strategy isn't available" and the benchmark is skipped rather
    than aborting the whole run.
    """
    try:
        from cassandra.serializers import find_serializer
    except ImportError:
        print("   Cython serializer not available, skipping (import error)")
        return None, None, None

    protocol_version = 4

    try:
        # Get the Cython serializer
        serializer = find_serializer(vector_type)

        # Check if we got the Cython serializer (not generic fallback)
        if serializer.__class__.__name__ != "SerVectorType":
            return None, None, None

        start = time.perf_counter()
        for _ in range(iterations):
            result = serializer.serialize(values, protocol_version)
        end = time.perf_counter()
    except (AttributeError, ImportError, TypeError) as exc:
        print(
            f"   Cython serializer not available, skipping "
            f"({type(exc).__name__}: {exc})"
        )
        return None, None, None

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, result


def benchmark_bind_statement(vector_type, values, iterations=10000):
    """Benchmark the real end-to-end bind path for 1 vector column.

    Uses an actual (non-mocked) ``PreparedStatement`` so each iteration goes
    through the genuine ``PreparedStatement.bind()`` -> ``BoundStatement()``
    + ``BoundStatement.bind()`` production code path in cassandra/query.py
    (construction, column metadata lookup, and serialization), rather than
    manually assembling a ``BoundStatement`` and calling
    ``vector_type.serialize()`` directly. This means "Overhead vs baseline"
    reflects genuine bind() overhead, not just serialization.
    """
    from unittest.mock import MagicMock

    try:
        from cassandra.query import PreparedStatement
    except ImportError:
        return None, None, None

    # Column metadata only needs the handful of attributes that
    # BoundStatement.__init__()/bind() actually read; a lightweight mock
    # keeps this benchmark independent of full ColumnMetadata construction.
    col_meta_mock = MagicMock()
    col_meta_mock.keyspace_name = "test_ks"
    col_meta_mock.table_name = "test_table"
    col_meta_mock.name = "vec_col"
    col_meta_mock.type = vector_type

    # A real (not mocked) PreparedStatement, so prepared.bind(values) runs
    # the actual production bind() code rather than a mock stand-in.
    prepared = PreparedStatement(
        column_metadata=[col_meta_mock],
        query_id=b"\x00",
        routing_key_indexes=None,
        query="INSERT INTO test_table (vec_col) VALUES (?)",
        keyspace="test_ks",
        protocol_version=4,
        result_metadata=None,
        result_metadata_id=None,
    )

    bound = None
    start = time.perf_counter()
    for _ in range(iterations):
        # The real public entry point: PreparedStatement.bind() ->
        # BoundStatement(self).bind(values).
        bound = prepared.bind([values])
    end = time.perf_counter()

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, bound.values[0]


def verify_results(reference, *results):
    """Verify that all serialization results produce identical bytes."""
    for i, result in enumerate(results):
        if result is None:
            continue
        if result != reference:
            print(
                f"  Result {i} mismatch: {len(result)} bytes vs {len(reference)} bytes (reference)"
            )
            # Show first divergence
            for j in range(min(len(result), len(reference))):
                if result[j] != reference[j]:
                    print(
                        f"  First difference at byte {j}: {result[j]:#04x} vs {reference[j]:#04x}"
                    )
                    break
            return False
    return True


def run_benchmark_suite(vector_size, element_type, type_name, iterations=10000):
    """Run complete benchmark suite for a given vector configuration."""
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"Benchmark: Vector<{type_name}, {vector_size}>")
    print(f"{sep}")
    print(f"Iterations: {iterations:,}")

    # Create vector type
    cass_typename = f"org.apache.cassandra.db.marshal.{element_type.__name__}"
    vector_typename = (
        f"org.apache.cassandra.db.marshal.VectorType({cass_typename}, {vector_size})"
    )
    vector_type = lookup_casstype(vector_typename)

    values = create_test_values(vector_size, element_type)

    # Get reference serialization for verification
    reference_bytes = vector_type.serialize(values, 4)
    data_size = len(reference_bytes)

    print(f"Serialized size: {data_size:,} bytes")
    print()

    # Collect results for verification
    all_results = []

    # 1. Current implementation (baseline)
    print("1. Current implementation (io.BytesIO loop, baseline)...")
    elapsed, per_op, result = benchmark_current_implementation(
        vector_type, values, iterations
    )
    all_results.append(result)
    print(f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} us")
    baseline_time = per_op

    # 2. struct.pack batch format string
    print("2. Python struct.pack batch format string...")
    elapsed, per_op, result = benchmark_struct_pack(vector_type, values, iterations)
    all_results.append(result)
    if per_op is not None:
        speedup = baseline_time / per_op
        print(
            f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} us, Speedup: {speedup:.2f}x"
        )
    else:
        print("   Not applicable for this type")

    # 3. Cython serializer
    print("3. Cython SerVectorType serializer...")
    elapsed, per_op, result = benchmark_cython_serializer(
        vector_type, values, iterations
    )
    all_results.append(result)
    if per_op is not None:
        speedup = baseline_time / per_op
        print(
            f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} us, Speedup: {speedup:.2f}x"
        )
    else:
        print("   Cython serializers not available")

    # 4. BoundStatement.bind() end-to-end
    #
    # Unlike the strategies above, this path is expected to be *slower* than
    # the baseline (it does real bind() work: construction, column metadata
    # lookup, and serialization -- not just serialization). So the ratio is
    # reported as overhead (per_op / baseline_time, an "Nx slower" figure),
    # not as a speedup (baseline_time / per_op) -- using the speedup formula
    # here would produce a fraction like 0.33x under an "overhead" label,
    # which reads backwards (looks like a 3x speedup when it's actually 3x
    # more time).
    print("4. BoundStatement.bind() end-to-end (1 vector column)...")
    elapsed, per_op, result = benchmark_bind_statement(vector_type, values, iterations)
    all_results.append(result)
    if per_op is not None:
        overhead = per_op / baseline_time
        print(
            f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} us, "
            f"Overhead vs baseline: {overhead:.2f}x slower "
            f"(+{(overhead - 1) * 100:.0f}%)"
        )
    else:
        print("   BoundStatement benchmark not available")

    # Verify results
    print("\nVerifying results...")
    if verify_results(reference_bytes, *all_results):
        print("  All results match!")
    else:
        print("  Result mismatch detected!")

    return baseline_time


def main():
    """Run all benchmarks."""
    # Pin to single CPU core for consistent measurements
    try:
        import os

        os.sched_setaffinity(0, {0})  # Pin to CPU core 0
        print("Pinned to CPU core 0 for consistent measurements")
    except (AttributeError, OSError) as e:
        print(f"Could not pin to single core: {e}")
        print("Running without CPU affinity...")

    sep = "=" * 80
    print(sep)
    print("VectorType Serialization Performance Benchmark")
    print(sep)

    # Test configurations: (vector_size, element_type, type_name, iterations)
    test_configs = [
        # Small vectors
        (3, FloatType, "float", 50000),
        # Medium vectors (common in ML)
        (128, FloatType, "float", 10000),
        # Large vectors (embeddings)
        (768, FloatType, "float", 5000),
        (1536, FloatType, "float", 2000),
        # Other types
        (128, DoubleType, "double", 10000),
        (768, DoubleType, "double", 5000),
        (1536, DoubleType, "double", 2000),
        (128, Int32Type, "int", 10000),
    ]

    summary = []

    for vector_size, element_type, type_name, iterations in test_configs:
        baseline = run_benchmark_suite(vector_size, element_type, type_name, iterations)
        summary.append((f"Vector<{type_name}, {vector_size}>", baseline))

    # Print summary
    print(f"\n{sep}")
    print("SUMMARY - Serialization Baseline Performance (io.BytesIO loop)")
    print(sep)
    for config, baseline_time in summary:
        print(f"{config:30s}: {baseline_time:8.2f} us")

    print(f"\n{sep}")
    print("Benchmark complete!")
    print(sep)


if __name__ == "__main__":
    main()
