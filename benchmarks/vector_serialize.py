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

import sys
import time
import struct

# Add parent directory to path
sys.path.insert(0, '.')

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
    if subtype is FloatType or (isinstance(subtype, type) and issubclass(subtype, FloatType)):
        format_str = f'>{vector_size}f'
    elif subtype is DoubleType or (isinstance(subtype, type) and issubclass(subtype, DoubleType)):
        format_str = f'>{vector_size}d'
    elif subtype is Int32Type or (isinstance(subtype, type) and issubclass(subtype, Int32Type)):
        format_str = f'>{vector_size}i'
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
    """Benchmark Cython SerVectorType serializer (when available)."""
    try:
        from cassandra.serializers import find_serializer
    except ImportError:
        return None, None, None

    protocol_version = 4

    # Get the Cython serializer
    serializer = find_serializer(vector_type)

    # Check if we got the Cython serializer (not generic fallback)
    if serializer.__class__.__name__ != 'SerVectorType':
        return None, None, None

    start = time.perf_counter()
    for _ in range(iterations):
        result = serializer.serialize(values, protocol_version)
    end = time.perf_counter()

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, result


def benchmark_bind_statement(vector_type, values, iterations=10000):
    """Benchmark BoundStatement.bind() end-to-end with 1 vector column.

    This simulates the full bind path for a prepared statement with a single
    vector column, including column metadata lookup and serialization.
    """
    from unittest.mock import MagicMock

    try:
        from cassandra.query import BoundStatement, PreparedStatement, UNSET_VALUE
    except ImportError:
        return None, None, None

    # Create a mock PreparedStatement with one vector column
    col_meta_mock = MagicMock()
    col_meta_mock.keyspace_name = 'test_ks'
    col_meta_mock.table_name = 'test_table'
    col_meta_mock.name = 'vec_col'
    col_meta_mock.type = vector_type

    prepared = MagicMock(spec=PreparedStatement)
    prepared.protocol_version = 4
    prepared.column_metadata = [col_meta_mock]
    prepared.column_encryption_policy = None
    prepared.routing_key_indexes = None
    prepared.is_idempotent = False
    prepared.result_metadata = None
    prepared.keyspace = 'test_ks'

    start = time.perf_counter()
    for _ in range(iterations):
        bs = BoundStatement.__new__(BoundStatement)
        bs.prepared_statement = prepared
        bs.values = []
        bs.raw_values = [values]
        # Inline the core serialization path (no CE policy)
        bs.values.append(vector_type.serialize(values, 4))
    end = time.perf_counter()

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, bs.values[0]


def verify_results(reference, *results):
    """Verify that all serialization results produce identical bytes."""
    for i, result in enumerate(results):
        if result is None:
            continue
        if result != reference:
            print(f"  Result {i} mismatch: {len(result)} bytes vs {len(reference)} bytes (reference)")
            # Show first divergence
            for j in range(min(len(result), len(reference))):
                if result[j] != reference[j]:
                    print(f"  First difference at byte {j}: {result[j]:#04x} vs {reference[j]:#04x}")
                    break
            return False
    return True


def run_benchmark_suite(vector_size, element_type, type_name, iterations=10000):
    """Run complete benchmark suite for a given vector configuration."""
    sep = '=' * 80
    print(f"\n{sep}")
    print(f"Benchmark: Vector<{type_name}, {vector_size}>")
    print(f"{sep}")
    print(f"Iterations: {iterations:,}")

    # Create vector type
    cass_typename = f'org.apache.cassandra.db.marshal.{element_type.__name__}'
    vector_typename = f'org.apache.cassandra.db.marshal.VectorType({cass_typename}, {vector_size})'
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
        vector_type, values, iterations)
    all_results.append(result)
    print(f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} us")
    baseline_time = per_op

    # 2. struct.pack batch format string
    print("2. Python struct.pack batch format string...")
    elapsed, per_op, result = benchmark_struct_pack(
        vector_type, values, iterations)
    all_results.append(result)
    if per_op is not None:
        speedup = baseline_time / per_op
        print(f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} us, Speedup: {speedup:.2f}x")
    else:
        print("   Not applicable for this type")

    # 3. Cython serializer
    print("3. Cython SerVectorType serializer...")
    elapsed, per_op, result = benchmark_cython_serializer(
        vector_type, values, iterations)
    all_results.append(result)
    if per_op is not None:
        speedup = baseline_time / per_op
        print(f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} us, Speedup: {speedup:.2f}x")
    else:
        print("   Cython serializers not available")

    # 4. BoundStatement.bind() end-to-end
    print("4. BoundStatement.bind() end-to-end (1 vector column)...")
    elapsed, per_op, result = benchmark_bind_statement(
        vector_type, values, iterations)
    all_results.append(result)
    if per_op is not None:
        speedup = baseline_time / per_op
        print(f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} us, Overhead vs baseline: {speedup:.2f}x")
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

    sep = '=' * 80
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


if __name__ == '__main__':
    main()
