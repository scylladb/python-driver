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
Benchmark for VectorType deserialization performance.

Tests different optimization strategies:
1. Current implementation (Python with struct.unpack/numpy)
2. Python struct.unpack only
3. Numpy frombuffer + tolist()
4. Cython DesVectorType deserializer

Run with: python benchmarks/vector_deserialize.py
"""

import sys
import time
import struct

# Add parent directory to path
sys.path.insert(0, '.')

from cassandra.cqltypes import FloatType, DoubleType, Int32Type, LongType
from cassandra.marshal import float_pack, double_pack, int32_pack, int64_pack


def create_test_data(vector_size, element_type):
    """Create serialized test data for a vector."""
    if element_type == FloatType:
        values = [float(i * 0.1) for i in range(vector_size)]
        pack_fn = float_pack
    elif element_type == DoubleType:
        values = [float(i * 0.1) for i in range(vector_size)]
        pack_fn = double_pack
    elif element_type == Int32Type:
        values = list(range(vector_size))
        pack_fn = int32_pack
    elif element_type == LongType:
        values = list(range(vector_size))
        pack_fn = int64_pack
    else:
        raise ValueError(f"Unsupported element type: {element_type}")

    # Serialize the vector
    serialized = b''.join(pack_fn(v) for v in values)

    return serialized, values


def benchmark_current_implementation(vector_type, serialized_data, iterations=10000):
    """Benchmark the current VectorType.deserialize implementation."""
    protocol_version = 4

    start = time.perf_counter()
    for _ in range(iterations):
        result = vector_type.deserialize(serialized_data, protocol_version)
    end = time.perf_counter()

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, result


def benchmark_struct_optimization(vector_type, serialized_data, iterations=10000):
    """Benchmark struct.unpack optimization."""
    vector_size = vector_type.vector_size
    subtype = vector_type.subtype

    # Determine format string - subtype is a class, use identity or issubclass
    if subtype is FloatType or (isinstance(subtype, type) and issubclass(subtype, FloatType)):
        format_str = f'>{vector_size}f'
    elif subtype is DoubleType or (isinstance(subtype, type) and issubclass(subtype, DoubleType)):
        format_str = f'>{vector_size}d'
    elif subtype is Int32Type or (isinstance(subtype, type) and issubclass(subtype, Int32Type)):
        format_str = f'>{vector_size}i'
    elif subtype is LongType or (isinstance(subtype, type) and issubclass(subtype, LongType)):
        format_str = f'>{vector_size}q'
    else:
        return None, None, None

    start = time.perf_counter()
    for _ in range(iterations):
        result = list(struct.unpack(format_str, serialized_data))
    end = time.perf_counter()

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, result


def benchmark_numpy_optimization(vector_type, serialized_data, iterations=10000):
    """Benchmark numpy.frombuffer optimization."""
    try:
        import numpy as np
    except ImportError:
        return None, None, None

    vector_size = vector_type.vector_size
    subtype = vector_type.subtype

    # Determine dtype
    if subtype is FloatType or (isinstance(subtype, type) and issubclass(subtype, FloatType)):
        dtype = '>f4'
    elif subtype is DoubleType or (isinstance(subtype, type) and issubclass(subtype, DoubleType)):
        dtype = '>f8'
    elif subtype is Int32Type or (isinstance(subtype, type) and issubclass(subtype, Int32Type)):
        dtype = '>i4'
    elif subtype is LongType or (isinstance(subtype, type) and issubclass(subtype, LongType)):
        dtype = '>i8'
    else:
        return None, None, None

    start = time.perf_counter()
    for _ in range(iterations):
        arr = np.frombuffer(serialized_data, dtype=dtype, count=vector_size)
        result = arr.tolist()
    end = time.perf_counter()

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, result


def benchmark_cython_deserializer(vector_type, serialized_data, iterations=10000):
    """Benchmark Cython DesVectorType deserializer."""
    try:
        from cassandra.deserializers import find_deserializer
    except ImportError:
        return None, None, None

    protocol_version = 4

    # Get the Cython deserializer
    deserializer = find_deserializer(vector_type)

    # Check if we got the Cython deserializer
    if deserializer.__class__.__name__ != 'DesVectorType':
        return None, None, None

    start = time.perf_counter()
    for _ in range(iterations):
        result = deserializer.deserialize_bytes(serialized_data, protocol_version)
    end = time.perf_counter()

    elapsed = end - start
    per_op = (elapsed / iterations) * 1_000_000  # microseconds

    return elapsed, per_op, result


def verify_results(expected, *results):
    """Verify that all results match expected values."""
    for i, result in enumerate(results):
        if result is None:
            continue
        if len(result) != len(expected):
            print(f"  ❌ Result {i} length mismatch: {len(result)} vs {len(expected)}")
            return False
        for j, (a, b) in enumerate(zip(result, expected)):
            # Use relative tolerance for floating point comparison
            if isinstance(a, float) and isinstance(b, float):
                # Allow 0.01% relative error for floats
                if abs(a - b) > max(abs(a), abs(b)) * 1e-4 + 1e-7:
                    print(f"  ❌ Result {i} value mismatch at index {j}: {a} vs {b}")
                    return False
            elif abs(a - b) > 1e-9:
                print(f"  ❌ Result {i} value mismatch at index {j}: {a} vs {b}")
                return False
    return True


def run_benchmark_suite(vector_size, element_type, type_name, iterations=10000):
    """Run complete benchmark suite for a given vector configuration."""
    print(f"\n{'='*80}")
    print(f"Benchmark: Vector<{type_name}, {vector_size}>")
    print(f"{'='*80}")
    print(f"Iterations: {iterations:,}")

    # Create test data
    from cassandra.cqltypes import lookup_casstype
    cass_typename = f'org.apache.cassandra.db.marshal.{element_type.__name__}'
    vector_typename = f'org.apache.cassandra.db.marshal.VectorType({cass_typename}, {vector_size})'
    vector_type = lookup_casstype(vector_typename)

    serialized_data, expected_values = create_test_data(vector_size, element_type)
    data_size = len(serialized_data)

    print(f"Serialized size: {data_size:,} bytes")
    print()

    # Run benchmarks
    results = []

    # 1. Current implementation (baseline)
    print("1. Current implementation (baseline)...")
    elapsed, per_op, result_current = benchmark_current_implementation(
        vector_type, serialized_data, iterations)
    results.append(result_current)
    print(f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} μs")
    baseline_time = per_op

    # 2. Struct optimization
    print("2. Python struct.unpack optimization...")
    elapsed, per_op, result_struct = benchmark_struct_optimization(
        vector_type, serialized_data, iterations)
    results.append(result_struct)
    if per_op is not None:
        speedup = baseline_time / per_op
        print(f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} μs, Speedup: {speedup:.2f}x")
    else:
        print("   Not applicable for this type")

    # 3. Numpy with tolist()
    print("3. Numpy frombuffer + tolist()...")
    elapsed, per_op, result_numpy = benchmark_numpy_optimization(
        vector_type, serialized_data, iterations)
    results.append(result_numpy)
    if per_op is not None:
        speedup = baseline_time / per_op
        print(f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} μs, Speedup: {speedup:.2f}x")
    else:
        print("   Numpy not available")

    # 4. Cython deserializer
    print("4. Cython DesVectorType deserializer...")
    elapsed, per_op, result_cython = benchmark_cython_deserializer(
        vector_type, serialized_data, iterations)
    if per_op is not None:
        results.append(result_cython)
        speedup = baseline_time / per_op
        print(f"   Total: {elapsed:.4f}s, Per-op: {per_op:.2f} μs, Speedup: {speedup:.2f}x")
    else:
        print("   Cython deserializers not available")

    # Verify results
    print("\nVerifying results...")
    if verify_results(expected_values, *results):
        print("  ✓ All results match!")
    else:
        print("  ✗ Result mismatch detected!")

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

    print("="*80)
    print("VectorType Deserialization Performance Benchmark")
    print("="*80)

    # Test configurations: (vector_size, element_type, type_name, iterations)
    test_configs = [
        # Small vectors
        (3, FloatType, "float", 50000),
        (4, FloatType, "float", 50000),

        # Medium vectors (common in ML)
        (128, FloatType, "float", 10000),
        (384, FloatType, "float", 10000),

        # Large vectors (embeddings)
        (768, FloatType, "float", 5000),
        (1536, FloatType, "float", 2000),

        # Other types (smaller iteration counts)
        (128, DoubleType, "double", 10000),
        (128, Int32Type, "int", 10000),
    ]

    summary = []

    for vector_size, element_type, type_name, iterations in test_configs:
        baseline = run_benchmark_suite(vector_size, element_type, type_name, iterations)
        summary.append((f"Vector<{type_name}, {vector_size}>", baseline))

    # Print summary
    print("\n" + "="*80)
    print("SUMMARY - Current Implementation Performance")
    print("="*80)
    for config, baseline_time in summary:
        print(f"{config:30s}: {baseline_time:8.2f} μs")

    print("\n" + "="*80)
    print("Benchmark complete!")
    print("="*80)


if __name__ == '__main__':
    main()
