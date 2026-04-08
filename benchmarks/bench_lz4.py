# Copyright 2026 ScyllaDB, Inc.
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
Microbenchmark comparing the Python lz4 wrappers (connection.py) against
the Cython direct-C-linkage wrappers (cython_lz4.pyx) for the CQL binary
protocol v4 LZ4 compression path.

Usage (pin to one core for stable numbers):

    taskset -c 0 python benchmarks/bench_lz4.py

Payload sizes tested: 1 KB, 8 KB, 64 KB.
"""

import os
import struct
import timeit

# ---------------------------------------------------------------------------
# Python wrappers (duplicated from connection.py to avoid import side-effects)
# ---------------------------------------------------------------------------
try:
    import lz4.block as lz4_block
    HAS_PYTHON_LZ4 = True
except ImportError:
    HAS_PYTHON_LZ4 = False
    print("WARNING: lz4 Python package not available, skipping Python benchmarks")

int32_pack = struct.Struct('>i').pack


def py_lz4_compress(byts):
    return int32_pack(len(byts)) + lz4_block.compress(byts)[4:]


def py_lz4_decompress(byts):
    return lz4_block.decompress(byts[3::-1] + byts[4:])


# ---------------------------------------------------------------------------
# Cython wrappers
# ---------------------------------------------------------------------------
try:
    from cassandra.cython_lz4 import lz4_compress as cy_lz4_compress
    from cassandra.cython_lz4 import lz4_decompress as cy_lz4_decompress
    HAS_CYTHON = True
except ImportError:
    HAS_CYTHON = False
    print("WARNING: cassandra.cython_lz4 not available, skipping Cython benchmarks")


# ---------------------------------------------------------------------------
# Benchmark helpers
# ---------------------------------------------------------------------------
SIZES = {
    "1KB": 1024,
    "8KB": 8 * 1024,
    "64KB": 64 * 1024,
}

# Number of inner-loop iterations per timeit.repeat() call.
INNER = 10_000
# Number of repetitions (we report the minimum).
REPEAT = 5


def make_payload(size):
    """Generate a pseudo-realistic compressible payload."""
    # Mix of repetitive and random-ish bytes to simulate CQL result rows.
    chunk = (b"row_value_" + os.urandom(6)) * (size // 16 + 1)
    return chunk[:size]


def bench(label, func, arg, inner=INNER, repeat=REPEAT):
    """Return the best per-call time in nanoseconds."""
    times = timeit.repeat(lambda: func(arg), number=inner, repeat=repeat)
    best = min(times) / inner
    ns = best * 1e9
    return ns


def main():
    if not HAS_PYTHON_LZ4 and not HAS_CYTHON:
        print("ERROR: Neither lz4 Python package nor cassandra.cython_lz4 available.")
        return

    headers = f"{'Payload':<8} {'Operation':<12} "
    if HAS_PYTHON_LZ4:
        headers += f"{'Python (ns)':>12} "
    if HAS_CYTHON:
        headers += f"{'Cython (ns)':>12} "
    if HAS_PYTHON_LZ4 and HAS_CYTHON:
        headers += f"{'Speedup':>8}"
    print(headers)
    print("-" * len(headers))

    for size_label, size in SIZES.items():
        payload = make_payload(size)

        # -- compress --
        py_compressed = None
        cy_compressed = None

        row = f"{size_label:<8} {'compress':<12} "
        if HAS_PYTHON_LZ4:
            py_ns = bench("py_compress", py_lz4_compress, payload)
            py_compressed = py_lz4_compress(payload)
            row += f"{py_ns:>12.1f} "
        if HAS_CYTHON:
            cy_ns = bench("cy_compress", cy_lz4_compress, payload)
            cy_compressed = cy_lz4_compress(payload)
            row += f"{cy_ns:>12.1f} "
            if HAS_PYTHON_LZ4:
                speedup = py_ns / cy_ns if cy_ns > 0 else float('inf')
                row += f"{speedup:>7.2f}x"
        print(row)

        # Verify cross-compatibility: Cython can decompress Python's output
        if HAS_PYTHON_LZ4 and HAS_CYTHON:
            assert cy_lz4_decompress(py_compressed) == payload, "cross-compat failed (py->cy)"
            assert py_lz4_decompress(cy_compressed) == payload, "cross-compat failed (cy->py)"

        # -- decompress --
        row = f"{size_label:<8} {'decompress':<12} "
        if HAS_PYTHON_LZ4:
            py_ns = bench("py_decompress", py_lz4_decompress, py_compressed)
            row += f"{py_ns:>12.1f} "
        if HAS_CYTHON:
            # Use Cython-compressed data for the Cython decompress benchmark
            cy_ns = bench("cy_decompress", cy_lz4_decompress, cy_compressed)
            row += f"{cy_ns:>12.1f} "
            if HAS_PYTHON_LZ4:
                speedup = py_ns / cy_ns if cy_ns > 0 else float('inf')
                row += f"{speedup:>7.2f}x"
        print(row)

        print()


if __name__ == "__main__":
    main()
