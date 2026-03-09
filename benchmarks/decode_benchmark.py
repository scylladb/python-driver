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
Isolated benchmark for ProtocolHandler.decode_message().

Measures the throughput of decoding synthetic RESULT/ROWS messages of
varying sizes.  Does NOT require a live Cassandra/Scylla cluster.

Run on both ``master`` and the ``remove_copies`` branch to compare:

    python benchmarks/decode_benchmark.py
    python benchmarks/decode_benchmark.py --scenarios small_100,large_5k_1KB
    python benchmarks/decode_benchmark.py --cython-only --iterations 20
    python benchmarks/decode_benchmark.py --cprofile medium_1k_1KB
"""

from __future__ import print_function

import argparse
import gc
import os
import statistics
import struct
import sys
import time

# ---------------------------------------------------------------------------
# Pin to a single CPU core for consistent results
# ---------------------------------------------------------------------------
try:
    os.sched_setaffinity(0, {0})
except (AttributeError, OSError):
    # sched_setaffinity is Linux-only; silently skip on other platforms
    pass

# ---------------------------------------------------------------------------
# Make sure the driver package is importable from the repo root
# ---------------------------------------------------------------------------
_benchdir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(_benchdir, ".."))

from io import BytesIO
from cassandra.marshal import int32_pack, int64_pack, double_pack
from cassandra.protocol import (
    write_int,
    write_short,
    write_string,
    write_value,
    ProtocolHandler,
    _ProtocolHandler,
    HAVE_CYTHON,
)

# ---------------------------------------------------------------------------
# CQL type codes (native protocol v4)
# ---------------------------------------------------------------------------
TYPE_BIGINT = 0x0002
TYPE_BLOB = 0x0003
TYPE_DOUBLE = 0x0007
TYPE_INT = 0x0009
TYPE_VARCHAR = 0x000D

# Metadata flag
_FLAGS_GLOBAL_TABLES_SPEC = 0x0001

# ResultMessage kind
_RESULT_KIND_ROWS = 0x0002

# ResultMessage opcode
_OPCODE_RESULT = 0x08


# ======================================================================
# Synthetic message construction
# ======================================================================


def _build_rows_body(columns, row_values_fn, row_count):
    """
    Build the raw bytes for a RESULT/ROWS message body.

    Parameters
    ----------
    columns : list of (name: str, type_code: int)
        Column definitions.
    row_values_fn : callable() -> list[bytes|None]
        Returns one row of pre-encoded cell values each time it is called.
    row_count : int
        Number of rows to encode.

    Returns
    -------
    bytes
        Complete RESULT body ready for ``decode_message()``.
    """
    buf = BytesIO()

    # kind = ROWS
    write_int(buf, _RESULT_KIND_ROWS)

    # --- metadata ---
    write_int(buf, _FLAGS_GLOBAL_TABLES_SPEC)
    write_int(buf, len(columns))
    write_string(buf, "ks")
    write_string(buf, "tbl")
    for col_name, type_code in columns:
        write_string(buf, col_name)
        write_short(buf, type_code)

    # --- rows ---
    write_int(buf, row_count)
    for _ in range(row_count):
        for cell in row_values_fn():
            write_value(buf, cell)

    return buf.getvalue()


# ======================================================================
# Scenario definitions
# ======================================================================


def _make_text(size):
    """Return a UTF-8 encoded bytes value of exactly *size* bytes."""
    return b"x" * size


def _scenario_small_100():
    """100 rows, 3 cols (text 50B, int, bigint) ~3 KB"""
    columns = [
        ("col_text", TYPE_VARCHAR),
        ("col_int", TYPE_INT),
        ("col_bigint", TYPE_BIGINT),
    ]
    text_val = _make_text(50)
    int_val = int32_pack(42)
    bigint_val = int64_pack(123456789)
    row = lambda: [text_val, int_val, bigint_val]
    return _build_rows_body(columns, row, 100)


def _scenario_medium_1k_256B():
    """1000 rows, 3 cols (text 256B, int, bigint) ~273 KB"""
    columns = [
        ("col_text", TYPE_VARCHAR),
        ("col_int", TYPE_INT),
        ("col_bigint", TYPE_BIGINT),
    ]
    text_val = _make_text(256)
    int_val = int32_pack(42)
    bigint_val = int64_pack(123456789)
    row = lambda: [text_val, int_val, bigint_val]
    return _build_rows_body(columns, row, 1000)


def _scenario_medium_1k_1KB():
    """1000 rows, 3 cols (text 1024B, int, bigint) ~1 MB"""
    columns = [
        ("col_text", TYPE_VARCHAR),
        ("col_int", TYPE_INT),
        ("col_bigint", TYPE_BIGINT),
    ]
    text_val = _make_text(1024)
    int_val = int32_pack(42)
    bigint_val = int64_pack(123456789)
    row = lambda: [text_val, int_val, bigint_val]
    return _build_rows_body(columns, row, 1000)


def _scenario_large_5k_1KB():
    """5000 rows, 3 cols (text 1024B, int, bigint) ~5 MB"""
    columns = [
        ("col_text", TYPE_VARCHAR),
        ("col_int", TYPE_INT),
        ("col_bigint", TYPE_BIGINT),
    ]
    text_val = _make_text(1024)
    int_val = int32_pack(42)
    bigint_val = int64_pack(123456789)
    row = lambda: [text_val, int_val, bigint_val]
    return _build_rows_body(columns, row, 5000)


def _scenario_large_1k_4KB():
    """1000 rows, 3 cols (text 4096B, int, bigint) ~4 MB"""
    columns = [
        ("col_text", TYPE_VARCHAR),
        ("col_int", TYPE_INT),
        ("col_bigint", TYPE_BIGINT),
    ]
    text_val = _make_text(4096)
    int_val = int32_pack(42)
    bigint_val = int64_pack(123456789)
    row = lambda: [text_val, int_val, bigint_val]
    return _build_rows_body(columns, row, 1000)


def _scenario_wide_5k_doubles():
    """5000 rows, 10 cols (10x double) ~586 KB"""
    columns = [("col_d%d" % i, TYPE_DOUBLE) for i in range(10)]
    vals = [double_pack(1.0 + i * 0.1) for i in range(10)]
    row = lambda: list(vals)
    return _build_rows_body(columns, row, 5000)


def _scenario_wide_1k_20cols():
    """1000 rows, 20 cols (10x text 64B, 5x int, 3x bigint, 2x double) ~850 KB"""
    columns = []
    for i in range(10):
        columns.append(("col_text%d" % i, TYPE_VARCHAR))
    for i in range(5):
        columns.append(("col_int%d" % i, TYPE_INT))
    for i in range(3):
        columns.append(("col_bigint%d" % i, TYPE_BIGINT))
    for i in range(2):
        columns.append(("col_double%d" % i, TYPE_DOUBLE))

    text_val = _make_text(64)
    int_val = int32_pack(42)
    bigint_val = int64_pack(123456789)
    double_val = double_pack(3.14159)

    def row():
        cells = []
        for _ in range(10):
            cells.append(text_val)
        for _ in range(5):
            cells.append(int_val)
        for _ in range(3):
            cells.append(bigint_val)
        for _ in range(2):
            cells.append(double_val)
        return cells

    return _build_rows_body(columns, row, 1000)


def _scenario_blob_1k_16KB():
    """1000 rows, 2 cols (int, 16 KB blob) ~16 MB"""
    columns = [
        ("col_int", TYPE_INT),
        ("col_blob", TYPE_BLOB),
    ]
    int_val = int32_pack(42)
    blob_val = os.urandom(16384)
    row = lambda: [int_val, blob_val]
    return _build_rows_body(columns, row, 1000)


SCENARIOS = {
    "small_100": ("100 rows, 3 cols (text 50B, int, bigint)", _scenario_small_100),
    "medium_1k_256B": (
        "1000 rows, 3 cols (text 256B, int, bigint)",
        _scenario_medium_1k_256B,
    ),
    "medium_1k_1KB": (
        "1000 rows, 3 cols (text 1024B, int, bigint)",
        _scenario_medium_1k_1KB,
    ),
    "large_5k_1KB": (
        "5000 rows, 3 cols (text 1024B, int, bigint)",
        _scenario_large_5k_1KB,
    ),
    "large_1k_4KB": (
        "1000 rows, 3 cols (text 4096B, int, bigint)",
        _scenario_large_1k_4KB,
    ),
    "wide_5k_doubles": ("5000 rows, 10 cols (10x double)", _scenario_wide_5k_doubles),
    "wide_1k_20cols": (
        "1000 rows, 20 cols (10x text64, 5x int, ...)",
        _scenario_wide_1k_20cols,
    ),
    "blob_1k_16KB": ("1000 rows, 2 cols (int, 16 KB blob)", _scenario_blob_1k_16KB),
}

# Ordered list so output is deterministic
SCENARIO_ORDER = [
    "small_100",
    "medium_1k_256B",
    "medium_1k_1KB",
    "large_5k_1KB",
    "large_1k_4KB",
    "wide_5k_doubles",
    "wide_1k_20cols",
    "blob_1k_16KB",
]


# ======================================================================
# Benchmark runner
# ======================================================================


def _decode(handler, body):
    """Call decode_message with the standard benchmark parameters."""
    return handler.decode_message(
        protocol_version=4,
        protocol_features=None,
        user_type_map={},
        stream_id=0,
        flags=0,
        opcode=_OPCODE_RESULT,
        body=body,
        decompressor=None,
        result_metadata=None,
    )


def _run_iterations(handler, body, iterations, warmup):
    """
    Run *warmup* + *iterations* decode calls, return list of elapsed
    times (seconds) for the measured iterations only.
    """
    # Warm-up: let JIT / caches settle
    for _ in range(warmup):
        _decode(handler, body)

    gc.disable()
    try:
        times = []
        for _ in range(iterations):
            t0 = time.perf_counter()
            _decode(handler, body)
            t1 = time.perf_counter()
            times.append(t1 - t0)
    finally:
        gc.enable()
    return times


def _format_time(seconds):
    """Human-readable time string.  Always reports in microseconds for
    consistent cross-scenario comparison."""
    return "%.1f us" % (seconds * 1e6)


def _format_throughput(body_size, seconds):
    """MB/s throughput string."""
    mb = body_size / (1024 * 1024)
    return "%.1f MB/s" % (mb / seconds)


def _report(label, times, body_size, row_count):
    """Print a single result line."""
    t_min = min(times)
    t_med = statistics.median(times)
    t_mean = statistics.mean(times)
    rows_per_sec = row_count / t_med

    if rows_per_sec >= 1e6:
        rps_str = "%.2fM rows/s" % (rows_per_sec / 1e6)
    elif rows_per_sec >= 1e3:
        rps_str = "%.0fK rows/s" % (rows_per_sec / 1e3)
    else:
        rps_str = "%.0f rows/s" % rows_per_sec

    print(
        "  %-14s  min=%s  median=%s  mean=%s  (%s, %s)"
        % (
            label,
            _format_time(t_min),
            _format_time(t_med),
            _format_time(t_mean),
            _format_throughput(body_size, t_med),
            rps_str,
        )
    )


def _extract_row_count(scenario_name):
    """Infer the row count from the scenario name for rows/s reporting."""
    mapping = {
        "small_100": 100,
        "medium_1k_256B": 1000,
        "medium_1k_1KB": 1000,
        "large_5k_1KB": 5000,
        "large_1k_4KB": 1000,
        "wide_5k_doubles": 5000,
        "wide_1k_20cols": 1000,
        "blob_1k_16KB": 1000,
    }
    return mapping.get(scenario_name, 0)


def run_benchmark(
    scenarios, iterations, warmup, cython_only, python_only, cprofile_scenario
):
    """
    Run the benchmark for each requested scenario.
    """
    print("=" * 78)
    print("Decode Benchmark")
    print("=" * 78)
    print("  Cython available : %s" % HAVE_CYTHON)
    print("  Iterations       : %d (+ %d warmup)" % (iterations, warmup))
    print("  CPU pinned       : %s" % _is_pinned())
    print()

    handlers = []
    if not python_only:
        if HAVE_CYTHON:
            handlers.append(("Cython", ProtocolHandler))
        elif not cython_only:
            print("  [NOTE] Cython extensions not available, skipping Cython path\n")
    if not cython_only:
        handlers.append(("Python", _ProtocolHandler))

    if not handlers:
        print(
            "ERROR: no handlers selected (Cython not available and --cython-only set)"
        )
        sys.exit(1)

    profiler = None
    if cprofile_scenario:
        import cProfile

        profiler = cProfile.Profile()

    for name in scenarios:
        desc, builder = SCENARIOS[name]
        body = builder()
        body_size = len(body)
        row_count = _extract_row_count(name)

        print("Scenario: %s  (%s, %s body)" % (name, desc, _format_size(body_size)))

        for label, handler in handlers:
            if profiler and name == cprofile_scenario:
                profiler.enable()

            times = _run_iterations(handler, body, iterations, warmup)

            if profiler and name == cprofile_scenario:
                profiler.disable()

            _report(label + ":", times, body_size, row_count)

        print()

    if profiler:
        print("-" * 78)
        print("cProfile results for scenario '%s':" % cprofile_scenario)
        print("-" * 78)
        import pstats

        stats = pstats.Stats(profiler)
        stats.strip_dirs()
        stats.sort_stats("cumulative")
        stats.print_stats(30)


def _format_size(nbytes):
    """Human-readable byte size."""
    if nbytes >= 1024 * 1024:
        return "%.1f MB" % (nbytes / (1024 * 1024))
    elif nbytes >= 1024:
        return "%.1f KB" % (nbytes / 1024)
    else:
        return "%d B" % nbytes


def _is_pinned():
    """Check if the process is pinned to a single CPU core."""
    try:
        affinity = os.sched_getaffinity(0)
        return len(affinity) == 1
    except (AttributeError, OSError):
        return False


# ======================================================================
# CLI
# ======================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Isolated decode_message benchmark (no cluster required)"
    )
    parser.add_argument(
        "--iterations",
        "-n",
        type=int,
        default=10,
        help="Number of timed iterations per scenario (default: 10)",
    )
    parser.add_argument(
        "--warmup",
        "-w",
        type=int,
        default=3,
        help="Number of warmup iterations (default: 3)",
    )
    parser.add_argument(
        "--scenarios",
        "-s",
        type=str,
        default=None,
        help="Comma-separated list of scenarios to run (default: all). "
        "Available: %s" % ", ".join(SCENARIO_ORDER),
    )
    parser.add_argument(
        "--cython-only",
        action="store_true",
        default=False,
        help="Only benchmark the Cython (fast) path",
    )
    parser.add_argument(
        "--python-only",
        action="store_true",
        default=False,
        help="Only benchmark the pure-Python path",
    )
    parser.add_argument(
        "--cprofile",
        type=str,
        default=None,
        metavar="SCENARIO",
        help="Enable cProfile for the named scenario and print top-30 stats",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        default=False,
        help="List available scenarios and exit",
    )

    args = parser.parse_args()

    if args.list:
        print("Available scenarios:")
        for name in SCENARIO_ORDER:
            desc, builder = SCENARIOS[name]
            print("  %-20s  %s" % (name, desc))
        sys.exit(0)

    if args.cython_only and args.python_only:
        parser.error("--cython-only and --python-only are mutually exclusive")

    if args.scenarios:
        selected = [s.strip() for s in args.scenarios.split(",")]
        for s in selected:
            if s not in SCENARIOS:
                parser.error("Unknown scenario: %s" % s)
    else:
        selected = list(SCENARIO_ORDER)

    if args.cprofile and args.cprofile not in selected:
        parser.error(
            "--cprofile scenario '%s' is not in the selected scenarios" % args.cprofile
        )

    run_benchmark(
        scenarios=selected,
        iterations=args.iterations,
        warmup=args.warmup,
        cython_only=args.cython_only,
        python_only=args.python_only,
        cprofile_scenario=args.cprofile,
    )


if __name__ == "__main__":
    main()
