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
Benchmark: ExecuteMessage._write_query_params() and send_body() for vector
INSERT workloads.

Compares five approaches for the parameter serialization hot loop:

  1. baseline       – current code (calling write_value() per param)
  2. pr788_inline   – PR #788 inlining (local aliases, inline write_value)
  3. buf_accum      – buffer accumulation (collect parts in list, single join)
  4. combined       – inlining + buffer accumulation
  5. module_current – whatever the loaded module provides (.so or .py)

Variants 1-4 are standalone pure-Python functions that call into
Cython-compiled helpers (write_value, write_string, etc.) when the .so is
loaded.  Variant 5 calls the actual module method directly.

NOTE: To compare Cython vs pure-Python for variant 5, move the .so aside:
  mv cassandra/protocol.cpython-*-linux-gnu.so{,.bak}

Usage:
  python benchmarks/bench_execute_write_params.py
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
from cassandra.protocol import (
    ExecuteMessage,
    _QueryMessage,
    ProtocolHandler,
    write_consistency_level,
    write_byte,
    write_uint,
    write_short,
    write_int,
    write_long,
    write_string,
    write_value,
    _UNSET_VALUE,
    _VALUES_FLAG,
    _WITH_SERIAL_CONSISTENCY_FLAG,
    _PAGE_SIZE_FLAG,
    _WITH_PAGING_STATE_FLAG,
    _PROTOCOL_TIMESTAMP_FLAG,
    _WITH_KEYSPACE_FLAG,
)
from cassandra import ProtocolVersion
from cassandra.marshal import int32_pack, uint16_pack, uint8_pack, uint64_pack

# ---------------------------------------------------------------------------
# Pre-computed constants (as in PR #788)
# ---------------------------------------------------------------------------
_INT32_NEG1 = int32_pack(-1)  # NULL marker
_INT32_NEG2 = int32_pack(-2)  # UNSET marker


# ===================================================================
# Variant 1: baseline – mirrors current _write_query_params exactly
# ===================================================================


def baseline_write_query_params(msg, f, protocol_version):
    write_consistency_level(f, msg.consistency_level)
    flags = 0x00
    if msg.query_params is not None:
        flags |= _VALUES_FLAG
    if msg.serial_consistency_level:
        flags |= _WITH_SERIAL_CONSISTENCY_FLAG
    if msg.fetch_size:
        flags |= _PAGE_SIZE_FLAG
    if msg.paging_state:
        flags |= _WITH_PAGING_STATE_FLAG
    if msg.timestamp is not None:
        flags |= _PROTOCOL_TIMESTAMP_FLAG
    if getattr(msg, "keyspace", None) is not None:
        if ProtocolVersion.uses_keyspace_flag(protocol_version):
            flags |= _WITH_KEYSPACE_FLAG
    if ProtocolVersion.uses_int_query_flags(protocol_version):
        write_uint(f, flags)
    else:
        write_byte(f, flags)
    if msg.query_params is not None:
        write_short(f, len(msg.query_params))
        for param in msg.query_params:
            write_value(f, param)
    if msg.fetch_size:
        write_int(f, msg.fetch_size)
    if msg.paging_state:
        write_string(f, msg.paging_state)
    if msg.serial_consistency_level:
        write_consistency_level(f, msg.serial_consistency_level)
    if msg.timestamp is not None:
        write_long(f, msg.timestamp)


def baseline_send_body(msg, f, protocol_version):
    write_string(f, msg.query_id)
    if ProtocolVersion.uses_prepared_metadata(protocol_version):
        write_string(f, msg.result_metadata_id)
    baseline_write_query_params(msg, f, protocol_version)


# ===================================================================
# Variant 2: pr788_inline – inline write_value with local aliases
# ===================================================================


def pr788_write_query_params(msg, f, protocol_version):
    write_consistency_level(f, msg.consistency_level)
    flags = 0x00
    if msg.query_params is not None:
        flags |= _VALUES_FLAG
    if msg.serial_consistency_level:
        flags |= _WITH_SERIAL_CONSISTENCY_FLAG
    if msg.fetch_size:
        flags |= _PAGE_SIZE_FLAG
    if msg.paging_state:
        flags |= _WITH_PAGING_STATE_FLAG
    if msg.timestamp is not None:
        flags |= _PROTOCOL_TIMESTAMP_FLAG
    if getattr(msg, "keyspace", None) is not None:
        if ProtocolVersion.uses_keyspace_flag(protocol_version):
            flags |= _WITH_KEYSPACE_FLAG
    if ProtocolVersion.uses_int_query_flags(protocol_version):
        write_uint(f, flags)
    else:
        write_byte(f, flags)
    if msg.query_params is not None:
        write_short(f, len(msg.query_params))
        _fw = f.write
        _i32 = int32_pack
        for param in msg.query_params:
            if param is None:
                _fw(_INT32_NEG1)
            elif param is _UNSET_VALUE:
                _fw(_INT32_NEG2)
            else:
                _fw(_i32(len(param)))
                _fw(param)
    if msg.fetch_size:
        write_int(f, msg.fetch_size)
    if msg.paging_state:
        write_string(f, msg.paging_state)
    if msg.serial_consistency_level:
        write_consistency_level(f, msg.serial_consistency_level)
    if msg.timestamp is not None:
        write_long(f, msg.timestamp)


def pr788_send_body(msg, f, protocol_version):
    write_string(f, msg.query_id)
    if ProtocolVersion.uses_prepared_metadata(protocol_version):
        write_string(f, msg.result_metadata_id)
    pr788_write_query_params(msg, f, protocol_version)


# ===================================================================
# Variant 3: buf_accum – collect all writes in a list, single join
# ===================================================================


def bufaccum_write_query_params(msg, f, protocol_version):
    parts = []
    _p = parts.append
    _i32 = int32_pack
    _u16 = uint16_pack
    _u8 = uint8_pack
    _u64 = uint64_pack

    _p(_u16(msg.consistency_level))

    flags = 0x00
    if msg.query_params is not None:
        flags |= _VALUES_FLAG
    if msg.serial_consistency_level:
        flags |= _WITH_SERIAL_CONSISTENCY_FLAG
    if msg.fetch_size:
        flags |= _PAGE_SIZE_FLAG
    if msg.paging_state:
        flags |= _WITH_PAGING_STATE_FLAG
    if msg.timestamp is not None:
        flags |= _PROTOCOL_TIMESTAMP_FLAG
    if getattr(msg, "keyspace", None) is not None:
        if ProtocolVersion.uses_keyspace_flag(protocol_version):
            flags |= _WITH_KEYSPACE_FLAG

    if ProtocolVersion.uses_int_query_flags(protocol_version):
        from cassandra.marshal import uint32_pack

        _p(uint32_pack(flags))
    else:
        _p(_u8(flags))

    if msg.query_params is not None:
        _p(_u16(len(msg.query_params)))
        for param in msg.query_params:
            if param is None:
                _p(_INT32_NEG1)
            elif param is _UNSET_VALUE:
                _p(_INT32_NEG2)
            else:
                _p(_i32(len(param)))
                _p(param)

    if msg.fetch_size:
        _p(_i32(msg.fetch_size))
    if msg.paging_state:
        ps = msg.paging_state
        if isinstance(ps, str):
            ps = ps.encode("utf8")
        _p(_u16(len(ps)))
        _p(ps)
    if msg.serial_consistency_level:
        _p(_u16(msg.serial_consistency_level))
    if msg.timestamp is not None:
        _p(_u64(msg.timestamp))

    f.write(b"".join(parts))


def bufaccum_send_body(msg, f, protocol_version):
    write_string(f, msg.query_id)
    if ProtocolVersion.uses_prepared_metadata(protocol_version):
        write_string(f, msg.result_metadata_id)
    bufaccum_write_query_params(msg, f, protocol_version)


# ===================================================================
# Variant 4: combined – inline write_value + buffer accumulation
#   (single len+data concat per param, then single join)
# ===================================================================


def combined_write_query_params(msg, f, protocol_version):
    parts = []
    _p = parts.append
    _i32 = int32_pack
    _u16 = uint16_pack
    _u8 = uint8_pack
    _u64 = uint64_pack

    _p(_u16(msg.consistency_level))

    flags = 0x00
    if msg.query_params is not None:
        flags |= _VALUES_FLAG
    if msg.serial_consistency_level:
        flags |= _WITH_SERIAL_CONSISTENCY_FLAG
    if msg.fetch_size:
        flags |= _PAGE_SIZE_FLAG
    if msg.paging_state:
        flags |= _WITH_PAGING_STATE_FLAG
    if msg.timestamp is not None:
        flags |= _PROTOCOL_TIMESTAMP_FLAG
    if getattr(msg, "keyspace", None) is not None:
        if ProtocolVersion.uses_keyspace_flag(protocol_version):
            flags |= _WITH_KEYSPACE_FLAG

    if ProtocolVersion.uses_int_query_flags(protocol_version):
        from cassandra.marshal import uint32_pack

        _p(uint32_pack(flags))
    else:
        _p(_u8(flags))

    if msg.query_params is not None:
        _p(_u16(len(msg.query_params)))
        for param in msg.query_params:
            if param is None:
                _p(_INT32_NEG1)
            elif param is _UNSET_VALUE:
                _p(_INT32_NEG2)
            else:
                _p(_i32(len(param)) + param)  # single concat per param

    if msg.fetch_size:
        _p(_i32(msg.fetch_size))
    if msg.paging_state:
        ps = msg.paging_state
        if isinstance(ps, str):
            ps = ps.encode("utf8")
        _p(_u16(len(ps)))
        _p(ps)
    if msg.serial_consistency_level:
        _p(_u16(msg.serial_consistency_level))
    if msg.timestamp is not None:
        _p(_u64(msg.timestamp))

    f.write(b"".join(parts))


def combined_send_body(msg, f, protocol_version):
    write_string(f, msg.query_id)
    if ProtocolVersion.uses_prepared_metadata(protocol_version):
        write_string(f, msg.result_metadata_id)
    combined_write_query_params(msg, f, protocol_version)


# ===================================================================
# Test scenarios
# ===================================================================


def make_vector_params(dim):
    """Simulate a prepared INSERT with (int32_key, float_vector) params.

    Returns a list of pre-serialized bytes, as BoundStatement.bind() would
    produce *after* calling col_type.serialize() on each value.
    """
    int_key = int32_pack(42)  # 4 bytes
    vector_bytes = struct.pack(f">{dim}f", *([0.1] * dim))  # dim * 4 bytes
    return [int_key, vector_bytes]


def make_scalar_params(n, size=20):
    """Simulate n text columns of `size` bytes each."""
    return [b"\x41" * size for _ in range(n)]


PROTO_VERSION = 4
ITERATIONS = 500_000
REPEATS = 5

SCENARIOS = [
    ("128D vector INSERT (2 params)", make_vector_params(128)),
    ("768D vector INSERT (2 params)", make_vector_params(768)),
    ("1536D vector INSERT (2 params)", make_vector_params(1536)),
    ("scalar 10 text cols (10 params)", make_scalar_params(10, 20)),
]

# _write_query_params variants (the core hot path)
WQP_VARIANTS = [
    ("1_baseline", baseline_write_query_params),
    ("2_pr788_inline", pr788_write_query_params),
    ("3_buf_accum", bufaccum_write_query_params),
    ("4_combined", combined_write_query_params),
]

# send_body variants (includes query_id framing)
SB_VARIANTS = [
    ("1_baseline", baseline_send_body),
    ("2_pr788_inline", pr788_send_body),
    ("3_buf_accum", bufaccum_send_body),
    ("4_combined", combined_send_body),
]


# ===================================================================
# Benchmark helpers
# ===================================================================


def verify_output(ref_fn, test_fn, msg, pv):
    """Verify two functions produce byte-identical output."""
    f1 = io.BytesIO()
    ref_fn(msg, f1, pv)
    ref_bytes = f1.getvalue()

    f2 = io.BytesIO()
    test_fn(msg, f2, pv)
    test_bytes = f2.getvalue()

    if ref_bytes != test_bytes:
        for i, (a, b) in enumerate(zip(ref_bytes, test_bytes)):
            if a != b:
                return False, f"diff at byte {i}: ref=0x{a:02x}, test=0x{b:02x}"
        if len(ref_bytes) != len(test_bytes):
            return False, f"len diff: ref={len(ref_bytes)}, test={len(test_bytes)}"
    return True, ""


def bench_fn(fn, msg, pv, iterations, repeats):
    """Benchmark a single function, return best ns/call."""
    f = io.BytesIO()

    def run():
        f.seek(0)
        f.truncate()
        fn(msg, f, pv)

    t = timeit.repeat(run, number=iterations, repeat=repeats, timer=time.process_time)
    return min(t) / iterations * 1e9


def make_execute_msg(params):
    """Create a realistic ExecuteMessage for a prepared INSERT."""
    return ExecuteMessage(
        query_id=b"\x01\x02\x03\x04\x05\x06\x07\x08",  # 8-byte prepared query ID
        query_params=params,
        consistency_level=1,  # ONE
        timestamp=1234567890123456,  # typical microsecond timestamp
        # No serial CL, no fetch_size, no paging — typical INSERT
    )


# ===================================================================
# Main
# ===================================================================


def main():
    is_cython = cassandra.protocol.__file__.endswith(".so")
    print(f"Python:  {sys.version.split()[0]}")
    print(f"Module:  {cassandra.protocol.__file__}")
    print(f"Cython:  {'YES (.so loaded)' if is_cython else 'NO (pure Python .py)'}")
    print(f"Config:  proto v{PROTO_VERSION}, {ITERATIONS:,} iters, best of {REPEATS}")
    print()
    print("NOTE: Variants 1-4 are standalone pure-Python functions.")
    print(
        "      They call Cython-compiled helpers (write_value, etc.) when .so is loaded."
    )
    print("      'module' calls the actual loaded module method directly.")
    print()

    # Grab the base-class _write_query_params to bypass ExecuteMessage's
    # super() overhead — gives a fair comparison with standalone functions.
    _module_wqp = _QueryMessage._write_query_params

    for scenario_label, params in SCENARIOS:
        msg = make_execute_msg(params)
        total_param_bytes = sum(len(p) for p in params)
        print(f"=== {scenario_label} (payload: {total_param_bytes:,} bytes) ===")
        print()

        # ---- _write_query_params benchmarks ----
        print("  _write_query_params() [core hot path]:")
        print(f"    {'variant':20s}  {'ns/call':>10s}  {'vs baseline':>11s}")
        print(f"    {'-------':20s}  {'-------':>10s}  {'-----------':>11s}")

        baseline_wqp_ns = None
        for var_label, var_fn in WQP_VARIANTS:
            ok, err = verify_output(
                baseline_write_query_params, var_fn, msg, PROTO_VERSION
            )
            if not ok:
                print(f"    {var_label:20s}  MISMATCH: {err}")
                continue
            ns = bench_fn(var_fn, msg, PROTO_VERSION, ITERATIONS, REPEATS)
            if baseline_wqp_ns is None:
                baseline_wqp_ns = ns
            speedup = baseline_wqp_ns / ns
            print(f"    {var_label:20s}  {ns:8.1f}    {speedup:5.2f}x")

        # Module variant (bypass super() for fair comparison)
        def module_wqp(m, f, pv):
            _module_wqp(m, f, pv)

        ok, err = verify_output(
            baseline_write_query_params, module_wqp, msg, PROTO_VERSION
        )
        if ok:
            ns = bench_fn(module_wqp, msg, PROTO_VERSION, ITERATIONS, REPEATS)
            speedup = baseline_wqp_ns / ns if baseline_wqp_ns else 0
            label = "5_module" + (" (cython)" if is_cython else " (py)")
            print(f"    {label:20s}  {ns:8.1f}    {speedup:5.2f}x")
        else:
            print(f"    5_module             MISMATCH: {err}")

        print()

        # ---- send_body benchmarks ----
        print("  send_body() [includes query_id framing]:")
        print(f"    {'variant':20s}  {'ns/call':>10s}  {'vs baseline':>11s}")
        print(f"    {'-------':20s}  {'-------':>10s}  {'-----------':>11s}")

        baseline_sb_ns = None
        for var_label, var_fn in SB_VARIANTS:
            ok, err = verify_output(baseline_send_body, var_fn, msg, PROTO_VERSION)
            if not ok:
                print(f"    {var_label:20s}  MISMATCH: {err}")
                continue
            ns = bench_fn(var_fn, msg, PROTO_VERSION, ITERATIONS, REPEATS)
            if baseline_sb_ns is None:
                baseline_sb_ns = ns
            speedup = baseline_sb_ns / ns
            print(f"    {var_label:20s}  {ns:8.1f}    {speedup:5.2f}x")

        # Module send_body (direct method call, no lambda)
        def module_sb(m, f, pv):
            m.send_body(f, pv)

        ok, err = verify_output(baseline_send_body, module_sb, msg, PROTO_VERSION)
        if ok:
            ns = bench_fn(module_sb, msg, PROTO_VERSION, ITERATIONS, REPEATS)
            speedup = baseline_sb_ns / ns if baseline_sb_ns else 0
            label = "5_module" + (" (cython)" if is_cython else " (py)")
            print(f"    {label:20s}  {ns:8.1f}    {speedup:5.2f}x")
        else:
            print(f"    5_module             MISMATCH: {err}")

        print()

        # ---- encode_message benchmark (full wire frame) ----
        print("  encode_message() [full wire frame]:")

        def run_encode():
            return ProtocolHandler.encode_message(
                msg,
                stream_id=1,
                protocol_version=PROTO_VERSION,
                compressor=None,
                allow_beta_protocol_version=False,
            )

        ref_frame = run_encode()
        t = timeit.repeat(
            run_encode, number=ITERATIONS, repeat=REPEATS, timer=time.process_time
        )
        enc_ns = min(t) / ITERATIONS * 1e9
        print(f"    {'current':20s}  {enc_ns:8.1f}    (frame: {len(ref_frame)} bytes)")
        print()
        print()


if __name__ == "__main__":
    main()
