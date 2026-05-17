#!/usr/bin/env python3
"""
Benchmark: Cython metadata_parser vs Python BytesIO metadata parsing.

Isolates metadata parsing (recv_results_metadata) separately from row parsing.
Also measures the combined recv_results_rows path.

Run with:  taskset -c 0 python3 bench_metadata_parsing.py
"""

import struct
import io
import time
import sys
import uuid

# ── helpers ──────────────────────────────────────────────────────────────

def write_short(buf, v):
    buf.write(struct.pack('>H', v))

def write_int(buf, v):
    buf.write(struct.pack('>i', v))

def write_string(buf, s):
    if isinstance(s, str):
        s = s.encode('utf8')
    write_short(buf, len(s))
    buf.write(s)

def write_type(buf, type_code, subtypes=()):
    """Write a CQL type option to buf."""
    write_short(buf, type_code)
    for st in subtypes:
        if isinstance(st, tuple):
            write_type(buf, st[0], st[1:])
        else:
            write_type(buf, st)


# Type codes
UUID_TYPE = 0x000C
VARCHAR_TYPE = 0x000D
INT_TYPE = 0x0009
BIGINT_TYPE = 0x0002
BOOLEAN_TYPE = 0x0004
DOUBLE_TYPE = 0x0007
TIMESTAMP_TYPE = 0x000B
LIST_TYPE = 0x0020
MAP_TYPE = 0x0021
SET_TYPE = 0x0022


def build_metadata_bytes(colcount, type_codes_list, global_table_spec=True,
                         no_metadata=False):
    """Build just the metadata portion of a RESULT ROWS message."""
    buf = io.BytesIO()
    if no_metadata:
        write_int(buf, 0x0004)  # NO_METADATA flag
        write_int(buf, colcount)
        return buf.getvalue()

    flags = 0x0001 if global_table_spec else 0x0000
    write_int(buf, flags)
    write_int(buf, colcount)

    if global_table_spec:
        write_string(buf, 'test_ks')
        write_string(buf, 'test_cf')

    for i in range(colcount):
        if not global_table_spec:
            write_string(buf, 'test_ks')
            write_string(buf, 'test_cf')
        write_string(buf, 'col_%d' % i)
        tc = type_codes_list[i % len(type_codes_list)]
        if isinstance(tc, tuple):
            write_type(buf, tc[0], tc[1:])
        else:
            write_type(buf, tc)

    return buf.getvalue()


def build_rows_message(colcount, type_codes_list, nrows=0, global_table_spec=True):
    """Build full RESULT ROWS body (metadata + rowcount + row data)."""
    meta_bytes = build_metadata_bytes(colcount, type_codes_list, global_table_spec)
    buf = io.BytesIO()
    buf.write(meta_bytes)
    write_int(buf, nrows)
    # Write minimal row data
    for _ in range(nrows):
        for i in range(colcount):
            tc = type_codes_list[i % len(type_codes_list)]
            base_tc = tc[0] if isinstance(tc, tuple) else tc
            if base_tc == UUID_TYPE:
                write_int(buf, 16)
                buf.write(uuid.uuid4().bytes)
            elif base_tc in (VARCHAR_TYPE,):
                val = b'test_value'
                write_int(buf, len(val))
                buf.write(val)
            elif base_tc in (INT_TYPE,):
                write_int(buf, 4)
                buf.write(struct.pack('>i', 42))
            elif base_tc in (BIGINT_TYPE, TIMESTAMP_TYPE, DOUBLE_TYPE):
                write_int(buf, 8)
                buf.write(struct.pack('>q', 12345678))
            elif base_tc == BOOLEAN_TYPE:
                write_int(buf, 1)
                buf.write(b'\x01')
            elif base_tc in (LIST_TYPE, SET_TYPE):
                write_int(buf, 4)
                buf.write(struct.pack('>i', 0))
            elif base_tc == MAP_TYPE:
                write_int(buf, 4)
                buf.write(struct.pack('>i', 0))
            else:
                write_int(buf, 4)
                buf.write(b'\x00\x00\x00\x00')
    return buf.getvalue()


def bench_fn(label, fn, iterations, warmup=500):
    """Benchmark a zero-arg callable. Returns (mean_ns, cv_pct)."""
    for _ in range(warmup):
        fn()
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        fn()
        t1 = time.perf_counter_ns()
        times.append(t1 - t0)
    times.sort()
    trim = max(1, len(times) // 20)
    trimmed = times[trim:-trim]
    mean_ns = sum(trimmed) / len(trimmed)
    variance = sum((t - mean_ns) ** 2 for t in trimmed) / len(trimmed)
    stddev = variance ** 0.5
    cv_pct = (stddev / mean_ns * 100) if mean_ns > 0 else 0
    print(f"  {label:50s}  {mean_ns:8.0f} ns  (cv {cv_pct:4.1f}%,  n={len(trimmed)})")
    return mean_ns, cv_pct


def main():
    from cassandra.protocol import ProtocolHandler, ResultMessage, read_int as py_read_int
    from cassandra.cython_deps import HAVE_CYTHON
    from cassandra.metadata_parser import make_recv_results_metadata
    from cassandra.bytesio import BytesIOReader

    print(f"HAVE_CYTHON: {HAVE_CYTHON}")
    print(f"Python: {sys.version}")
    print()

    recv_meta_br, recv_prep_br = make_recv_results_metadata()
    fast_cls = ProtocolHandler.message_types_by_opcode[0x08]
    slow_cls = ResultMessage

    iterations = 10000

    simple_types = [UUID_TYPE, VARCHAR_TYPE, INT_TYPE, BIGINT_TYPE, BOOLEAN_TYPE,
                    DOUBLE_TYPE, TIMESTAMP_TYPE, VARCHAR_TYPE, INT_TYPE, UUID_TYPE]

    # =================================================================
    print("=" * 72)
    print("PART 1: recv_results_metadata ONLY (metadata parsing isolation)")
    print("=" * 72)
    print()

    for colcount, label_suffix in [(3, "3 cols"), (10, "10 cols"), (50, "50 cols")]:
        types = simple_types[:colcount] if colcount <= len(simple_types) else simple_types
        meta_bytes = build_metadata_bytes(colcount, types)
        print(f"--- {label_suffix}, simple scalars ({len(meta_bytes)} bytes) ---")

        # Python: recv_results_metadata on BytesIO
        def py_meta(data=meta_bytes):
            f = io.BytesIO(data)
            msg = slow_cls(2)
            msg.recv_results_metadata(f, {})
            return msg
        t_py, _ = bench_fn("Python BytesIO recv_results_metadata", py_meta, iterations)

        # Cython: recv_results_metadata_br on BytesIOReader
        def cy_meta(data=meta_bytes):
            reader = BytesIOReader(data)
            msg = slow_cls(2)
            recv_meta_br(msg, reader, {})
            return msg
        t_cy, _ = bench_fn("Cython BytesIOReader recv_results_metadata", cy_meta, iterations)
        print(f"  Speedup: {t_py/t_cy:.2f}x")
        print()

    # NO_METADATA
    nm_bytes = build_metadata_bytes(10, simple_types, no_metadata=True)
    print(f"--- NO_METADATA, 10 cols ({len(nm_bytes)} bytes) ---")
    def py_nm(data=nm_bytes):
        f = io.BytesIO(data)
        msg = slow_cls(2)
        msg.recv_results_metadata(f, {})
    def cy_nm(data=nm_bytes):
        reader = BytesIOReader(data)
        msg = slow_cls(2)
        recv_meta_br(msg, reader, {})
    t_py_nm, _ = bench_fn("Python BytesIO recv_results_metadata", py_nm, iterations)
    t_cy_nm, _ = bench_fn("Cython BytesIOReader recv_results_metadata", cy_nm, iterations)
    print(f"  Speedup: {t_py_nm/t_cy_nm:.2f}x")
    print()

    # Collections
    coll_types = [UUID_TYPE, VARCHAR_TYPE,
                  (LIST_TYPE, VARCHAR_TYPE),
                  (SET_TYPE, INT_TYPE),
                  (MAP_TYPE, VARCHAR_TYPE, INT_TYPE),
                  INT_TYPE, BIGINT_TYPE, BOOLEAN_TYPE,
                  (LIST_TYPE, UUID_TYPE),
                  VARCHAR_TYPE]
    coll_bytes = build_metadata_bytes(10, coll_types)
    print(f"--- 10 cols with collections ({len(coll_bytes)} bytes) ---")
    def py_coll(data=coll_bytes):
        f = io.BytesIO(data)
        msg = slow_cls(2)
        msg.recv_results_metadata(f, {})
    def cy_coll(data=coll_bytes):
        reader = BytesIOReader(data)
        msg = slow_cls(2)
        recv_meta_br(msg, reader, {})
    t_py_coll, _ = bench_fn("Python BytesIO recv_results_metadata", py_coll, iterations)
    t_cy_coll, _ = bench_fn("Cython BytesIOReader recv_results_metadata", cy_coll, iterations)
    print(f"  Speedup: {t_py_coll/t_cy_coll:.2f}x")
    print()

    # =================================================================
    print("=" * 72)
    print("PART 2: recv_results_rows (metadata + row parsing combined)")
    print("=" * 72)
    print()

    for colcount, nrows, iters in [(10, 0, 10000), (10, 10, 5000),
                                    (10, 100, 2000), (50, 0, 5000)]:
        types = simple_types[:colcount] if colcount <= len(simple_types) else simple_types
        data = build_rows_message(colcount, types, nrows=nrows)
        print(f"--- {colcount} cols, {nrows} rows ({len(data)} bytes) ---")

        def fast_fn(data=data):
            f = io.BytesIO(data)
            msg = fast_cls(2)
            msg.recv_results_rows(f, 4, {}, None, None)
        def slow_fn(data=data):
            f = io.BytesIO(data)
            msg = slow_cls(2)
            msg.recv_results_rows(f, 4, {}, None, None)

        t_fast, _ = bench_fn("Cython (new: single BytesIOReader)", fast_fn, iters)
        t_slow, _ = bench_fn("Python BytesIO (baseline)", slow_fn, iters)
        print(f"  Speedup: {t_slow/t_fast:.2f}x")
        print()


if __name__ == '__main__':
    main()
