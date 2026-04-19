#!/usr/bin/env python3
"""
Benchmark: recv_results_rows — origin/master vs new Cython metadata parser.

Compares the ACTUAL Cython recv_results_rows path (FastResultMessage)
as it exists in the currently-built code.

Run with:  taskset -c 0 python3 bench_ab.py
"""

import struct
import io
import time
import sys
import uuid


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
    write_short(buf, type_code)
    for st in subtypes:
        if isinstance(st, tuple):
            write_type(buf, st[0], st[1:])
        else:
            write_type(buf, st)


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


def build_rows_message(colcount, type_codes_list, nrows=0):
    buf = io.BytesIO()
    write_int(buf, 0x0001)  # GLOBAL_TABLES_SPEC
    write_int(buf, colcount)
    write_string(buf, 'test_ks')
    write_string(buf, 'test_cf')
    for i in range(colcount):
        write_string(buf, 'col_%d' % i)
        tc = type_codes_list[i % len(type_codes_list)]
        if isinstance(tc, tuple):
            write_type(buf, tc[0], tc[1:])
        else:
            write_type(buf, tc)
    write_int(buf, nrows)
    for _ in range(nrows):
        for i in range(colcount):
            tc = type_codes_list[i % len(type_codes_list)]
            base_tc = tc[0] if isinstance(tc, tuple) else tc
            if base_tc == UUID_TYPE:
                write_int(buf, 16); buf.write(uuid.uuid4().bytes)
            elif base_tc == VARCHAR_TYPE:
                v = b'test_value'; write_int(buf, len(v)); buf.write(v)
            elif base_tc == INT_TYPE:
                write_int(buf, 4); buf.write(struct.pack('>i', 42))
            elif base_tc in (BIGINT_TYPE, TIMESTAMP_TYPE, DOUBLE_TYPE):
                write_int(buf, 8); buf.write(struct.pack('>q', 12345678))
            elif base_tc == BOOLEAN_TYPE:
                write_int(buf, 1); buf.write(b'\x01')
            elif base_tc in (LIST_TYPE, SET_TYPE, MAP_TYPE):
                write_int(buf, 4); buf.write(struct.pack('>i', 0))
            else:
                write_int(buf, 4); buf.write(b'\x00\x00\x00\x00')
    return buf.getvalue()


def build_no_metadata_message(colcount=10):
    buf = io.BytesIO()
    write_int(buf, 0x0004)  # NO_METADATA
    write_int(buf, colcount)
    write_int(buf, 0)       # 0 rows
    return buf.getvalue()


def bench(label, fn, iterations, warmup=1000):
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
    var = sum((t - mean_ns)**2 for t in trimmed) / len(trimmed)
    cv = (var**0.5 / mean_ns * 100) if mean_ns else 0
    print(f"  {label:50s}  {mean_ns:9.0f} ns  (cv {cv:4.1f}%)")
    return mean_ns


def main():
    from cassandra.protocol import ProtocolHandler
    from cassandra.cython_deps import HAVE_CYTHON

    print(f"HAVE_CYTHON: {HAVE_CYTHON}")
    print(f"Python: {sys.version}")

    fast_cls = ProtocolHandler.message_types_by_opcode[0x08]
    print(f"FastResultMessage: {fast_cls}")
    print()

    simple_types = [UUID_TYPE, VARCHAR_TYPE, INT_TYPE, BIGINT_TYPE, BOOLEAN_TYPE,
                    DOUBLE_TYPE, TIMESTAMP_TYPE, VARCHAR_TYPE, INT_TYPE, UUID_TYPE]

    scenarios = [
        ("10 cols, 0 rows",     10, simple_types, 0,  10000),
        ("3 cols, 0 rows",       3, simple_types[:3], 0, 10000),
        ("50 cols, 0 rows",     50, simple_types, 0,   5000),
        ("10 cols, 10 rows",    10, simple_types, 10,  5000),
        ("10 cols, 100 rows",   10, simple_types, 100, 2000),
        ("10 cols, 1000 rows",  10, simple_types, 1000, 500),
    ]

    results = {}
    for desc, colcount, types, nrows, iters in scenarios:
        data = build_rows_message(colcount, types, nrows)
        print(f"--- {desc} ({len(data)} bytes) ---")

        def fn(data=data):
            f = io.BytesIO(data)
            msg = fast_cls(2)
            msg.recv_results_rows(f, 4, {}, None, None)

        t = bench("Cython recv_results_rows", fn, iters)
        results[desc] = t
        print()

    # NO_METADATA with result_metadata
    from cassandra.cqltypes import (UUIDType, VarcharType, Int32Type, LongType,
                                     BooleanType, DoubleType, DateType)
    result_md = [
        ('ks', 'cf', 'c%d' % i, [UUIDType, VarcharType, Int32Type, LongType,
                                   BooleanType, DoubleType, DateType, VarcharType,
                                   Int32Type, UUIDType][i])
        for i in range(10)
    ]
    nm_data = build_no_metadata_message(10)
    print(f"--- NO_METADATA, 10 cols, 0 rows ({len(nm_data)} bytes) ---")
    def nm_fn(data=nm_data, md=result_md):
        f = io.BytesIO(data)
        msg = fast_cls(2)
        msg.recv_results_rows(f, 4, {}, md, None)
    t = bench("Cython recv_results_rows", nm_fn, 10000)
    results["NO_METADATA"] = t
    print()

    print("=" * 60)
    print("SUMMARY (copy these numbers for A/B comparison)")
    print("=" * 60)
    for k, v in results.items():
        print(f"  {k:30s}  {v:9.0f} ns")


if __name__ == '__main__':
    main()
