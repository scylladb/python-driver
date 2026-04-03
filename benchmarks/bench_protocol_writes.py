#!/usr/bin/env python3
"""
Benchmark: inline write_value in protocol message serialization.

Compares the original _write_query_params / BatchMessage.send_body
(calling write_value() helper per parameter) against the optimized version
(inlined write_value with pre-computed constants and local variable aliases).

NOTE: If the Cython .so is present, move it aside first:
  mv cassandra/protocol.cpython-*-linux-gnu.so{,.bak}
Otherwise Python loads the compiled .so and ignores the .py changes.
"""

import io
import time
import timeit
from enum import Enum

from cassandra.protocol import (
    QueryMessage,
    BatchMessage,
    write_consistency_level,
    write_byte,
    write_uint,
    write_short,
    write_int,
    write_long,
    write_longstring,
    write_value,
    write_string,
    _UNSET_VALUE,
    _VALUES_FLAG,
    _WITH_SERIAL_CONSISTENCY_FLAG,
    _PAGE_SIZE_FLAG,
    _WITH_PAGING_STATE_FLAG,
    _PROTOCOL_TIMESTAMP_FLAG,
)
from cassandra import ProtocolVersion
import cassandra.protocol


class FakeBatchType(Enum):
    LOGGED = 0
    UNLOGGED = 1


# ---- Old implementations (baseline, before inlining) ----


def old_write_query_params(self, f, protocol_version):
    write_consistency_level(f, self.consistency_level)
    flags = 0x00
    if self.query_params is not None:
        flags |= _VALUES_FLAG
    if self.serial_consistency_level:
        flags |= _WITH_SERIAL_CONSISTENCY_FLAG
    if self.fetch_size:
        flags |= _PAGE_SIZE_FLAG
    if self.paging_state:
        flags |= _WITH_PAGING_STATE_FLAG
    if self.timestamp is not None:
        flags |= _PROTOCOL_TIMESTAMP_FLAG
    if ProtocolVersion.uses_int_query_flags(protocol_version):
        write_uint(f, flags)
    else:
        write_byte(f, flags)
    if self.query_params is not None:
        write_short(f, len(self.query_params))
        for param in self.query_params:
            write_value(f, param)
    if self.fetch_size:
        write_int(f, self.fetch_size)
    if self.paging_state:
        write_longstring(f, self.paging_state)
    if self.serial_consistency_level:
        write_consistency_level(f, self.serial_consistency_level)
    if self.timestamp is not None:
        write_long(f, self.timestamp)
    if self.keyspace is not None:
        write_string(f, self.keyspace)


def old_batch_send_body(self, f, protocol_version):
    write_byte(f, self.batch_type.value)
    write_short(f, len(self.queries))
    for prepared, string_or_query_id, params in self.queries:
        if not prepared:
            write_byte(f, 0)
            write_longstring(f, string_or_query_id)
        else:
            write_byte(f, 1)
            write_short(f, len(string_or_query_id))
            f.write(string_or_query_id)
        write_short(f, len(params))
        for param in params:
            write_value(f, param)
    write_consistency_level(f, self.consistency_level)
    flags = 0
    if self.serial_consistency_level:
        flags |= _WITH_SERIAL_CONSISTENCY_FLAG
    if self.timestamp is not None:
        flags |= _PROTOCOL_TIMESTAMP_FLAG
    if ProtocolVersion.uses_int_query_flags(protocol_version):
        write_int(f, flags)
    else:
        write_byte(f, flags)
    if self.serial_consistency_level:
        write_consistency_level(f, self.serial_consistency_level)
    if self.timestamp is not None:
        write_long(f, self.timestamp)


# ---- Benchmark helpers ----

PROTO_VERSION = 4  # v4 is the most common
ITERATIONS = 1_000_000
REPEATS = 5


def bench(label, msg, old_fn, new_fn):
    """Run benchmark comparing old vs new, using process_time for stability."""
    f = io.BytesIO()

    def run_old():
        f.seek(0)
        f.truncate()
        old_fn(msg, f, PROTO_VERSION)

    def run_new():
        f.seek(0)
        f.truncate()
        new_fn(msg, f, PROTO_VERSION)

    # Verify byte-for-byte correctness
    run_old()
    old_bytes = f.getvalue()
    run_new()
    new_bytes = f.getvalue()
    assert old_bytes == new_bytes, f"Output mismatch for {label}"

    t_old = timeit.repeat(
        run_old, number=ITERATIONS, repeat=REPEATS, timer=time.process_time
    )
    t_new = timeit.repeat(
        run_new, number=ITERATIONS, repeat=REPEATS, timer=time.process_time
    )
    best_old = min(t_old) / ITERATIONS * 1e9
    best_new = min(t_new) / ITERATIONS * 1e9
    speedup = best_old / best_new

    print(f"  {label}:")
    print(f"    Old (best of {REPEATS}): {best_old:.1f} ns/call")
    print(f"    New (best of {REPEATS}): {best_new:.1f} ns/call")
    print(f"    Speedup: {speedup:.2f}x")


if __name__ == "__main__":
    print(f"Module loaded: {cassandra.protocol.__file__}")
    if cassandra.protocol.__file__.endswith(".so"):
        print("WARNING: Cython .so loaded. Move it aside to benchmark .py changes.")
    print(
        f"Benchmark: inline write_value (proto v{PROTO_VERSION}, "
        f"{ITERATIONS} iters, best of {REPEATS})"
    )
    print()

    print("=== _write_query_params ===")
    for n in (3, 10, 50):
        msg = QueryMessage(
            query="INSERT INTO ks.tbl (a,b,c) VALUES (?,?,?)",
            query_params=[b"\x00" * 20 for _ in range(n)],
            consistency_level=1,
            fetch_size=5000,
            timestamp=1234567890123,
        )
        bench(
            f"{n} params",
            msg,
            old_write_query_params,
            lambda m, f, pv: m._write_query_params(f, pv),
        )

    print()
    print("=== BatchMessage.send_body ===")
    for nq, np in ((1, 3), (5, 3), (20, 5)):
        queries = [
            (
                False,
                "INSERT INTO ks.tbl (a,b,c) VALUES (?,?,?)",
                [b"\x00" * 20 for _ in range(np)],
            )
            for _ in range(nq)
        ]
        msg = BatchMessage(
            batch_type=FakeBatchType.LOGGED,
            queries=queries,
            consistency_level=1,
            timestamp=1234567890123,
        )
        bench(
            f"{nq} queries x {np} params",
            msg,
            old_batch_send_body,
            lambda m, f, pv: m.send_body(f, pv),
        )
