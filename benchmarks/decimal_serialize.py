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
Benchmark for DecimalType.serialize digit-to-integer conversion.

Compares:
  1. String join approach: int(''.join([str(d) for d in digits]))
  2. Arithmetic loop: reduce via multiply-and-add

Run with: python benchmarks/decimal_serialize.py
"""

import sys
import time
from decimal import Decimal

sys.path.insert(0, ".")

from cassandra.cqltypes import DecimalType


# --- isolated digit-conversion benchmarks ---


def digits_via_string(digits):
    return int("".join([str(d) for d in digits]))


def digits_via_arithmetic(digits):
    n = 0
    for d in digits:
        n = n * 10 + d
    return n


def benchmark_digit_conversion(label, func, digits, iterations):
    start = time.perf_counter()
    for _ in range(iterations):
        func(digits)
    elapsed = time.perf_counter() - start
    us = elapsed / iterations * 1_000_000
    print(
        f"  {label:30s}: {us:8.3f} us/call  ({iterations} iterations, {elapsed:.3f}s total)"
    )
    return elapsed


def benchmark_full_serialize(label, dec_values, iterations):
    proto = 4
    start = time.perf_counter()
    for _ in range(iterations):
        for v in dec_values:
            DecimalType.serialize(v, proto)
    elapsed = time.perf_counter() - start
    calls = iterations * len(dec_values)
    us = elapsed / calls * 1_000_000
    print(f"  {label:30s}: {us:8.3f} us/call  ({calls} calls, {elapsed:.3f}s total)")
    return elapsed


def main():
    print("=" * 70)
    print("DecimalType serialize benchmark")
    print("=" * 70)

    test_cases = [
        ("small (3 digits)", Decimal("3.14")),
        ("medium (10 digits)", Decimal("1234567890.0123456789")),
        ("large (30 digits)", Decimal("1" * 30 + ".0")),
    ]

    iterations = 200_000

    print()
    print("--- Isolated digit-to-integer conversion ---")
    for name, dec_val in test_cases:
        sign, digits, exponent = dec_val.as_tuple()
        print(f"\n  [{name}] {len(digits)} digits")
        t_str = benchmark_digit_conversion(
            "string join", digits_via_string, digits, iterations
        )
        t_arith = benchmark_digit_conversion(
            "arithmetic loop", digits_via_arithmetic, digits, iterations
        )
        speedup = t_str / t_arith
        print(f"  {'speedup':30s}: {speedup:.2f}x")

    print()
    print("--- Full DecimalType.serialize (current implementation) ---")
    dec_values = [tc[1] for tc in test_cases]
    benchmark_full_serialize("serialize (all sizes)", dec_values, iterations // 3)

    # Round-trip correctness check
    print()
    print("--- Correctness check ---")
    proto = 4
    for name, dec_val in test_cases:
        serialized = DecimalType.serialize(dec_val, proto)
        deserialized = DecimalType.deserialize(serialized, proto)
        ok = deserialized == dec_val
        print(
            f"  {name:30s}: {'PASS' if ok else 'FAIL'}  ({dec_val} -> {deserialized})"
        )

    print()


if __name__ == "__main__":
    main()
