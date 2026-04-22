#!/usr/bin/env python
"""
Benchmark: guard update_custom_payload calls vs unconditional method calls.

In the common case both query.custom_payload and custom_payload are None,
so the method calls are pure overhead (enter function, check 'if other:',
return).  Guarding with a truthiness check at the call site avoids the
function-call overhead entirely.
"""

import sys
import time

ITERS = 5_000_000


class FakeMessage:
    custom_payload = None

    def update_custom_payload(self, other):
        if other:
            if not self.custom_payload:
                self.custom_payload = {}
            self.custom_payload.update(other)


class FakeQuery:
    custom_payload = None


def bench_unconditional(msg, query, custom_payload, n):
    """Two unconditional method calls (old path)."""
    t0 = time.perf_counter_ns()
    for _ in range(n):
        msg.update_custom_payload(query.custom_payload)
        msg.update_custom_payload(custom_payload)
    return (time.perf_counter_ns() - t0) / n


def bench_guarded(msg, query, custom_payload, n):
    """Guarded: skip calls when payloads are None/falsy (new path)."""
    t0 = time.perf_counter_ns()
    for _ in range(n):
        if query.custom_payload:
            msg.update_custom_payload(query.custom_payload)
        if custom_payload:
            msg.update_custom_payload(custom_payload)
    return (time.perf_counter_ns() - t0) / n


def main():
    print(f"Python {sys.version}\n")

    msg = FakeMessage()
    query = FakeQuery()
    custom_payload = None  # common case: no execute_as

    ns_old = bench_unconditional(msg, query, custom_payload, ITERS)
    ns_new = bench_guarded(msg, query, custom_payload, ITERS)
    saving = ns_old - ns_new
    speedup = ns_old / ns_new if ns_new else float('inf')

    print(f"=== update_custom_payload guard ({ITERS:,} iters, common case: both None) ===\n")
    print(f"  Unconditional calls (old): {ns_old:.1f} ns")
    print(f"  Guarded calls (new):       {ns_new:.1f} ns")
    print(f"  Saving: {saving:.1f} ns ({speedup:.2f}x)")


if __name__ == "__main__":
    main()
