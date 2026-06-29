#!/usr/bin/env python
"""
Benchmark: caching self.session.cluster as a local variable.

Measures the cost of repeated self.session.cluster double-lookups
vs. a single local assignment.
"""

import sys
import time

ITERS = 5_000_000


class FakeCluster:
    class control_connection:
        _tablets_routing_v1 = True
    protocol_version = 5
    class metadata:
        class _tablets:
            @staticmethod
            def add_tablet(ks, tbl, tablet):
                pass


class FakeSession:
    cluster = FakeCluster()


class FakeResponseFuture:
    def __init__(self):
        self.session = FakeSession()


def bench_double_lookup(rf, n):
    """Simulates 3 accesses to self.session.cluster (tablet routing block)."""
    t0 = time.perf_counter_ns()
    for _ in range(n):
        _ = rf.session.cluster.control_connection
        _ = rf.session.cluster.protocol_version
        _ = rf.session.cluster.metadata
    return (time.perf_counter_ns() - t0) / n


def bench_cached_local(rf, n):
    """Simulates caching session.cluster in a local."""
    t0 = time.perf_counter_ns()
    for _ in range(n):
        cluster = rf.session.cluster
        _ = cluster.control_connection
        _ = cluster.protocol_version
        _ = cluster.metadata
    return (time.perf_counter_ns() - t0) / n


def main():
    print(f"Python {sys.version}\n")
    rf = FakeResponseFuture()

    ns_old = bench_double_lookup(rf, ITERS)
    ns_new = bench_cached_local(rf, ITERS)
    saving = ns_old - ns_new
    speedup = ns_old / ns_new if ns_new else float('inf')

    print(f"=== self.session.cluster caching ({ITERS:,} iters) ===\n")
    print(f"  3x self.session.cluster (old): {ns_old:.1f} ns")
    print(f"  1x local + 3x local (new):     {ns_new:.1f} ns")
    print(f"  Saving: {saving:.1f} ns ({speedup:.2f}x)")


if __name__ == "__main__":
    main()
