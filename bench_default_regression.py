#!/usr/bin/env python3
"""Micro-benchmark to isolate Default(DCAware) regression.

Tests the exact hot path: DefaultLoadBalancingPolicy.make_query_plan
-> DCAwareRoundRobinPolicy.make_query_plan
-> (on PR) make_query_plan_with_exclusion delegation overhead
"""
import time
import uuid
import statistics
from unittest.mock import Mock

from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    DefaultLoadBalancingPolicy,
)
from cassandra.pool import Host
from cassandra.policies import SimpleConvictionPolicy


class DefaultEndPoint:
    def __init__(self, addr):
        self.address = str(addr)
        self._port = 9042
    def resolve(self):
        return (self.address, self._port)
    def __repr__(self):
        return f"{self.address}:{self._port}"
    def __hash__(self):
        return hash((self.address, self._port))
    def __eq__(self, other):
        return isinstance(other, DefaultEndPoint) and self.address == other.address and self._port == other._port


NUM_QUERIES = 100_000
NUM_ITERATIONS = 7

def create_hosts():
    hosts = []
    for dc in range(5):
        for rack in range(3):
            for node in range(3):
                h = Host(DefaultEndPoint(f"10.{dc}.{rack}.{node}"),
                         SimpleConvictionPolicy, host_id=uuid.uuid4())
                h.set_location_info(f"dc{dc}", f"rack{rack}")
                h.set_up()
                hosts.append(h)
    return hosts


def make_query():
    q = Mock()
    q.keyspace = None
    q.target_host = None
    return q


def bench_direct_dcaware(policy, query):
    """Call DCAware.make_query_plan directly."""
    times = []
    for _ in range(NUM_ITERATIONS):
        start = time.perf_counter_ns()
        for _ in range(NUM_QUERIES):
            for _ in policy.make_query_plan("ks", query):
                pass
        elapsed = time.perf_counter_ns() - start
        times.append(elapsed / NUM_QUERIES)
    return statistics.median(times)


def bench_via_default(policy, query):
    """Call Default(DCAware).make_query_plan."""
    times = []
    for _ in range(NUM_ITERATIONS):
        start = time.perf_counter_ns()
        for _ in range(NUM_QUERIES):
            for _ in policy.make_query_plan("ks", query):
                pass
        elapsed = time.perf_counter_ns() - start
        times.append(elapsed / NUM_QUERIES)
    return statistics.median(times)


def bench_with_exclusion_direct(policy, query):
    """Call DCAware.make_query_plan_with_exclusion directly (empty excluded)."""
    times = []
    for _ in range(NUM_ITERATIONS):
        start = time.perf_counter_ns()
        for _ in range(NUM_QUERIES):
            for _ in policy.make_query_plan_with_exclusion("ks", query):
                pass
        elapsed = time.perf_counter_ns() - start
        times.append(elapsed / NUM_QUERIES)
    return statistics.median(times)


def main():
    hosts = create_hosts()
    cluster = Mock()
    cluster.metadata = Mock()
    cluster.metadata.get_host = Mock(return_value=None)

    query = make_query()

    # DCAware direct
    dcaware = DCAwareRoundRobinPolicy(local_dc="dc0", used_hosts_per_remote_dc=1)
    dcaware.populate(cluster, hosts)

    ns1 = bench_direct_dcaware(dcaware, query)
    ns2 = bench_with_exclusion_direct(dcaware, query)

    # Default(DCAware)
    dcaware2 = DCAwareRoundRobinPolicy(local_dc="dc0", used_hosts_per_remote_dc=1)
    default_policy = DefaultLoadBalancingPolicy(dcaware2)
    default_policy.populate(cluster, hosts)

    ns3 = bench_via_default(default_policy, query)

    print(f"{'Path':<45} {'ns/op':>10}")
    print("-" * 57)
    print(f"{'DCAware.make_query_plan (delegation)':<45} {ns1:>10.0f}")
    print(f"{'DCAware.make_query_plan_with_exclusion()':<45} {ns2:>10.0f}")
    print(f"{'Default -> DCAware.make_query_plan':<45} {ns3:>10.0f}")
    print()
    print(f"Delegation overhead: {ns1 - ns2:.0f} ns/op ({(ns1/ns2 - 1)*100:.1f}%)")


if __name__ == "__main__":
    main()
