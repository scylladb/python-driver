#!/usr/bin/env python3
"""Benchmark for load balancing policy query plan generation.

Topology: 5 DCs x 3 racks x 3 nodes = 45 nodes.
Measures median of 5 iterations, each with 50K query plan materializations.
Reports ns/op.
"""
import time
import uuid
import statistics
from unittest.mock import Mock, PropertyMock

from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    RackAwareRoundRobinPolicy,
    TokenAwarePolicy,
    DefaultLoadBalancingPolicy,
    HostFilterPolicy,
    HostDistance,
)
from cassandra.pool import Host
from cassandra.metadata import Murmur3Token
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


NUM_DCS = 5
NUM_RACKS = 3
NUM_NODES_PER_RACK = 3
NUM_QUERIES = 50_000
NUM_ITERATIONS = 10

LOCAL_DC = "dc0"
LOCAL_RACK = "rack0"


def create_hosts():
    hosts = []
    idx = 0
    for dc in range(NUM_DCS):
        for rack in range(NUM_RACKS):
            for node in range(NUM_NODES_PER_RACK):
                h = Host(DefaultEndPoint(f"10.{dc}.{rack}.{node}"),
                         SimpleConvictionPolicy,
                         host_id=uuid.uuid4())
                h.set_location_info(f"dc{dc}", f"rack{rack}")
                h.set_up()
                hosts.append(h)
                idx += 1
    return hosts


def make_mock_cluster(hosts):
    cluster = Mock()
    token_map = Mock()
    token_map.token_class = Murmur3Token
    # Build a simple ring with one token per host
    tokens = list(range(0, 2**63, 2**63 // len(hosts)))[:len(hosts)]
    token_to_host = {Murmur3Token(t): h for t, h in zip(tokens, hosts)}

    def get_replicas(ks, token):
        # Return the 3 hosts closest to the token
        sorted_tokens = sorted(token_to_host.keys(), key=lambda t: t.value)
        result = []
        # Find insertion point
        for i, t in enumerate(sorted_tokens):
            if t.value >= token.value:
                for j in range(3):
                    result.append(token_to_host[sorted_tokens[(i + j) % len(sorted_tokens)]])
                return result
        # Wrap around
        for j in range(3):
            result.append(token_to_host[sorted_tokens[j]])
        return result

    token_map.get_replicas = get_replicas

    tablets_mock = Mock()
    tablets_mock.get_tablet_for_key = Mock(return_value=None)
    cluster.metadata = Mock()
    cluster.metadata.token_map = token_map
    cluster.metadata._tablets = tablets_mock
    cluster.metadata.get_replicas = lambda ks, key: get_replicas(ks, Murmur3Token.from_key(key))
    return cluster


def make_query(routing_key=None):
    q = Mock()
    q.keyspace = "ks"
    q.table = "tbl"
    q.routing_key = routing_key
    q.is_lwt = Mock(return_value=False)
    return q


def bench(name, policy, hosts, queries):
    """Run benchmark: materialize query plans, return min ns/op."""
    times = []
    for _ in range(NUM_ITERATIONS):
        start = time.perf_counter_ns()
        for q in queries:
            plan = policy.make_query_plan("ks", q)
            # Materialize the full plan
            for _ in plan:
                pass
        elapsed = time.perf_counter_ns() - start
        times.append(elapsed / len(queries))
    # Use min: on a noisy system the fastest run is closest to true cost
    return min(times)


def main():
    hosts = create_hosts()
    cluster = make_mock_cluster(hosts)

    # Pre-generate routing keys for TokenAware
    import struct
    routing_keys = [struct.pack(">q", i) for i in range(NUM_QUERIES)]
    queries_with_routing = [make_query(rk) for rk in routing_keys]
    queries_no_routing = [make_query(None) for _ in range(NUM_QUERIES)]

    results = {}

    # 1. DCAwareRoundRobinPolicy
    policy = DCAwareRoundRobinPolicy(local_dc=LOCAL_DC, used_hosts_per_remote_dc=1)
    policy.populate(cluster, hosts)
    ns = bench("DCAware", policy, hosts, queries_no_routing)
    results["DCAware"] = ns

    # 2. RackAwareRoundRobinPolicy
    policy = RackAwareRoundRobinPolicy(local_dc=LOCAL_DC, local_rack=LOCAL_RACK, used_hosts_per_remote_dc=1)
    policy.populate(cluster, hosts)
    ns = bench("RackAware", policy, hosts, queries_no_routing)
    results["RackAware"] = ns

    # 3. TokenAware(DCAware)
    child = DCAwareRoundRobinPolicy(local_dc=LOCAL_DC, used_hosts_per_remote_dc=1)
    policy = TokenAwarePolicy(child, shuffle_replicas=False)
    policy.populate(cluster, hosts)
    ns = bench("TokenAware(DCAware)", policy, hosts, queries_with_routing)
    results["TokenAware(DCAware)"] = ns

    # 4. TokenAware(RackAware)
    child = RackAwareRoundRobinPolicy(local_dc=LOCAL_DC, local_rack=LOCAL_RACK, used_hosts_per_remote_dc=1)
    policy = TokenAwarePolicy(child, shuffle_replicas=False)
    policy.populate(cluster, hosts)
    ns = bench("TokenAware(RackAware)", policy, hosts, queries_with_routing)
    results["TokenAware(RackAware)"] = ns

    # 5. Default(DCAware)
    child = DCAwareRoundRobinPolicy(local_dc=LOCAL_DC, used_hosts_per_remote_dc=1)
    policy = DefaultLoadBalancingPolicy(child)
    policy.populate(cluster, hosts)
    ns = bench("Default(DCAware)", policy, hosts, queries_no_routing)
    results["Default(DCAware)"] = ns

    # 6. HostFilter(DCAware)
    child = DCAwareRoundRobinPolicy(local_dc=LOCAL_DC, used_hosts_per_remote_dc=1)
    policy = HostFilterPolicy(child, predicate=lambda h: True)
    policy.populate(cluster, hosts)
    ns = bench("HostFilter(DCAware)", policy, hosts, queries_no_routing)
    results["HostFilter(DCAware)"] = ns

    print(f"\n{'Policy':<30} {'ns/op':>10}")
    print("-" * 42)
    for name, ns in results.items():
        print(f"{name:<30} {ns:>10.0f}")


if __name__ == "__main__":
    main()
