"""A micro-benchmark for performance of load balancing policies.

Measures query plan generation throughput for various load balancing policy
configurations using a simulated cluster topology (5 DCs, 3 racks/DC,
3 nodes/rack = 45 nodes) with 100,000 deterministic queries.

Usage:
    pytest -m benchmark tests/performance/
    pytest --benchmark-only tests/performance/
"""

import uuid
import struct
import os
from unittest.mock import Mock
import pytest

from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    RackAwareRoundRobinPolicy,
    TokenAwarePolicy,
    DefaultLoadBalancingPolicy,
    HostFilterPolicy,
    SimpleConvictionPolicy,
)
from cassandra.pool import Host


class MockEndPoint:
    """Mock for Connection/EndPoint since Host expects it."""

    __slots__ = ("address",)

    def __init__(self, address):
        self.address = address

    def __str__(self):
        return self.address


class MockStatement:
    """Mock statement with a routing key for token-aware routing."""

    __slots__ = ("routing_key", "keyspace", "table")

    def __init__(self, routing_key, keyspace="ks", table="tbl"):
        self.routing_key = routing_key
        self.keyspace = keyspace
        self.table = table

    def is_lwt(self):
        return False


class MockTokenMap:
    __slots__ = ("token_class", "get_replicas_func")

    def __init__(self, get_replicas_func):
        self.token_class = Mock()
        self.token_class.from_key = lambda k: k
        self.get_replicas_func = get_replicas_func

    def get_replicas(self, keyspace, token):
        return self.get_replicas_func(keyspace, token)


class MockTablets:
    __slots__ = ()

    def get_tablet_for_key(self, keyspace, table, key):
        return None


class MockMetadata:
    __slots__ = ("_tablets", "token_map", "get_replicas_func", "hosts_by_address")

    def __init__(self, get_replicas_func, hosts_by_address):
        self._tablets = MockTablets()
        self.token_map = MockTokenMap(get_replicas_func)
        self.get_replicas_func = get_replicas_func
        self.hosts_by_address = hosts_by_address

    def can_support_partitioner(self):
        return True

    def get_replicas(self, keyspace, key):
        return self.get_replicas_func(keyspace, key)

    def get_host(self, addr):
        return self.hosts_by_address.get(addr)


class MockCluster:
    __slots__ = ("metadata",)

    def __init__(self, metadata):
        self.metadata = metadata


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def vnode_cluster():
    """Build a simulated 45-node cluster: 5 DCs x 3 racks x 3 nodes.

    Returns a dict with:
        hosts          - list of Host objects
        hosts_map      - {host_id: Host}
        replicas_map   - {routing_key_bytes: [replica_host, ...]}
    """
    if hasattr(os, "sched_setaffinity"):
        try:
            cpu = list(os.sched_getaffinity(0))[0]
            os.sched_setaffinity(0, {cpu})
        except Exception:
            pass

    hosts = []
    hosts_map = {}
    replicas_map = {}

    dcs = ["dc{}".format(i) for i in range(5)]
    racks = ["rack{}".format(i) for i in range(3)]
    nodes_per_rack = 3

    ip_counter = 0
    subnet_counter = 0
    for dc in dcs:
        for rack in racks:
            subnet_counter += 1
            for node_idx in range(nodes_per_rack):
                ip_counter += 1
                address = "127.0.{}.{}".format(subnet_counter, node_idx + 1)
                h_id = uuid.UUID(int=ip_counter)
                h = Host(MockEndPoint(address), SimpleConvictionPolicy, host_id=h_id)
                h.set_location_info(dc, rack)
                hosts.append(h)
                hosts_map[h_id] = h

    # Pre-calculate replica assignments for 100k routing keys.
    query_count = 100_000
    for i in range(query_count):
        key = struct.pack(">I", i)
        replicas = [hosts[(i + r) % len(hosts)] for r in range(3)]
        replicas_map[key] = replicas

    return {
        "hosts": hosts,
        "hosts_map": hosts_map,
        "replicas_map": replicas_map,
    }


@pytest.fixture(scope="module")
def benchmark_queries():
    """Generate 100,000 deterministic mock queries."""
    query_count = 100_000
    return [MockStatement(routing_key=struct.pack(">I", i)) for i in range(query_count)]


def _setup_cluster_mock(hosts, replicas_map):
    """Wire up a MockCluster with metadata that resolves replicas."""
    hosts_by_address = {}
    for host in hosts:
        addr = getattr(host, "address", None)
        if addr is None and getattr(host, "endpoint", None) is not None:
            addr = getattr(host.endpoint, "address", None)
        if addr is not None:
            hosts_by_address[addr] = host

    get_replicas_func = lambda ks, key: replicas_map.get(key, [])
    metadata = MockMetadata(get_replicas_func, hosts_by_address)
    return MockCluster(metadata)


def _populate_policy(policy, hosts, replicas_map):
    """Create cluster mock and populate the policy with hosts."""
    cluster = _setup_cluster_mock(hosts, replicas_map)
    policy.populate(cluster, hosts)
    return policy


def _run_all_query_plans(policy, queries):
    """Execute make_query_plan for every query, consuming the iterator."""
    for q in queries:
        for _ in policy.make_query_plan(working_keyspace="ks", query=q):
            pass


# ---------------------------------------------------------------------------
# Benchmarks — each uses pytest-benchmark for accurate timing & reporting
# ---------------------------------------------------------------------------


@pytest.mark.benchmark
def test_dc_aware(benchmark, vnode_cluster, benchmark_queries):
    """Benchmark DCAwareRoundRobinPolicy."""
    policy = DCAwareRoundRobinPolicy(local_dc="dc0", used_hosts_per_remote_dc=1)
    _populate_policy(policy, vnode_cluster["hosts"], vnode_cluster["replicas_map"])
    benchmark(_run_all_query_plans, policy, benchmark_queries)


@pytest.mark.benchmark
def test_rack_aware(benchmark, vnode_cluster, benchmark_queries):
    """Benchmark RackAwareRoundRobinPolicy."""
    policy = RackAwareRoundRobinPolicy(
        local_dc="dc0", local_rack="rack0", used_hosts_per_remote_dc=1
    )
    _populate_policy(policy, vnode_cluster["hosts"], vnode_cluster["replicas_map"])
    benchmark(_run_all_query_plans, policy, benchmark_queries)


@pytest.mark.benchmark
def test_token_aware_wrapping_dc_aware(benchmark, vnode_cluster, benchmark_queries):
    """Benchmark TokenAwarePolicy wrapping DCAwareRoundRobinPolicy."""
    child = DCAwareRoundRobinPolicy(local_dc="dc0", used_hosts_per_remote_dc=1)
    policy = TokenAwarePolicy(child, shuffle_replicas=False)
    _populate_policy(policy, vnode_cluster["hosts"], vnode_cluster["replicas_map"])
    benchmark(_run_all_query_plans, policy, benchmark_queries)


@pytest.mark.benchmark
def test_token_aware_wrapping_rack_aware(benchmark, vnode_cluster, benchmark_queries):
    """Benchmark TokenAwarePolicy wrapping RackAwareRoundRobinPolicy."""
    child = RackAwareRoundRobinPolicy(
        local_dc="dc0", local_rack="rack0", used_hosts_per_remote_dc=1
    )
    policy = TokenAwarePolicy(child, shuffle_replicas=False)
    _populate_policy(policy, vnode_cluster["hosts"], vnode_cluster["replicas_map"])
    benchmark(_run_all_query_plans, policy, benchmark_queries)


@pytest.mark.benchmark
def test_default_wrapping_dc_aware(benchmark, vnode_cluster, benchmark_queries):
    """Benchmark DefaultLoadBalancingPolicy wrapping DCAwareRoundRobinPolicy."""
    child = DCAwareRoundRobinPolicy(local_dc="dc0", used_hosts_per_remote_dc=1)
    policy = DefaultLoadBalancingPolicy(child)
    _populate_policy(policy, vnode_cluster["hosts"], vnode_cluster["replicas_map"])
    benchmark(_run_all_query_plans, policy, benchmark_queries)


@pytest.mark.benchmark
def test_host_filter_wrapping_dc_aware(benchmark, vnode_cluster, benchmark_queries):
    """Benchmark HostFilterPolicy wrapping DCAwareRoundRobinPolicy."""
    child = DCAwareRoundRobinPolicy(local_dc="dc0", used_hosts_per_remote_dc=1)
    policy = HostFilterPolicy(
        child_policy=child, predicate=lambda host: host.rack != "rack2"
    )
    _populate_policy(policy, vnode_cluster["hosts"], vnode_cluster["replicas_map"])
    benchmark(_run_all_query_plans, policy, benchmark_queries)
