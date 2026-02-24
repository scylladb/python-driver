import time
import uuid
import struct
import os
import statistics
from unittest.mock import Mock
import pytest

"A micro-benchmark for performance of policies"

from cassandra.policies import (
    DCAwareRoundRobinPolicy,
    RackAwareRoundRobinPolicy,
    TokenAwarePolicy,
    DefaultLoadBalancingPolicy,
    HostFilterPolicy
)
from cassandra.pool import Host
from cassandra.cluster import SimpleConvictionPolicy

# Mock for Connection/EndPoint since Host expects it
class MockEndPoint(object):
    __slots__ = ('address',)

    def __init__(self, address):
        self.address = address
    def __str__(self):
        return self.address

class MockStatement(object):
    __slots__ = ('routing_key', 'keyspace', 'table')

    def __init__(self, routing_key, keyspace="ks", table="tbl"):
        self.routing_key = routing_key
        self.keyspace = keyspace
        self.table = table

    def is_lwt(self):
        return False

class MockTokenMap(object):
    __slots__ = ('token_class', 'get_replicas_func')
    def __init__(self, get_replicas_func):
        self.token_class = Mock()
        self.token_class.from_key = lambda k: k
        self.get_replicas_func = get_replicas_func

    def get_replicas(self, keyspace, token):
        return self.get_replicas_func(keyspace, token)

class MockTablets(object):
    __slots__ = ()
    def get_tablet_for_key(self, keyspace, table, key):
        return None

class MockMetadata(object):
    __slots__ = ('_tablets', 'token_map', 'get_replicas_func', 'hosts_by_address')
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

class MockCluster(object):
    __slots__ = ('metadata',)
    def __init__(self, metadata):
        self.metadata = metadata

@pytest.fixture(scope="module")
def benchmark_setup():
    """Setup test data that will be shared across all benchmark tests"""
    if hasattr(os, 'sched_setaffinity'):
         try:
             # Pin to the first available CPU
             cpu = list(os.sched_getaffinity(0))[0]
             os.sched_setaffinity(0, {cpu})
             print(f"Pinned to CPU {cpu}")
         except Exception as e:
             print(f"Could not pin CPU: {e}")

    # 1. Topology: 5 DCs, 3 Racks/DC, 3 Nodes/Rack = 45 Nodes
    hosts = []
    hosts_map = {}  # host_id -> Host
    replicas_map = {}  # routing_key -> list of replica hosts

    # Deterministic generation
    dcs = ['dc{}'.format(i) for i in range(5)]
    racks = ['rack{}'.format(i) for i in range(3)]
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

    # 2. Queries: 100,000 deterministic queries
    query_count = 100000
    queries = []
    # We'll use simple packed integers as routing keys
    for i in range(query_count):
        key = struct.pack('>I', i)
        queries.append(MockStatement(routing_key=key))

        # Pre-calculate replicas for TokenAware:
        # Deterministically pick 3 replicas based on the key index
        # This simulates the metadata.get_replicas behavior
        # We pick index i, i+1, i+2 mod 45
        replicas = []
        for r in range(3):
            idx = (i + r) % len(hosts)
            replicas.append(hosts[idx])
        replicas_map[key] = replicas

    return {
        'hosts': hosts,
        'hosts_map': hosts_map,
        'replicas_map': replicas_map,
        'queries': queries,
        'query_count': query_count,
    }


def _get_replicas_side_effect(replicas_map, keyspace, key):
    return replicas_map.get(key, [])


def _setup_cluster_mock(hosts, replicas_map):
    hosts_by_address = {}
    for host in hosts:
        addr = getattr(host, 'address', None)
        if addr is None and getattr(host, 'endpoint', None) is not None:
            addr = getattr(host.endpoint, 'address', None)
        if addr is not None:
            hosts_by_address[addr] = host
    
    get_replicas_func = lambda ks, key: _get_replicas_side_effect(replicas_map, ks, key)
    metadata = MockMetadata(get_replicas_func, hosts_by_address)
    return MockCluster(metadata)


def _run_benchmark(name, policy, setup_data):
    """Run a benchmark for a given policy"""
    hosts = setup_data['hosts']
    queries = setup_data['queries']
    replicas_map = setup_data['replicas_map']
    
    # Setup
    cluster = _setup_cluster_mock(hosts, replicas_map)
    policy.populate(cluster, hosts)

    # Warmup
    for _ in range(100):
        list(policy.make_query_plan(working_keyspace="ks", query=queries[0]))

    # Run multiple iterations to reduce noise
    iterations = 5
    timings = []

    for _ in range(iterations):
        start_time = time.perf_counter()
        for q in queries:
            # We consume the iterator to ensure full plan generation cost is paid
            for _ in policy.make_query_plan(working_keyspace="ks", query=q):
                pass
        end_time = time.perf_counter()
        timings.append(end_time - start_time)

    # Use median to filter outliers
    duration = statistics.median(timings)

    count = len(queries)
    ops_per_sec = count / duration
    kops = int(ops_per_sec / 1000)

    print(f"\n{name:<30} | {count:<10} | {duration:<10.4f} | {kops:<10} Kops/s")
    return ops_per_sec


@pytest.mark.benchmark
def test_dc_aware(benchmark_setup):
    """Benchmark DCAwareRoundRobinPolicy"""
    # Local DC = dc0, 1 remote host per DC
    policy = DCAwareRoundRobinPolicy(local_dc='dc0', used_hosts_per_remote_dc=1)
    _run_benchmark("DCAware", policy, benchmark_setup)


@pytest.mark.benchmark
def test_rack_aware(benchmark_setup):
    """Benchmark RackAwareRoundRobinPolicy"""
    # Local DC = dc0, Local Rack = rack0, 1 remote host per DC
    policy = RackAwareRoundRobinPolicy(local_dc='dc0', local_rack='rack0', used_hosts_per_remote_dc=1)
    _run_benchmark("RackAware", policy, benchmark_setup)


@pytest.mark.benchmark
def test_token_aware_wrapping_dc_aware(benchmark_setup):
    """Benchmark TokenAwarePolicy wrapping DCAwareRoundRobinPolicy"""
    child = DCAwareRoundRobinPolicy(local_dc='dc0', used_hosts_per_remote_dc=1)
    policy = TokenAwarePolicy(child, shuffle_replicas=False)  # False for strict determinism in test if needed
    _run_benchmark("TokenAware(DCAware)", policy, benchmark_setup)


@pytest.mark.benchmark
def test_token_aware_wrapping_rack_aware(benchmark_setup):
    """Benchmark TokenAwarePolicy wrapping RackAwareRoundRobinPolicy"""
    child = RackAwareRoundRobinPolicy(local_dc='dc0', local_rack='rack0', used_hosts_per_remote_dc=1)
    policy = TokenAwarePolicy(child, shuffle_replicas=False)
    _run_benchmark("TokenAware(RackAware)", policy, benchmark_setup)


@pytest.mark.benchmark
def test_default_wrapping_dc_aware(benchmark_setup):
    """Benchmark DefaultLoadBalancingPolicy wrapping DCAwareRoundRobinPolicy"""
    child = DCAwareRoundRobinPolicy(local_dc='dc0', used_hosts_per_remote_dc=1)
    policy = DefaultLoadBalancingPolicy(child)
    _run_benchmark("Default(DCAware)", policy, benchmark_setup)


@pytest.mark.benchmark
def test_host_filter_wrapping_dc_aware(benchmark_setup):
    """Benchmark HostFilterPolicy wrapping DCAwareRoundRobinPolicy"""
    child = DCAwareRoundRobinPolicy(local_dc='dc0', used_hosts_per_remote_dc=1)
    policy = HostFilterPolicy(child_policy=child, predicate=lambda host: host.rack != 'rack2')
    _run_benchmark("HostFilter(DCAware)", policy, benchmark_setup)
