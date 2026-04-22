#!/usr/bin/env python3
"""A/B comparison: master vs PR for Default(DCAware) regression.

Stashes PR changes, benchmarks master, restores PR, benchmarks PR,
all in one script using subprocess to avoid module caching.
"""
import subprocess
import sys
import json

BENCH_CODE = '''
import time, uuid, statistics
from unittest.mock import Mock
from cassandra.policies import DCAwareRoundRobinPolicy, DefaultLoadBalancingPolicy, SimpleConvictionPolicy
from cassandra.pool import Host

class EP:
    def __init__(self, a):
        self.address = str(a)
        self._port = 9042
    def resolve(self):
        return (self.address, self._port)
    def __repr__(self):
        return f"{self.address}:{self._port}"
    def __hash__(self):
        return hash((self.address, self._port))
    def __eq__(self, o):
        return isinstance(o, EP) and self.address == o.address

hosts = []
for dc in range(5):
    for rack in range(3):
        for node in range(3):
            h = Host(EP(f"10.{dc}.{rack}.{node}"), SimpleConvictionPolicy, host_id=uuid.uuid4())
            h.set_location_info(f"dc{dc}", f"rack{rack}")
            h.set_up()
            hosts.append(h)

cluster = Mock()
cluster.metadata = Mock()
cluster.metadata.get_host = Mock(return_value=None)

child = DCAwareRoundRobinPolicy(local_dc="dc0", used_hosts_per_remote_dc=1)
policy = DefaultLoadBalancingPolicy(child)
policy.populate(cluster, hosts)

q = Mock()
q.keyspace = None
q.target_host = None

N, ITERS = 100_000, 7
times = []
for _ in range(ITERS):
    s = time.perf_counter_ns()
    for _ in range(N):
        for _ in policy.make_query_plan("ks", q):
            pass
    times.append((time.perf_counter_ns() - s) / N)

print(f"{statistics.median(times):.0f}")
'''

def run_bench():
    result = subprocess.run(
        ["taskset", "-c", "0", sys.executable, "-c", BENCH_CODE],
        capture_output=True, text=True, timeout=120
    )
    if result.returncode != 0:
        print(f"STDERR: {result.stderr}", file=sys.stderr)
        raise RuntimeError(f"Benchmark failed: {result.stderr}")
    return float(result.stdout.strip())

# Run PR version 3 times
print("Running PR version...")
pr_results = []
for i in range(3):
    ns = run_bench()
    pr_results.append(ns)
    print(f"  Run {i+1}: {ns:.0f} ns/op")

# Switch to master
subprocess.run(["git", "stash"], capture_output=True)
subprocess.run(["git", "checkout", "origin/master", "--", "cassandra/policies.py"], capture_output=True)

print("Running master version...")
master_results = []
for i in range(3):
    ns = run_bench()
    master_results.append(ns)
    print(f"  Run {i+1}: {ns:.0f} ns/op")

# Restore PR
subprocess.run(["git", "checkout", "pr-651", "--", "cassandra/policies.py"], capture_output=True)
subprocess.run(["git", "stash", "pop"], capture_output=True, check=False)

pr_med = statistics.median(pr_results)
master_med = statistics.median(master_results)
print(f"\nDefault(DCAware) - master:  {master_med:.0f} ns/op")
print(f"Default(DCAware) - PR:      {pr_med:.0f} ns/op")
print(f"Difference:                 {pr_med - master_med:+.0f} ns/op ({(pr_med/master_med - 1)*100:+.1f}%)")

import statistics
