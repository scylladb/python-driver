# Copyright ScyllaDB, Inc.
# Licensed under the Apache License, Version 2.0.

"""Cluster-free admission and telemetry benchmark.

Run from a checkout with the driver dependencies installed::

    python benchmarks/bench_management_runtime.py
    python benchmarks/bench_management_runtime.py --module-source /tmp/baseline.py

Each size uses two warm-ups and seven measured runs. Startup, owner progress,
and cleanup are excluded from timing. Admission holds the runtime condition
through a burst to measure admission bookkeeping without owner scheduling
noise. Snapshots run with the indicated number of retained pending records.
Results are descriptive medians; there is no timing-based pass/fail threshold.
"""

import argparse
from collections import deque
import importlib.util
from pathlib import Path
from statistics import median
import sys
from threading import Event, get_ident
from time import perf_counter_ns


# Allow direct invocation from the benchmarks directory.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def load_runtime(source):
    if source is None:
        import cassandra._management as management
        return management
    spec = importlib.util.spec_from_file_location('_bench_management', source)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def sample(module, size, snapshots):
    deliveries = deque()
    manager = module.ManagementRuntime(deliveries.append, limit=size + 1)
    ready, idle = Event(), set()
    condition_wait = manager._condition.wait
    def await_work(timeout=None):
        # All runners enter this hook under the runtime condition. Thread.start
        # alone can return before their initial context setup has finished.
        idle.add(get_ident())
        if len(idle) == len(manager._threads):
            ready.set()
        return condition_wait(timeout)
    manager._condition.wait = await_work
    try:
        manager.start()
        if not ready.wait(5):
            raise RuntimeError('Management runners did not become idle')
        def retain(record):
            return module._PENDING
        with manager._condition:
            manager._condition.wait = condition_wait
            started = perf_counter_ns()
            for _ in range(size):
                manager.submit(retain)
            admission_ns = perf_counter_ns() - started
        # A FIFO fence excludes owner progress from snapshot timing.
        manager.submit(lambda record: None).result(5)
        with manager._condition:
            assert len(manager._records) == size
        started = perf_counter_ns()
        for _ in range(snapshots):
            manager.counts()
        snapshot_ns = perf_counter_ns() - started
        return admission_ns / size, snapshot_ns / snapshots
    finally:
        manager._condition.wait = condition_wait
        manager.close()
        while deliveries:
            deliveries.popleft()()
        manager.join(5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--module-source', type=Path,
                        help='load a baseline runtime from this Python file')
    parser.add_argument('--snapshots', type=int, default=10000)
    args = parser.parse_args()
    if args.snapshots < 1:
        parser.error('--snapshots must be positive')
    module = load_runtime(args.module_source)
    print('retained  admission ns/operation  snapshot ns/call (medians, 7 runs)')
    for size in (1, 64, 256, 1024):
        for _ in range(2):
            sample(module, size, args.snapshots)
        runs = [sample(module, size, args.snapshots) for _ in range(7)]
        admission, snapshot = (median(values) for values in zip(*runs))
        print(f'{size:8d}  {admission:22.0f}  {snapshot:16.0f}')


if __name__ == '__main__':
    main()
