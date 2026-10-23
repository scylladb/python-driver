#!/usr/bin/env python3
"""bench_ingest.py -- Vector ingestion benchmark for scylla-driver variants.

Measures insert throughput (rows/sec) for a given driver variant against a
ScyllaDB cluster with vector-search support.

Variants:
    A  -- scylla-driver (stock/master), execute_concurrent, list[float]
    A2 -- scylla-driver (stock/master), execute_concurrent, numpy
    B  -- scylla-driver (enhanced), execute_concurrent, list[float]
    C  -- scylla-driver (enhanced), execute_concurrent, numpy bulk
    D  -- python-rs-driver (Rust-backed), asyncio.gather
    E  -- scylla-driver (enhanced), decoupled executor, numpy bulk
    F  -- scylla-driver (enhanced), free-threaded, execute_concurrent, numpy

Usage:
    python bench_ingest.py --variant A|A2|B|C|D|E|F \
        --host 127.0.0.1 --port 9042 \
        --dataset-dir ~/vector_bench/dataset/cohere/cohere_medium_1m \
        --dim 768 --concurrency 1000 --max-rows 100000 --runs 3
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

from pyarrow.parquet import ParquetFile


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wait_for_shard_connections(session, timeout=10):
    """Wait until all shard connections are established."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for pool in session._pools.values():
            if hasattr(pool.host, 'sharding_info') and pool.host.sharding_info:
                n_shards = pool.host.sharding_info.shards_count
                if len(pool._connections) >= n_shards:
                    return len(pool._connections)
        time.sleep(0.2)
    # Return what we have
    for pool in session._pools.values():
        return len(pool._connections)
    return 0


# ---------------------------------------------------------------------------
# Schema / table helpers (shared across variants)
# ---------------------------------------------------------------------------

KEYSPACE = "vdb_bench"
TABLE = "vdb_bench_collection"

PROGRESS_INTERVAL = 10_000  # print progress every N rows


def cql_create_keyspace():
    return (
        f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
        f"WITH replication = {{'class': 'NetworkTopologyStrategy', "
        f"'replication_factor': '1'}} "
        f"AND tablets = {{'enabled': 'true'}}"
    )


def cql_create_table(dim: int):
    return (
        f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} ("
        f"  id int PRIMARY KEY,"
        f"  vector vector<float, {dim}>"
        f") WITH caching = {{'keys': 'NONE', 'rows_per_partition': 'NONE'}}"
    )


CQL_TRUNCATE = f"TRUNCATE {KEYSPACE}.{TABLE}"
CQL_INSERT = f"INSERT INTO {KEYSPACE}.{TABLE} (id, vector) VALUES (?, ?)"


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def load_params_pylist(dataset_dir: str, dim: int, max_rows: int):
    """Load parameters as list[float] via to_pylist() -- the slow path."""
    pf = ParquetFile(f"{dataset_dir}/shuffle_train.parquet")
    all_params = []
    total = 0
    for batch in pf.iter_batches(min(max_rows, 10000)):
        ids = batch.column("id").to_pylist()
        embeddings = batch.column("emb").to_pylist()
        all_params.extend(zip(ids, embeddings))
        total += len(ids)
        if max_rows > 0 and total >= max_rows:
            break
    return all_params[:max_rows] if max_rows > 0 else all_params


def load_params_numpy(dataset_dir: str, dim: int, max_rows: int):
    """Load parameters as numpy arrays -- zero-copy from Arrow."""
    import numpy as np
    pf = ParquetFile(f"{dataset_dir}/shuffle_train.parquet")
    batch = next(pf.iter_batches(max_rows))
    ids = batch.column("id").to_pylist()
    emb_col = batch.column("emb")
    arr = np.frombuffer(emb_col.values.buffers()[1], dtype=np.float32).reshape(-1, dim)
    n = min(len(ids), max_rows) if max_rows > 0 else len(ids)
    return [(ids[i], arr[i]) for i in range(n)]


# ---------------------------------------------------------------------------
# Ingestion functions
#
# Each returns (total_rows, elapsed_seconds) for a single run.
# They receive a pre-connected session, prepared statement, and pre-loaded
# params list.  The caller handles schema setup, truncation, warmup, and
# multiple runs.
# ---------------------------------------------------------------------------

def ingest_execute_concurrent(session, prepared, params, concurrency):
    """Ingest using execute_concurrent_with_args (variants A, A2, B, C, F)."""
    from cassandra.concurrent import execute_concurrent_with_args

    n = len(params)
    t0 = time.perf_counter()

    # Feed params in chunks matching concurrency to avoid materialising
    # a huge intermediate list inside execute_concurrent_with_args.
    for start in range(0, n, concurrency):
        chunk = params[start:start + concurrency]
        execute_concurrent_with_args(session, prepared, chunk,
                                     concurrency=concurrency,
                                     raise_on_first_error=True)
        total = start + len(chunk)
        if total % PROGRESS_INTERVAL < concurrency:
            elapsed = time.perf_counter() - t0
            print(f"  [{total:>8,} rows] {total/elapsed:,.0f} rows/sec",
                  file=sys.stderr)

    elapsed = time.perf_counter() - t0
    return n, elapsed


def ingest_decoupled(session, prepared, params, concurrency):
    """Ingest using DecoupledExecutor (variant E).

    Callbacks on the event loop just signal completion; a dedicated
    submitter thread handles execute_async in batches.  This frees
    the libev event loop from doing request serialisation / routing work.
    """
    import threading
    from collections import deque

    class _DecoupledExecutor:
        __slots__ = ('session', 'prepared', 'params', 'total',
                     'concurrency', 'submitted',
                     '_completed', '_completed_lock',
                     'done_event', 'error', '_ready', '_ready_event',
                     '_stopped')

        def __init__(self, session, prepared, params, concurrency):
            self.session = session
            self.prepared = prepared
            self.params = params
            self.total = len(params)
            self.concurrency = concurrency
            self.submitted = 0
            self._completed = 0
            self._completed_lock = threading.Lock()
            self.done_event = threading.Event()
            self.error = None
            self._ready = deque()
            self._ready_event = threading.Event()
            self._stopped = False

        def run(self):
            ea = self.session.execute_async
            prep = self.prepared
            params = self.params
            batch = min(self.concurrency, self.total)
            # Submit initial batch *before* starting the submitter thread
            # so self.submitted is visible without a race.
            for i in range(batch):
                f = ea(prep, params[i], timeout=None)
                f.add_callbacks(callback=self._on_done, callback_args=(f,),
                                errback=self._on_err, errback_args=(f,))
            self.submitted = batch
            # If all items submitted in initial batch, just wait for completions
            if self.total <= batch:
                self.done_event.wait()
                if self.error:
                    raise self.error
                return
            submitter = threading.Thread(target=self._submitter_loop,
                                        daemon=True, name="decoupled-submitter")
            submitter.start()
            self.done_event.wait()
            self._stopped = True
            self._ready_event.set()
            if self.error:
                raise self.error

        def _on_done(self, _result, future):
            future.clear_callbacks()
            with self._completed_lock:
                self._completed += 1
                done = self._completed >= self.total
            if done:
                self.done_event.set()
            else:
                self._ready.append(1)
                self._ready_event.set()

        def _on_err(self, exc, _future):
            self.error = exc
            self._stopped = True
            self.done_event.set()

        def _submitter_loop(self):
            ea = self.session.execute_async
            prep = self.prepared
            params = self.params
            total = self.total
            ready = self._ready
            ready_event = self._ready_event
            submitted = self.submitted
            while not self._stopped:
                ready_event.wait()
                ready_event.clear()
                count = 0
                while True:
                    try:
                        ready.popleft()
                        count += 1
                    except IndexError:
                        break
                end = min(submitted + count, total)
                try:
                    for i in range(submitted, end):
                        f = ea(prep, params[i], timeout=None)
                        f.add_callbacks(callback=self._on_done, callback_args=(f,),
                                        errback=self._on_err, errback_args=(f,))
                except Exception as exc:
                    self.error = exc
                    self.done_event.set()
                    return
                submitted = end
                if submitted >= total:
                    # All submitted; _on_done will set done_event
                    # when the last one completes
                    return

    n = len(params)
    t0 = time.perf_counter()

    # Process in chunks to show progress and avoid holding all params
    # in a single executor (matches execute_concurrent behaviour).
    for start in range(0, n, concurrency):
        chunk = params[start:start + concurrency]
        executor = _DecoupledExecutor(session, prepared, chunk, concurrency)
        executor.run()
        total = start + len(chunk)
        if total % PROGRESS_INTERVAL < concurrency:
            elapsed = time.perf_counter() - t0
            print(f"  [{total:>8,} rows] {total/elapsed:,.0f} rows/sec",
                  file=sys.stderr)

    elapsed = time.perf_counter() - t0
    return n, elapsed


def ingest_rust_asyncio(session_builder_args, prepared_stmt_cql, params, concurrency):
    """Ingest using python-rs-driver with asyncio.gather (variant D).

    Unlike the scylla-driver variants, this creates its own async session
    because python-rs-driver uses a fundamentally different API.
    """
    import asyncio

    async def _run():
        from scylla.enums import Consistency
        from scylla.execution_profile import ExecutionProfile
        from scylla.session_builder import SessionBuilder

        host, port = session_builder_args
        profile = ExecutionProfile(consistency=Consistency.One)
        builder = SessionBuilder([host], port, execution_profile=profile)
        s = await builder.connect()

        await s.execute(cql_create_keyspace())
        await s.execute(f"USE {KEYSPACE}")

        prepared = await s.prepare(prepared_stmt_cql)
        prepared = prepared.with_consistency(Consistency.One)

        # Rust driver needs list[float], not numpy arrays
        n = len(params)
        t0 = time.perf_counter()

        for chunk_start in range(0, n, concurrency):
            chunk_end = min(chunk_start + concurrency, n)
            coros = [
                s.execute(prepared, [params[i][0], params[i][1]])
                for i in range(chunk_start, chunk_end)
            ]
            await asyncio.gather(*coros)
            if chunk_end % PROGRESS_INTERVAL < concurrency:
                elapsed = time.perf_counter() - t0
                print(f"  [{chunk_end:>8,} rows] {chunk_end/elapsed:,.0f} rows/sec",
                      file=sys.stderr)

        elapsed = time.perf_counter() - t0
        return n, elapsed

    return asyncio.run(_run())


# ---------------------------------------------------------------------------
# Variant definitions
# ---------------------------------------------------------------------------

# Each variant is: (label, data_loader, ingestion_function, is_rust)
# data_loader: 'pylist' or 'numpy'
# ingestion_function: one of the ingest_* functions above

VARIANTS = {
    "A":  ("scylla-driver (master), execute_concurrent, list[float]",
           'pylist', ingest_execute_concurrent, False),
    "A2": ("scylla-driver (master), execute_concurrent, numpy",
           'numpy', ingest_execute_concurrent, False),
    "A3": ("scylla-driver (master), decoupled executor, numpy",
           'numpy', ingest_decoupled, False),
    "B":  ("scylla-driver (enhanced), execute_concurrent, list[float]",
           'pylist', ingest_execute_concurrent, False),
    "C":  ("scylla-driver (enhanced), execute_concurrent, numpy bulk",
           'numpy', ingest_execute_concurrent, False),
    "D":  ("python-rs-driver, asyncio.gather",
           'numpy_tolist', ingest_rust_asyncio, True),
    "E":  ("scylla-driver (enhanced), decoupled executor, numpy bulk",
           'numpy', ingest_decoupled, False),
    "F":  ("scylla-driver (enhanced), free-threaded, execute_concurrent, numpy bulk",
           'numpy', ingest_execute_concurrent, False),
}


def main():
    parser = argparse.ArgumentParser(description="Vector ingestion benchmark")
    parser.add_argument("--variant", required=True, choices=VARIANTS.keys(),
                        help="Which driver variant to benchmark")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9042)
    parser.add_argument("--dataset-dir",
                        default=os.path.expanduser("~/vector_bench/dataset/cohere/cohere_medium_1m"))
    parser.add_argument("--max-rows", type=int, default=100_000,
                        help="Number of rows to ingest per run (default: 100000)")
    parser.add_argument("--concurrency", type=int, default=1000,
                        help="Max in-flight requests (default: 1000)")
    parser.add_argument("--dim", type=int, default=768)
    parser.add_argument("--runs", type=int, default=3,
                        help="Number of timed runs (default: 3)")
    parser.add_argument("--warmup-rows", type=int, default=1000,
                        help="Rows to ingest as warmup before timed runs (default: 1000)")
    parser.add_argument("--warmup-sleep", type=float, default=3.0,
                        help="Seconds to sleep after warmup (default: 3.0)")
    parser.add_argument("--inter-run-sleep", type=float, default=2.0,
                        help="Seconds to sleep between runs (default: 2.0)")
    args = parser.parse_args()

    label, data_loader, ingest_fn, is_rust = VARIANTS[args.variant]
    concurrency = args.concurrency
    max_rows = args.max_rows

    print(f"Running variant {args.variant}: {label}", file=sys.stderr)
    print(f"  host={args.host}:{args.port}  dim={args.dim}  concurrency={concurrency}",
          file=sys.stderr)
    print(f"  max_rows={max_rows}  runs={args.runs}  warmup_rows={args.warmup_rows}",
          file=sys.stderr)
    print(f"  dataset={args.dataset_dir}", file=sys.stderr)
    print("", file=sys.stderr)

    # ---- Load data ----
    print("  Loading data...", file=sys.stderr)
    t_load = time.perf_counter()
    if data_loader == 'pylist':
        params = load_params_pylist(args.dataset_dir, args.dim, max_rows)
    elif data_loader == 'numpy':
        params = load_params_numpy(args.dataset_dir, args.dim, max_rows)
    elif data_loader == 'numpy_tolist':
        # Rust driver needs list[float], not numpy arrays
        import numpy as np
        pf = ParquetFile(f"{args.dataset_dir}/shuffle_train.parquet")
        batch = next(pf.iter_batches(max_rows))
        ids = batch.column("id").to_pylist()
        emb_col = batch.column("emb")
        arr = np.frombuffer(emb_col.values.buffers()[1], dtype=np.float32).reshape(-1, args.dim)
        n = min(len(ids), max_rows) if max_rows > 0 else len(ids)
        params = [(ids[i], arr[i].tolist()) for i in range(n)]
    else:
        raise ValueError(f"Unknown data_loader: {data_loader}")
    print(f"  Loaded {len(params):,} rows in {time.perf_counter()-t_load:.1f}s",
          file=sys.stderr)

    # ---- Rust driver path (different session model) ----
    if is_rust:
        # Rust driver handles its own session; we just need truncate via cqlsh
        # For simplicity, the rust ingest function handles everything.
        # TODO: add warmup + multi-run support for rust variant
        total, elapsed = ingest_fn((args.host, args.port), CQL_INSERT, params, concurrency)
        rows_per_sec = total / elapsed if elapsed > 0 else 0
        result = {
            "variant": args.variant, "label": label,
            "rows": total, "runs": 1,
            "rates": [round(rows_per_sec, 1)],
            "best": round(rows_per_sec, 1),
            "avg": round(rows_per_sec, 1),
            "worst": round(rows_per_sec, 1),
        }
        print(json.dumps(result))
        print(f"\n  Result: {rows_per_sec:,.0f} rows/sec", file=sys.stderr)
        return

    # ---- scylla-driver path ----
    from cassandra import ConsistencyLevel
    from cassandra.cluster import Cluster

    cluster = Cluster([args.host], port=args.port, compression=False)
    session = cluster.connect()

    session.execute(cql_create_keyspace())
    session.set_keyspace(KEYSPACE)
    session.execute(cql_create_table(args.dim))
    session.execute(CQL_TRUNCATE)

    prepared = session.prepare(CQL_INSERT)
    prepared.consistency_level = ConsistencyLevel.ONE

    n_conns = _wait_for_shard_connections(session)
    print(f"  shard connections: {n_conns}", file=sys.stderr)

    # ---- Warmup ----
    if args.warmup_rows > 0:
        warmup_params = params[:args.warmup_rows]
        print(f"  Warmup: {len(warmup_params)} rows...", file=sys.stderr)
        ingest_fn(session, prepared, warmup_params, concurrency)
        print(f"  Warmup done, sleeping {args.warmup_sleep}s...", file=sys.stderr)
        time.sleep(args.warmup_sleep)

    # ---- Timed runs ----
    rates = []
    for run_idx in range(args.runs):
        session.execute(CQL_TRUNCATE)
        if args.inter_run_sleep > 0 and run_idx > 0:
            time.sleep(args.inter_run_sleep)

        print(f"  --- Run {run_idx+1}/{args.runs} ---", file=sys.stderr)
        total, elapsed = ingest_fn(session, prepared, params, concurrency)
        rate = total / elapsed if elapsed > 0 else 0
        rates.append(rate)
        print(f"  Run {run_idx+1}: {total:,} rows in {elapsed:.2f}s = {rate:,.0f} rows/sec",
              file=sys.stderr)

    cluster.shutdown()

    best = max(rates)
    avg = sum(rates) / len(rates)
    worst = min(rates)

    result = {
        "variant": args.variant,
        "label": label,
        "rows": total,
        "runs": args.runs,
        "rates": [round(r, 1) for r in rates],
        "best": round(best, 1),
        "avg": round(avg, 1),
        "worst": round(worst, 1),
    }

    # Print JSON to stdout (machine-readable), human-readable to stderr
    print(json.dumps(result))
    print("", file=sys.stderr)
    print(f"  Summary: best={best:,.0f}  avg={avg:,.0f}  worst={worst:,.0f} rows/sec",
          file=sys.stderr)


if __name__ == "__main__":
    main()
