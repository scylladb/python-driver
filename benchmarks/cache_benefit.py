#!/usr/bin/env python3
"""
Simple benchmark demonstrating LRU cache benefit for repeated type lookups.
"""
import os
import sys
import time

# Pin to single CPU
if hasattr(os, 'sched_setaffinity'):
    try:
        cpu = list(os.sched_getaffinity(0))[0]
        os.sched_setaffinity(0, {cpu})
        print(f"Benchmark pinned to CPU {cpu}\n")
    except Exception:
        # Best-effort CPU pinning: ignore failures (unsupported platform, permissions, etc.).
        pass

repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from cassandra.cqltypes import lookup_casstype, parse_casstype_args, lookup_casstype_simple

print("=" * 80)
print("LRU CACHE BENEFIT DEMONSTRATION")
print("=" * 80)
print("\nShowing performance improvement from caching parse_casstype_args()")
print("Scenario: Same complex type string looked up 100,000 times")
print("(simulates repeated query execution with same schema)\n")

# Test types
types_to_test = [
    ("Simple", "org.apache.cassandra.db.marshal.UTF8Type"),
    ("List", "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)"),
    ("Map", "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"),
    ("Nested", "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type))"),
]

iterations = 100000

print(f"{'Type':<15} {'Time (100k calls)':<20} {'Per call':<15} {'Cache Info'}")
print("-" * 80)

for name, type_str in types_to_test:
    # Clear cache before test
    if hasattr(parse_casstype_args, 'cache_clear'):
        parse_casstype_args.cache_clear()
    # Also clear lookup_casstype_simple cache for accurate measurements
    if hasattr(lookup_casstype_simple, 'cache_clear'):
        lookup_casstype_simple.cache_clear()

    # Run test
    start = time.perf_counter()
    for _ in range(iterations):
        lookup_casstype(type_str)
    elapsed = time.perf_counter() - start

    per_call = (elapsed / iterations) * 1_000_000  # Convert to microseconds

    # Get cache stats
    if hasattr(parse_casstype_args, 'cache_info'):
        cache_info = parse_casstype_args.cache_info()
        cache_str = f"hits={cache_info.hits} misses={cache_info.misses}"
    else:
        cache_str = "no cache"

    print(f"{name:<15} {elapsed*1000:>8.2f} ms          {per_call:>6.3f} μs      {cache_str}")

print("\n" + "=" * 80)
print("INTERPRETATION")
print("=" * 80)
print("\nFor complex types (List, Map, Nested):")
print("  • First call (miss=1): Parses with regex scanner")
print("  • Subsequent calls (hits=99,999): Return cached result")
print("  • Speedup: Nearly instant lookups after first parse\n")

print("For simple types:")
print("  • Fast path: Never calls parse_casstype_args")
print("  • Direct dict lookup in _casstypes")
print("  • No cache needed (already optimized)\n")

# Demonstrate cache effectiveness with mixed types
print("\n" + "=" * 80)
print("REAL-WORLD SCENARIO: Mixed Schema with 4 Columns")
print("=" * 80)
print("\nSchema: (id: UUID, name: text, age: int, tags: list<text>)")
print("Simulating 10,000 query executions...\n")

schema_types = [
    "org.apache.cassandra.db.marshal.UUIDType",
    "org.apache.cassandra.db.marshal.UTF8Type",
    "org.apache.cassandra.db.marshal.Int32Type",
    "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.UTF8Type)",
]

# Clear cache
if hasattr(parse_casstype_args, 'cache_clear'):
    parse_casstype_args.cache_clear()
# Also clear lookup_casstype_simple cache for accurate measurements
if hasattr(lookup_casstype_simple, 'cache_clear'):
    lookup_casstype_simple.cache_clear()

queries = 10000
start = time.perf_counter()
for _ in range(queries):
    for type_str in schema_types:
        lookup_casstype(type_str)
elapsed = time.perf_counter() - start

total_lookups = queries * len(schema_types)
per_query = (elapsed / queries) * 1_000_000  # μs
per_lookup = (elapsed / total_lookups) * 1_000_000  # μs

print(f"Total queries:     {queries:>10,}")
print(f"Total lookups:     {total_lookups:>10,}")
print(f"Total time:        {elapsed*1000:>10.2f} ms")
print(f"Per query:         {per_query:>10.3f} μs")
print(f"Per lookup:        {per_lookup:>10.3f} μs")

if hasattr(parse_casstype_args, 'cache_info'):
    cache_info = parse_casstype_args.cache_info()
    print(f"\nCache statistics:")
    print(f"  Hits:            {cache_info.hits:>10,}")
    print(f"  Misses:          {cache_info.misses:>10,}")
    print(f"  Hit rate:        {cache_info.hits / (cache_info.hits + cache_info.misses) * 100:>10.1f}%")

print("\n" + "=" * 80)
print("KEY FINDING")
print("=" * 80)
print("\nThe LRU cache on parse_casstype_args() provides:")
print("  ✓ Fast repeated lookups of complex types (List, Map, Set, Tuple)")
print("  ✓ Natural fast path for simple types (no parentheses = no parsing)")
print("  ✓ Significant performance improvement for real-world query workloads")
print()
