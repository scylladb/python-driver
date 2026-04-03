#!/usr/bin/env python
# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Benchmark: collection serialization - BytesIO vs b"".join(parts).

Measures end-to-end serialize_safe performance for List, Map, Tuple, and UserType
with varying collection sizes.
"""

import timeit
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cassandra.cqltypes import (
    ListType,
    SetType,
    MapType,
    TupleType,
    UserType,
    Int32Type,
    UTF8Type,
    FloatType,
)

PROTOCOL_VERSION = 4

# Build parameterized types
ListOfInt = ListType.apply_parameters([Int32Type])
SetOfInt = SetType.apply_parameters([Int32Type])
MapIntToStr = MapType.apply_parameters([Int32Type, UTF8Type])


# For TupleType and UserType, we need to set subtypes on a subclass
class TestTupleType(TupleType):
    subtypes = (
        Int32Type,
        Int32Type,
        Int32Type,
        Int32Type,
        Int32Type,
        Int32Type,
        Int32Type,
        Int32Type,
        Int32Type,
        Int32Type,
    )


class TestUserType(UserType):
    subtypes = (Int32Type, UTF8Type, FloatType, Int32Type, UTF8Type)
    fieldnames = ("id", "name", "score", "age", "email")
    typename = "test_udt"
    keyspace = "test_ks"
    mapped_class = None
    tuple_type = None


def run_bench(label, fn, args, n):
    # Warm up
    for _ in range(min(1000, n)):
        fn(*args)
    t = timeit.timeit(lambda: fn(*args), number=n)
    ns_per_call = t / n * 1e9
    print(f"  {label:45s} {t:.3f}s  ({ns_per_call:.2f} ns/call)")
    return t, ns_per_call


# Test data
list_10 = list(range(10))
list_100 = list(range(100))
list_1000 = list(range(1000))
list_with_nulls = [i if i % 3 != 0 else None for i in range(100)]

map_10 = {i: f"value_{i}" for i in range(10)}
map_100 = {i: f"value_{i}" for i in range(100)}

tuple_10 = tuple(range(10))
udt_val = (1, "test_name", 3.14, 25, "test@example.com")

N_SMALL = 500_000
N_MED = 100_000
N_LARGE = 10_000

print(f"Collection serialization benchmark")
print(f"=" * 70)

results = {}

print(f"\nListType.serialize (list of int32):")
_, r = run_bench(
    "10 elements", ListOfInt.serialize, (list_10, PROTOCOL_VERSION), N_SMALL
)
results["list_10"] = r
_, r = run_bench(
    "100 elements", ListOfInt.serialize, (list_100, PROTOCOL_VERSION), N_MED
)
results["list_100"] = r
_, r = run_bench(
    "1000 elements", ListOfInt.serialize, (list_1000, PROTOCOL_VERSION), N_LARGE
)
results["list_1000"] = r
_, r = run_bench(
    "100 elements (33% null)",
    ListOfInt.serialize,
    (list_with_nulls, PROTOCOL_VERSION),
    N_MED,
)
results["list_100_nulls"] = r

print(f"\nMapType.serialize (map<int32, text>):")
_, r = run_bench(
    "10 entries", MapIntToStr.serialize, (map_10, PROTOCOL_VERSION), N_SMALL
)
results["map_10"] = r
_, r = run_bench(
    "100 entries", MapIntToStr.serialize, (map_100, PROTOCOL_VERSION), N_MED
)
results["map_100"] = r

print(f"\nTupleType.serialize (10 x int32):")
_, r = run_bench(
    "10 fields", TestTupleType.serialize, (tuple_10, PROTOCOL_VERSION), N_SMALL
)
results["tuple_10"] = r

print(f"\nUserType.serialize (5 fields: int, text, float, int, text):")
_, r = run_bench(
    "5 fields", TestUserType.serialize, (udt_val, PROTOCOL_VERSION), N_SMALL
)
results["udt_5"] = r

# Print summary for easy comparison
print(f"\n{'=' * 70}")
print("Summary (ns/call):")
for k, v in results.items():
    print(f"  {k:25s}: {v:.2f} ns")
