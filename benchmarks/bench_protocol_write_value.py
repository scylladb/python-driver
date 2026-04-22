# Copyright DataStax, Inc.
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
Micro-benchmark: pre-computed int32_pack constants for null/unset markers.

Measures the speedup from using pre-computed _INT32_NEG1/_INT32_NEG2
constants vs calling int32_pack(-1)/int32_pack(-2) per parameter.

Run:
    python benchmarks/bench_protocol_write_value.py
"""
import timeit
import struct

int32_pack = struct.Struct('>i').pack

# Pre-computed constants (the optimization)
_INT32_NEG1 = int32_pack(-1)
_INT32_NEG2 = int32_pack(-2)

_UNSET_VALUE = object()


def bench_write_value_constants():
    """Benchmark pre-computed constants vs per-call int32_pack."""
    # Simulate a typical parameter list: mix of values, nulls, and unsets
    params = [b'\x00\x00\x00\x01'] * 8 + [None] * 3 + [_UNSET_VALUE] * 1

    def run_precomputed():
        parts = []
        _parts_append = parts.append
        _i32 = int32_pack
        for param in params:
            if param is None:
                _parts_append(_INT32_NEG1)
            elif param is _UNSET_VALUE:
                _parts_append(_INT32_NEG2)
            else:
                _parts_append(_i32(len(param)))
                _parts_append(param)
        return b"".join(parts)

    def run_pack_each_time():
        parts = []
        _parts_append = parts.append
        _i32 = int32_pack
        for param in params:
            if param is None:
                _parts_append(_i32(-1))
            elif param is _UNSET_VALUE:
                _parts_append(_i32(-2))
            else:
                _parts_append(_i32(len(param)))
                _parts_append(param)
        return b"".join(parts)

    # Verify identical output
    assert run_precomputed() == run_pack_each_time()

    n = 500_000
    t_precomputed = timeit.timeit(run_precomputed, number=n)
    t_pack = timeit.timeit(run_pack_each_time, number=n)

    print(f"Pre-computed constants ({n} iters, {len(params)} params): "
          f"{t_precomputed:.3f}s  ({t_precomputed / n * 1e6:.2f} us/call)")
    print(f"Pack each time         ({n} iters, {len(params)} params): "
          f"{t_pack:.3f}s  ({t_pack / n * 1e6:.2f} us/call)")
    speedup = t_pack / t_precomputed
    print(f"Speedup: {speedup:.2f}x")


def main():
    bench_write_value_constants()


if __name__ == '__main__':
    main()
