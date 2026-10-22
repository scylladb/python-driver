# Copyright 2026 ScyllaDB, Inc.
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
Unit tests for the Cython LZ4 direct-C-linkage wrappers.

Tests verify:
  - Round-trip correctness at various payload sizes
  - Wire-format compatibility with the Python lz4 wrappers in connection.py
  - Edge cases (empty input, minimal input, header-only frames)
  - Error handling (truncated frames, corrupt payloads)
"""

import os
import random
import struct
import threading
import unittest

try:
    from cassandra import cython_lz4
    from cassandra.cython_lz4 import lz4_compress, lz4_decompress
    HAS_CYTHON_LZ4 = True
except ImportError:
    HAS_CYTHON_LZ4 = False

try:
    import lz4.block as lz4_block
    HAS_PYTHON_LZ4 = True
except ImportError:
    HAS_PYTHON_LZ4 = False

int32_pack = struct.Struct('>i').pack


def _py_lz4_compress(byts):
    """Python LZ4 compress wrapper (same logic as connection.py)."""
    return int32_pack(len(byts)) + lz4_block.compress(byts)[4:]


def _py_lz4_decompress(byts):
    """Python LZ4 decompress wrapper (same logic as connection.py)."""
    return lz4_block.decompress(byts[3::-1] + byts[4:])


@unittest.skipUnless(HAS_CYTHON_LZ4, "cassandra.cython_lz4 extension not available")
class CythonLZ4Test(unittest.TestCase):
    """Tests for cassandra.cython_lz4.lz4_compress / lz4_decompress."""

    def test_round_trip_small(self):
        """Round-trip a small payload."""
        data = b"Hello, CQL!" * 10
        self.assertEqual(lz4_decompress(lz4_compress(data)), data)

    def test_round_trip_1kb(self):
        data = os.urandom(512) + b"\x00" * 512  # 1 KB, partially compressible
        self.assertEqual(lz4_decompress(lz4_compress(data)), data)

    def test_round_trip_8kb(self):
        data = (b"row_data_" + os.urandom(7)) * 512  # ~8 KB
        data = data[:8192]
        self.assertEqual(lz4_decompress(lz4_compress(data)), data)

    def test_round_trip_64kb(self):
        data = os.urandom(65536)
        self.assertEqual(lz4_decompress(lz4_compress(data)), data)

    def test_round_trip_heap_buffer(self):
        """Inputs above the 16 KiB stack threshold use the malloc path."""
        # LZ4_compressBound(16384) = 16384 + 16384/255 + 16 = 16464,
        # which exceeds STACK_ALLOC_THRESHOLD, so lz4_compress must fall
        # back to the heap buffer and free it on success.
        data = os.urandom(16384)
        self.assertEqual(lz4_decompress(lz4_compress(data)), data)

    def test_round_trip_empty(self):
        """Empty input should round-trip to empty bytes."""
        compressed = lz4_compress(b"")
        self.assertEqual(lz4_decompress(compressed), b"")

    def test_round_trip_single_byte(self):
        data = b"\x42"
        self.assertEqual(lz4_decompress(lz4_compress(data)), data)

    def test_compress_header_format(self):
        """Verify the 4-byte big-endian uncompressed length header."""
        data = b"x" * 300
        compressed = lz4_compress(data)
        # First 4 bytes should be big-endian length of original data
        header = struct.unpack('>I', compressed[:4])[0]
        self.assertEqual(header, 300)

    def test_decompress_too_short(self):
        """Frames shorter than 4 bytes should raise ValueError."""
        with self.assertRaises(ValueError):
            lz4_decompress(b"")
        with self.assertRaises(ValueError):
            lz4_decompress(b"\x00")
        with self.assertRaises(ValueError):
            lz4_decompress(b"\x00\x00\x00")

    def test_decompress_zero_length_header(self):
        """Zero declared size requires a valid empty LZ4 block (one token byte)."""
        # lz4_compress(b"") emits a zero BE length header + a single 0x00
        # token that is a valid empty LZ4 block.
        self.assertEqual(lz4_decompress(b"\x00\x00\x00\x00\x00"), b"")

    def test_decompress_zero_length_header_missing_block(self):
        """Zero declared size with no compressed block should raise."""
        with self.assertRaises(RuntimeError):
            lz4_decompress(b"\x00\x00\x00\x00")

    def test_decompress_zero_length_header_invalid_block(self):
        """Zero declared size with a non-empty block token should raise."""
        with self.assertRaises(RuntimeError):
            lz4_decompress(b"\x00\x00\x00\x00\xff")

    def test_decompress_corrupt_payload(self):
        """Corrupted compressed data should raise RuntimeError."""
        # Valid header claiming 1000 bytes, but garbage payload
        bad_frame = struct.pack('>I', 1000) + b"\xff" * 20
        with self.assertRaises(RuntimeError):
            lz4_decompress(bad_frame)

    def test_decompress_oversized_header(self):
        """Header claiming > 256 MiB should raise ValueError."""
        # 0x10000001 = 256 MiB + 1
        huge_header = struct.pack('>I', 0x10000001) + b"\x00" * 10
        with self.assertRaises(ValueError):
            lz4_decompress(huge_header)

    def test_round_trip_all_zeros(self):
        """All-zero payloads compress extremely well; verify correctness."""
        data = b"\x00" * 10000
        self.assertEqual(lz4_decompress(lz4_compress(data)), data)

    def test_round_trip_all_ones(self):
        data = b"\xff" * 10000
        self.assertEqual(lz4_decompress(lz4_compress(data)), data)

    def test_compress_rejects_none(self):
        """None input should raise TypeError (enforced by Cython bytes type)."""
        with self.assertRaises(TypeError):
            lz4_compress(None)

    def test_decompress_rejects_none(self):
        with self.assertRaises(TypeError):
            lz4_decompress(None)

    def test_compress_rejects_non_bytes(self):
        """bytearray and other non-bytes types should raise TypeError."""
        with self.assertRaises(TypeError):
            lz4_compress(bytearray(b"hello"))
        with self.assertRaises(TypeError):
            lz4_compress(memoryview(b"hello"))
        with self.assertRaises(TypeError):
            lz4_compress("hello")

    def test_decompress_rejects_non_bytes(self):
        with self.assertRaises(TypeError):
            lz4_decompress(bytearray(b"\x00\x00\x00\x05hello"))
        with self.assertRaises(TypeError):
            lz4_decompress("hello")

    def test_decompress_header_only_nonzero(self):
        """A 4-byte header claiming non-zero size with no payload should fail."""
        header_only = struct.pack('>I', 10)  # claims 10 bytes, but no data
        with self.assertRaises(RuntimeError):
            lz4_decompress(header_only)


def _payloads(size):
    """Incompressible, highly compressible and mixed payloads of *size* bytes."""
    rnd = random.Random(size)
    yield "random", os.urandom(size)
    yield "zeros", b"\x00" * size
    yield "text", (b"SELECT * FROM ks.tbl WHERE pk = ?;" * (size // 34 + 1))[:size]
    yield "mixed", bytes(rnd.getrandbits(8) if i % 3 else 0x41 for i in range(size))


def _sizes():
    t = cython_lz4._NOGIL_THRESHOLD
    return (0, 1, 2, 3, 4, 15, 16, 255, 256, 16383, 16384, 16385,
            t // 2, t - 1, t, t + 1, 2 * t, 1 << 20)


@unittest.skipUnless(HAS_CYTHON_LZ4, "cassandra.cython_lz4 extension not available")
class CythonLZ4NoGilThresholdTest(unittest.TestCase):
    """Both the GIL-held (small) and GIL-released (large) paths must behave identically."""

    def test_threshold_constant(self):
        t = cython_lz4._NOGIL_THRESHOLD
        self.assertIsInstance(t, int)
        self.assertEqual(t, 2048)
        # Below the 16 KiB stack/heap split, so _sizes() covers all three paths.
        self.assertLess(t, 16384)

    def test_round_trip_straddling_threshold(self):
        for size in _sizes():
            for kind, data in _payloads(size):
                with self.subTest(size=size, kind=kind):
                    compressed = lz4_compress(data)
                    self.assertIs(type(compressed), bytes)
                    self.assertEqual(struct.unpack('>I', compressed[:4])[0], size)
                    out = lz4_decompress(compressed)
                    self.assertIs(type(out), bytes)
                    self.assertEqual(out, data)

    def test_round_trip_large(self):
        for size in (1 << 20, 4 << 20):
            for kind, data in _payloads(size):
                with self.subTest(size=size, kind=kind):
                    self.assertEqual(lz4_decompress(lz4_compress(data)), data)

    def test_compressed_size_far_below_threshold_decompresses_above(self):
        """Decompress picks the path by declared (output) size, not input size."""
        t = cython_lz4._NOGIL_THRESHOLD
        for size in (t - 1, t, t + 1, 64 * 1024):
            data = b"\x00" * size
            compressed = lz4_compress(data)
            with self.subTest(size=size):
                self.assertLess(len(compressed), t)
                self.assertEqual(lz4_decompress(compressed), data)

    def test_repeated_calls_are_independent(self):
        t = cython_lz4._NOGIL_THRESHOLD
        datas = [os.urandom(n) for n in (10, t + 1, 100, t - 1, t, 1)]
        frames = [lz4_compress(d) for d in datas]
        for d, f in zip(datas, frames):
            self.assertEqual(lz4_compress(d), f)
        for d, f in zip(reversed(datas), reversed(frames)):
            self.assertEqual(lz4_decompress(f), d)

    def test_input_not_mutated(self):
        for size in (100, cython_lz4._NOGIL_THRESHOLD + 1):
            data = os.urandom(size)
            copy = bytes(bytearray(data))
            frame = lz4_compress(data)
            frame_copy = bytes(bytearray(frame))
            lz4_decompress(frame)
            with self.subTest(size=size):
                self.assertEqual(data, copy)
                self.assertEqual(frame, frame_copy)

    # -- error handling must be identical on both sides of the threshold --

    def _sizes_around_threshold(self):
        t = cython_lz4._NOGIL_THRESHOLD
        return (1000, t - 1, t, t + 1, 1 << 20)

    def test_corrupt_payload(self):
        for size in self._sizes_around_threshold():
            bad = struct.pack('>I', size) + b"\xff" * 20
            with self.subTest(size=size):
                with self.assertRaisesRegex(
                        RuntimeError, r"^LZ4_decompress_safe\(\) failed with error code -\d+; "
                                      r"compressed payload may be malformed$"):
                    lz4_decompress(bad)

    def test_truncated_payload(self):
        for size in self._sizes_around_threshold():
            frame = lz4_compress(os.urandom(size))
            for cut in (5, len(frame) // 2, len(frame) - 1):
                with self.subTest(size=size, cut=cut):
                    with self.assertRaisesRegex(RuntimeError,
                                                r"^LZ4_decompress_safe\(\) failed"):
                        lz4_decompress(frame[:cut])

    def test_header_only(self):
        for size in self._sizes_around_threshold():
            with self.subTest(size=size):
                with self.assertRaisesRegex(RuntimeError,
                                            r"^LZ4_decompress_safe\(\) failed"):
                    lz4_decompress(struct.pack('>I', size))

    def test_declared_length_larger_than_actual(self):
        """Valid block whose header overstates the size: short-output error."""
        for size in self._sizes_around_threshold():
            frame = lz4_compress(os.urandom(size))
            bad = struct.pack('>I', size + 1) + frame[4:]
            with self.subTest(size=size):
                with self.assertRaisesRegex(
                        RuntimeError,
                        r"^LZ4_decompress_safe\(\) produced %d bytes but header "
                        r"declared %d$" % (size, size + 1)):
                    lz4_decompress(bad)

    def test_declared_length_smaller_than_actual(self):
        """Header understates the size: output buffer overflow is rejected."""
        for size in self._sizes_around_threshold():
            frame = lz4_compress(os.urandom(size))
            bad = struct.pack('>I', size - 1) + frame[4:]
            with self.subTest(size=size):
                with self.assertRaisesRegex(RuntimeError,
                                            r"^LZ4_decompress_safe\(\) failed"):
                    lz4_decompress(bad)

    def test_declared_length_oversized(self):
        for declared in (0x10000001, 0x7FFFFFFF, 0x80000000, 0xFFFFFFFF):
            frame = struct.pack('>I', declared) + lz4_compress(os.urandom(100))[4:]
            with self.subTest(declared=declared):
                with self.assertRaisesRegex(
                        ValueError,
                        r"^Declared uncompressed size %d exceeds safety limit of "
                        r"268435456 bytes; frame header may be corrupt$" % declared):
                    lz4_decompress(frame)

    def test_too_short_message(self):
        for n in range(4):
            with self.subTest(n=n):
                with self.assertRaisesRegex(
                        ValueError,
                        r"^LZ4-compressed frame too short: need at least 4 bytes "
                        r"for the length header, got %d$" % n):
                    lz4_decompress(b"\x00" * n)

    # -- accepted input types are unchanged: exact bytes only --

    def test_rejects_non_bytes_both_sides_of_threshold(self):
        class BytesSub(bytes):
            pass

        for size in (10, cython_lz4._NOGIL_THRESHOLD + 1):
            data = os.urandom(size)
            frame = lz4_compress(data)
            for conv in (bytearray, memoryview, BytesSub):
                with self.subTest(size=size, type=conv.__name__):
                    with self.assertRaises(TypeError):
                        lz4_compress(conv(data))
                    with self.assertRaises(TypeError):
                        lz4_decompress(conv(frame))
            for fn in (lz4_compress, lz4_decompress):
                with self.subTest(size=size, fn=fn.__name__):
                    with self.assertRaises(TypeError):
                        fn(None)
                    with self.assertRaises(TypeError):
                        fn("x" * size)

    # -- concurrency --

    def test_concurrent_mixed_sizes(self):
        t = cython_lz4._NOGIL_THRESHOLD
        sizes = (0, 1, 100, 4096, t - 1, t, t + 1, 3 * t, 1 << 20)
        inputs = [d for n in sizes for _, d in _payloads(n)]
        frames = [lz4_compress(d) for d in inputs]
        n_threads, rounds = 8, 5
        barrier = threading.Barrier(n_threads)
        errors = []

        def worker(seed):
            order = list(range(len(inputs)))
            random.Random(seed).shuffle(order)
            try:
                barrier.wait()
                for _ in range(rounds):
                    for i in order:
                        f = lz4_compress(inputs[i])
                        if f != frames[i]:
                            errors.append(("compress", seed, i))
                        if lz4_decompress(f) != inputs[i]:
                            errors.append(("decompress", seed, i))
            except Exception as e:
                errors.append(("exception", seed, repr(e)))

        threads = [threading.Thread(target=worker, args=(s,)) for s in range(n_threads)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertEqual(errors, [])

    def test_concurrent_errors_and_successes(self):
        """Failing calls on some threads must not disturb others."""
        t = cython_lz4._NOGIL_THRESHOLD
        good = [os.urandom(n) for n in (10, t - 1, t + 1, 1 << 20)]
        bad = [struct.pack('>I', n) + b"\xff" * 20 for n in (10, t - 1, t + 1, 1 << 20)]
        barrier = threading.Barrier(6)
        errors = []

        def ok_worker():
            try:
                barrier.wait()
                for _ in range(5):
                    for d in good:
                        if lz4_decompress(lz4_compress(d)) != d:
                            errors.append("mismatch")
            except Exception as e:
                errors.append(("ok_exception", repr(e)))

        def bad_worker():
            try:
                barrier.wait()
                for _ in range(5):
                    for f in bad:
                        try:
                            lz4_decompress(f)
                            errors.append("no error")
                        except RuntimeError:
                            pass
            except Exception as e:
                errors.append(("bad_exception", repr(e)))

        threads = [threading.Thread(target=w) for w in (ok_worker, bad_worker) * 3]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        self.assertEqual(errors, [])


@unittest.skipUnless(HAS_CYTHON_LZ4 and HAS_PYTHON_LZ4,
                     "Both cassandra.cython_lz4 and lz4 package required")
class CythonLZ4CrossCompatTest(unittest.TestCase):
    """Verify wire-format compatibility between Cython and Python wrappers."""

    def _check_cross_compat(self, data):
        """Assert both directions of cross-compatibility."""
        py_compressed = _py_lz4_compress(data)
        cy_compressed = lz4_compress(data)

        # Cython decompresses Python's output
        self.assertEqual(lz4_decompress(py_compressed), data)
        # Python decompresses Cython's output
        self.assertEqual(_py_lz4_decompress(cy_compressed), data)

    def test_cross_compat_small(self):
        self._check_cross_compat(b"Hello, world!" * 50)

    def test_cross_compat_1kb(self):
        self._check_cross_compat(os.urandom(1024))

    def test_cross_compat_8kb(self):
        self._check_cross_compat(os.urandom(8192))

    def test_cross_compat_64kb(self):
        self._check_cross_compat(os.urandom(65536))

    def test_cross_compat_empty(self):
        self._check_cross_compat(b"")

    def test_cross_compat_straddling_threshold(self):
        for size in _sizes():
            for kind, data in _payloads(size):
                with self.subTest(size=size, kind=kind):
                    self._check_cross_compat(data)

    def test_cross_compat_large(self):
        for kind, data in _payloads(4 << 20):
            with self.subTest(kind=kind):
                self._check_cross_compat(data)

    def test_length_prefix_matches_lz4_block(self):
        """Same size prefix as lz4.block, but big-endian.

        Compressed blocks are not compared byte-for-byte: system liblz4 and the
        lz4 package's bundled copy may legitimately emit different encodings.
        """
        for size in _sizes():
            for kind, data in _payloads(size):
                with self.subTest(size=size, kind=kind):
                    cy = lz4_compress(data)
                    raw = lz4_block.compress(data)
                    self.assertEqual(cy[:4], struct.pack('>I', size))
                    self.assertEqual(raw[:4], struct.pack('<I', size))
                    self.assertEqual(cy[:4], _py_lz4_compress(data)[:4])

    def test_raw_block_interop(self):
        """CQL framing: BE prefix + raw block decodes with lz4.block and vice versa."""
        for size in _sizes():
            for kind, data in _payloads(size):
                with self.subTest(size=size, kind=kind):
                    cy = lz4_compress(data)
                    self.assertEqual(
                        lz4_block.decompress(cy[4:], uncompressed_size=size), data)
                    raw = lz4_block.compress(data, store_size=False)
                    self.assertEqual(lz4_decompress(struct.pack('>I', size) + raw), data)

    def test_error_parity_with_python_wrapper(self):
        """Corrupt frames rejected by lz4.block are rejected by cython_lz4 too."""
        t = cython_lz4._NOGIL_THRESHOLD
        for size in (1000, t - 1, t, t + 1):
            frame = lz4_compress(os.urandom(size))
            for bad in (struct.pack('>I', size) + b"\xff" * 20,
                        frame[:len(frame) // 2],
                        struct.pack('>I', size - 1) + frame[4:]):
                with self.subTest(size=size, n=len(bad)):
                    with self.assertRaises(Exception):
                        _py_lz4_decompress(bad)
                    with self.assertRaises(RuntimeError):
                        lz4_decompress(bad)

    def test_connection_prefers_cython_codec(self):
        """The connection layer selects the Cython codec when present."""
        from cassandra import connection

        self.assertIs(connection.locally_supported_compressions['lz4'][0],
                      lz4_compress)
        self.assertIs(connection.locally_supported_compressions['lz4'][1],
                      lz4_decompress)
        self.assertIs(connection.segment_codec_lz4.compressor, lz4_compress)
        self.assertIs(connection.segment_codec_lz4.decompressor, lz4_decompress)


if __name__ == "__main__":
    unittest.main()
