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
import struct
import unittest

try:
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
        """A 4-byte zero header (plus any trailing bytes) means empty output."""
        self.assertEqual(lz4_decompress(b"\x00\x00\x00\x00"), b"")
        self.assertEqual(lz4_decompress(b"\x00\x00\x00\x00\xff"), b"")

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


if __name__ == "__main__":
    unittest.main()
