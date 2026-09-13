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

import unittest
import pytest

try:
    from cassandra.bytesio import BytesIOReader

    has_cython = True
except ImportError:
    has_cython = False


@pytest.mark.skipif(not has_cython, reason="Cython extensions not compiled")
class BytesIOReaderTest(unittest.TestCase):
    """Tests for the Cython BytesIOReader, including the offset parameter.

    Note: BytesIOReader.read() is a cdef method, so it cannot be called
    directly from Python.  Reading with an offset is exercised through the
    end-to-end decode_message test in test_protocol.py which goes through
    the Cython row parser path (remaining_buffer -> BytesIOReader(buf, offset)).
    """

    def test_construct_no_offset(self):
        # Should not raise
        reader = BytesIOReader(b"\x00\x01\x02\x03\x04\x05")

    def test_construct_with_zero_offset(self):
        reader = BytesIOReader(b"hello world", 0)

    def test_construct_with_offset(self):
        reader = BytesIOReader(b"header_row_data", 7)

    def test_construct_offset_at_end(self):
        data = b"abcdef"
        reader = BytesIOReader(data, len(data))

    def test_construct_negative_offset_raises(self):
        with self.assertRaises(ValueError):
            BytesIOReader(b"hello", -1)

    def test_construct_offset_past_end_raises(self):
        with self.assertRaises(ValueError):
            BytesIOReader(b"hello", 6)

    def test_construct_offset_way_past_end_raises(self):
        with self.assertRaises(ValueError):
            BytesIOReader(b"hello", 100)

    def test_construct_empty_buffer_zero_offset(self):
        reader = BytesIOReader(b"", 0)

    def test_construct_empty_buffer_nonzero_offset_raises(self):
        with self.assertRaises(ValueError):
            BytesIOReader(b"", 1)
