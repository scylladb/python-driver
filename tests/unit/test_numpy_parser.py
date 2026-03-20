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

import struct
import unittest

try:
    import numpy as np
    from cassandra.numpy_parser import NumpyParser
    from cassandra.bytesio import BytesIOReader
    from cassandra.parsing import ParseDesc
    from cassandra.deserializers import obj_array, make_deserializers

    HAVE_NUMPY = True
except ImportError:
    HAVE_NUMPY = False

from cassandra import cqltypes


@unittest.skipUnless(HAVE_NUMPY, "NumPy not available")
class TestNumpyParserVectorType(unittest.TestCase):
    """Tests for VectorType support in NumpyParser"""

    def _create_vector_type(self, subtype, vector_size):
        """Helper to create a VectorType class"""
        return type(
            f"VectorType({vector_size})",
            (cqltypes.VectorType,),
            {"vector_size": vector_size, "subtype": subtype},
        )

    def _serialize_vectors(self, vectors, format_char):
        """Serialize a list of vectors using struct.pack"""
        buffer = bytearray()
        # Write row count
        buffer.extend(struct.pack(">i", len(vectors)))
        # Write each vector
        for vector in vectors:
            # Write byte size of vector (doesn't include size prefix in CQL)
            byte_size = len(vector) * struct.calcsize(f">{format_char}")
            buffer.extend(struct.pack(">i", byte_size))
            # Write vector elements
            buffer.extend(struct.pack(f">{len(vector)}{format_char}", *vector))
        return bytes(buffer)

    def _serialize_vectors_via_vector_type(self, vector_type, vectors, protocol_version=5):
        """Serialize a list of vectors using the actual production
        cassandra.cqltypes.VectorType.serialize(), instead of hand-rolling
        wire bytes with struct.pack.

        This is important: the struct.pack-based helper above bakes in the
        assumption that every subtype has a fixed per-element width on the
        wire. That assumption is wrong for subtypes whose serial_size() is
        None (e.g. ShortType/smallint), which are vint length-prefixed per
        element instead. Using the real serializer as ground truth is what
        catches a mismatch between the numpy_parser fast-path dtype table
        and the actual wire format -- which is exactly the bug class this
        helper guards against.
        """
        buffer = bytearray()
        buffer.extend(struct.pack('>i', len(vectors)))
        for vector in vectors:
            payload = vector_type.serialize(list(vector), protocol_version)
            buffer.extend(struct.pack('>i', len(payload)))
            buffer.extend(payload)
        return bytes(buffer)

    def test_vector_float_2d_array(self):
        """Test that VectorType<float> creates and populates a 2D NumPy array"""
        vector_size = 4
        vector_type = self._create_vector_type(cqltypes.FloatType, vector_size)

        # Create test data: 3 rows of 4-dimensional float vectors
        vectors = [
            [1.0, 2.0, 3.0, 4.0],
            [5.0, 6.0, 7.0, 8.0],
            [9.0, 10.0, 11.0, 12.0],
        ]

        # Serialize the data
        serialized = self._serialize_vectors(vectors, "f")

        # Parse with NumpyParser
        parser = NumpyParser()
        reader = BytesIOReader(serialized)

        desc = ParseDesc(
            colnames=["vec"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)

        # Verify result structure
        self.assertIn("vec", result)
        arr = result["vec"]

        # Verify it's a 2D array with correct shape
        self.assertEqual(arr.ndim, 2)
        self.assertEqual(arr.shape, (3, 4))

        # Verify the data
        expected = np.array(vectors, dtype=np.float32)
        np.testing.assert_array_almost_equal(arr, expected)

    def test_vector_double_2d_array(self):
        """Test that VectorType<double> creates and populates a 2D NumPy array"""
        vector_size = 3
        vector_type = self._create_vector_type(cqltypes.DoubleType, vector_size)

        # Create test data: 2 rows of 3-dimensional double vectors
        vectors = [
            [1.5, 2.5, 3.5],
            [4.5, 5.5, 6.5],
        ]

        serialized = self._serialize_vectors(vectors, "d")

        parser = NumpyParser()
        reader = BytesIOReader(serialized)

        desc = ParseDesc(
            colnames=["embedding"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)

        arr = result["embedding"]
        self.assertEqual(arr.shape, (2, 3))

        expected = np.array(vectors, dtype=np.float64)
        np.testing.assert_array_almost_equal(arr, expected)

    def test_vector_int32_2d_array(self):
        """Test that VectorType<int> creates and populates a 2D NumPy array"""
        vector_size = 128
        vector_type = self._create_vector_type(cqltypes.Int32Type, vector_size)

        # Create test data: 2 rows of 128-dimensional int vectors
        vectors = [
            list(range(0, 128)),
            list(range(128, 256)),
        ]

        serialized = self._serialize_vectors(vectors, "i")

        parser = NumpyParser()
        reader = BytesIOReader(serialized)

        desc = ParseDesc(
            colnames=["features"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)

        arr = result["features"]
        self.assertEqual(arr.shape, (2, 128))

        expected = np.array(vectors, dtype=np.int32)
        np.testing.assert_array_equal(arr, expected)

    def test_vector_int64_2d_array(self):
        """Test that VectorType<bigint> creates and populates a 2D NumPy array"""
        vector_size = 5
        vector_type = self._create_vector_type(cqltypes.LongType, vector_size)

        vectors = [
            [100, 200, 300, 400, 500],
            [600, 700, 800, 900, 1000],
        ]

        serialized = self._serialize_vectors(vectors, "q")

        parser = NumpyParser()
        reader = BytesIOReader(serialized)

        desc = ParseDesc(
            colnames=["ids"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)

        arr = result["ids"]
        self.assertEqual(arr.shape, (2, 5))

        expected = np.array(vectors, dtype=np.int64)
        np.testing.assert_array_equal(arr, expected)

    def test_vector_smallint_falls_back_and_round_trips_via_real_serializer(self):
        """Regression test for the ShortType fast-path bug.

        ShortType (smallint) does not override serial_size() in
        cassandra.cqltypes, so Cassandra/Scylla vint-prefixes each element
        of a vector<smallint, N> column instead of encoding it with a fixed
        2-byte stride. If ShortType were (incorrectly) present in
        numpy_parser's fast-path dtype table, this test would fail with a
        ValueError ("received N bytes but array stride is M") because the
        real wire format -- produced here by the actual production
        VectorType.serialize(), not a hand-rolled struct.pack payload --
        does not match the fixed 2-byte-per-element layout the fast path
        would assume. Using the real serializer as ground truth is exactly
        what a hand-rolled struct.pack-based test would fail to catch.

        Instead, ShortType must fall back to the same safe, slower
        object-array path used for other subtypes without a fixed width
        (e.g. UTF8Type), and still round-trip correctly.
        """
        vector_size = 4
        vector_type = self._create_vector_type(cqltypes.ShortType, vector_size)

        vectors = [
            [1, 2, 3, 4],
            [-5, 6, -7, 8],
            [0, 32767, -32768, 100],
        ]

        serialized = self._serialize_vectors_via_vector_type(vector_type, vectors)

        parser = NumpyParser()
        reader = BytesIOReader(serialized)

        desc = ParseDesc(
            colnames=["small_vec"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=make_deserializers([vector_type]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)

        arr = result["small_vec"]

        # ShortType must NOT be in the fast-path dtype table: it should
        # fall back to a 1D object array (a list per row), not a 2D
        # fixed-width numeric array.
        self.assertEqual(arr.ndim, 1)
        self.assertEqual(arr.dtype, np.dtype("O"))
        self.assertEqual(arr.shape, (len(vectors),))

        for i, expected_vector in enumerate(vectors):
            self.assertEqual(list(arr[i]), expected_vector)

    def test_vector_float_round_trips_via_real_serializer(self):
        """Round-trip a fixed-width (float) vector column through the
        actual production VectorType.serialize(), not a hand-rolled
        struct.pack payload, and verify the fast path still reconstructs
        the original values correctly.
        """
        vector_size = 4
        vector_type = self._create_vector_type(cqltypes.FloatType, vector_size)

        vectors = [
            [1.5, 2.5, 3.5, 4.5],
            [-1.0, 0.0, 100.25, -3.75],
        ]

        serialized = self._serialize_vectors_via_vector_type(vector_type, vectors)

        parser = NumpyParser()
        reader = BytesIOReader(serialized)

        desc = ParseDesc(
            colnames=["vec"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=make_deserializers([vector_type]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)
        arr = result["vec"]

        # FloatType has a fixed serial_size(), so it should still use the
        # fast path: a 2D numeric masked array, not an object array.
        self.assertEqual(arr.ndim, 2)
        self.assertNotEqual(arr.dtype, np.dtype("O"))
        self.assertEqual(arr.shape, (len(vectors), vector_size))

        expected = np.array(vectors, dtype=np.float32)
        np.testing.assert_array_almost_equal(arr, expected)

    def test_mixed_columns_with_vectors(self):
        """Test parsing multiple columns including VectorType"""
        vector_type = self._create_vector_type(cqltypes.FloatType, 3)

        # Serialize: int32 column, vector column
        buffer = bytearray()
        buffer.extend(struct.pack(">i", 2))  # row count

        # Row 1: id=1, vec=[1.0, 2.0, 3.0]
        buffer.extend(struct.pack(">i", 4))  # int32 size
        buffer.extend(struct.pack(">i", 1))  # id value
        buffer.extend(struct.pack(">i", 12))  # vector size (3 floats)
        buffer.extend(struct.pack(">3f", 1.0, 2.0, 3.0))

        # Row 2: id=2, vec=[4.0, 5.0, 6.0]
        buffer.extend(struct.pack(">i", 4))
        buffer.extend(struct.pack(">i", 2))
        buffer.extend(struct.pack(">i", 12))
        buffer.extend(struct.pack(">3f", 4.0, 5.0, 6.0))

        parser = NumpyParser()
        reader = BytesIOReader(bytes(buffer))

        desc = ParseDesc(
            colnames=["id", "vec"],
            coltypes=[cqltypes.Int32Type, vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None, None]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)

        # Verify id column (1D array)
        self.assertEqual(result["id"].shape, (2,))
        np.testing.assert_array_equal(result["id"], np.array([1, 2], dtype=np.int32))

        # Verify vec column (2D array)
        self.assertEqual(result["vec"].shape, (2, 3))
        expected_vecs = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype=np.float32)
        np.testing.assert_array_almost_equal(result["vec"], expected_vecs)

    def test_large_vector_dimensions(self):
        """Test VectorType with large dimensions (e.g., 384 for embeddings)"""
        vector_size = 384
        vector_type = self._create_vector_type(cqltypes.FloatType, vector_size)

        # Create one row with a 384-dimensional vector
        vectors = [[float(i) for i in range(384)]]

        serialized = self._serialize_vectors(vectors, "f")

        parser = NumpyParser()
        reader = BytesIOReader(serialized)

        desc = ParseDesc(
            colnames=["embedding"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)

        arr = result["embedding"]
        self.assertEqual(arr.shape, (1, 384))

        expected = np.array(vectors, dtype=np.float32)
        np.testing.assert_array_almost_equal(arr, expected)

    def test_null_vector_sets_mask(self):
        """Test that a NULL vector (size = -1) sets the mask correctly"""
        vector_size = 3
        vector_type = self._create_vector_type(cqltypes.FloatType, vector_size)

        # Serialize: 2 rows, first is a valid vector, second is NULL (size = -1)
        buffer = bytearray()
        buffer.extend(struct.pack(">i", 2))  # row count

        # Row 1: valid vector [1.0, 2.0, 3.0]
        buffer.extend(struct.pack(">i", 12))  # byte size (3 floats * 4 bytes)
        buffer.extend(struct.pack(">3f", 1.0, 2.0, 3.0))

        # Row 2: NULL vector
        buffer.extend(struct.pack(">i", -1))  # -1 signals NULL

        parser = NumpyParser()
        reader = BytesIOReader(bytes(buffer))

        desc = ParseDesc(
            colnames=["vec"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)

        arr = result["vec"]
        self.assertEqual(arr.shape, (2, 3))

        # First row should not be masked
        self.assertFalse(arr.mask[0].any())
        np.testing.assert_array_almost_equal(
            arr[0], np.array([1.0, 2.0, 3.0], dtype=np.float32)
        )

        # Second row should be fully masked (NULL)
        self.assertTrue(arr.mask[1].all())

    def test_null_vector_data_bytes_are_zeroed(self):
        """Test that a NULL vector's underlying .data bytes are zeroed.

        Setting the mask alone is not enough: the row's underlying buffer
        (arr.stride bytes -- the whole vector, not just one scalar) is
        never written to for a NULL value, so it would otherwise retain
        whatever uninitialized heap memory the allocator happened to hand
        back. That memory is directly observable via `.data` or via
        `.filled()`-adjacent access, so unpack_row() must explicitly zero
        it rather than leaving it as an info leak. This matters much more
        for vectors than for scalars: a NULL float[768] embedding leaves
        3072 uninitialized bytes exposed per row instead of just 4.
        """
        vector_size = 8
        vector_type = self._create_vector_type(cqltypes.FloatType, vector_size)

        buffer = bytearray()
        buffer.extend(struct.pack(">i", 2))  # row count

        # Row 1: a valid vector with distinctive non-zero values.
        valid_vector = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        buffer.extend(struct.pack(">i", vector_size * 4))
        buffer.extend(struct.pack(">8f", *valid_vector))

        # Row 2: NULL vector.
        buffer.extend(struct.pack(">i", -1))

        parser = NumpyParser()
        reader = BytesIOReader(bytes(buffer))

        desc = ParseDesc(
            colnames=["vec"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)
        arr = result["vec"]

        self.assertTrue(arr.mask[1].all())

        # `.data` bypasses the mask and exposes the raw underlying buffer.
        null_row_bytes = arr.data[1].tobytes()
        self.assertEqual(null_row_bytes, b"\x00" * len(null_row_bytes))

    def test_unsupported_subtype_falls_back_to_object_array(self):
        """Test that an unsupported vector subtype falls back to an object array"""
        vector_size = 2
        vector_type = self._create_vector_type(cqltypes.UTF8Type, vector_size)

        # For an unsupported subtype, make_array should produce a 1D object
        # array (not a 2D numeric array), and parsing goes through the
        # deserializer/object path instead of the memcpy fast-path.
        from cassandra.numpy_parser import make_array

        arr = make_array(vector_type, 5)
        self.assertEqual(arr.ndim, 1)
        self.assertEqual(arr.dtype, np.dtype("O"))
        self.assertEqual(arr.shape, (5,))

    def test_unsupported_subtype_falls_back_end_to_end_via_parse_rows(self):
        """End-to-end companion to test_unsupported_subtype_falls_back_to_object_array.

        That test only unit-tests make_array() in isolation. This test
        drives the full NumpyParser.parse_rows() entry point (using the
        real VectorType.serialize() as ground truth for the wire bytes),
        to confirm the fallback path also parses and round-trips correctly
        end to end, not just that make_array() picks the right dtype.
        """
        vector_size = 2
        vector_type = self._create_vector_type(cqltypes.UTF8Type, vector_size)

        vectors = [
            ["hello", "world"],
            ["foo", "bar"],
        ]

        serialized = self._serialize_vectors_via_vector_type(vector_type, vectors)

        parser = NumpyParser()
        reader = BytesIOReader(serialized)

        desc = ParseDesc(
            colnames=["tags"],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=make_deserializers([vector_type]),
            protocol_version=5,
        )

        result = parser.parse_rows(reader, desc)
        arr = result["tags"]

        self.assertEqual(arr.ndim, 1)
        self.assertEqual(arr.dtype, np.dtype("O"))
        self.assertEqual(arr.shape, (len(vectors),))

        for i, expected_vector in enumerate(vectors):
            self.assertEqual(list(arr[i]), expected_vector)


if __name__ == "__main__":
    unittest.main()
