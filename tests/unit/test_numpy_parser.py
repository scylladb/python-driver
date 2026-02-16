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

import struct
import unittest
from unittest.mock import Mock

try:
    import numpy as np
    from cassandra.numpy_parser import NumpyParser
    from cassandra.bytesio import BytesIOReader
    from cassandra.parsing import ParseDesc
    from cassandra.deserializers import obj_array
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
            f'VectorType({vector_size})',
            (cqltypes.VectorType,),
            {'vector_size': vector_size, 'subtype': subtype}
        )

    def _serialize_vectors(self, vectors, format_char):
        """Serialize a list of vectors using struct.pack"""
        buffer = bytearray()
        # Write row count
        buffer.extend(struct.pack('>i', len(vectors)))
        # Write each vector
        for vector in vectors:
            # Write byte size of vector (doesn't include size prefix in CQL)
            byte_size = len(vector) * struct.calcsize(f'>{format_char}')
            buffer.extend(struct.pack('>i', byte_size))
            # Write vector elements
            buffer.extend(struct.pack(f'>{len(vector)}{format_char}', *vector))
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
        serialized = self._serialize_vectors(vectors, 'f')
        
        # Parse with NumpyParser
        parser = NumpyParser()
        reader = BytesIOReader(serialized)
        
        desc = ParseDesc(
            colnames=['vec'],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5
        )
        
        result = parser.parse_rows(reader, desc)
        
        # Verify result structure
        self.assertIn('vec', result)
        arr = result['vec']
        
        # Verify it's a 2D array with correct shape
        self.assertEqual(arr.ndim, 2)
        self.assertEqual(arr.shape, (3, 4))
        
        # Verify the data
        expected = np.array(vectors, dtype='<f4')  # little-endian after conversion
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
        
        serialized = self._serialize_vectors(vectors, 'd')
        
        parser = NumpyParser()
        reader = BytesIOReader(serialized)
        
        desc = ParseDesc(
            colnames=['embedding'],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5
        )
        
        result = parser.parse_rows(reader, desc)
        
        arr = result['embedding']
        self.assertEqual(arr.shape, (2, 3))
        
        expected = np.array(vectors, dtype='<f8')
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
        
        serialized = self._serialize_vectors(vectors, 'i')
        
        parser = NumpyParser()
        reader = BytesIOReader(serialized)
        
        desc = ParseDesc(
            colnames=['features'],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5
        )
        
        result = parser.parse_rows(reader, desc)
        
        arr = result['features']
        self.assertEqual(arr.shape, (2, 128))
        
        expected = np.array(vectors, dtype='<i4')
        np.testing.assert_array_equal(arr, expected)

    def test_vector_int64_2d_array(self):
        """Test that VectorType<bigint> creates and populates a 2D NumPy array"""
        vector_size = 5
        vector_type = self._create_vector_type(cqltypes.LongType, vector_size)
        
        vectors = [
            [100, 200, 300, 400, 500],
            [600, 700, 800, 900, 1000],
        ]
        
        serialized = self._serialize_vectors(vectors, 'q')
        
        parser = NumpyParser()
        reader = BytesIOReader(serialized)
        
        desc = ParseDesc(
            colnames=['ids'],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5
        )
        
        result = parser.parse_rows(reader, desc)
        
        arr = result['ids']
        self.assertEqual(arr.shape, (2, 5))
        
        expected = np.array(vectors, dtype='<i8')
        np.testing.assert_array_equal(arr, expected)

    def test_vector_int16_2d_array(self):
        """Test that VectorType<smallint> creates and populates a 2D NumPy array"""
        vector_size = 8
        vector_type = self._create_vector_type(cqltypes.ShortType, vector_size)
        
        vectors = [
            [1, 2, 3, 4, 5, 6, 7, 8],
            [9, 10, 11, 12, 13, 14, 15, 16],
        ]
        
        serialized = self._serialize_vectors(vectors, 'h')
        
        parser = NumpyParser()
        reader = BytesIOReader(serialized)
        
        desc = ParseDesc(
            colnames=['small_vec'],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5
        )
        
        result = parser.parse_rows(reader, desc)
        
        arr = result['small_vec']
        self.assertEqual(arr.shape, (2, 8))
        
        expected = np.array(vectors, dtype='<i2')
        np.testing.assert_array_equal(arr, expected)

    def test_mixed_columns_with_vectors(self):
        """Test parsing multiple columns including VectorType"""
        vector_type = self._create_vector_type(cqltypes.FloatType, 3)
        
        # Serialize: int32 column, vector column
        buffer = bytearray()
        buffer.extend(struct.pack('>i', 2))  # row count
        
        # Row 1: id=1, vec=[1.0, 2.0, 3.0]
        buffer.extend(struct.pack('>i', 4))    # int32 size
        buffer.extend(struct.pack('>i', 1))    # id value
        buffer.extend(struct.pack('>i', 12))   # vector size (3 floats)
        buffer.extend(struct.pack('>3f', 1.0, 2.0, 3.0))
        
        # Row 2: id=2, vec=[4.0, 5.0, 6.0]
        buffer.extend(struct.pack('>i', 4))
        buffer.extend(struct.pack('>i', 2))
        buffer.extend(struct.pack('>i', 12))
        buffer.extend(struct.pack('>3f', 4.0, 5.0, 6.0))
        
        parser = NumpyParser()
        reader = BytesIOReader(bytes(buffer))
        
        desc = ParseDesc(
            colnames=['id', 'vec'],
            coltypes=[cqltypes.Int32Type, vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None, None]),
            protocol_version=5
        )
        
        result = parser.parse_rows(reader, desc)
        
        # Verify id column (1D array)
        self.assertEqual(result['id'].shape, (2,))
        np.testing.assert_array_equal(result['id'], np.array([1, 2], dtype='<i4'))
        
        # Verify vec column (2D array)
        self.assertEqual(result['vec'].shape, (2, 3))
        expected_vecs = np.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]], dtype='<f4')
        np.testing.assert_array_almost_equal(result['vec'], expected_vecs)

    def test_large_vector_dimensions(self):
        """Test VectorType with large dimensions (e.g., 384 for embeddings)"""
        vector_size = 384
        vector_type = self._create_vector_type(cqltypes.FloatType, vector_size)
        
        # Create one row with a 384-dimensional vector
        vectors = [[float(i) for i in range(384)]]
        
        serialized = self._serialize_vectors(vectors, 'f')
        
        parser = NumpyParser()
        reader = BytesIOReader(serialized)
        
        desc = ParseDesc(
            colnames=['embedding'],
            coltypes=[vector_type],
            column_encryption_policy=None,
            coldescs=None,
            deserializers=obj_array([None]),
            protocol_version=5
        )
        
        result = parser.parse_rows(reader, desc)
        
        arr = result['embedding']
        self.assertEqual(arr.shape, (1, 384))
        
        expected = np.array(vectors, dtype='<f4')
        np.testing.assert_array_almost_equal(arr, expected)


if __name__ == '__main__':
    unittest.main()
