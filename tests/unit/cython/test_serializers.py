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
Tests for Cython-optimized serializers (cassandra.serializers).

Verifies byte-for-byte equivalence with the Python-level cqltype.serialize()
implementations, plus correct error behavior for edge cases.
"""

import math
import struct
import unittest

from tests.unit.cython.utils import cythontest

from cassandra.cython_deps import HAVE_CYTHON

try:
    from tests import VERIFY_CYTHON
except ImportError:
    VERIFY_CYTHON = False

from cassandra.cqltypes import (
    FloatType,
    DoubleType,
    Int32Type,
    VectorType,
    UTF8Type,
    LongType,
    BooleanType,
)

# Import serializers only if Cython is available (compiled .so present).
# When VERIFY_CYTHON is set (CI mode), let ImportError propagate so build
# failures are not silently swallowed.
if HAVE_CYTHON or VERIFY_CYTHON:
    from cassandra.serializers import (
        Serializer,
        SerFloatType,
        SerDoubleType,
        SerInt32Type,
        SerVectorType,
        GenericSerializer,
        find_serializer,
        make_serializers,
    )

# Protocol version used in tests (value doesn't affect scalar serialization)
PROTO = 4


def _make_vector_type(subtype, size):
    """Create a VectorType parameterized with the given subtype class and size."""
    return VectorType.apply_parameters([subtype, size], None)


# ---------------------------------------------------------------------------
# Scalar serializer equivalence tests
# ---------------------------------------------------------------------------


@cythontest
class TestSerFloatTypeEquivalence(unittest.TestCase):
    """Verify SerFloatType produces identical bytes to FloatType.serialize()."""

    def setUp(self):
        self.ser = SerFloatType(FloatType)

    def _assert_equiv(self, value):
        cython_bytes = self.ser.serialize(value, PROTO)
        python_bytes = FloatType.serialize(value, PROTO)
        self.assertEqual(cython_bytes, python_bytes, "Mismatch for value %r" % value)

    def test_zero(self):
        self._assert_equiv(0.0)

    def test_negative_zero(self):
        self._assert_equiv(-0.0)

    def test_positive_values(self):
        for val in [1.0, 0.5, 3.14, 100.0, 1e10]:
            self._assert_equiv(val)

    def test_negative_values(self):
        for val in [-1.0, -0.5, -3.14, -100.0, -1e10]:
            self._assert_equiv(val)

    def test_flt_max(self):
        flt_max = 3.4028234663852886e38
        self._assert_equiv(flt_max)
        self._assert_equiv(-flt_max)

    def test_flt_max_rounding_boundary(self):
        """Values slightly above FLT_MAX that round down should be accepted.

        3.4028235e38 is above FLT_MAX (3.4028234663852886e38) but
        struct.pack('>f', 3.4028235e38) rounds it down to FLT_MAX.
        The Cython serializer must accept the same inputs.
        """
        self._assert_equiv(3.4028235e38)
        self._assert_equiv(-3.4028235e38)

    def test_subnormal_values(self):
        """Subnormal (denormalized) floats should serialize correctly."""
        self._assert_equiv(1e-45)
        self._assert_equiv(-1e-45)
        self._assert_equiv(1.4e-45)  # smallest positive float32

    def test_inf(self):
        self._assert_equiv(float("inf"))

    def test_neg_inf(self):
        self._assert_equiv(float("-inf"))

    def test_nan(self):
        """NaN bytes must match (NaN != NaN, so compare bytes directly)."""
        cython_bytes = self.ser.serialize(float("nan"), PROTO)
        python_bytes = FloatType.serialize(float("nan"), PROTO)
        self.assertEqual(cython_bytes, python_bytes)

    def test_overflow_positive(self):
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize(1e40, PROTO)

    def test_overflow_negative(self):
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize(-1e40, PROTO)

    def test_overflow_dbl_max(self):
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize(1.7976931348623157e308, PROTO)

    def test_type_error_string(self):
        with self.assertRaises(TypeError):
            self.ser.serialize("not a float", PROTO)

    def test_type_error_none(self):
        with self.assertRaises(TypeError):
            self.ser.serialize(None, PROTO)


@cythontest
class TestSerDoubleTypeEquivalence(unittest.TestCase):
    """Verify SerDoubleType produces identical bytes to DoubleType.serialize()."""

    def setUp(self):
        self.ser = SerDoubleType(DoubleType)

    def _assert_equiv(self, value):
        cython_bytes = self.ser.serialize(value, PROTO)
        python_bytes = DoubleType.serialize(value, PROTO)
        self.assertEqual(cython_bytes, python_bytes, "Mismatch for value %r" % value)

    def test_zero(self):
        self._assert_equiv(0.0)

    def test_negative_zero(self):
        self._assert_equiv(-0.0)

    def test_normal_values(self):
        for val in [1.0, -1.0, 3.14, -3.14, 1e100, -1e100, 1e-100]:
            self._assert_equiv(val)

    def test_dbl_max(self):
        self._assert_equiv(1.7976931348623157e308)
        self._assert_equiv(-1.7976931348623157e308)

    def test_inf(self):
        self._assert_equiv(float("inf"))
        self._assert_equiv(float("-inf"))

    def test_nan(self):
        cython_bytes = self.ser.serialize(float("nan"), PROTO)
        python_bytes = DoubleType.serialize(float("nan"), PROTO)
        self.assertEqual(cython_bytes, python_bytes)

    def test_type_error_string(self):
        with self.assertRaises(TypeError):
            self.ser.serialize("not a double", PROTO)

    def test_type_error_none(self):
        with self.assertRaises(TypeError):
            self.ser.serialize(None, PROTO)


@cythontest
class TestSerInt32TypeEquivalence(unittest.TestCase):
    """Verify SerInt32Type produces identical bytes to Int32Type.serialize()."""

    def setUp(self):
        self.ser = SerInt32Type(Int32Type)

    def _assert_equiv(self, value):
        cython_bytes = self.ser.serialize(value, PROTO)
        python_bytes = Int32Type.serialize(value, PROTO)
        self.assertEqual(cython_bytes, python_bytes, "Mismatch for value %r" % value)

    def test_zero(self):
        self._assert_equiv(0)

    def test_positive_values(self):
        for val in [1, 42, 127, 255, 32767, 65535]:
            self._assert_equiv(val)

    def test_negative_values(self):
        for val in [-1, -42, -128, -32768]:
            self._assert_equiv(val)

    def test_int32_max(self):
        self._assert_equiv(2147483647)

    def test_int32_min(self):
        self._assert_equiv(-2147483648)

    def test_overflow_positive(self):
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize(2147483648, PROTO)

    def test_overflow_negative(self):
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize(-2147483649, PROTO)

    def test_overflow_large_python_int(self):
        """Python ints have arbitrary precision; must still reject out-of-range."""
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize(2**100, PROTO)

    def test_overflow_large_negative_python_int(self):
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize(-(2**100), PROTO)

    def test_type_error_string(self):
        with self.assertRaises(TypeError):
            self.ser.serialize("not an int", PROTO)

    def test_type_error_none(self):
        with self.assertRaises(TypeError):
            self.ser.serialize(None, PROTO)

    def test_index_protocol(self):
        """Objects implementing __index__ should be accepted, like struct.pack."""

        class MyInt:
            def __index__(self):
                return 42

        cython_bytes = self.ser.serialize(MyInt(), PROTO)
        python_bytes = Int32Type.serialize(42, PROTO)
        self.assertEqual(cython_bytes, python_bytes)


# ---------------------------------------------------------------------------
# VectorType serializer equivalence tests
# ---------------------------------------------------------------------------


@cythontest
class TestSerVectorTypeFloat(unittest.TestCase):
    """Verify SerVectorType float fast-path matches VectorType.serialize()."""

    def setUp(self):
        self.vec_type = _make_vector_type(FloatType, 4)
        self.ser = SerVectorType(self.vec_type)

    def _assert_equiv(self, values):
        cython_bytes = self.ser.serialize(values, PROTO)
        python_bytes = self.vec_type.serialize(values, PROTO)
        self.assertEqual(
            cython_bytes, python_bytes, "Mismatch for values %r" % (values,)
        )

    def test_basic(self):
        self._assert_equiv([1.0, 2.0, 3.0, 4.0])

    def test_zeros(self):
        self._assert_equiv([0.0, 0.0, 0.0, 0.0])

    def test_negative(self):
        self._assert_equiv([-1.0, -2.5, -0.001, -100.0])

    def test_mixed_special(self):
        self._assert_equiv([float("inf"), float("-inf"), 0.0, -0.0])

    def test_nan_element(self):
        """NaN in vector should serialize identically."""
        cython_bytes = self.ser.serialize([1.0, float("nan"), 3.0, 4.0], PROTO)
        python_bytes = self.vec_type.serialize([1.0, float("nan"), 3.0, 4.0], PROTO)
        self.assertEqual(cython_bytes, python_bytes)

    def test_element_overflow(self):
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize([1.0, 1e40, 3.0, 4.0], PROTO)

    def test_wrong_length_short(self):
        with self.assertRaises(ValueError):
            self.ser.serialize([1.0, 2.0], PROTO)

    def test_wrong_length_long(self):
        with self.assertRaises(ValueError):
            self.ser.serialize([1.0, 2.0, 3.0, 4.0, 5.0], PROTO)

    def test_empty_list_for_nonempty_vector(self):
        with self.assertRaises(ValueError):
            self.ser.serialize([], PROTO)


@cythontest
class TestSerVectorTypeDouble(unittest.TestCase):
    """Verify SerVectorType double fast-path matches VectorType.serialize()."""

    def setUp(self):
        self.vec_type = _make_vector_type(DoubleType, 3)
        self.ser = SerVectorType(self.vec_type)

    def _assert_equiv(self, values):
        cython_bytes = self.ser.serialize(values, PROTO)
        python_bytes = self.vec_type.serialize(values, PROTO)
        self.assertEqual(
            cython_bytes, python_bytes, "Mismatch for values %r" % (values,)
        )

    def test_basic(self):
        self._assert_equiv([1.0, 2.0, 3.0])

    def test_large_values(self):
        self._assert_equiv([1e100, -1e100, 1e-100])

    def test_special(self):
        self._assert_equiv([float("inf"), float("-inf"), 0.0])


@cythontest
class TestSerVectorTypeInt32(unittest.TestCase):
    """Verify SerVectorType int32 fast-path matches VectorType.serialize()."""

    def setUp(self):
        self.vec_type = _make_vector_type(Int32Type, 3)
        self.ser = SerVectorType(self.vec_type)

    def _assert_equiv(self, values):
        cython_bytes = self.ser.serialize(values, PROTO)
        python_bytes = self.vec_type.serialize(values, PROTO)
        self.assertEqual(
            cython_bytes, python_bytes, "Mismatch for values %r" % (values,)
        )

    def test_basic(self):
        self._assert_equiv([1, 2, 3])

    def test_boundaries(self):
        self._assert_equiv([2147483647, -2147483648, 0])

    def test_element_overflow(self):
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize([1, 2147483648, 3], PROTO)

    def test_element_overflow_negative(self):
        with self.assertRaises((OverflowError, struct.error)):
            self.ser.serialize([1, -2147483649, 3], PROTO)

    def test_vector_index_protocol(self):
        """Objects implementing __index__ in vector elements should be accepted."""

        class MyInt:
            def __index__(self):
                return 42

        cython_bytes = self.ser.serialize([MyInt(), MyInt(), MyInt()], PROTO)
        python_bytes = self.vec_type.serialize([42, 42, 42], PROTO)
        self.assertEqual(cython_bytes, python_bytes)


@cythontest
class TestSerVectorTypeGenericFallback(unittest.TestCase):
    """Verify SerVectorType generic fallback matches VectorType.serialize()."""

    def test_utf8_vector(self):
        vec_type = _make_vector_type(UTF8Type, 3)
        ser = SerVectorType(vec_type)
        values = ["hello", "world", "test"]
        cython_bytes = ser.serialize(values, PROTO)
        python_bytes = vec_type.serialize(values, PROTO)
        self.assertEqual(cython_bytes, python_bytes)

    def test_boolean_vector(self):
        vec_type = _make_vector_type(BooleanType, 4)
        ser = SerVectorType(vec_type)
        values = [True, False, True, False]
        cython_bytes = ser.serialize(values, PROTO)
        python_bytes = vec_type.serialize(values, PROTO)
        self.assertEqual(cython_bytes, python_bytes)


@cythontest
class TestSerVectorTypeHighDimensional(unittest.TestCase):
    """Test with realistic high-dimensional vectors (embedding use case)."""

    def test_float_1536_dim(self):
        """1536-dim float vector (typical for embedding models)."""
        vec_type = _make_vector_type(FloatType, 1536)
        ser = SerVectorType(vec_type)
        values = [float(i) / 1536.0 for i in range(1536)]
        cython_bytes = ser.serialize(values, PROTO)
        python_bytes = vec_type.serialize(values, PROTO)
        self.assertEqual(cython_bytes, python_bytes)
        self.assertEqual(len(cython_bytes), 1536 * 4)

    def test_double_768_dim(self):
        vec_type = _make_vector_type(DoubleType, 768)
        ser = SerVectorType(vec_type)
        values = [float(i) / 768.0 for i in range(768)]
        cython_bytes = ser.serialize(values, PROTO)
        python_bytes = vec_type.serialize(values, PROTO)
        self.assertEqual(cython_bytes, python_bytes)
        self.assertEqual(len(cython_bytes), 768 * 8)


# ---------------------------------------------------------------------------
# Round-trip tests (serialize with Cython, deserialize with Python)
# ---------------------------------------------------------------------------


@cythontest
class TestSerializerRoundTrip(unittest.TestCase):
    """Serialize with Cython, deserialize with Python cqltype.deserialize()."""

    def test_float_round_trip(self):
        ser = SerFloatType(FloatType)
        for val in [0.0, 1.0, -1.0, 3.14, float("inf"), float("-inf")]:
            serialized = ser.serialize(val, PROTO)
            deserialized = FloatType.deserialize(serialized, PROTO)
            if math.isinf(val):
                self.assertEqual(val, deserialized)
            else:
                self.assertAlmostEqual(val, deserialized, places=5)

    def test_double_round_trip(self):
        ser = SerDoubleType(DoubleType)
        for val in [0.0, 1.0, -1.0, 3.141592653589793, 1e100, float("inf")]:
            serialized = ser.serialize(val, PROTO)
            deserialized = DoubleType.deserialize(serialized, PROTO)
            self.assertEqual(val, deserialized)

    def test_int32_round_trip(self):
        ser = SerInt32Type(Int32Type)
        for val in [0, 1, -1, 2147483647, -2147483648, 42]:
            serialized = ser.serialize(val, PROTO)
            deserialized = Int32Type.deserialize(serialized, PROTO)
            self.assertEqual(val, deserialized)

    def test_float_vector_round_trip(self):
        vec_type = _make_vector_type(FloatType, 4)
        ser = SerVectorType(vec_type)
        values = [1.5, -2.5, 3.14, 0.0]
        serialized = ser.serialize(values, PROTO)
        deserialized = vec_type.deserialize(serialized, PROTO)
        for orig, deser in zip(values, deserialized):
            self.assertAlmostEqual(orig, deser, places=5)

    def test_int32_vector_round_trip(self):
        vec_type = _make_vector_type(Int32Type, 3)
        ser = SerVectorType(vec_type)
        values = [2147483647, -2147483648, 0]
        serialized = ser.serialize(values, PROTO)
        deserialized = vec_type.deserialize(serialized, PROTO)
        self.assertEqual(list(deserialized), values)


@cythontest
class TestSerVectorTypeIterableInput(unittest.TestCase):
    """Verify vector serializer accepts non-subscriptable iterables.

    The Python VectorType.serialize() only requires len() + iteration,
    so the Cython path must accept the same inputs after normalization.
    """

    def test_iterable_with_len_no_getitem(self):
        """A custom iterable with __len__ + __iter__ but no __getitem__."""

        class IterableOnly:
            def __init__(self, data):
                self._data = list(data)

            def __len__(self):
                return len(self._data)

            def __iter__(self):
                return iter(self._data)

        vec_type = _make_vector_type(FloatType, 3)
        ser = SerVectorType(vec_type)
        values = IterableOnly([1.0, 2.0, 3.0])
        cython_bytes = ser.serialize(values, PROTO)
        python_bytes = vec_type.serialize([1.0, 2.0, 3.0], PROTO)
        self.assertEqual(cython_bytes, python_bytes)

    def test_iterable_int32_vector(self):
        """Non-subscriptable iterable with int32 fast-path."""

        class IterableOnly:
            def __init__(self, data):
                self._data = list(data)

            def __len__(self):
                return len(self._data)

            def __iter__(self):
                return iter(self._data)

        vec_type = _make_vector_type(Int32Type, 3)
        ser = SerVectorType(vec_type)
        values = IterableOnly([1, 2, 3])
        cython_bytes = ser.serialize(values, PROTO)
        python_bytes = vec_type.serialize([1, 2, 3], PROTO)
        self.assertEqual(cython_bytes, python_bytes)

    def test_tuple_input(self):
        """Tuple input should work (already subscriptable, but verify)."""
        vec_type = _make_vector_type(Int32Type, 3)
        ser = SerVectorType(vec_type)
        cython_bytes = ser.serialize((1, 2, 3), PROTO)
        python_bytes = vec_type.serialize([1, 2, 3], PROTO)
        self.assertEqual(cython_bytes, python_bytes)


# ---------------------------------------------------------------------------
# Factory function tests
# ---------------------------------------------------------------------------


@cythontest
class TestFindSerializer(unittest.TestCase):
    """Test find_serializer() returns correct serializer types."""

    def test_float_type(self):
        ser = find_serializer(FloatType)
        self.assertIsInstance(ser, SerFloatType)

    def test_double_type(self):
        ser = find_serializer(DoubleType)
        self.assertIsInstance(ser, SerDoubleType)

    def test_int32_type(self):
        ser = find_serializer(Int32Type)
        self.assertIsInstance(ser, SerInt32Type)

    def test_vector_type(self):
        vec_type = _make_vector_type(FloatType, 3)
        ser = find_serializer(vec_type)
        self.assertIsInstance(ser, SerVectorType)

    def test_unknown_type_gets_generic(self):
        ser = find_serializer(UTF8Type)
        self.assertIsInstance(ser, GenericSerializer)

    def test_generic_delegates_to_python(self):
        ser = find_serializer(LongType)
        self.assertIsInstance(ser, GenericSerializer)
        result = ser.serialize(42, PROTO)
        expected = LongType.serialize(42, PROTO)
        self.assertEqual(result, expected)

    def test_unparameterized_vector_type_gets_generic(self):
        """Un-parameterized VectorType (base class) should not crash."""
        ser = find_serializer(VectorType)
        self.assertIsInstance(ser, GenericSerializer)


@cythontest
class TestMakeSerializers(unittest.TestCase):
    """Test make_serializers() batch factory."""

    def test_basic(self):
        types = [FloatType, DoubleType, Int32Type, UTF8Type]
        serializers = make_serializers(types)
        self.assertEqual(len(serializers), 4)
        self.assertIsInstance(serializers[0], SerFloatType)
        self.assertIsInstance(serializers[1], SerDoubleType)
        self.assertIsInstance(serializers[2], SerInt32Type)
        self.assertIsInstance(serializers[3], GenericSerializer)

    def test_empty(self):
        serializers = make_serializers([])
        self.assertEqual(len(serializers), 0)

    def test_with_vector_type(self):
        vec_type = _make_vector_type(FloatType, 3)
        serializers = make_serializers([vec_type, Int32Type])
        self.assertEqual(len(serializers), 2)
        self.assertIsInstance(serializers[0], SerVectorType)
        self.assertIsInstance(serializers[1], SerInt32Type)
