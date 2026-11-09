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

"""Cluster.register_user_type() is per-cluster, so the UDT class cache must key on the mapping."""

import io
import struct
import unittest
from binascii import hexlify

from cassandra.cqltypes import (
    Int32Type,
    UserType,
    lookup_casstype,
    mkUnrecognizedType,
)
from cassandra.protocol import ResultMessage


# A keyspace/UDT name that cannot collide with anything the driver itself
# uses.  The leading underscores also keep the types created below out of
# the global _casstypes/_cqltypes registries.
_KEYSPACE = '_udt_mapping_test_ks'
_UDT_NAME = '_udt_mapping_test_type'
_FIELD_NAMES = ('field',)


class _MappedClassA(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _MappedClassB(object):
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def _write_string(s):
    encoded = s.encode('utf-8')
    return struct.pack('>H', len(encoded)) + encoded


def _udt_option(keyspace, udt_name, field_names=_FIELD_NAMES):
    """Encode a USER type option (option id 48) as it arrives on the wire."""
    out = struct.pack('>H', 48)
    out += _write_string(keyspace)
    out += _write_string(udt_name)
    out += struct.pack('>H', len(field_names))
    for field_name in field_names:
        out += _write_string(field_name)
        out += struct.pack('>H', 9)  # Int32Type
    return out


def _udt_value(value):
    """Encode a UDT value (a tuple of one int field) the way it arrives."""
    return struct.pack('>i', 4) + struct.pack('>i', value)  # length, int


def _read_udt_type(user_type_map):
    """Read a UDT type the way ResultMessage.read_type() does."""
    return ResultMessage.read_type(
        io.BytesIO(_udt_option(_KEYSPACE, _UDT_NAME)), user_type_map)


class UdtMappedClassTest(unittest.TestCase):

    def setUp(self):
        UserType.evict_udt_class(_KEYSPACE, _UDT_NAME)

    def tearDown(self):
        UserType.evict_udt_class(_KEYSPACE, _UDT_NAME)

    # -- the mapping is part of the class identity -------------------------

    def test_different_mappings_give_different_classes(self):
        cls_a = UserType.make_udt_class(_KEYSPACE, _UDT_NAME, _FIELD_NAMES,
                                        (Int32Type,), _MappedClassA)
        cls_b = UserType.make_udt_class(_KEYSPACE, _UDT_NAME, _FIELD_NAMES,
                                        (Int32Type,), _MappedClassB)
        cls_none = UserType.make_udt_class(_KEYSPACE, _UDT_NAME, _FIELD_NAMES,
                                           (Int32Type,))

        self.assertIsNot(cls_a, cls_b)
        self.assertIsNot(cls_a, cls_none)
        self.assertIsNot(cls_b, cls_none)

        self.assertIs(cls_a.mapped_class, _MappedClassA)
        self.assertIs(cls_b.mapped_class, _MappedClassB)
        self.assertIsNone(cls_none.mapped_class)

    def test_same_mapping_reuses_the_cached_class(self):
        cls_a = UserType.make_udt_class(_KEYSPACE, _UDT_NAME, _FIELD_NAMES,
                                        (Int32Type,), _MappedClassA)
        cls_a2 = UserType.make_udt_class(_KEYSPACE, _UDT_NAME, _FIELD_NAMES,
                                         (Int32Type,), _MappedClassA)
        self.assertIs(cls_a, cls_a2)

    def test_evict_udt_class_removes_every_mapping(self):
        mappings = (_MappedClassA, _MappedClassB, None)
        originals = [UserType.make_udt_class(_KEYSPACE, _UDT_NAME, _FIELD_NAMES,
                                             (Int32Type,), m) for m in mappings]

        UserType.evict_udt_class(_KEYSPACE, _UDT_NAME)

        for mapped_class, original in zip(mappings, originals):
            self.assertIsNot(
                original,
                UserType.make_udt_class(_KEYSPACE, _UDT_NAME, _FIELD_NAMES,
                                        (Int32Type,), mapped_class))

    # -- through the protocol path -----------------------------------------

    def test_unregistered_cluster_does_not_clobber_a_registered_mapping(self):
        """A cluster without a registration must not clear another one's."""
        registered = {'_udt_mapping_test_ks': {'_udt_mapping_test_type': _MappedClassA}}

        cls_registered = _read_udt_type(registered)
        self.assertIs(cls_registered.mapped_class, _MappedClassA)

        # a second cluster, in the same process, that never registered the UDT
        cls_unregistered = _read_udt_type({})

        self.assertIsNot(cls_registered, cls_unregistered)
        self.assertIsNone(cls_unregistered.mapped_class)
        # ... and the first cluster keeps its own mapping
        self.assertIs(cls_registered.mapped_class, _MappedClassA)

    def test_clusters_with_different_mappings_are_independent(self):
        map_a = {'_udt_mapping_test_ks': {'_udt_mapping_test_type': _MappedClassA}}
        map_b = {'_udt_mapping_test_ks': {'_udt_mapping_test_type': _MappedClassB}}

        cls_a = _read_udt_type(map_a)
        cls_b = _read_udt_type(map_b)

        self.assertIsNot(cls_a, cls_b)
        self.assertIs(cls_a.mapped_class, _MappedClassA)
        self.assertIs(cls_b.mapped_class, _MappedClassB)

    def test_deserialization_uses_the_registered_class(self):
        cls_a = _read_udt_type(
            {'_udt_mapping_test_ks': {'_udt_mapping_test_type': _MappedClassA}})
        cls_b = _read_udt_type({})

        value_a = cls_a.deserialize_safe(_udt_value(5), 5)
        value_b = cls_b.deserialize_safe(_udt_value(5), 5)

        self.assertIsInstance(value_a, _MappedClassA)
        self.assertEqual(value_a.kwargs, {'field': 5})
        # the unregistered cluster gets the plain namedtuple
        self.assertNotIsInstance(value_b, _MappedClassA)
        self.assertEqual(tuple(value_b), (5,))

    # -- through the cass-type-string path ---------------------------------

    def test_apply_parameters_resolves_the_mapping(self):
        """UserType.apply_parameters() must resolve the mapping too.

        This is the path used when a UDT is described by a Cassandra type
        string rather than by a USER type option.
        """
        keyspace_type = mkUnrecognizedType(_KEYSPACE)
        udt_name_type = mkUnrecognizedType(hexlify(_UDT_NAME.encode()).decode())
        subtypes = [keyspace_type, udt_name_type, Int32Type]
        names = [None, None, hexlify(_FIELD_NAMES[0].encode()).decode()]

        user_type_map = {'_udt_mapping_test_ks': {'_udt_mapping_test_type': _MappedClassA}}

        cls = UserType.apply_parameters(subtypes, names, user_type_map)
        self.assertIs(cls.mapped_class, _MappedClassA)

        # and the same UDT without a mapping is a different class
        cls_unmapped = UserType.apply_parameters(subtypes, names, None)
        self.assertIsNot(cls, cls_unmapped)
        self.assertIsNone(cls_unmapped.mapped_class)

    def test_lookup_casstype_resolves_the_mapping(self):
        """lookup_casstype() must pass the user_type_map down."""
        hex_name = hexlify(_UDT_NAME.encode()).decode()
        hex_field = hexlify(_FIELD_NAMES[0].encode()).decode()
        type_string = ('org.apache.cassandra.db.marshal.UserType({ks},{name},'
                       '{field}:org.apache.cassandra.db.marshal.Int32Type)'
                       .format(ks=_KEYSPACE, name=hex_name, field=hex_field))

        user_type_map = {'_udt_mapping_test_ks': {'_udt_mapping_test_type': _MappedClassA}}

        cls = lookup_casstype(type_string, user_type_map)
        self.assertIs(cls.mapped_class, _MappedClassA)

        cls_unmapped = lookup_casstype(type_string)
        self.assertIsNone(cls_unmapped.mapped_class)
        self.assertIsNot(cls, cls_unmapped)


if __name__ == '__main__':
    unittest.main()
