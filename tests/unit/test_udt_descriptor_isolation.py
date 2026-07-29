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

import io
import struct
from concurrent.futures import ThreadPoolExecutor

import pytest

from cassandra import type_codes
from cassandra.cqltypes import Int32Type, UserType
from cassandra.protocol import ResultMessage


KEYSPACE = 'udt_descriptor_isolation'
UDT_NAME = 'address'
FIELD_NAMES = ('number',)
FIELD_TYPES = (Int32Type,)


class AddressA:

    def __init__(self, number):
        self.number = number


class AddressB:

    def __init__(self, number):
        self.number = number


class UnhashableMappingMeta(type):
    __hash__ = None


class UnhashableAddress(metaclass=UnhashableMappingMeta):

    def __init__(self, number):
        self.number = number


@pytest.fixture(autouse=True)
def clear_udt_descriptor_variants():
    UserType.evict_udt_class(KEYSPACE, UDT_NAME)
    yield
    UserType.evict_udt_class(KEYSPACE, UDT_NAME)


def _pack_string(value):
    value = value.encode('utf-8')
    return struct.pack('>H', len(value)) + value


def _udt_type_description():
    return b''.join((
        struct.pack('>H', type_codes.UserType),
        _pack_string(KEYSPACE),
        _pack_string(UDT_NAME),
        struct.pack('>H', 1),
        _pack_string(FIELD_NAMES[0]),
        struct.pack('>H', type_codes.Int32Type),
    ))


def _read_udt_type(mapped_class):
    user_type_map = {KEYSPACE: {UDT_NAME: mapped_class}}
    return ResultMessage.read_type(
        io.BytesIO(_udt_type_description()), user_type_map)


def _udt_binary_value(number):
    encoded_number = struct.pack('>i', number)
    return struct.pack('>i', len(encoded_number)) + encoded_number


def test_protocol_udt_descriptors_do_not_mutate_across_mappings():
    descriptor_a = _read_udt_type(AddressA)
    descriptor_b = _read_udt_type(AddressB)

    assert descriptor_a is not descriptor_b
    assert descriptor_a.mapped_class is AddressA
    assert descriptor_b.mapped_class is AddressB

    value_a = descriptor_a.from_binary(_udt_binary_value(1), 4)
    value_b = descriptor_b.from_binary(_udt_binary_value(2), 4)
    assert isinstance(value_a, AddressA)
    assert isinstance(value_b, AddressB)
    assert value_a.number == 1
    assert value_b.number == 2


def test_same_mapping_reuses_its_immutable_descriptor():
    first = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=AddressA)
    second = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=AddressA)

    assert first is second
    assert first.mapped_class is AddressA


def test_unhashable_mapping_class_has_an_isolated_descriptor():
    descriptor = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=UnhashableAddress)

    assert descriptor.mapped_class is UnhashableAddress
    assert isinstance(
        descriptor.from_binary(_udt_binary_value(3), 4),
        UnhashableAddress)


def test_evict_removes_every_mapping_variant():
    descriptor_a = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=AddressA)
    descriptor_b = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=AddressB)
    descriptor_unmapped = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES)

    UserType.evict_udt_class(KEYSPACE, UDT_NAME)

    assert UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=AddressA) is not descriptor_a
    assert UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=AddressB) is not descriptor_b
    assert UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES) is not descriptor_unmapped


def test_mapped_variants_do_not_replace_picklable_unmapped_tuple():
    unmapped = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES)
    registered_tuple = unmapped.tuple_type

    mapped_a = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=AddressA)
    mapped_b = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=AddressB)

    assert mapped_a.tuple_type is None
    assert mapped_b.tuple_type is None
    assert getattr(
        UserType._module,
        '{}_{}'.format(KEYSPACE, UDT_NAME)) is registered_tuple


def test_legacy_casstype_lookup_does_not_borrow_registered_mapping():
    class KeyspaceType:

        @classmethod
        def cass_parameterized_type(cls):
            return KEYSPACE

    class UdtNameType:
        cassname = UDT_NAME.encode('ascii').hex()

    mapped = UserType.make_udt_class(
        KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
        mapped_class=AddressA)
    field_name = FIELD_NAMES[0].encode('ascii').hex()

    parsed = UserType.apply_parameters(
        (KeyspaceType, UdtNameType, Int32Type),
        (None, None, field_name))

    assert parsed is not mapped
    assert parsed.mapped_class is None


def test_concurrent_mapping_creation_and_eviction_is_safe():
    mappings = (AddressA, AddressB, UnhashableAddress, None)

    def create_variants():
        for _ in range(100):
            for mapped_class in mappings:
                descriptor = UserType.make_udt_class(
                    KEYSPACE, UDT_NAME, FIELD_NAMES, FIELD_TYPES,
                    mapped_class=mapped_class)
                assert descriptor.mapped_class is mapped_class

    def evict_variants():
        for _ in range(100):
            UserType.evict_udt_class(KEYSPACE, UDT_NAME)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(create_variants),
            executor.submit(create_variants),
            executor.submit(evict_variants),
            executor.submit(evict_variants),
        ]
        for future in futures:
            future.result()
