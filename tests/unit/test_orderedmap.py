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

import unittest

from cassandra.util import OrderedMap, OrderedMapSerializedKey
from cassandra.cqltypes import EMPTY, UTF8Type, lookup_casstype
from tests.util import assertListEqual
import pytest

class OrderedMapTest(unittest.TestCase):
    def test_init(self):
        a = OrderedMap(zip(['one', 'three', 'two'], [1, 3, 2]))
        b = OrderedMap([('one', 1), ('three', 3), ('two', 2)])
        c = OrderedMap(a)
        builtin = {'one': 1, 'two': 2, 'three': 3}
        assert a == b
        assert a == c
        assert a == builtin
        assert OrderedMap([(1, 1), (1, 2)]) == {1: 2}

        d = OrderedMap({'': 3}, key1='v1', key2='v2')
        assert d[''] == 3
        assert d['key1'] == 'v1'
        assert d['key2'] == 'v2'

        with pytest.raises(TypeError):
            OrderedMap('too', 'many', 'args')

    def test_contains(self):
        keys = ['first', 'middle', 'last']

        om = OrderedMap()

        om = OrderedMap(zip(keys, range(len(keys))))

        for k in keys:
            assert k in om
            assert not k not in om

        assert 'notthere' not in om
        assert not 'notthere' in om

    def test_keys(self):
        keys = ['first', 'middle', 'last']
        om = OrderedMap(zip(keys, range(len(keys))))

        assertListEqual(list(om.keys()), keys)

    def test_values(self):
        keys = ['first', 'middle', 'last']
        values = list(range(len(keys)))
        om = OrderedMap(zip(keys, values))

        assertListEqual(list(om.values()), values)

    def test_items(self):
        keys = ['first', 'middle', 'last']
        items = list(zip(keys, range(len(keys))))
        om = OrderedMap(items)

        assertListEqual(list(om.items()), items)

    def test_get(self):
        keys = ['first', 'middle', 'last']
        om = OrderedMap(zip(keys, range(len(keys))))

        for v, k in enumerate(keys):
            assert om.get(k) == v

        assert om.get('notthere', 'default') == 'default'
        assert om.get('notthere') is None

    def test_equal(self):
        d1 = {'one': 1}
        d12 = {'one': 1, 'two': 2}
        om1 = OrderedMap({'one': 1})
        om12 = OrderedMap([('one', 1), ('two', 2)])
        om21 = OrderedMap([('two', 2), ('one', 1)])

        assert om1 == d1
        assert om12 == d12
        assert om21 == d12
        assert om1 != om12
        assert om12 != om1
        assert om12 != om21
        assert om1 != d12
        assert om12 != d1
        assert om1 != EMPTY

        assert not OrderedMap([('three', 3), ('four', 4)]) == d12

    def test_getitem(self):
        keys = ['first', 'middle', 'last']
        om = OrderedMap(zip(keys, range(len(keys))))

        for v, k in enumerate(keys):
            assert om[k] == v

        with pytest.raises(KeyError):
            om['notthere']

    def test_iter(self):
        keys = ['first', 'middle', 'last']
        values = list(range(len(keys)))
        items = list(zip(keys, values))
        om = OrderedMap(items)

        itr = iter(om)
        assert sum([1 for _ in itr]) == len(keys)
        with pytest.raises(StopIteration):
            next(itr)

        assert list(iter(om)) == keys
        assert list(om.items()) == items
        assert list(om.values()) == values

    def test_len(self):
        assert len(OrderedMap()) == 0
        assert len(OrderedMap([(1, 1)])) == 1

    def test_mutable_keys(self):
        d = {'1': 1}
        s = set([1, 2, 3])
        om = OrderedMap([(d, 'dict'), (s, 'set')])

    def test_strings(self):
        # changes in 3.x
        d = {'map': 'inner'}
        s = set([1, 2, 3])
        assert repr(OrderedMap([('two', 2), ('one', 1), (d, 'value'), (s, 'another')])) == "OrderedMap([('two', 2), ('one', 1), (%r, 'value'), (%r, 'another')])" % (d, s)

        assert str(OrderedMap([('two', 2), ('one', 1), (d, 'value'), (s, 'another')])) == "{'two': 2, 'one': 1, %r: 'value', %r: 'another'}" % (d, s)

    def test_popitem(self):
        item = (1, 2)
        om = OrderedMap((item,))
        assert om.popitem() == item
        with pytest.raises(KeyError):
            om.popitem()

    def test_delitem(self):
        om = OrderedMap({1: 1, 2: 2})

        with pytest.raises(KeyError):
            om.__delitem__(3)

        del om[1]
        assert om == {2: 2}
        del om[2]
        assert not om

        with pytest.raises(KeyError):
            om.__delitem__(1)


class OrderedMapSerializedKeyTest(unittest.TestCase):
    def test_init(self):
        om = OrderedMapSerializedKey(UTF8Type, 3)
        assert om == {}

    def test_normalized_lookup(self):
        key_type = lookup_casstype('MapType(UTF8Type, Int32Type)')
        protocol_version = 3
        om = OrderedMapSerializedKey(key_type, protocol_version)
        key_ascii = {'one': 1}
        key_unicode = {u'two': 2}
        om._insert_unchecked(key_ascii, key_type.serialize(key_ascii, protocol_version), object())
        om._insert_unchecked(key_unicode, key_type.serialize(key_unicode, protocol_version), object())

        # type lookup is normalized by key_type
        # PYTHON-231
        assert om[{'one': 1}] is om[{u'one': 1}]
        assert om[{'two': 2}] is om[{u'two': 2}]
        assert om[{'one': 1}] is not om[{'two': 2}]
