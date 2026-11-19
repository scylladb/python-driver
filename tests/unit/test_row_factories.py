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


import warnings

import pytest

from cassandra.query import named_tuple_factory


@pytest.mark.parametrize("column_count", [200, 300])
def test_named_tuple_factory_wide_rows(column_count):
    colnames = ['col{}'.format(i) for i in range(column_count)]
    values = ['value{}'.format(i) for i in range(column_count)]

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        rows = named_tuple_factory(colnames, [values])

    assert not caught
    row, = rows
    assert isinstance(row, tuple)
    assert row._fields == tuple(colnames)
    assert tuple(row) == tuple(values)
    assert row.col0 == values[0]
    assert getattr(row, colnames[-1]) == values[-1]
