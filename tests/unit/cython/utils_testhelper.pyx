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

import datetime

from cassandra.cython_utils cimport datetime_from_timestamp, datetime_from_ms_timestamp


def test_datetime_from_timestamp():
    assert datetime_from_timestamp(1454781157.123456) == datetime.datetime(2016, 2, 6, 17, 52, 37, 123456)
    # PYTHON-452
    assert datetime_from_timestamp(2177403010.123456) == datetime.datetime(2038, 12, 31, 10, 10, 10, 123456)


def test_datetime_from_ms_timestamp():
    # epoch
    assert datetime_from_ms_timestamp(0) == datetime.datetime(1970, 1, 1)
    # positive with millisecond precision
    assert datetime_from_ms_timestamp(1454781157123) == datetime.datetime(2016, 2, 6, 17, 52, 37, 123000)
    # large positive far from epoch (GH-532)
    assert datetime_from_ms_timestamp(10413792000001) == datetime.datetime(2300, 1, 1, 0, 0, 0, 1000)
    # negative timestamp
    assert datetime_from_ms_timestamp(-770172256000) == datetime.datetime(1945, 8, 5, 23, 15, 44)
    # large negative with millisecond precision
    assert datetime_from_ms_timestamp(-11676095999999) == datetime.datetime(1600, 1, 1, 0, 0, 0, 1000)
