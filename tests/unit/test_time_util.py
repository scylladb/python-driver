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

from cassandra import marshal
from cassandra import util
import calendar
import datetime
import time
import uuid
import pytest


class TimeUtilTest(unittest.TestCase):
    def test_datetime_from_timestamp(self):
        assert util.datetime_from_timestamp(0) == datetime.datetime(1970, 1, 1)
        # large negative; test PYTHON-110 workaround for windows
        assert util.datetime_from_timestamp(-62135596800) == datetime.datetime(1, 1, 1)
        assert util.datetime_from_timestamp(-62135596199) == datetime.datetime(1, 1, 1, 0, 10, 1)

        assert util.datetime_from_timestamp(253402300799) == datetime.datetime(9999, 12, 31, 23, 59, 59)

        assert util.datetime_from_timestamp(0.123456) == datetime.datetime(1970, 1, 1, 0, 0, 0, 123456)

        assert util.datetime_from_timestamp(2177403010.123456) == datetime.datetime(2038, 12, 31, 10, 10, 10, 123456)

    def test_times_from_uuid1(self):
        node = uuid.getnode()
        now = time.time()
        u = uuid.uuid1(node, 0)

        t = util.unix_time_from_uuid1(u)
        assert now == pytest.approx(t, abs=1e-2)

        dt = util.datetime_from_uuid1(u)
        t = calendar.timegm(dt.timetuple()) + dt.microsecond / 1e6
        assert now == pytest.approx(t, abs=1e-2)

    def test_uuid_from_time(self):
        t = time.time()
        seq = 0x2aa5
        node = uuid.getnode()
        u = util.uuid_from_time(t, node, seq)
        # using AlmostEqual because time precision is different for
        # some platforms
        assert util.unix_time_from_uuid1(u) == pytest.approx(t, abs=1e-4)
        assert u.node == node
        assert u.clock_seq == seq

        # random node
        u1 = util.uuid_from_time(t, clock_seq=seq)
        u2 = util.uuid_from_time(t, clock_seq=seq)
        assert util.unix_time_from_uuid1(u1) == pytest.approx(t, abs=1e-4)
        assert util.unix_time_from_uuid1(u2) == pytest.approx(t, abs=1e-4)
        assert u.clock_seq == seq
        # not impossible, but we shouldn't get the same value twice
        assert u1.node != u2.node

        # random seq
        u1 = util.uuid_from_time(t, node=node)
        u2 = util.uuid_from_time(t, node=node)
        assert util.unix_time_from_uuid1(u1) == pytest.approx(t, abs=1e-4)
        assert util.unix_time_from_uuid1(u2) == pytest.approx(t, abs=1e-4)
        assert u.node == node
        # not impossible, but we shouldn't get the same value twice
        assert u1.clock_seq != u2.clock_seq

        # node too large
        with pytest.raises(ValueError):
            u = util.uuid_from_time(t, node=2 ** 48)

        # clock_seq too large
        with pytest.raises(ValueError):
            u = util.uuid_from_time(t, clock_seq=0x4000)

        # construct from datetime
        dt = util.datetime_from_timestamp(t)
        u = util.uuid_from_time(dt, node, seq)
        assert util.unix_time_from_uuid1(u) == pytest.approx(t, abs=1e-4)
        assert u.node == node
        assert u.clock_seq == seq

#   0                   1                   2                   3
#    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
#   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#   |                          time_low                             |
#   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#   |       time_mid                |         time_hi_and_version   |
#   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#   |clk_seq_hi_res |  clk_seq_low  |         node (0-1)            |
#   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
#   |                         node (2-5)                            |
#   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    def test_min_uuid(self):
        u = util.min_uuid_from_time(0)
        # cassandra does a signed comparison of the remaining bytes
        for i in range(8, 16):
            assert marshal.int8_unpack(u.bytes[i:i + 1]) == -128

    def test_max_uuid(self):
        u = util.max_uuid_from_time(0)
        # cassandra does a signed comparison of the remaining bytes
        # the first non-time byte has the variant in it
        # This byte is always negative, but should be the smallest negative
        # number with high-order bits '10'
        assert marshal.int8_unpack(u.bytes[8:9]) == -65
        for i in range(9, 16):
            assert marshal.int8_unpack(u.bytes[i:i + 1]) == 127
