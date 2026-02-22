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
import datetime
from cassandra.cqltypes import DateType
from cassandra.marshal import int64_pack


class TimestampPrecisionTests(unittest.TestCase):
    """
    Tests for timestamp precision with large values (far from epoch).
    See: https://github.com/scylladb/python-driver/issues/XXX
    """

    def test_large_timestamp_roundtrip(self):
        """
        Test that timestamps far from epoch (> 300 years) maintain precision
        through serialize/deserialize cycle.
        """
        # Timestamp for "2300-01-01 00:00:00.001" in milliseconds
        # This is far enough from epoch that float precision is lost
        original_ms = 10413792000001  # 2300-01-01 00:00:00.001
        
        # Pack as int64 (simulating database storage)
        packed = int64_pack(original_ms)
        
        # Deserialize back
        dt = DateType.deserialize(packed, 0)
        
        # Serialize again
        repacked = DateType.serialize(dt, 0)
        
        # Unpack and compare
        from cassandra.marshal import int64_unpack
        result_ms = int64_unpack(repacked)
        
        # Should be exactly equal
        assert result_ms == original_ms, \
            f"Expected {original_ms}, got {result_ms}, difference: {result_ms - original_ms}"

    def test_year_2300_timestamp_precision(self):
        """
        Test the specific case from the issue report:
        timestamp "2300-01-01 00:00:00.001" should maintain precision.
        """
        # Create datetime for 2300-01-01 00:00:00.001
        dt = datetime.datetime(2300, 1, 1, 0, 0, 0, 1000)  # 1000 microseconds = 1 millisecond
        
        # Serialize to bytes
        packed = DateType.serialize(dt, 0)
        
        # Deserialize back
        dt_restored = DateType.deserialize(packed, 0)
        
        # Serialize again
        repacked = DateType.serialize(dt_restored, 0)
        
        # They should be exactly equal
        assert packed == repacked, \
            f"Serialization not stable: {packed.hex()} != {repacked.hex()}"
        
        # The microseconds should be preserved
        assert dt_restored.microsecond == 1000, \
            f"Expected 1000 microseconds, got {dt_restored.microsecond}"

    def test_various_large_timestamps(self):
        """
        Test multiple timestamps far from epoch to ensure precision is maintained.
        """
        # Various timestamps > 300 years from epoch (in milliseconds)
        test_timestamps_ms = [
            10413792000001,  # 2300-01-01 00:00:00.001
            10413792000999,  # 2300-01-01 00:00:00.999
            15768000000000,  # 2469-12-31 12:00:00.000
            20000000000001,  # ~2603 with millisecond precision
            -10413792000001, # ~1640 BCE
        ]
        
        for original_ms in test_timestamps_ms:
            with self.subTest(timestamp_ms=original_ms):
                # Pack as int64
                packed = int64_pack(original_ms)
                
                # Deserialize
                dt = DateType.deserialize(packed, 0)
                
                # Serialize again
                repacked = DateType.serialize(dt, 0)
                
                # Unpack and compare
                from cassandra.marshal import int64_unpack
                result_ms = int64_unpack(repacked)
                
                # Should be exactly equal
                assert result_ms == original_ms, \
                    f"Expected {original_ms}, got {result_ms}, difference: {result_ms - original_ms}"

    def test_small_timestamp_still_works(self):
        """
        Ensure that timestamps close to epoch still work correctly.
        """
        # Timestamp close to epoch (well within float precision)
        original_ms = 1000000000000  # 2001-09-09 01:46:40.000
        
        packed = int64_pack(original_ms)
        dt = DateType.deserialize(packed, 0)
        repacked = DateType.serialize(dt, 0)
        
        from cassandra.marshal import int64_unpack
        result_ms = int64_unpack(repacked)
        
        assert result_ms == original_ms


if __name__ == '__main__':
    unittest.main()
