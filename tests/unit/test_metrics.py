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

"""
Unit tests for the self-contained metrics module.
"""

import threading
import unittest

from cassandra.metrics import (
    IntStat, Stat, PmfStat, StatsCollection,
    collection, init, getStats, _stats_registry, _registry_lock
)


class IntStatTest(unittest.TestCase):
    """Tests for IntStat class."""

    def test_initial_value(self):
        stat = IntStat('test_counter')
        self.assertEqual(stat.value, 0)
        self.assertEqual(int(stat), 0)

    def test_increment(self):
        stat = IntStat('test_counter')
        stat += 1
        self.assertEqual(stat.value, 1)
        stat += 5
        self.assertEqual(stat.value, 6)

    def test_thread_safety(self):
        stat = IntStat('test_counter')
        num_threads = 10
        increments_per_thread = 1000

        def increment():
            nonlocal stat
            for _ in range(increments_per_thread):
                stat += 1

        threads = [threading.Thread(target=increment) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(stat.value, num_threads * increments_per_thread)

    def test_repr(self):
        stat = IntStat('my_counter')
        stat += 42
        self.assertEqual(repr(stat), "IntStat(my_counter=42)")


class StatTest(unittest.TestCase):
    """Tests for Stat (gauge) class."""

    def test_basic_gauge(self):
        counter = [0]
        stat = Stat('test_gauge', lambda: counter[0])

        self.assertEqual(stat.value, 0)
        counter[0] = 10
        self.assertEqual(stat.value, 10)
        counter[0] = 42
        self.assertEqual(stat.value, 42)

    def test_repr(self):
        stat = Stat('my_gauge', lambda: 123)
        self.assertEqual(repr(stat), "Stat(my_gauge=123)")


class PmfStatTest(unittest.TestCase):
    """Tests for PmfStat class."""

    def test_empty_stats(self):
        stat = PmfStat('test_timer')
        stats = stat._get_stats()

        self.assertEqual(stats['count'], 0)
        self.assertEqual(stats['min'], 0.0)
        self.assertEqual(stats['max'], 0.0)
        self.assertEqual(stats['mean'], 0.0)
        self.assertEqual(stats['stddev'], 0.0)
        self.assertEqual(stats['median'], 0.0)

    def test_single_value(self):
        stat = PmfStat('test_timer')
        stat.addValue(10.0)
        stats = stat._get_stats()

        self.assertEqual(stats['count'], 1)
        self.assertEqual(stats['min'], 10.0)
        self.assertEqual(stats['max'], 10.0)
        self.assertEqual(stats['mean'], 10.0)
        self.assertEqual(stats['stddev'], 0.0)
        self.assertEqual(stats['median'], 10.0)

    def test_multiple_values(self):
        stat = PmfStat('test_timer')
        for v in [1, 2, 3, 4, 5]:
            stat.addValue(v)
        stats = stat._get_stats()

        self.assertEqual(stats['count'], 5)
        self.assertEqual(stats['min'], 1.0)
        self.assertEqual(stats['max'], 5.0)
        self.assertEqual(stats['mean'], 3.0)
        self.assertEqual(stats['median'], 3.0)

    def test_dict_like_access(self):
        stat = PmfStat('test_timer')
        stat.addValue(5.0)

        self.assertEqual(stat['count'], 1)
        self.assertEqual(stat['mean'], 5.0)
        self.assertIn('count', stat.keys())
        self.assertIn('mean', stat.keys())

    def test_percentiles(self):
        stat = PmfStat('test_timer')
        # Add values 1-100
        for v in range(1, 101):
            stat.addValue(v)
        stats = stat._get_stats()

        self.assertEqual(stats['count'], 100)
        self.assertEqual(stats['min'], 1.0)
        self.assertEqual(stats['max'], 100.0)
        # Median should be around 50
        self.assertAlmostEqual(stats['median'], 50.5, delta=1)
        # 75th percentile should be around 75
        self.assertAlmostEqual(stats['75percentile'], 75.25, delta=1)
        # 95th percentile should be around 95
        self.assertAlmostEqual(stats['95percentile'], 95.05, delta=1)

    def test_thread_safety(self):
        stat = PmfStat('test_timer')
        num_threads = 10
        values_per_thread = 100

        def add_values():
            for i in range(values_per_thread):
                stat.addValue(i)

        threads = [threading.Thread(target=add_values) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        stats = stat._get_stats()
        self.assertEqual(stats['count'], num_threads * values_per_thread)


class StatsCollectionTest(unittest.TestCase):
    """Tests for StatsCollection class."""

    def test_access_stats_by_attribute(self):
        int_stat = IntStat('errors')
        pmf_stat = PmfStat('latency')

        coll = StatsCollection('test', int_stat, pmf_stat)

        self.assertIs(coll.errors, int_stat)
        self.assertIs(coll.latency, pmf_stat)

    def test_augmented_assignment(self):
        int_stat = IntStat('errors')
        coll = StatsCollection('test', int_stat)

        coll.errors += 1
        self.assertEqual(int_stat.value, 1)
        coll.errors += 5
        self.assertEqual(int_stat.value, 6)

    def test_get_stats_dict(self):
        int_stat = IntStat('errors')
        int_stat += 3
        pmf_stat = PmfStat('latency')
        pmf_stat.addValue(10.0)
        gauge = Stat('connections', lambda: 5)

        coll = StatsCollection('test', int_stat, pmf_stat, gauge)
        stats_dict = coll._get_stats_dict()

        self.assertEqual(stats_dict['errors'], 3)
        self.assertEqual(stats_dict['connections'], 5)
        self.assertIsInstance(stats_dict['latency'], dict)
        self.assertEqual(stats_dict['latency']['count'], 1)
        self.assertEqual(stats_dict['latency']['mean'], 10.0)

    def test_nonexistent_attribute(self):
        coll = StatsCollection('test', IntStat('errors'))
        with self.assertRaises(AttributeError):
            _ = coll.nonexistent

    def test_cannot_set_new_attribute(self):
        coll = StatsCollection('test', IntStat('errors'))
        with self.assertRaises(AttributeError):
            coll.new_attr = 123


class CollectionFunctionTest(unittest.TestCase):
    """Tests for the collection() function."""

    def setUp(self):
        # Clean up registry before each test
        with _registry_lock:
            keys_to_remove = [k for k in _stats_registry.keys()
                              if k.startswith('test_')]
            for k in keys_to_remove:
                del _stats_registry[k]

    def test_registers_in_global_registry(self):
        coll = collection('test_coll', IntStat('counter'))

        with _registry_lock:
            self.assertIn('test_coll', _stats_registry)
            self.assertIs(_stats_registry['test_coll'], coll)

    def test_get_stats_returns_dict(self):
        int_stat = IntStat('counter')
        int_stat += 10
        collection('test_stats', int_stat)

        stats = getStats()
        self.assertIn('test_stats', stats)
        self.assertEqual(stats['test_stats']['counter'], 10)


class InitFunctionTest(unittest.TestCase):
    """Tests for the init() function."""

    def setUp(self):
        # Clean up registry before each test
        with _registry_lock:
            keys_to_remove = [k for k in _stats_registry.keys()
                              if k.startswith('test') or k == 'request']
            for k in keys_to_remove:
                del _stats_registry[k]

    def test_creates_instance_stats(self):
        class Analyzer:
            requests = PmfStat('request size')
            errors = IntStat('errors')

            def __init__(self):
                init(self, '/test_analyzer')

        analyzer = Analyzer()

        # Instance should have its own stats
        self.assertIsInstance(analyzer.requests, PmfStat)
        self.assertIsInstance(analyzer.errors, IntStat)

        # Should be different from class-level stats
        self.assertIsNot(analyzer.requests, Analyzer.requests)
        self.assertIsNot(analyzer.errors, Analyzer.errors)

    def test_instance_stats_are_independent(self):
        class Analyzer:
            errors = IntStat('errors')

            def __init__(self):
                init(self, '/test_analyzer')

        a1 = Analyzer()
        a2 = Analyzer()

        a1.errors += 5
        a2.errors += 10

        self.assertEqual(a1.errors.value, 5)
        self.assertEqual(a2.errors.value, 10)

    def test_strips_leading_slash(self):
        class Analyzer:
            errors = IntStat('errors')

            def __init__(self):
                init(self, '/test_path')

        Analyzer()

        with _registry_lock:
            self.assertIn('test_path', _stats_registry)
            self.assertNotIn('/test_path', _stats_registry)


if __name__ == '__main__':
    unittest.main()
