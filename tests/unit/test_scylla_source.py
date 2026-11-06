"""Contracts for pinning Scylla master and partitioning its upstream suites."""

import importlib.util
from pathlib import Path
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    'scylla_source',
    Path(__file__).resolve().parents[2] / 'scripts/scylla_source.py',
)
source_ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(source_ci)


class MasterSelectionTest(unittest.TestCase):
    def test_pins_master_even_when_other_refs_are_advertised(self):
        revision = 'a' * 40
        refs = (f'{"b" * 40} refs/heads/branch-2026.3\n'
                f'{revision} refs/heads/master\n')
        self.assertEqual(source_ci.master_source(refs), {
            'ref': 'master', 'revision': revision,
        })

    def test_missing_or_duplicate_master_fails(self):
        entry = f'{"a" * 40} refs/heads/master\n'
        for refs in ('', f'{"b" * 40} refs/tags/master\n', entry * 2):
            with (
                self.subTest(refs=refs),
                self.assertRaisesRegex(ValueError, 'exactly one'),
            ):
                source_ci.master_source(refs)

    def test_non_commit_master_fails(self):
        for revision in ('master', 'a' * 39, 'z' * 40):
            with (
                self.subTest(revision=revision),
                self.assertRaisesRegex(ValueError, 'Invalid'),
            ):
                source_ci.master_source(f'{revision} refs/heads/master')


class TestPlanTest(unittest.TestCase):
    def test_all_files_including_nested_suites_appear_once(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            expected = [f'test/cluster/test_case_{i}.py' for i in range(19)]
            expected += [
                'test/cqlpy/cassandra_tests/nested_test.py',
                'test/cql/approval_test.cql', 'test/new_suite/new_tests.py',
            ]
            ignored = [
                'test/cluster/conftest.py', 'test/cluster/helper.py',
                'test/cluster/manual/test_manual.py',
                'test/pylib/test_helper.py', 'test/boost/test_compiled.py',
                'test/vector_search/a_test.cc',
            ]
            for name in expected + ignored:
                path = source / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
            jobs = source_ci.test_plan(source)
            assigned = [name for job in jobs for name in job['files']]
            self.assertCountEqual(assigned, expected)
            self.assertEqual(len(assigned), len(set(assigned)))
            self.assertEqual(
                len([job for job in jobs if job['suite'] == 'cluster']), 8,
            )
            self.assertTrue(all(job['files'] for job in jobs))

    def test_empty_plan_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            (source / 'test').mkdir()
            with self.assertRaisesRegex(ValueError, 'No Python/CQL'):
                source_ci.test_plan(source)
