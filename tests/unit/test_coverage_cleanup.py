import importlib.machinery
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

from scripts.clean_cython_extensions import clean_cython_extensions


class CoverageCleanupTest(TestCase):
    def test_removes_python_and_cython_only_extensions(self):
        with TemporaryDirectory() as root:
            package = Path(root) / 'cassandra'
            package.mkdir()
            suffix = importlib.machinery.EXTENSION_SUFFIXES[0]
            for name in ('row_parser', 'cython_lz4'):
                (package / (name + '.pyx')).touch()
            removed = [package / (name + suffix) for name in
                       ('cluster', 'row_parser', 'cython_lz4')]
            preserved = [package / ('cmurmur3' + suffix),
                         package / 'row_parser.foreign-abi.so']
            for path in removed + preserved:
                path.touch()

            clean_cython_extensions(package)

            self.assertTrue(all(not path.exists() for path in removed))
            self.assertTrue(all(path.exists() for path in preserved))
