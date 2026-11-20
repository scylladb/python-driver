"""Remove extension files that a pure-Python coverage build must not import."""

import importlib.machinery
from pathlib import Path


def clean_cython_extensions(package=Path('cassandra')):
    candidates = {'cluster', 'concurrent', 'connection', 'cqltypes', 'metadata',
                  'pool', 'protocol', 'query', 'util', 'shard_info'}
    candidates.update(path.stem for path in package.glob('*.pyx'))
    for name in candidates:
        for suffix in importlib.machinery.EXTENSION_SUFFIXES:
            path = package / (name + suffix)
            if path.exists():
                path.unlink()


if __name__ == '__main__':
    clean_cython_extensions()
