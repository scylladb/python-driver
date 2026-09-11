"""Pytest configuration for performance benchmarks.

When running benchmarks with ``--benchmark-autosave`` or ``--benchmark-save=NAME``,
results are stored in ``.benchmarks/`` at the repository root (the pytest-benchmark
default).  Use ``pytest-benchmark compare`` to diff saved runs::

    pytest -m benchmark --benchmark-autosave
    pytest-benchmark compare .benchmarks/Linux-*/*.json
"""
