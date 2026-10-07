"""Regression tests for the direct NumPy import checker."""

import importlib.util
from pathlib import Path


CHECKER_PATH = Path(__file__).parents[2] / 'scripts' / 'check_numpy_imports.py'
spec = importlib.util.spec_from_file_location('check_numpy_imports', CHECKER_PATH)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def test_checker_rejects_nested_python_imports(tmp_path):
    source = tmp_path / 'module.py'
    source.write_text('def use():\n    import numpy as np\n    from numpy.linalg import norm\n')
    assert list(checker.direct_imports(source)) == [2, 3]


def test_checker_rejects_cython_runtime_import_and_cimport(tmp_path):
    source = tmp_path / 'module.pyx'
    source.write_text(
        'cimport numpy\n'
        'from numpy cimport ndarray\n'
        'import numpy as np\n'
        'from numpy import array\n'
        'import os, numpy\n'
    )
    assert list(checker.direct_imports(source)) == [1, 2, 3, 4, 5]


def test_checker_rejects_numpy_cimport_in_pxd(tmp_path):
    source = tmp_path / 'module.pxd'
    source.write_text('cimport numpy\nfrom numpy cimport ndarray\n')
    assert list(checker.direct_imports(source)) == [1, 2]


def test_checker_allows_cimport_only_in_numpy_parser(tmp_path, monkeypatch):
    source = tmp_path / 'numpy_parser.pyx'
    source.write_text('cimport numpy\nfrom numpy cimport ndarray\nimport numpy\n')
    monkeypatch.setattr(checker, 'NUMPY_PARSER', source)
    assert list(checker.direct_imports(source)) == [3]


def test_checker_rejects_cython_imports_after_semicolons(tmp_path):
    source = tmp_path / 'module.pyx'
    source.write_text(
        'x = 1; import numpy as np\n'
        'cimport numpy; import numpy.linalg\n'
        'text = "import numpy; from numpy import array"\n'
        '# import numpy; from numpy import array\n'
        'from numpy cimport ndarray; from numpy import array\n'
    )
    assert list(checker.direct_imports(source)) == [1, 2, 2, 5, 5]
