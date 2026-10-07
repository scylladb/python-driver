# Copyright 2026 ScyllaDB, Inc.
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

import importlib.util

import pytest

from tests.unit.utils import run_isolated_subprocess


def test_cluster_import_does_not_load_numpy():
    run_isolated_subprocess("""
assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules

from cassandra.cluster import Cluster

assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules
""", timeout=10)


def test_ordinary_driver_module_imports_do_not_load_numpy():
    run_isolated_subprocess("""
import importlib
from pathlib import Path

import cassandra
from cassandra import DependencyException
driver = Path(cassandra.__file__).parent
assert 'numpy' not in sys.modules

modules = sorted({
    '.'.join(path.relative_to(driver).with_suffix('').parts[:-1]
             if path.stem == '__init__' else
             path.relative_to(driver).with_suffix('').parts)
    for path in driver.rglob('*')
    if path.suffix in ('.py', '.pyx')
})
for module in modules:
    if module in ('', 'numpy_parser'):
        continue  # Explicit NumPy feature; importing it requests NumPy.
    try:
        importlib.import_module('cassandra.' + module)
    except (ImportError, DependencyException):
        pass  # Optional dependency is unavailable.
    assert 'numpy' not in sys.modules, module
""", timeout=30)


def test_numpy_import_is_attempted_once_when_unavailable():
    run_isolated_subprocess("""
import builtins
from unittest.mock import patch

sys.modules['numpy'] = None
from cassandra.numpy_support import _get_numpy, numpy_available, _require_numpy

original_import = builtins.__import__
attempts = []

def tracked_import(name, *args, **kwargs):
    if name == 'numpy':
        attempts.append(name)
    return original_import(name, *args, **kwargs)

with patch('builtins.__import__', tracked_import):
    assert _get_numpy() is None
    assert _get_numpy() is None
    assert not numpy_available()
    try:
        _require_numpy()
    except ImportError:
        pass
    else:
        raise AssertionError('require_numpy() should reject missing NumPy')

assert attempts == ['numpy']
""", timeout=10)


def test_require_numpy_preserves_import_failure():
    run_isolated_subprocess("""
import builtins
from unittest.mock import patch
from cassandra.numpy_support import _get_numpy, _require_numpy

original_import = builtins.__import__
failure = ImportError('missing libopenblas')
attempts = []

def broken_numpy_import(name, *args, **kwargs):
    if name == 'numpy':
        attempts.append(name)
        raise failure
    return original_import(name, *args, **kwargs)

with patch('builtins.__import__', broken_numpy_import):
    assert _get_numpy() is None
    assert _get_numpy() is None
    try:
        _require_numpy()
    except ImportError as exc:
        assert exc.__cause__ is failure
        assert str(exc.__cause__) == 'missing libopenblas'
    else:
        raise AssertionError('require_numpy() should reject broken NumPy')

assert attempts == ['numpy']
""", timeout=10)


def test_concurrent_numpy_import_is_attempted_once_when_unavailable():
    run_isolated_subprocess("""
import builtins
from threading import Barrier, BrokenBarrierError, Thread
from unittest.mock import patch

sys.modules['numpy'] = None
from cassandra.numpy_support import _get_numpy

start = Barrier(3)
imports = Barrier(2)
attempts = []
results = []
original_import = builtins.__import__

def tracked_import(name, *args, **kwargs):
    if name == 'numpy':
        attempts.append(name)
        try:
            imports.wait(timeout=0.2)
        except BrokenBarrierError:
            pass
    return original_import(name, *args, **kwargs)

def check_numpy():
    start.wait()
    results.append(_get_numpy())

with patch('builtins.__import__', tracked_import):
    threads = [Thread(target=check_numpy) for _ in range(2)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=3)

assert all(not thread.is_alive() for thread in threads)
assert results == [None, None]
assert attempts == ['numpy']
""", timeout=10)


def test_concurrent_numpy_protocol_handler_is_built_once():
    run_isolated_subprocess("""
import types
from threading import Barrier, BrokenBarrierError, Thread
from unittest.mock import patch

import cassandra.protocol as protocol
import cassandra.numpy_support as numpy_support

parser_module = types.ModuleType('cassandra.numpy_parser')
parser_module.NumpyParser = lambda: object()
sys.modules['cassandra.numpy_parser'] = parser_module

start = Barrier(3)
builds = Barrier(2)
build_calls = []
results = []

def build_handler(parser):
    build_calls.append(parser)
    try:
        builds.wait(timeout=0.2)
    except BrokenBarrierError:
        pass
    return object()

def get_handler():
    start.wait()
    results.append(numpy_support.get_numpy_protocol_handler())

with patch.object(protocol, 'HAVE_CYTHON', True), \\
     patch.object(protocol, 'cython_protocol_handler', build_handler), \\
     patch.object(numpy_support, 'numpy_available', return_value=True):
    threads = [Thread(target=get_handler) for _ in range(2)]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join(timeout=3)

assert all(not thread.is_alive() for thread in threads)
assert len(build_calls) == 1
assert len(results) == 2 and results[0] is results[1]
""", timeout=10)


def test_numpy_protocol_handler_loads_numpy_on_access():
    if importlib.util.find_spec('numpy') is None:
        pytest.skip("NumPy is unavailable")
    if importlib.util.find_spec('cassandra.row_parser') is None:
        pytest.skip("Cython extensions are unavailable")

    run_isolated_subprocess("""
import cassandra.protocol as protocol
from cassandra.numpy_support import numpy_available, get_numpy_protocol_handler

assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules
assert 'NumpyProtocolHandler' in dir(protocol)
assert protocol.HAVE_CYTHON

import warnings
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always', DeprecationWarning)
    from cassandra.protocol import NumpyProtocolHandler
assert any(issubclass(item.category, DeprecationWarning) for item in caught)
assert any('cassandra.numpy_support.get_numpy_protocol_handler()' in str(item.message)
           for item in caught)
assert all(item.filename == '<string>' for item in caught
           if issubclass(item.category, DeprecationWarning))

assert NumpyProtocolHandler is not None
assert 'numpy' in sys.modules
assert 'cassandra.numpy_parser' in sys.modules
assert numpy_available()
assert get_numpy_protocol_handler() is NumpyProtocolHandler
""", timeout=10)


def test_numpy_protocol_handler_is_unavailable_without_numpy():
    run_isolated_subprocess("""
sys.modules['numpy'] = None

import cassandra.protocol as protocol
from cassandra.numpy_support import numpy_available, get_numpy_protocol_handler

assert 'cassandra.numpy_parser' not in sys.modules
assert not numpy_available()
assert get_numpy_protocol_handler() is None
import warnings
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always', DeprecationWarning)
    from cassandra.cython_deps import HAVE_NUMPY
    from cassandra.protocol import NumpyProtocolHandler
assert not HAVE_NUMPY
assert sum(issubclass(item.category, DeprecationWarning) for item in caught) >= 2
assert any('cassandra.numpy_support.numpy_available()' in str(item.message)
           for item in caught)

assert NumpyProtocolHandler is None
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always', DeprecationWarning)
    assert protocol.NumpyProtocolHandler is None
assert caught
assert sys.modules['numpy'] is None
assert 'cassandra.numpy_parser' not in sys.modules
""", timeout=10)


def test_numpy_protocol_handler_is_unavailable_without_cython():
    run_isolated_subprocess("""
sys.modules['cassandra.row_parser'] = None

import cassandra.protocol as protocol
from cassandra.numpy_support import get_numpy_protocol_handler

assert not protocol.HAVE_CYTHON
assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules
assert get_numpy_protocol_handler() is None

import warnings
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always', DeprecationWarning)
    from cassandra.protocol import NumpyProtocolHandler
assert caught

assert NumpyProtocolHandler is None
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always', DeprecationWarning)
    assert protocol.NumpyProtocolHandler is None
assert caught
assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules
""", timeout=10)
