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

"""In-process tests for cassandra.numpy_support.

The subprocess tests in test_protocol_imports.py check the import contract
against a clean interpreter; these drive each branch in this process so
coverage can see them. Every test resets the module's cached state.
"""

import sys
import types
from unittest.mock import patch

import pytest

import cassandra.numpy_support as numpy_support
import cassandra.protocol as protocol


@pytest.fixture
def fresh(monkeypatch):
    """Reset the cached NumPy and handler state for one test."""
    monkeypatch.setattr(numpy_support, '_NUMPY', numpy_support._NOT_CHECKED)
    monkeypatch.setattr(numpy_support, '_NUMPY_IMPORT_ERROR', None)
    monkeypatch.setattr(numpy_support, '_NUMPY_PROTOCOL_HANDLER',
                        numpy_support._NOT_CHECKED)
    return numpy_support


@pytest.fixture
def fake_numpy(monkeypatch):
    module = types.ModuleType('numpy')
    monkeypatch.setitem(sys.modules, 'numpy', module)
    return module


@pytest.fixture
def no_numpy(monkeypatch):
    # A None entry makes "import numpy" raise ImportError.
    monkeypatch.setitem(sys.modules, 'numpy', None)


def test_get_numpy_returns_and_caches_module(fresh, fake_numpy):
    assert fresh._get_numpy() is fake_numpy
    assert fresh._NUMPY is fake_numpy
    assert fresh._NUMPY_IMPORT_ERROR is None
    assert fresh.numpy_available()
    assert fresh._require_numpy() is fake_numpy


def test_get_numpy_caches_import_failure(fresh, no_numpy):
    assert fresh._get_numpy() is None
    assert isinstance(fresh._NUMPY_IMPORT_ERROR, ImportError)
    assert not fresh.numpy_available()


def test_get_numpy_does_not_retry_after_failure(fresh, monkeypatch):
    monkeypatch.setitem(sys.modules, 'numpy', None)
    assert fresh._get_numpy() is None

    # NumPy becomes importable, but the failed lookup stays cached.
    monkeypatch.setitem(sys.modules, 'numpy', types.ModuleType('numpy'))
    assert fresh._get_numpy() is None
    assert not fresh.numpy_available()


def test_require_numpy_raises_and_chains_import_error(fresh, no_numpy):
    with pytest.raises(ImportError, match="NumPy is required") as excinfo:
        fresh._require_numpy()
    assert excinfo.value.__cause__ is fresh._NUMPY_IMPORT_ERROR
    assert isinstance(excinfo.value.__cause__, ImportError)


def test_handler_is_none_without_cython(fresh, fake_numpy):
    with patch.object(protocol, 'HAVE_CYTHON', False):
        assert fresh.get_numpy_protocol_handler() is None
    assert fresh._NUMPY_PROTOCOL_HANDLER is None


def test_handler_is_none_without_numpy(fresh, no_numpy):
    with patch.object(protocol, 'HAVE_CYTHON', True):
        assert fresh.get_numpy_protocol_handler() is None


def test_handler_is_built_with_cython_and_numpy(fresh, fake_numpy, monkeypatch):
    parser_module = types.ModuleType('cassandra.numpy_parser')

    class NumpyParser(object):
        pass

    parser_module.NumpyParser = NumpyParser
    monkeypatch.setitem(sys.modules, 'cassandra.numpy_parser', parser_module)
    sentinel = object()
    with patch.object(protocol, 'HAVE_CYTHON', True), \
            patch.object(protocol, 'cython_protocol_handler',
                         return_value=sentinel) as build:
        assert fresh.get_numpy_protocol_handler() is sentinel
    (parser,), _ = build.call_args
    assert isinstance(parser, NumpyParser)


def test_handler_result_is_cached(fresh, fake_numpy):
    with patch.object(protocol, 'HAVE_CYTHON', False):
        assert fresh.get_numpy_protocol_handler() is None
    with patch.object(protocol, 'HAVE_CYTHON', True), \
            patch.object(protocol, 'cython_protocol_handler') as build:
        assert fresh.get_numpy_protocol_handler() is None
    build.assert_not_called()
