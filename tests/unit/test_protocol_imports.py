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


def test_numpy_protocol_handler_loads_numpy_on_access():
    if importlib.util.find_spec('numpy') is None:
        pytest.skip("NumPy is unavailable")
    if importlib.util.find_spec('cassandra.row_parser') is None:
        pytest.skip("Cython extensions are unavailable")

    run_isolated_subprocess("""
import cassandra.protocol as protocol

assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules
assert 'NumpyProtocolHandler' in dir(protocol)
assert protocol.HAVE_CYTHON

from cassandra.protocol import NumpyProtocolHandler

assert NumpyProtocolHandler is not None
assert 'numpy' in sys.modules
assert 'cassandra.numpy_parser' in sys.modules
assert protocol.NumpyProtocolHandler is NumpyProtocolHandler
""", timeout=10)


def test_numpy_protocol_handler_is_unavailable_without_numpy():
    run_isolated_subprocess("""
sys.modules['numpy'] = None

import cassandra.protocol as protocol

assert 'cassandra.numpy_parser' not in sys.modules
from cassandra.protocol import NumpyProtocolHandler

assert NumpyProtocolHandler is None
assert protocol.NumpyProtocolHandler is None
assert sys.modules['numpy'] is None
assert 'cassandra.numpy_parser' not in sys.modules
""", timeout=10)


def test_numpy_protocol_handler_is_unavailable_without_cython():
    run_isolated_subprocess("""
sys.modules['cassandra.row_parser'] = None

import cassandra.protocol as protocol

assert not protocol.HAVE_CYTHON
assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules

from cassandra.protocol import NumpyProtocolHandler

assert NumpyProtocolHandler is None
assert protocol.NumpyProtocolHandler is None
assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules
""", timeout=10)
