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

from pathlib import Path
import subprocess
import sys
import tempfile


def _run_import_subprocess(script):
    """Run import checks without modules already loaded by the test suite."""
    driver_path = str(Path(__file__).parents[2])
    script = "import sys\nsys.path.append({!r})\n".format(driver_path) + script
    with tempfile.TemporaryDirectory() as temp_dir:
        result = subprocess.run(
            [sys.executable, '-c', script],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=temp_dir,
        )

    assert result.returncode == 0, (
        "Subprocess failed\nstdout:\n{}\nstderr:\n{}".format(
            result.stdout, result.stderr))


def test_cluster_import_does_not_load_numpy():
    _run_import_subprocess("""
assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules

from cassandra.cluster import Cluster

assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules
""")


def test_numpy_protocol_handler_loads_numpy_on_access():
    _run_import_subprocess("""
import cassandra.protocol as protocol

assert 'numpy' not in sys.modules
assert 'cassandra.numpy_parser' not in sys.modules
assert 'NumpyProtocolHandler' in dir(protocol)

from cassandra.protocol import NumpyProtocolHandler
from cassandra.cython_deps import HAVE_NUMPY

if protocol.HAVE_CYTHON and HAVE_NUMPY:
    assert NumpyProtocolHandler is not None
    assert 'numpy' in sys.modules
    assert 'cassandra.numpy_parser' in sys.modules
else:
    assert NumpyProtocolHandler is None
    assert 'cassandra.numpy_parser' not in sys.modules

assert protocol.NumpyProtocolHandler is NumpyProtocolHandler
""")


def test_numpy_protocol_handler_is_unavailable_without_numpy():
    _run_import_subprocess("""
sys.modules['numpy'] = None

import cassandra.protocol as protocol

assert 'cassandra.numpy_parser' not in sys.modules
from cassandra.protocol import NumpyProtocolHandler

assert NumpyProtocolHandler is None
assert protocol.NumpyProtocolHandler is None
assert sys.modules['numpy'] is None
assert 'cassandra.numpy_parser' not in sys.modules
""")


def test_numpy_protocol_handler_is_unavailable_without_cython():
    _run_import_subprocess("""
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
""")
