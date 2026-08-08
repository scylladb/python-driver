# Copyright ScyllaDB, Inc.
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
Regression test for a circular import that made cassandra.cython_deps.HAVE_CYTHON
incorrectly report False, depending on which module a process happened to import
first.

cassandra.cython_deps determines HAVE_CYTHON by importing cassandra.row_parser.
cassandra.row_parser (via cassandra.deserializers, and cassandra.deserializers'
import of cassandra.cqltypes) used to import HAVE_NUMPY back out of
cassandra.cython_deps. When cassandra.cython_deps was the first cassandra module
touched in a process, Python would register it in sys.modules before running its
body, so that re-entrant "from cassandra.cython_deps import HAVE_NUMPY" would hit
the partially initialized module and raise ImportError -- which cassandra.cython_deps
then swallowed via its own `except ImportError: HAVE_CYTHON = False`, permanently
(and incorrectly) marking Cython as unavailable for the rest of the process.

Because the bug only manifests when cassandra.cython_deps is the *first*
cassandra-related import, it must be exercised in a fresh interpreter -- importing
things in the "wrong" order in-process within the existing test run would not
reproduce it (whatever ran earlier already primed sys.modules).
"""

import subprocess
import sys
import unittest
from pathlib import Path

try:
    from tests import VERIFY_CYTHON
except ImportError:
    VERIFY_CYTHON = False

# Deliberately avoid tests.unit.cython.utils.cythontest / cassandra.cython_deps.HAVE_CYTHON
# here: whether that flag is (correctly) True is exactly what this module is testing, and
# by the time this test module is collected some other test/import may already have primed
# sys.modules['cassandra.cython_deps'] in a way that hides a regression. Instead, probe
# for the compiled extension directly and independently of cython_deps's own bookkeeping.
try:
    import cassandra.row_parser  # noqa: F401
    _CYTHON_EXTENSION_BUILT = True
except ImportError:
    _CYTHON_EXTENSION_BUILT = False

cythonbuilt = unittest.skipUnless(_CYTHON_EXTENSION_BUILT or VERIFY_CYTHON,
                                   'Cython extensions are not built')


def _run_first_import(first_import_statement):
    """
    Run `first_import_statement` as the very first cassandra-related statement
    in a brand new interpreter, then report HAVE_CYTHON/HAVE_NUMPY.
    """
    driver_path = str(Path(__file__).parent.parent.parent.parent)
    script = (
        "import sys\n"
        # Append (not insert at 0) so an installed/compiled build of the driver
        # takes precedence over the in-tree source if both are on sys.path.
        "sys.path.append({driver_path!r})\n"
        "{first_import_statement}\n"
        "from cassandra.cython_deps import HAVE_CYTHON, HAVE_NUMPY\n"
        "print('HAVE_CYTHON=%s' % HAVE_CYTHON)\n"
        "print('HAVE_NUMPY=%s' % HAVE_NUMPY)\n"
    ).format(driver_path=driver_path, first_import_statement=first_import_statement)

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        "subprocess failed\nstdout:\n%s\nstderr:\n%s" % (result.stdout, result.stderr)
    )
    return result.stdout


class CythonDepsImportOrderTest(unittest.TestCase):
    """
    Verify that cassandra.cython_deps.HAVE_CYTHON is reported consistently
    regardless of which cassandra module a fresh process imports first.

    Skipped unless the compiled Cython extensions are actually available,
    since that's the only scenario where HAVE_CYTHON is expected to be True.
    """

    @cythonbuilt
    def test_cython_deps_imported_first(self):
        # This is the case that used to trigger the circular import: nothing
        # else has touched the cassandra package yet.
        output = _run_first_import("from cassandra.cython_deps import HAVE_CYTHON")
        self.assertIn("HAVE_CYTHON=True", output)

    @cythonbuilt
    def test_row_parser_imported_first(self):
        output = _run_first_import("import cassandra.row_parser")
        self.assertIn("HAVE_CYTHON=True", output)

    @cythonbuilt
    def test_deserializers_imported_first(self):
        output = _run_first_import("import cassandra.deserializers")
        self.assertIn("HAVE_CYTHON=True", output)

    @cythonbuilt
    def test_cqltypes_imported_first(self):
        output = _run_first_import("import cassandra.cqltypes")
        self.assertIn("HAVE_CYTHON=True", output)
