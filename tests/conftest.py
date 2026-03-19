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

import glob
import importlib.machinery
import os
import warnings

# Directory containing the Cython-compiled driver modules.
_CASSANDRA_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "cassandra")


def pytest_configure(config):
    """Warn when a compiled Cython extension is older than its .py source.

    Python's import system prefers compiled extensions (.so / .pyd) over pure
    Python (.py) files.  If a developer edits a .py file without rebuilding
    the Cython extensions (``python setup.py build_ext --inplace``), the tests
    will silently run the *old* compiled code, masking any regressions in the
    Python source.

    This hook detects such staleness at test-session startup so the developer
    is alerted immediately.
    """
    seen = set()
    stale = []
    # Use the current interpreter's extension suffixes so we only check
    # extensions that would actually be loaded (correct ABI tag), and
    # handle both .so (POSIX) and .pyd (Windows) automatically.
    for suffix in importlib.machinery.EXTENSION_SUFFIXES:
        for ext_path in glob.glob(os.path.join(_CASSANDRA_DIR, f"*{suffix}")):
            module_name = os.path.basename(ext_path).split(".")[0]
            if module_name in seen:
                continue
            py_path = os.path.join(_CASSANDRA_DIR, module_name + ".py")
            if os.path.exists(py_path) and os.path.getmtime(py_path) > os.path.getmtime(
                ext_path
            ):
                seen.add(module_name)
                stale.append((module_name, ext_path, py_path))

    if stale:
        names = ", ".join(m for m, _, _ in stale)
        warnings.warn(
            f"Stale Cython extension(s) detected: {names}. "
            f"The .py source is newer than the compiled extension — tests "
            f"will run the OLD compiled code, not your latest changes. "
            f"Rebuild with:  python setup.py build_ext --inplace",
            stacklevel=1,
        )
