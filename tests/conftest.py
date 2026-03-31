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

import importlib.machinery
import os
import warnings

# Directory containing the Cython-compiled driver modules.
_CASSANDRA_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "cassandra")


def pytest_configure(config):
    """Warn when a compiled Cython extension is older than its .py source.

    Python's import system prefers compiled extensions (.so / .pyd) over pure
    Python (.py) files.  If a developer edits a .py file without rebuilding
    the Cython extensions, the tests
    will silently run the *old* compiled code, masking any regressions in the
    Python source.

    This hook detects such staleness at test-session startup so the developer
    is alerted immediately.
    """
    stale = []
    # Iterate over .py sources and, for each module, look for the first
    # existing compiled extension in EXTENSION_SUFFIXES order.  This mirrors
    # how Python's import machinery selects an extension module, and avoids
    # globbing patterns like "*{suffix}" that can pick up ABI-tagged
    # extensions built for other Python versions.
    if os.path.isdir(_CASSANDRA_DIR):
        for entry in os.listdir(_CASSANDRA_DIR):
            if not entry.endswith(".py"):
                continue
            module_name, _ = os.path.splitext(entry)
            py_path = os.path.join(_CASSANDRA_DIR, entry)
            # For this module, find the first extension file Python would load.
            for suffix in importlib.machinery.EXTENSION_SUFFIXES:
                ext_path = os.path.join(_CASSANDRA_DIR, module_name + suffix)
                if not os.path.exists(ext_path):
                    continue
                if os.path.getmtime(py_path) > os.path.getmtime(ext_path):
                    stale.append((module_name, ext_path, py_path))
                # Only consider the first matching suffix; this is the one
                # the import system would actually use.
                break

    if stale:
        names = ", ".join(m for m, _, _ in stale)
        warnings.warn(
            f"Stale Cython extension(s) detected: {names}. "
            f"The .py source is newer than the compiled extension — tests "
            f"will run the OLD compiled code, not your latest changes. "
            f"Rebuild with:  uv sync --reinstall-package scylla-driver\n"
            f"Or use 'uv run pytest' which handles rebuilds automatically.",
            stacklevel=1,
        )
