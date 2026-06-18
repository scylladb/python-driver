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

import pytest

# Directory containing the Cython-compiled driver modules.
_CASSANDRA_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "cassandra")

# When set (e.g. in CI) a skipped test is turned into a failure. Tests skip
# themselves when their requirements are missing (a library is not installed,
# the wrong event loop is selected, ...). That is convenient locally, but in CI
# it is a footgun: a test may be silently skipped because we forgot to install
# something. Enabling this forces every skip to be explicit on the command line
# (via -k / --ignore / --deselect) instead of being hidden in the output.
_NO_SKIP = bool(os.environ.get("CASS_DRIVER_NO_SKIP"))


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """Turn skips into failures when CASS_DRIVER_NO_SKIP is set.

    xfailed tests (which are reported as skipped) are left untouched so that
    ``xfail_strict`` keeps working as configured.
    """
    outcome = yield
    if not _NO_SKIP:
        return
    report = outcome.get_result()
    if report.skipped and not hasattr(report, "wasxfail"):
        reason = ""
        if isinstance(report.longrepr, tuple) and len(report.longrepr) == 3:
            reason = report.longrepr[2]
        elif report.longrepr:
            reason = str(report.longrepr)
        report.outcome = "failed"
        report.longrepr = (
            "Test was skipped but skipping is disabled in this environment "
            "(CASS_DRIVER_NO_SKIP is set). Run it in a suitable configuration "
            "or deselect it explicitly on the command line. "
            "Original skip reason: {!r}".format(reason)
        )


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
