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

"Exercise release metadata and publication guards in the primary unit suite."

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch


def load_helpers():
    directory = Path(__file__).resolve().parents[2] / "scripts/release"

    def load(name, filename):
        spec = importlib.util.spec_from_file_location(
            name, directory / filename
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    signing = load("_driver_release_signing", "signing.py")
    # The CLI imports its adjacent signing module by name. Keep that alias
    # local
    # to loading the helper so collection cannot affect other unit tests.
    missing = object()
    previous = sys.modules.get("signing", missing)
    sys.modules["signing"] = signing
    try:
        return load("_driver_release", "release.py")
    finally:
        if previous is missing:
            del sys.modules["signing"]
        else:
            sys.modules["signing"] = previous


release = load_helpers()


class ReleaseVersionTests(unittest.TestCase):
    def test_canonical_stable_version(self):
        self.assertEqual(release.stable("3.29.14"), (3, 29, 14))
        self.assertGreater(release.stable("3.30.0"), release.stable("3.29.99"))

    def test_ambiguous_or_prerelease_versions_are_rejected(self):
        for version in (
            "3.29",
            "3.29.14rc1",
            "03.29.14",
            "3.29.14-scylla",
            "3.29.14\n",
        ):
            with (
                self.subTest(version=version),
                self.assertRaises(release.Invalid),
            ):
                release.stable(version)

    def test_preparation_promotes_changelog_and_updates_all_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = {
                "cassandra/__init__.py": "__version_info__ = (3, 29, 13)\n",
                "CHANGELOG.rst": "Unreleased\n==========\n\nBug "
                "Fixes\n---------\n* Fixed "
                "routing.\n\n3.29.13\n=======\n2026-11-02\n",
                "docs/conf.py": "TAGS = [\n    '3.29.13-scylla',\n]\n"
                "LATEST_VERS"
                "ION = '3.29.13-scylla'\n",
                "docs/installation.rst": "It should print something like "
                '"3.29.13".\n',
            }
            for name, content in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
            release.prepare_metadata(root, "3.29.14", "2026-11-05")
            self.assertEqual(
                (root / "cassandra/__init__.py").read_text(),
                "__version_info__ = (3, 29, 14)\n",
            )
            changelog = (root / "CHANGELOG.rst").read_text()
            self.assertTrue(
                changelog.startswith(
                    "Unreleased\n==========\n\n3.29.14\n=======\n2026-11-05\n"
                )
            )
            self.assertEqual(changelog.count("* Fixed routing."), 1)
            self.assertIn("3.29.13\n=======\n2026-11-02\n", changelog)
            conf = (root / "docs/conf.py").read_text()
            self.assertIn("    '3.29.14-scylla',\n]", conf)
            self.assertIn("LATEST_VERSION = '3.29.14-scylla'", conf)
            self.assertIn(
                '"3.29.14"', (root / "docs/installation.rst").read_text()
            )
            with self.assertRaisesRegex(release.Invalid, "increase"):
                release.prepare_metadata(root, "3.29.14", "2026-11-05")


class ReleasePublicationGuardTests(unittest.TestCase):
    def test_failed_publication_rerun_selects_existing_signed_checkpoint(self):
        with (
            patch.object(release, "restore", return_value=True) as restore,
            patch.object(release, "output") as output,
        ):
            release.restore_state(Path("/state"))
            restore.assert_called_once_with("release-state", Path("/state"))
            output.assert_called_once_with(retained="true")

    def test_missing_expected_checkpoint_cannot_enable_signing(self):
        with (
            patch.object(release, "restore", return_value=False),
            patch.object(release, "output") as output,
        ):
            with self.assertRaisesRegex(release.Invalid, "manual recovery"):
                release.restore_state(Path("/state"), require_retained=True)
            output.assert_not_called()

    def test_only_missing_pypi_files_are_selected(self):
        expected = {"linux.whl": "linux-hash", "windows.whl": "windows-hash"}
        uploaded = [
            {"filename": "linux.whl", "digests": {"sha256": "linux-hash"}}
        ]
        self.assertEqual(
            release.missing_packages(expected, uploaded), ["windows.whl"]
        )

    def test_conflicting_or_yanked_pypi_files_are_rejected(self):
        for item in (
            {"filename": "linux.whl", "digests": {"sha256": "changed"}},
            {"filename": "extra.whl", "digests": {"sha256": "original"}},
            {
                "filename": "linux.whl",
                "digests": {"sha256": "original"},
                "yanked": True,
            },
        ):
            with self.subTest(item=item), self.assertRaises(release.Invalid):
                release.missing_packages({"linux.whl": "original"}, [item])

    def test_branch_movement_requires_manual_recovery(self):
        with patch.object(
            release, "remote_ref", return_value="intervening-commit"
        ):
            with self.assertRaisesRegex(release.Invalid, "manual recovery"):
                release.check_branch({"baseline": "baseline"}, "signed-commit")

    def test_retained_signed_commit_is_an_allowed_recovery_baseline(self):
        with patch.object(release, "remote_ref", return_value="signed-commit"):
            release.check_branch({"baseline": "baseline"}, "signed-commit")

    def test_release_blockers_are_checked_past_the_first_page(self):
        first = [{"pull_request": {}, "html_url": "pull-request"}] * 100
        second = [
            {
                "html_url": "https://github.com/scylladb/python-driver/is"
                "sues/123"
            }
        ]
        with patch.object(release, "api", side_effect=[first, second]) as api:
            with self.assertRaisesRegex(release.Invalid, "/123"):
                release.blockers()
            self.assertIn("page=2", api.call_args.args[0])

    def test_blocker_api_errors_cannot_allow_publication(self):
        with patch.object(
            release, "api", side_effect=release.Invalid("request failed")
        ):
            with self.assertRaises(release.Invalid):
                release.blockers()
