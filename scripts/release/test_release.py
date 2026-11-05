"""
Release orchestration tests: real archives/Git/signatures, mocked
publication.
"""

from contextlib import contextmanager
from io import BytesIO
import os
from pathlib import Path
import shutil
import tarfile
import tempfile
import unittest
from unittest.mock import patch
import zipfile

import release
import signing
import test_signing

ROOT = Path(__file__).resolve().parents[2]


def metadata_fixture(root):
    files = {
        "cassandra/__init__.py": "__version_info__ = (3, 29, 13)\n",
        "docs/conf.py": (
            "TAGS = [\n    '3.29.13-scylla',\n]\nLATEST_VERSIO"
            "N = '3.29.13-scylla'\n"
        ),
        "docs/installation.rst": 'It should print something like "3.29.13".\n',
        "CHANGELOG.rst": (
            "Unreleased\n==========\n\nBug Fixes\n---------\n* "
            "Fixed an issue.\n\n3.29.13\n=======\n2026-11-02\n"
        ),
    }
    for name, content in files.items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content)


def archives(directory, version="3.29.14"):
    directory.mkdir(parents=True)
    metadata = (
        f"Metadata-Version: 2.4\nName: scylla-driver\n"
        f"Version: {version}\n"
    ).encode()
    wheel = (
        directory
        / f"scylla_driver-{version}-cp313-cp313-manylinux_2_28_x86_64.whl"
    )
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr(
            f"scylla_driver-{version}.dist-info/METADATA", metadata
        )
    source = directory / f"scylla_driver-{version}.tar.gz"
    with tarfile.open(source, "w:gz") as archive:
        member = tarfile.TarInfo(f"scylla_driver-{version}/PKG-INFO")
        member.size = len(metadata)
        archive.addfile(member, BytesIO(metadata))
    return wheel, source


class MetadataTests(unittest.TestCase):
    @contextmanager
    def source(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata_fixture(root)
            yield root

    def test_release_metadata(self):
        with self.source() as root:
            original = (root / "CHANGELOG.rst").read_text()
            release.prepare_metadata(root, "3.29.14", "2026-11-05")
            self.assertIn(
                "__version_info__ = (3, 29, 14)",
                (root / "cassandra/__init__.py").read_text(),
            )
            changelog = (root / "CHANGELOG.rst").read_text()
            self.assertTrue(
                changelog.startswith(
                    "Unreleased\n==========\n\n3.29.14\n=======\n2026-11-05\n"
                )
            )
            self.assertIn(
                original.split("3.29.13\n")[0].split("Bug Fixes")[1], changelog
            )
            self.assertIn(
                "LATEST_VERSION = '3.29.14-scylla'",
                (root / "docs/conf.py").read_text(),
            )
            self.assertIn(
                "    '3.29.14-scylla',\n]", (root / "docs/conf.py").read_text()
            )
            self.assertIn(
                '"3.29.14"', (root / "docs/installation.rst").read_text()
            )

    def test_invalid_and_nonincreasing_versions(self):
        for version in (
            "3.29.13",
            "3.28.0",
            "3.29.14rc1",
            "3.29",
            "03.29.14",
            "3.29.14\nx",
        ):
            with (
                self.subTest(version=version),
                self.source() as root,
                self.assertRaises(release.Invalid),
            ):
                release.prepare_metadata(root, version, "2026-11-05")

    def test_changelog_missing_or_empty(self):
        for text in (
            "No unreleased entries",
            "Unreleased\n==========\n\n3.29.13\n=======\nOld\n",
        ):
            with self.source() as root:
                (root / "CHANGELOG.rst").write_text(text)
                with self.assertRaises(release.Invalid):
                    release.prepare_metadata(root, "3.29.14", "2026-11-05")

    def test_package_metadata_and_filename(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / "dist"
            archives(directory)
            self.assertEqual(
                len(release.package_hashes(directory, "3.29.14")), 2
            )
            with self.assertRaises(release.Invalid):
                release.package_hashes(directory, "3.29.15")
            (directory / "unexpected.txt").write_text("unexpected")
            with self.assertRaises(release.Invalid):
                release.package_hashes(directory, "3.29.14")

    def test_missing_platform_artifact(self):
        with tempfile.TemporaryDirectory() as temp:
            builds = Path(temp) / "builds"
            builds.mkdir()
            with self.assertRaisesRegex(release.Invalid, "platform"):
                release.collect(builds, Path(temp) / "dist", "3.29.14")


class GuardTests(unittest.TestCase):
    def test_blockers_past_first_page(self):
        first = [{"pull_request": {}, "html_url": "pr"}] * 100
        second = [
            {
                "html_url": (
                    "https://github.com/scylladb/python-driver/issues/123"
                )
            }
        ]
        with patch.object(release, "api", side_effect=[first, second]) as api:
            with self.assertRaisesRegex(release.Invalid, "/123"):
                release.blockers()
            self.assertIn("page=2", api.call_args_list[-1].args[0])

    def test_pagination_failure_is_not_absence(self):
        with patch.object(
            release, "api", side_effect=release.Invalid("Network failed")
        ):
            with self.assertRaises(release.Invalid):
                release.blockers()

    def test_branch_advancement_and_recovery(self):
        for branch, signed, accepted in [
            ("baseline", None, True),
            ("release", "release", True),
            ("later", "release", False),
            (None, None, False),
        ]:
            with (
                self.subTest(branch=branch),
                patch.object(release, "remote_ref", return_value=branch),
            ):
                if accepted:
                    release.check_branch({"baseline": "baseline"}, signed)
                else:
                    with self.assertRaisesRegex(
                        release.Invalid, "manual recovery"
                    ):
                        release.check_branch({"baseline": "baseline"}, signed)

    def test_partial_upload_and_conflicts(self):
        expected = {"a.whl": "aaa", "b.whl": "bbb"}
        self.assertEqual(
            release.missing_packages(expected, []), ["a.whl", "b.whl"]
        )
        uploaded = [{"filename": "a.whl", "digests": {"sha256": "aaa"}}]
        self.assertEqual(
            release.missing_packages(expected, uploaded), ["b.whl"]
        )
        for files in (
            [{"filename": "a.whl", "digests": {"sha256": "changed"}}],
            [{"filename": "extra.whl", "digests": {"sha256": "aaa"}}],
            uploaded * 2,
            [dict(uploaded[0], yanked=True)],
        ):
            with self.subTest(files=files), self.assertRaises(release.Invalid):
                release.missing_packages(expected, files)

    def test_restore_expired_or_duplicate_artifacts_rejected(self):
        for artifacts in (
            [{"name": "release-state", "expired": True}],
            [{"name": "release-state", "expired": False}] * 2,
        ):
            with (
                patch.dict(os.environ, GITHUB_RUN_ID="12"),
                patch.object(release, "pages", return_value=iter(artifacts)),
                patch.object(release, "command") as command,
            ):
                with self.assertRaises(release.Invalid):
                    release.restore("release-state", Path("/unused"))
                command.assert_not_called()

    def test_missing_artifact_and_api_error_distinguished(self):
        with (
            patch.dict(os.environ, GITHUB_RUN_ID="12"),
            patch.object(release, "pages", return_value=iter([])),
        ):
            self.assertFalse(release.restore("release-state", Path("/unused")))
        with (
            patch.dict(os.environ, GITHUB_RUN_ID="12"),
            patch.object(
                release, "pages", side_effect=release.Invalid("denied")
            ),
        ):
            with self.assertRaises(release.Invalid):
                release.restore("release-state", Path("/unused"))

    def test_preflight_blocker_stops_upload_preparation(self):
        with (
            patch.object(release, "verify_state", return_value={}),
            patch.object(
                release, "blockers", side_effect=release.Invalid("blocked")
            ),
            patch.object(release, "pypi") as pypi,
        ):
            with self.assertRaises(release.Invalid):
                release.prepublish(Path("/unused"), Path("/unused"))
            pypi.assert_not_called()

    def test_finish_orders_pypi_git_release(self):
        state = {"commit": "signed"}
        for failing in (None, "verify_pypi", "push", "github_release"):
            seen = []

            def operation(name):
                def run(*args):
                    seen.append(name)
                    if name == failing:
                        raise release.Invalid("external failure")

                return run

            with (
                patch.object(release, "verify_state", return_value=state),
                patch.object(
                    release,
                    "verify_pypi",
                    side_effect=operation("verify_pypi"),
                ),
                patch.object(
                    release, "blockers", side_effect=operation("blockers")
                ),
                patch.object(release, "push", side_effect=operation("push")),
                patch.object(
                    release,
                    "github_release",
                    side_effect=operation("github_release"),
                ),
            ):
                if failing:
                    with self.assertRaises(release.Invalid):
                        release.finish(Path("/unused"))
                else:
                    release.finish(Path("/unused"))
                order = ["verify_pypi", "blockers", "push", "github_release"]
                self.assertEqual(
                    seen,
                    order[: order.index(failing) + 1] if failing else order,
                )


# Keep expensive disposable key generation in the existing signing fixture.
class SignedStateTests(test_signing.SigningFixture):
    @contextmanager
    def prepared(self):
        with self.repository() as directory:
            metadata_fixture(Path.cwd())
            release.git("add", ".")
            release.git(
                "-c",
                "user.name=Initial",
                "-c",
                "user.email=initial@example.invalid",
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "Metadata fixture",
            )
            baseline = release.git("rev-parse", "HEAD")
            os.environ.update(
                GITHUB_SHA=baseline,
                GITHUB_RUN_ID="123",
                GITHUB_REF="refs/heads/master",
                GITHUB_REPOSITORY=release.REPOSITORY,
                GITHUB_OUTPUT=str(directory / "outputs"),
            )
            candidate = directory / "candidate"
            with (
                patch.object(release, "blockers"),
                patch.object(release, "pypi", return_value=None),
                patch.object(release, "restore", return_value=False),
                patch.object(
                    release,
                    "remote_ref",
                    side_effect=lambda ref: baseline
                    if ref.endswith("/master")
                    else None,
                ),
            ):
                release.prepare(candidate, "3.29.14")
            # Credential jobs check out the original baseline, without metadata
            # changes.
            release.git("reset", "--hard", baseline)
            builds = directory / "builds"
            builds.mkdir()
            sample = directory / "sample"
            wheel, source = archives(sample)
            for index, name in enumerate(sorted(release.BUILD_ARTIFACTS)):
                target = builds / name
                target.mkdir()
                if name == "source-dist":
                    shutil.copyfile(source, target / source.name)
                else:
                    # Unique wheel tags stand in for the builder's independent
                    # platform artifacts.
                    destination = target / wheel.name.replace(
                        "x86_64.whl", f"platform{index}.whl"
                    )
                    shutil.copyfile(wheel, destination)
            yield directory, candidate, builds

    def test_tested_tree_signed_and_retained_bytes_reused(self):
        with self.prepared() as (directory, candidate, builds):
            state_dir = directory / "signed-state"
            release.create_signed(candidate, builds, state_dir)
            original = {
                p.relative_to(state_dir): p.read_bytes()
                for p in state_dir.rglob("*")
                if p.is_file()
            }
            state = release.verify_state(state_dir)
            self.assertEqual(
                state["tree"],
                release.git("rev-parse", state["candidate"] + "^{tree}"),
            )
            self.assertNotEqual(state["commit"], state["candidate"])
            # Verification and recovery work after the signing credentials are
            # removed.
            del os.environ["RELEASE_GPG_PRIVATE_KEY_FILE"]
            del os.environ["RELEASE_GPG_PASSPHRASE"]
            release.verify_state(state_dir)
            self.assertEqual(
                original,
                {
                    p.relative_to(state_dir): p.read_bytes()
                    for p in state_dir.rglob("*")
                    if p.is_file()
                },
            )
            with self.assertRaisesRegex(release.Invalid, "overwrite"):
                release.create_signed(candidate, builds, state_dir)
            signature = state_dir / "SHA256SUMS.asc"
            original_signature = signature.read_bytes()
            signature.unlink()
            with self.assertRaisesRegex(release.Invalid, "incomplete"):
                release.verify_state(state_dir)
            signature.write_bytes(original_signature)
            package = next((state_dir / "dist").iterdir())
            package.write_bytes(b"altered")
            with self.assertRaises(
                (release.Invalid, zipfile.BadZipFile, tarfile.ReadError)
            ):
                release.verify_state(state_dir)

    def test_resume_preparation_uses_original_candidate_even_after_pypi_upload(
        self,
    ):
        with self.prepared() as (directory, candidate, builds):
            original = (candidate / "candidate.json").read_bytes()
            with (
                patch.object(release, "blockers"),
                patch.object(release, "restore", return_value=True),
                patch.object(release, "pypi") as pypi,
            ):
                release.prepare(candidate, "3.29.14")
                pypi.assert_not_called()
            self.assertEqual(
                original, (candidate / "candidate.json").read_bytes()
            )

    def test_candidate_tag_and_tree_consistency(self):
        with self.prepared() as (directory, candidate, builds):
            info = release.read_json(candidate / "candidate.json")
            for key, value in [
                ("tag", "3.29.15-scylla"),
                ("tree", "0" * 40),
                ("baseline", "0" * 40),
            ]:
                changed = dict(info, **{key: value})
                release.write_json(candidate / "candidate.json", changed)
                with self.assertRaises(release.Invalid):
                    release.load_candidate(candidate)
            release.write_json(candidate / "candidate.json", info)

    def test_initial_launch_rejects_existing_tag_or_pypi_version(self):
        with self.prepared() as (directory, candidate, builds):
            baseline = os.environ["GITHUB_SHA"]
            for tag, published in [("existing-tag", None), (None, [])]:
                with (
                    patch.object(release, "blockers"),
                    patch.object(release, "restore", return_value=False),
                    patch.object(release, "pypi", return_value=published),
                    patch.object(
                        release,
                        "remote_ref",
                        side_effect=lambda ref: baseline
                        if ref.endswith("/master")
                        else tag,
                    ),
                ):
                    with self.assertRaises(release.Invalid):
                        release.prepare(
                            directory / "other-candidate", "3.29.14"
                        )

    def test_nonmaster_launch_rejected_before_network(self):
        with (
            patch.dict(os.environ, GITHUB_REF="refs/heads/branch-3.29"),
            patch.object(release, "blockers") as blockers,
        ):
            with self.assertRaises(release.Invalid):
                release.prepare(Path("/unused"), "3.29.14")
            blockers.assert_not_called()

    def test_signing_failure_removes_temporary_keyrings(self):
        with (
            self.prepared() as (directory, candidate, builds),
            patch.object(
                signing,
                "sign_files",
                side_effect=signing.SigningError("failed"),
            ),
        ):
            with self.assertRaises(signing.SigningError):
                release.create_signed(candidate, builds, directory / "state")
            self.assertFalse(list(directory.glob("release-signing-*")))


if __name__ == "__main__":
    unittest.main()
