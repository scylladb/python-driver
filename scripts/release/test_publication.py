"Exercise partial publication recovery without writing to external services."

from contextlib import contextmanager
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import release


class PublicationTests(unittest.TestCase):
    @contextmanager
    def github(self, published=False, immutable=True, partial=False):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            for name in (
                "package.whl",
                "package.whl.asc",
                "SHA256SUMS",
                "SHA256SUMS.asc",
                "publisher.asc",
            ):
                (directory / name).write_text(name)
            state = {
                "tag": "3.29.14-scylla",
                "commit": "commit",
                "tag_object": "signed-tag",
                "version": "3.29.14",
            }
            existing = {
                "id": 1,
                "tag_name": state["tag"],
                "draft": not published,
                "immutable": immutable,
            }
            assets = [
                {"name": p.name, "digest": "sha256:" + release.sha256(p)}
                for p in directory.iterdir()
            ]
            uploaded = assets[:1] if partial else assets.copy()
            events = []

            def api(endpoint, **kwargs):
                events.append(("api", endpoint))
                if endpoint == "releases/tags/" + state["tag"]:
                    return existing.copy()
                if endpoint == "releases/1":
                    return existing.copy()
                raise AssertionError(endpoint)

            def gh(*args, **kwargs):
                events.append(("gh", kwargs["payload"]))
                self.assertEqual(kwargs["payload"], {"draft": False})
                existing["draft"] = False

            def command(*args, **kwargs):
                events.append(("upload", args))
                name = Path(args[4]).name
                uploaded.append(
                    {
                        "name": name,
                        "digest": "sha256:" + release.sha256(directory / name),
                    }
                )

            with (
                patch.object(release, "remote_ref", return_value="signed-tag"),
                patch.object(release, "api", side_effect=api),
                patch.object(release, "gh", side_effect=gh),
                patch.object(
                    release,
                    "pages",
                    side_effect=lambda endpoint: iter(uploaded),
                ),
                patch.object(release, "command", side_effect=command),
            ):
                yield directory, state, existing, uploaded, events

    def test_partial_draft_upload_only_missing_assets_then_publish(self):
        with self.github(partial=True) as (
            directory,
            state,
            existing,
            uploaded,
            events,
        ):
            original = uploaded[0].copy()
            release.github_release(directory, state)
            self.assertEqual(uploaded[0], original)
            self.assertEqual(
                len([event for event in events if event[0] == "upload"]), 4
            )
            self.assertFalse(existing["draft"])
            self.assertEqual(
                [event[0] for event in events][-2:], ["gh", "api"]
            )

    def test_published_immutable_release_is_verification_only(self):
        with self.github(published=True) as (
            directory,
            state,
            existing,
            uploaded,
            events,
        ):
            release.github_release(directory, state)
            self.assertTrue(all(event[0] == "api" for event in events))

    def test_missing_published_asset_or_mutable_release_rejected(self):
        for partial, immutable in ((True, True), (False, False)):
            with self.github(
                published=True, partial=partial, immutable=immutable
            ) as (
                directory,
                state,
                existing,
                uploaded,
                events,
            ):
                with self.assertRaises(release.Invalid):
                    release.github_release(directory, state)
                self.assertTrue(all(event[0] == "api" for event in events))

    def test_changed_existing_signature_rejected_without_replacement(self):
        with self.github() as (directory, state, existing, uploaded, events):
            signature = next(
                a for a in uploaded if a["name"] == "package.whl.asc"
            )
            uploaded[:] = [signature]
            signature["digest"] = "sha256:changed"
            with self.assertRaisesRegex(release.Invalid, "hash differs"):
                release.github_release(directory, state)
            self.assertTrue(all(event[0] == "api" for event in events))

    def test_remote_tag_conflict_stops_github_publication(self):
        with (
            patch.object(release, "remote_ref", return_value="other-tag"),
            patch.object(release, "api") as api,
        ):
            with self.assertRaises(release.Invalid):
                release.github_release(
                    Path("/unused"),
                    {"tag": "3.29.14-scylla", "tag_object": "signed-tag"},
                )
            api.assert_not_called()

    def test_prepublication_missing_uploads_are_exact_retained_bytes(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / "signed"
            (directory / "dist").mkdir(parents=True)
            (directory / "dist/a.whl").write_bytes(b"original")
            (directory / "dist/b.whl").write_bytes(b"already uploaded")
            state = {
                "version": "3.29.14",
                "commit": "signed",
                "tag": "3.29.14-scylla",
                "tag_object": "tag",
                "packages": {
                    p.name: release.sha256(p)
                    for p in (directory / "dist").iterdir()
                },
            }
            uploaded = [
                {
                    "filename": "b.whl",
                    "digests": {"sha256": state["packages"]["b.whl"]},
                }
            ]
            pending = Path(temp) / "pending"
            with (
                patch.object(release, "verify_state", return_value=state),
                patch.object(release, "blockers"),
                patch.object(release, "publisher_ready"),
                patch.object(release, "check_branch"),
                patch.object(release, "api", return_value={"enabled": True}),
                patch.object(release, "remote_ref", return_value=None),
                patch.object(release, "pypi", return_value=uploaded),
                patch.object(release, "output") as output,
            ):
                release.prepublish(directory, pending)
            self.assertEqual([p.name for p in pending.iterdir()], ["a.whl"])
            self.assertEqual((pending / "a.whl").read_bytes(), b"original")
            output.assert_called_once_with(missing="true", commit="signed")

    def test_branch_advancement_stops_before_upload_directory_exists(self):
        with tempfile.TemporaryDirectory() as temp:
            pending = Path(temp) / "pending"
            with (
                patch.object(
                    release, "verify_state", return_value={"commit": "signed"}
                ),
                patch.object(release, "blockers"),
                patch.object(release, "publisher_ready"),
                patch.object(release, "api", return_value={"enabled": True}),
                patch.object(
                    release,
                    "check_branch",
                    side_effect=release.Invalid("master advanced"),
                ),
                patch.object(release, "pypi") as pypi,
            ):
                with self.assertRaises(release.Invalid):
                    release.prepublish(Path("/unused"), pending)
                self.assertFalse(pending.exists())
                pypi.assert_not_called()

    def test_pypi_verification_conflict_stops_immediately(self):
        with (
            patch.object(
                release,
                "pypi",
                return_value=[
                    {"filename": "a.whl", "digests": {"sha256": "other"}}
                ],
            ),
            patch.object(release.time, "sleep") as sleep,
        ):
            with self.assertRaises(release.Invalid):
                release.verify_pypi(
                    {"version": "3.29.14", "packages": {"a.whl": "expected"}}
                )
            sleep.assert_not_called()

    def test_push_is_atomic_and_has_no_force_or_tag_recreation(self):
        state = {
            "baseline": "baseline",
            "commit": "signed",
            "tag": "3.29.14-scylla",
            "tag_object": "signed-tag",
        }
        with (
            patch.object(
                release, "gh", return_value={"login": "scylladb-publisher"}
            ),
            patch.object(release, "check_branch"),
            patch.object(
                release, "remote_ref", side_effect=["signed-tag", "signed-tag"]
            ),
            patch.object(release, "git") as git,
        ):
            release.push(state)
            args = git.call_args.args
            self.assertIn("--atomic", args)
            self.assertNotIn("--force", args)
            self.assertEqual(
                args[-2:],
                (
                    "signed:refs/heads/master",
                    "refs/tags/3.29.14-scylla:refs/tags/3.29.14-scylla",
                ),
            )

    def test_publisher_bypass_is_specific_and_pr_rule_is_preserved(self):
        for members, actors, accepted in [
            (
                [{"login": "scylladb-publisher"}],
                [
                    {
                        "actor_type": "Team",
                        "actor_id": 99,
                        "bypass_mode": "always",
                    }
                ],
                True,
            ),
            (
                [{"login": "scylladb-publisher"}, {"login": "contributor"}],
                [
                    {
                        "actor_type": "Team",
                        "actor_id": 99,
                        "bypass_mode": "always",
                    }
                ],
                False,
            ),
            ([{"login": "scylladb-publisher"}], [], False),
        ]:
            with (
                patch.object(
                    release,
                    "gh",
                    side_effect=[
                        {"login": "scylladb-publisher"},
                        {"id": 99},
                        {"state": "active"},
                        members,
                    ],
                ),
                patch.object(
                    release,
                    "api",
                    side_effect=[
                        [{"type": "pull_request", "ruleset_id": 123}],
                        {"bypass_actors": actors},
                    ],
                ),
            ):
                if accepted:
                    release.publisher_ready()
                else:
                    with self.assertRaises(release.Invalid):
                        release.publisher_ready()

    def test_wrong_publisher_account_cannot_push(self):
        with (
            patch.object(release, "gh", return_value={"login": "contributor"}),
            patch.object(release, "git") as git,
        ):
            with self.assertRaises(release.Invalid):
                release.push({})
            git.assert_not_called()


if __name__ == "__main__":
    unittest.main()
