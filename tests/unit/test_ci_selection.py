"""Test shared local/CI impact selection without importing the driver."""

import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "select_tests",
    ROOT / "scripts/ci/select_tests.py",
)
selection = importlib.util.module_from_spec(spec)
spec.loader.exec_module(selection)


class SelectionTests(unittest.TestCase):
    def test_non_pr_ci_never_selects_paths(self):
        with patch.object(selection, "select_tests") as select:
            for event in (
                "merge_group", "workflow_dispatch", "push", "workflow_call"
            ):
                with self.subTest(event=event):
                    self.assertEqual(
                        selection.select_ci_tests("base", event)["mode"],
                        "full",
                    )
            select.assert_not_called()

    def test_ordinary_pr_uses_path_selection(self):
        with patch.object(selection, "select_tests") as select:
            select.return_value = selection.plan_tests(["docs/guide.rst"])
            self.assertEqual(
                selection.select_ci_tests(
                    "base", "pull_request",
                )["mode"], "none",
            )
            select.assert_called_once_with("base")

    def test_mixed_driver_and_helper_changes_keep_full_validation(self):
        for paths in (
            ["scripts/scylla_source.py", "cassandra/cluster.py"],
            ["cassandra/cluster.py", "docs/guide.rst"],
            ["tests/unit/test_ci_selection.py", "cassandra/row_parser.pyx"],
        ):
            with self.subTest(paths=paths):
                self.assertEqual(selection.plan_tests(paths)["mode"], "full")

    def test_documentation_only_skips_driver_tests(self):
        plan = selection.plan_tests([
            "docs/conf.py", "docs/pyproject.toml", "docs/guide.rst",
            "CONTRIBUTING.rst", "CHANGELOG.rst", "AGENTS.md", "LICENSE",
            ".github/pull_request_template.md",
            ".github/ISSUE_TEMPLATE/bug.md",
            ".github/workflows/docs-pr.yml",
            ".github/workflows/docs-pages.yml",
            ".gitignore", ".gitattributes", ".github/dependabot.yml",
        ])
        self.assertEqual(plan["mode"], "none")
        self.assertEqual(plan["unit"], [])
        self.assertEqual(plan["integration"], [])
        self.assertFalse(plan["wheels"])
        self.assertFalse(plan["numpy"])

    def test_known_helpers_select_their_tests(self):
        for helper, targets in (
            ("scripts/ci/select_tests.py", [
                "tests/unit/test_ci_selection.py",
            ]),
            ("scripts/scylla_source.py", ["tests/unit/test_scylla_source.py"]),
            ("scripts/release/signing.py", [
                "scripts/release", "tests/unit/test_release_helpers.py",
            ]),
        ):
            with self.subTest(helper=helper):
                plan = selection.plan_tests(["docs/guide.rst", helper])
                self.assertEqual(plan["mode"], "selected")
                self.assertEqual(plan["unit"], targets)
                self.assertEqual(plan["integration"], [])
                self.assertFalse(plan["wheels"])
                self.assertFalse(plan["numpy"])

    def test_numpy_helper_selects_only_numpy_lane(self):
        plan = selection.plan_tests(["scripts/check_numpy_imports.py"])
        self.assertTrue(plan["numpy"])
        self.assertEqual(plan["unit"], [])
        self.assertEqual(plan["integration"], [])
        self.assertFalse(plan["wheels"])

    def test_source_build_shared_and_unknown_changes_require_full_tests(self):
        for path in (
            "cassandra/cluster.py", "cassandra/row_parser.pyx",
            "README.rst", "pyproject.toml", "uv.lock", "setup.py",
            "setup.cfg", "tox.ini", "MANIFEST.in", "new-file",
            "tests/unit/utils.py", "tests/integration/conftest.py",
            "tests/unit/column_encryption/test_policies.py",
            "scripts/coverage.sh", "examples/client.py",
            ".github/workflows/select-tests.yml", "docs-extra/config.py",
        ):
            with self.subTest(path=path):
                plan = selection.plan_tests(["docs/guide.rst", path])
                self.assertEqual(plan["mode"], "full")
                self.assertTrue(plan["wheels"])
                self.assertTrue(plan["numpy"])
                self.assertEqual(
                    plan["unit"], ["tests/unit", "scripts/release"]
                )
                self.assertEqual(plan["integration"], selection.INTEGRATION)

    def test_shared_test_module_requires_full_tests(self):
        plan = selection.plan_tests(["tests/unit/test_control_connection.py"])
        self.assertEqual(plan["mode"], "full")
        self.assertIn("Shared test module", plan["reasons"][0])

    def test_deleted_test_or_missing_mapped_target_requires_full_tests(self):
        self.assertEqual(selection.plan_tests([
            "tests/unit/test_deleted.py",
        ])["mode"], "full")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "scripts").mkdir()
            (root / "scripts/scylla_source.py").touch()
            self.assertEqual(selection.plan_tests([
                "scripts/scylla_source.py",
            ], root)["mode"], "full")

    def test_unknown_base_and_missing_history_run_full_tests(self):
        self.assertEqual(selection.select_tests("")["mode"], "full")
        self.assertEqual(selection.select_tests("0" * 40)["mode"], "full")
        with patch.object(selection, "changed_paths", side_effect=(
            subprocess.CalledProcessError(128, "git diff")
        )):
            self.assertEqual(selection.select_tests("missing")["mode"], "full")

    def test_unit_and_integration_changes_are_combined_and_deduplicated(self):
        plan = selection.plan_tests([
            "tests/unit/test_ci_selection.py", "scripts/ci/select_tests.py",
            "tests/integration/standard/test_authentication.py",
        ])
        self.assertEqual(plan["mode"], "selected")
        self.assertEqual(plan["unit"], ["tests/unit/test_ci_selection.py"])
        self.assertEqual(plan["integration"], [
            "tests/integration/standard/test_authentication.py",
        ])

    def test_relative_imports_mark_test_modules_as_shared(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "tests/unit").mkdir(parents=True)
            (root / "tests/unit/test_shared.py").touch()
            (root / "tests/unit/test_user.py").write_text(
                "from . import test_shared\n",
            )
            plan = selection.plan_tests(["tests/unit/test_shared.py"], root)
            self.assertEqual(plan["mode"], "full")

    def test_recursive_import_inspection_falls_back_to_full_validation(self):
        with patch.object(
            selection, "imported_test_modules",
            side_effect=RecursionError("AST is too deep"),
        ):
            plan = selection.plan_tests(["tests/unit/test_ci_selection.py"])
        self.assertEqual(plan["mode"], "full")
        self.assertEqual(plan["reasons"], [
            "Cannot inspect shared test imports",
        ])


class RunnerTests(unittest.TestCase):
    def setUp(self):
        printing = patch("builtins.print")
        printing.start()
        self.addCleanup(printing.stop)

    def test_empty_plan_never_invokes_pytest(self):
        with patch.object(selection.subprocess, "run") as run:
            self.assertEqual(
                selection.run_tests(selection.plan_tests([]), "all"), 0
            )
            run.assert_not_called()

    def test_runs_only_explicit_targets_and_propagates_failures(self):
        plan = selection.plan_tests(["scripts/scylla_source.py"])
        with patch.object(selection.subprocess, "run") as run:
            run.return_value.returncode = 7
            self.assertEqual(selection.run_tests(plan, "all"), 7)
            args = run.call_args.args[0]
            self.assertIn("tests/unit/test_scylla_source.py", args)
            self.assertNotIn("tests/unit", args)
            self.assertEqual(run.call_count, 1)

    def test_dry_run_and_integration_suite_do_not_run_unit_tests(self):
        plan = selection.plan_tests(["scripts/ci/select_tests.py"])
        with patch.object(selection.subprocess, "run") as run:
            selection.run_tests(plan, "all", dry_run=True)
            selection.run_tests(plan, "integration")
            run.assert_not_called()

    def test_asyncio_test_selects_its_reactor(self):
        plan = selection.plan_tests(["tests/unit/io/test_asyncioreactor.py"])
        with patch.object(selection.subprocess, "run") as run:
            run.return_value.returncode = 0
            selection.run_tests(plan, "unit")
            self.assertEqual(
                run.call_args.kwargs["env"]["EVENT_LOOP_MANAGER"], "asyncio",
            )
            self.assertEqual(run.call_count, 1)

    def test_strict_unit_run_retains_explicit_exclusions_and_failure_status(
        self,
    ):
        plan = selection.plan_tests(["tests/unit/test_ci_selection.py"])
        with (
            patch.dict(os.environ, {"CASS_DRIVER_NO_SKIP": "1"}),
            patch.object(selection.subprocess, "run") as run,
        ):
            run.return_value.returncode = 1
            self.assertEqual(selection.run_tests(plan, "unit"), 1)
            command = run.call_args.args[0]
            self.assertIn("tests/unit/test_ci_selection.py", command)
            self.assertIn("--ignore=tests/unit/column_encryption", command)
            self.assertIn(
                "--ignore=tests/unit/io/test_asyncioreactor.py", command,
            )
            self.assertEqual(command[-2:], [
                "-k", "not test_deserialize_date_range_month",
            ])
            self.assertEqual(run.call_count, 1)

    def test_local_unit_run_without_strict_skips_keeps_all_selected_tests(
        self,
    ):
        plan = selection.plan_tests(["tests/unit/test_ci_selection.py"])
        with (
            patch.dict(os.environ, {}, clear=True),
            patch.object(selection.subprocess, "run") as run,
        ):
            run.return_value.returncode = 0
            self.assertEqual(selection.run_tests(plan, "unit"), 0)
            self.assertEqual(run.call_args.args[0], [
                "uv", "run", "--with", "lz4", "pytest", "-rf",
                "tests/unit/test_ci_selection.py",
            ])
            self.assertNotIn(
                "CASS_DRIVER_NO_SKIP", run.call_args.kwargs["env"],
            )
            self.assertEqual(run.call_count, 1)

    def test_all_suites_run_selected_integration_and_numpy_tests(self):
        plan = {
            "mode": "selected", "unit": [], "numpy": True,
            "integration": [
                "tests/integration/standard/test_authentication.py",
            ],
        }
        with patch.object(selection.subprocess, "run") as run:
            run.return_value.returncode = 0
            selection.run_tests(plan, "all")
            commands = [call.args[0] for call in run.call_args_list]
        self.assertEqual(len(commands), 3)
        self.assertEqual(commands[0][-1], plan["integration"][0])
        self.assertEqual(commands[1][-1], "scripts/check_numpy_imports.py")
        self.assertEqual(commands[2][-2:], selection.NUMPY_TESTS)


class CliTests(unittest.TestCase):
    def test_ci_output_preserves_the_exact_plan_with_unusual_paths(self):
        plan = selection.full_plan("A reason with\na newline")
        plan["unit"] = ["tests/unit/test_line\nbreak.py"]
        stream = io.StringIO()
        with (
            patch.object(
                selection.sys,
                "argv",
                [
                    "select_tests.py",
                    "--github-output",
                    "--plan-json",
                    json.dumps(plan),
                ],
            ),
            contextlib.redirect_stdout(stream),
        ):
            self.assertEqual(selection.main(), 0)
        outputs = dict(
            line.split("=", 1) for line in stream.getvalue().splitlines()
        )
        self.assertEqual(json.loads(outputs["plan"]), plan)
        self.assertEqual(outputs["mode"], "full")
        self.assertEqual(outputs["wheels"], "true")


class WorkflowScopeTests(unittest.TestCase):
    def test_narrow_plans_skip_unrelated_jobs_and_failed_selection_runs_full(
        self,
    ):
        for workflow, job, output, value in (
            ("build-test.yml", "test-wheels-build", "wheels", "false"),
            ("integration-tests.yml", "tests", "integration", "[]"),
            (
                "numpy-import-contract.yml",
                "numpy-import-contract",
                "numpy",
                "false",
            ),
        ):
            with self.subTest(workflow=workflow):
                outputs = {
                    "needs.changes.result": "success",
                    "needs.changes.outputs." + output: value,
                }
                self.assertFalse(
                    self.job_enabled(
                        workflow,
                        job,
                        "pull_request",
                        outputs=outputs,
                    )
                )
                outputs["needs.changes.result"] = "failure"
                self.assertTrue(
                    self.job_enabled(
                        workflow,
                        job,
                        "pull_request",
                        outputs=outputs,
                    )
                )

    @staticmethod
    def job_enabled(workflow, job_name, event, inputs=None, outputs=None):
        inputs = inputs or {}
        outputs = outputs or {}
        text = (ROOT / ".github/workflows" / workflow).read_text()
        job = text.split(f"\n  {job_name}:\n", 1)[1]
        condition = re.search(
            r"    if: (.+?)(?=\n    [a-z])", job, re.S,
        ).group(1).removeprefix(">-").strip()
        condition = condition.replace("github.event_name", repr(event))
        for key in ("run-on-pr", "full-matrix", "ignore-disable-label"):
            condition = condition.replace(
                "inputs." + key, repr(inputs.get(key, False))
            )
        condition = re.sub(
            r"needs\.changes\.(?:result|outputs\.[a-z]+)",
            lambda match: repr(outputs.get(match.group(), "")), condition,
        )
        condition = condition.replace("cancelled()", "False")
        condition = re.sub(r"contains\([^)]*\)", "False", condition)
        condition = condition.replace("&&", " and ").replace("||", " or ")
        condition = re.sub(r"!(?!=)", " not ", condition)
        return eval(" ".join(condition.split()), {"__builtins__": {}}, {})

    def selection_enabled(self, workflow, event, **inputs):
        return self.job_enabled(workflow, "changes", event, inputs)

    def test_non_pr_events_bypass_all_selection_jobs(self):
        for workflow in (
            "build-test.yml", "coverage.yml", "integration-tests.yml",
            "numpy-import-contract.yml",
        ):
            with self.subTest(workflow=workflow):
                self.assertTrue(
                    self.selection_enabled(workflow, "pull_request")
                )
                for event in ("merge_group", "push", "workflow_dispatch"):
                    self.assertFalse(self.selection_enabled(workflow, event))

    def test_pre_release_on_a_pr_bypasses_selection(self):
        self.assertFalse(self.selection_enabled(
            "coverage.yml", "pull_request", **{"run-on-pr": True},
        ))
        for inputs in (
            {"full-matrix": True}, {"ignore-disable-label": True},
        ):
            self.assertFalse(self.selection_enabled(
                "integration-tests.yml", "pull_request", **inputs,
            ))

    def test_full_jobs_run_when_non_pr_selector_is_skipped(self):
        for workflow, job, event in (
            ("build-test.yml", "test-wheels-build", "merge_group"),
            ("coverage.yml", "coverage", "merge_group"),
            ("coverage.yml", "coverage", "push"),
            ("coverage.yml", "coverage", "workflow_dispatch"),
            ("integration-tests.yml", "tests", "workflow_dispatch"),
            (
                "numpy-import-contract.yml", "numpy-import-contract",
                "workflow_dispatch",
            ),
        ):
            with self.subTest(workflow=workflow, event=event):
                self.assertTrue(self.job_enabled(
                    workflow, job, event,
                    outputs={"needs.changes.result": "skipped"},
                ))

    def test_pre_release_full_jobs_run_with_skipped_selectors(self):
        for workflow, job, inputs in (
            ("coverage.yml", "coverage", {"run-on-pr": True}),
            ("integration-tests.yml", "tests", {
                "full-matrix": True, "ignore-disable-label": True,
            }),
        ):
            self.assertTrue(self.job_enabled(
                workflow, job, "pull_request", inputs,
                {"needs.changes.result": "skipped"},
            ))


class GitDiffTests(unittest.TestCase):
    def setUp(self):
        git_environment = {
            key: value for key, value in os.environ.items()
            if not key.startswith("GIT_")
        }
        git_environment.update({
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
        })
        environment = patch.dict(os.environ, git_environment, clear=True)
        environment.start()
        self.addCleanup(environment.stop)

    def test_branch_point_and_dirty_dependency_inspection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args):
                return subprocess.check_output(
                    ["git", "-C", directory, *args], stderr=subprocess.DEVNULL,
                    timeout=30,
                ).decode().strip()

            git("init", "--template=")
            git("config", "user.name", "CI tests")
            git("config", "user.email", "ci@example.com")
            (root / "tests/unit").mkdir(parents=True)
            shared = root / "tests/unit/test_shared.py"
            user = root / "tests/unit/test_user.py"
            shared.write_text("original = True\n")
            user.write_text("from . import test_shared\n")
            git("add", ".")
            git("commit", "-m", "base")
            base = git("rev-parse", "HEAD")
            (root / "cassandra").mkdir()
            (root / "cassandra/module.py").touch()
            git("add", ".")
            git("commit", "-m", "upstream driver change")
            upstream = git("rev-parse", "HEAD")
            git("checkout", "-b", "topic", base)
            shared.write_text("changed = True\n")
            git("add", ".")
            git("commit", "-m", "topic test change")
            with patch.object(selection, "ROOT", root):
                self.assertEqual(
                    selection.changed_paths(upstream),
                    ["tests/unit/test_shared.py"],
                )
                self.assertEqual(
                    selection.select_tests(upstream)["mode"], "full"
                )
                user.write_text("# Local removal of shared import\n")
                plan = selection.select_tests(upstream)
                self.assertEqual(plan["mode"], "full")
                self.assertIn("Working tree", plan["reasons"][0])
                self.assertEqual(
                    selection.select_tests(upstream, worktree=True)["mode"],
                    "selected",
                )

    def test_complete_diff_and_local_worktree_include_all_changed_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args):
                return subprocess.check_output(
                    ["git", "-C", directory, *args], stderr=subprocess.DEVNULL,
                    timeout=30,
                ).decode().strip()

            git("init", "--template=")
            git("config", "user.name", "CI tests")
            git("config", "user.email", "ci@example.com")
            (root / "cassandra").mkdir()
            (root / "cassandra/module.py").write_text("removed = True\n")
            (root / "cassandra/moved.py").write_text("moved = True\n")
            (root / "unstaged.py").write_text("original = True\n")
            git("add", ".")
            git("commit", "-m", "base")
            base = git("rev-parse", "HEAD")
            (root / "docs").mkdir()
            git("mv", "cassandra/moved.py", "docs/moved.py")
            (root / "cassandra/module.py").unlink()
            odd_path = (
                "docs/line break.rst" if sys.platform == "win32"
                else "docs/line\nbreak.rst"
            )
            (root / odd_path).touch()
            for number in range(301):
                (root / f"docs/page-{number}.rst").touch()
            git("add", "-A")
            git("commit", "-m", "move production code into docs")
            (root / "docs/last.rst").touch()
            git("add", ".")
            git("commit", "-m", "last documentation change")
            (root / "docs/staged.rst").touch()
            git("add", ".")
            (root / "docs/last.rst").write_text("unstaged change")
            (root / "unstaged.py").write_text("unstaged = True\n")
            (root / "untracked.py").touch()
            with patch.object(selection, "ROOT", root):
                paths = selection.changed_paths(base)
                local = selection.changed_paths(base, worktree=True)
            for path in (
                "cassandra/moved.py",
                "cassandra/module.py",
                "docs/moved.py",
                odd_path,
                "docs/last.rst",
            ):
                self.assertIn(path, paths)
            self.assertGreater(len(paths), 300)
            self.assertNotIn("docs/staged.rst", paths)
            self.assertNotIn("untracked.py", paths)
            self.assertNotIn("unstaged.py", paths)
            self.assertIn("docs/staged.rst", local)
            self.assertIn("untracked.py", local)
            self.assertIn("unstaged.py", local)
            self.assertEqual(selection.plan_tests(paths)["mode"], "full")


if __name__ == "__main__":
    unittest.main()
