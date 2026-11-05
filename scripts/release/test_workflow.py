"""Evaluate the release job gates for skips, recovery, failure and dry runs."""

from pathlib import Path
import re
import unittest

WORKFLOW = (
    Path(__file__).resolve().parents[2] / ".github/workflows/release.yml"
)


def job_source(job):
    source = WORKFLOW.read_text().split(f"\n  {job}:\n", 1)[1]
    return re.split(r"\n  [a-z][a-z-]*:\n", source, maxsplit=1)[0]


def expression(condition, values, cancelled=False):
    # These job conditions use only literals, equality and boolean operators.
    # actionlint separately checks the complete YAML and GitHub expression
    # syntax.
    condition = (
        condition.removeprefix(">-")
        .strip()
        .removeprefix("${{")
        .removesuffix("}}")
        .strip()
    )
    condition = condition.replace("cancelled()", repr(cancelled))
    condition = re.sub(
        r"needs\.[a-z-]+\.(?:result|outputs\.[a-z-]+)|inputs\.[a-z-]+",
        lambda match: repr(values[match.group()]),
        condition,
    )
    condition = condition.replace("&&", " and ").replace("||", " or ")
    condition = re.sub(r"!(?!=)", " not ", condition)
    return eval(" ".join(condition.split()), {"__builtins__": {}}, {})


def gate(job, values, cancelled=False):
    condition = re.search(
        r"\n    if: (.+?)(?=\n    [a-z-]+:)", job_source(job), re.S
    ).group(1)
    return expression(condition, values, cancelled)


class WorkflowTests(unittest.TestCase):
    def test_signing_requires_all_validation_or_original_retained_state(self):
        baseline = {
            "needs.prepare.result": "success",
            "needs.prepare.outputs.retained": "false",
            "needs.build.result": "success",
            "needs.metadata.result": "success",
            "needs.integration.result": "success",
            "inputs.skip-integration-matrix": False,
        }
        self.assertTrue(gate("sign", baseline))
        for job in ("prepare", "build", "metadata", "integration"):
            for result in ("failure", "cancelled"):
                with self.subTest(job=job, result=result):
                    self.assertFalse(
                        gate(
                            "sign",
                            dict(baseline, **{f"needs.{job}.result": result}),
                        )
                    )
        skipped = dict(baseline, **{"needs.integration.result": "skipped"})
        self.assertFalse(gate("sign", skipped))
        self.assertTrue(
            gate(
                "sign",
                dict(skipped, **{"inputs.skip-integration-matrix": True}),
            )
        )
        retained = dict(
            baseline,
            **{
                "needs.prepare.outputs.retained": "true",
                "needs.build.result": "skipped",
                "needs.metadata.result": "skipped",
                "needs.integration.result": "skipped",
            },
        )
        self.assertTrue(gate("sign", retained))
        self.assertFalse(gate("sign", retained, cancelled=True))
        self.assertFalse(
            gate("sign", dict(retained, **{"needs.prepare.result": "failure"}))
        )

    def test_only_real_releases_use_the_approval_environment(self):
        environment = re.search(
            r"^    environment: (.+)$", job_source("sign"), re.M
        ).group(1)
        self.assertEqual(
            expression(environment, {"inputs.dry-run": True}),
            "release-dry-run",
        )
        self.assertEqual(
            expression(environment, {"inputs.dry-run": False}), "release"
        )
        self.assertIn("    environment: release\n", job_source("publish"))

    def test_dry_runs_preserve_project_and_integration_validation(self):
        for dry_run in (False, True):
            values = {
                "inputs.dry-run": dry_run,
                "inputs.skip-integration-matrix": False,
                "needs.prepare.outputs.retained": "false",
            }
            self.assertTrue(gate("build", values))
            self.assertTrue(gate("integration", values))
            self.assertFalse(
                gate(
                    "integration",
                    dict(values, **{"inputs.skip-integration-matrix": True}),
                )
            )
        self.assertIn(
            "ignore_tests: ${{ inputs.skip-tests }}", job_source("build")
        )
        matrix = WORKFLOW.with_name("release-matrix.yml").read_text()
        self.assertIn("scylla: [LATEST, PRIOR, LTS-LATEST, LTS-PRIOR]", matrix)
        self.assertIn("PROTOCOLS: '3,4'", matrix)
        for override in ("PYTEST_ADDOPTS", "--ignore=", "--deselect="):
            self.assertNotIn(override, matrix)

    def test_dry_runs_and_failures_cannot_publish_or_deploy_docs(self):
        for dry_run in (True, False):
            for result in ("success", "failure", "skipped", "cancelled"):
                for job, dependency in (
                    ("publish", "sign"),
                    ("docs", "publish"),
                ):
                    values = {
                        f"needs.{dependency}.result": result,
                        "inputs.dry-run": dry_run,
                    }
                    with self.subTest(job=job, result=result, dry_run=dry_run):
                        self.assertEqual(
                            gate(job, values),
                            result == "success" and not dry_run,
                        )
                        self.assertFalse(gate(job, values, cancelled=True))


if __name__ == "__main__":
    unittest.main()
