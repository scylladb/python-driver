"""Plan and run tests impacted by a Git change, locally or in CI."""

import argparse
import ast
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
import warnings


ROOT = Path(__file__).resolve().parents[2]
SUPPORT_FILES = {
    ".gitattributes", ".gitignore", ".github/dependabot.yml",
    "CONTRIBUTING.rst", "CHANGELOG.rst", "LICENSE",
    ".github/workflows/docs-pr.yml", ".github/workflows/docs-pages.yml",
}
INTEGRATION = ["tests/integration/standard", "tests/integration/cqlengine"]
NUMPY_TESTS = [
    "tests/unit/test_protocol_imports.py",
    "tests/unit/test_numpy_import_contract.py",
]
HELPER_TESTS = {
    "scripts/ci/select_tests.py": ["tests/unit/test_ci_selection.py"],
    "scripts/scylla_source.py": ["tests/unit/test_scylla_source.py"],
    "scripts/check_numpy_imports.py": NUMPY_TESTS,
    "scripts/release/release.py": [
        "tests/unit/test_release_helpers.py", "scripts/release",
    ],
    "scripts/release/signing.py": [
        "tests/unit/test_release_helpers.py", "scripts/release",
    ],
}


def full_plan(reason):
    return {
        "mode": "full", "wheels": True, "numpy": True,
        "unit": ["tests/unit", "scripts/release"],
        "integration": INTEGRATION.copy(),
        "reasons": [reason],
    }


def imported_test_modules(root):
    """Treat test modules imported elsewhere as shared test infrastructure."""
    imported = set()
    for path in (root / "tests").rglob("*.py"):
        package = ".".join(path.relative_to(root).with_suffix("").parts[:-1])
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", SyntaxWarning)
            tree = ast.parse(path.read_bytes(), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if node.level:
                    module = importlib.util.resolve_name(
                        "." * node.level + module, package,
                    )
                imported.add(module)
                imported.update(
                    module + "." + alias.name for alias in node.names
                )
    return imported


def plan_tests(paths, root=None):
    root = ROOT if root is None else root
    unit, integration, reasons = set(), set(), []
    numpy = False
    imported = None
    for path in paths:
        file = PurePosixPath(path)
        if (
            path in SUPPORT_FILES
            or path.startswith("docs/")
            or file.suffix == ".md"
        ):
            continue
        targets = HELPER_TESTS.get(path)
        if targets is not None:
            if not (root / path).is_file():
                return full_plan(f"Deleted helper: {path}")
            for target in targets:
                if not (root / target).exists():
                    return full_plan(f"Missing mapped tests: {target}")
            if targets == NUMPY_TESTS:
                numpy = True
            else:
                unit.update(targets)
            reasons.append(f"Mapped helper: {path}")
        elif (
            file.name.startswith("test_") and file.suffix == ".py"
            and (path.startswith("tests/unit/") or any(
                path.startswith(directory + "/") for directory in INTEGRATION
            ) or path.startswith("scripts/release/"))
            and not path.startswith("tests/unit/cython/")
            and not path.startswith("tests/unit/column_encryption/")
        ):
            if not (root / path).is_file():
                return full_plan(f"Deleted test module: {path}")
            if imported is None:
                try:
                    imported = imported_test_modules(root)
                except (
                    SyntaxError, ValueError, ImportError, OSError,
                    RecursionError,
                ):
                    return full_plan("Cannot inspect shared test imports")
            module = ".".join(file.with_suffix("").parts)
            if module in imported:
                return full_plan(f"Shared test module: {path}")
            if path in NUMPY_TESTS:
                numpy = True
            elif path.startswith("tests/integration/"):
                integration.add(path)
            elif path.startswith("scripts/release/"):
                unit.update(
                    ["tests/unit/test_release_helpers.py", "scripts/release"]
                )
            else:
                unit.add(path)
            reasons.append(f"Changed test: {path}")
        else:
            return full_plan(f"Shared, production or unmapped path: {path}")
    return {
        "mode": "selected" if unit or integration or numpy else "none",
        "wheels": False, "numpy": numpy,
        "unit": sorted(unit), "integration": sorted(integration),
        "reasons": reasons or ["Documentation/support changes only"],
    }


def changed_paths(base, worktree=False):
    base = (
        subprocess.check_output(
            ["git", "merge-base", base, "HEAD"],
            cwd=ROOT,
        )
        .decode()
        .strip()
    )
    # No rename detection: include deleted production sources in a move.
    args = ["git", "diff", "--no-renames", "--name-only", "-z", base]
    if not worktree:
        args.append("HEAD")
    result = subprocess.check_output([*args, "--"], cwd=ROOT)
    paths = [os.fsdecode(path) for path in result.split(b"\0") if path]
    if worktree:
        result = subprocess.check_output(
            [
                "git",
                "ls-files",
                "--others",
                "--exclude-standard",
                "-z",
            ],
            cwd=ROOT,
        )
        paths.extend(os.fsdecode(path) for path in result.split(b"\0") if path)
    return sorted(set(paths))


def select_tests(base, worktree=False):
    if not base or set(base) == {"0"}:
        return full_plan("No comparison base")
    try:
        # Import inspection uses local files. A dirty checkout cannot establish
        # the committed dependency graph, so keep validation conservative.
        if not worktree and subprocess.check_output(
            ["git", "status", "--porcelain"],
            cwd=ROOT,
        ):
            return full_plan("Working tree differs from committed HEAD")
        return plan_tests(changed_paths(base, worktree))
    except (subprocess.CalledProcessError, OSError):
        return full_plan("Comparison history unavailable")


def select_ci_tests(base, event_name):
    """Narrow automatic validation only for ordinary pull-request runs."""
    if event_name != "pull_request":
        return full_plan("CI selection is limited to ordinary pull requests")
    return select_tests(base)


def run_tests(plan, suite, dry_run=False):
    """Run explicit targets; never invoke pytest with an empty selection."""
    commands = []
    if suite in ("all", "unit") and plan["unit"]:
        normal = [p for p in plan["unit"] if p not in {
            "tests/unit/io/test_asyncioreactor.py",
            "tests/unit/io/test_asyncorereactor.py",
        }]
        full = plan["mode"] == "full"
        if normal:
            command = ["uv", "run", "--with", "lz4"]
            if full:
                command.extend(["--with", "pyasyncore"])
            command.extend(["pytest", "-rf", *normal])
            if full or os.environ.get("CASS_DRIVER_NO_SKIP"):
                command.extend([
                    "--ignore=tests/unit/io/test_asyncioreactor.py",
                    "--ignore=tests/unit/column_encryption",
                    "-k", "not test_deserialize_date_range_month",
                ])
            commands.append(({}, command))
        for path, reactor in (
            ("tests/unit/io/test_asyncioreactor.py", "asyncio"),
            ("tests/unit/io/test_asyncorereactor.py", "asyncore"),
        ):
            if path in plan["unit"] or (full and reactor == "asyncio"):
                commands.append(({"EVENT_LOOP_MANAGER": reactor}, [
                    "uv", "run", "--with", "pyasyncore", "pytest", "-rf", path,
                ]))
    if suite in ("all", "integration") and plan["integration"]:
        commands.append(
            ({}, ["uv", "run", "pytest", "-v", "-rf", *plan["integration"]])
        )
    if suite in ("all", "numpy") and plan["numpy"]:
        commands.extend([
            ({}, [sys.executable, "scripts/check_numpy_imports.py"]),
            ({}, ["uv", "run", "pytest", "-rf", *NUMPY_TESTS]),
        ])
    status = 0
    for env, command in commands:
        print(json.dumps({"env": env, "command": command}), flush=True)
        if not dry_run:
            result = subprocess.run(
                command, cwd=ROOT, env={**os.environ, **env}
            )
            status = result.returncode or status
    return status


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="origin/master")
    parser.add_argument(
        "--worktree", action="store_true",
        help="Include staged, unstaged and untracked files",
    )
    parser.add_argument(
        "--full", action="store_true", help="Force full validation"
    )
    parser.add_argument("--github-output", action="store_true")
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--suite", choices=("all", "unit", "integration", "numpy"),
        default="all",
    )
    parser.add_argument(
        "--plan-json", help="Consume the exact plan supplied by CI"
    )
    args = parser.parse_args()
    if args.full:
        plan = full_plan("Full validation explicitly requested")
    elif args.plan_json:
        plan = json.loads(args.plan_json)
    elif args.github_output:
        plan = select_ci_tests(args.base, os.environ.get("GITHUB_EVENT_NAME"))
    else:
        plan = select_tests(args.base, args.worktree)
    if args.github_output:
        print("plan=" + json.dumps(plan, separators=(",", ":")))
        for key in ("mode", "wheels", "numpy"):
            value = plan[key]
            print(f"{key}={str(value).lower()}")
        for key in ("unit", "integration"):
            print(f"{key}=" + json.dumps(plan[key], separators=(",", ":")))
    else:
        print(json.dumps(plan, indent=2))
    if args.run or args.dry_run:
        return run_tests(plan, args.suite, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
