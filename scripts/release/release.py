#!/usr/bin/env python3
"""
Prepare, retain, verify and publish one stable Python driver release.

Run only helpers checked out at the workflow launch SHA. No driver
imports or build commands run in jobs with release credentials. See
README.md for recovery.
"""

import argparse
from datetime import date
from email.parser import BytesParser
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time
from urllib.error import HTTPError
from urllib.request import urlopen
import zipfile

import signing

REPOSITORY = "scylladb/python-driver"
CANDIDATE_REF = "refs/heads/release-candidate"
SIGNED_REF = "refs/heads/release-signed"
BUILD_ARTIFACTS = {
    "source-dist",
    "wheels-linux-ubuntu-24.04",
    "wheels-linux-aarch64-ubuntu-24.04-arm",
    "wheels-macos-x86-macos-15-intel",
    "wheels-macos-arm-macos-15",
    "wheels-windows-windows-2022",
}


class Invalid(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise Invalid(message)


def command(*args, payload=None, env=None):
    result = subprocess.run(
        args,
        input=payload,
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )
    require(
        result.returncode == 0, f"{args[0]} operation failed; output withheld."
    )
    return result.stdout.strip()


def git(*args, **kwargs):
    return command("git", *args, **kwargs)


def gh(*args, payload=None, missing_ok=False):
    result = subprocess.run(
        ["gh", "api", "--hostname", "github.com", "--include", *args],
        input=json.dumps(payload) if payload is not None else None,
        capture_output=True,
        text=True,
        timeout=120,
    )
    headers, separator, body = result.stdout.partition("\n\n")
    status = headers.splitlines()[0].split() if headers else []
    require(
        separator and len(status) > 1 and status[0].startswith("HTTP/"),
        "GitHub request failed; response withheld.",
    )
    if missing_ok and status[1] == "404":
        return None
    require(
        result.returncode == 0 and status[1].startswith("2"),
        "GitHub request failed; response withheld.",
    )
    return json.loads(body) if body else None


def api(endpoint, **kwargs):
    return gh("repos/" + REPOSITORY + "/" + endpoint, **kwargs)


def pages(endpoint, key=None):
    page = 1
    while True:
        value = api(
            endpoint
            + ("&" if "?" in endpoint else "?")
            + f"per_page=100&page={page}"
        )
        items = value[key] if key else value
        yield from items
        if len(items) < 100:
            break
        page += 1


def blockers():
    issues = [
        item
        for item in pages("issues?state=open&labels=release-blocker")
        if "pull_request" not in item
    ]
    require(
        not issues,
        "Open release blockers: "
        + ", ".join(item["html_url"] for item in issues),
    )


def stable(version):
    require(
        re.fullmatch(
            r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)", version
        ),
        "Release version must be a canonical X.Y.Z stable version.",
    )
    return tuple(map(int, version.split(".")))


def pypi(version):
    stable(version)
    try:
        with urlopen(
            f"https://pypi.org/pypi/scylla-driver/{version}/json", timeout=60
        ) as response:
            return json.load(response)["urls"]
    except HTTPError as error:
        if error.code == 404:
            return None
        raise


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def sha256(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def output(**values):
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as stream:
        for key, value in values.items():
            stream.write(f"{key}={value}\n")


def restore(name, directory):
    """
    Artifacts belong to this run, never a previous dispatch or a moving
    branch.
    """
    matches = [
        item
        for item in pages(
            f"actions/runs/{os.environ['GITHUB_RUN_ID']}/artifacts",
            "artifacts",
        )
        if item["name"] == name
    ]
    require(len(matches) <= 1, "Ambiguous recovery artifacts.")
    if not matches:
        return False
    require(
        not matches[0]["expired"],
        "Recovery artifact expired; manual recovery required.",
    )
    command(
        "gh",
        "run",
        "download",
        os.environ["GITHUB_RUN_ID"],
        "--repo",
        REPOSITORY,
        "--name",
        name,
        "--dir",
        str(directory),
    )
    return True


def restore_state(directory, require_retained=False):
    """Recheck this run's checkpoint even when preparation was not rerun."""
    retained = restore("release-state", directory)
    require(
        retained or not require_retained,
        "Expected signed checkpoint is missing; manual recovery required.",
    )
    output(retained=str(retained).lower())


def replace_once(text, pattern, replacement):
    text, count = re.subn(
        pattern, lambda match: replacement(match), text, flags=re.M
    )
    require(count == 1, "Metadata layout changed; update the release helper.")
    return text


def prepare_metadata(root, version, released):
    wanted = stable(version)
    date.fromisoformat(released)
    init = root / "cassandra/__init__.py"
    text = init.read_text()
    current = re.findall(
        r"^__version_info__ = \((\d+), (\d+), (\d+)\)$", text, re.M
    )
    require(
        len(current) == 1 and wanted > tuple(map(int, current[0])),
        "Release must increase the package version.",
    )
    init.write_text(
        replace_once(
            text,
            r"^__version_info__ = .+$",
            lambda m: "__version_info__ = " + str(wanted),
        )
    )
    changelog = root / "CHANGELOG.rst"
    text = changelog.read_text()
    require(
        text.startswith("Unreleased\n==========\n"),
        "Expected leading Unreleased heading.",
    )
    boundary = re.search(r"^\d+\.\d+\.\d+\n=+\n", text, re.M)
    require(
        boundary is not None, "Expected previous stable changelog section."
    )
    entries = text[len("Unreleased\n==========\n") : boundary.start()].strip()
    require(entries, "No Unreleased changelog entries.")
    require(
        not re.search(r"^" + re.escape(version) + r"\n=+", text, re.M),
        "Release changelog section already exists.",
    )
    changelog.write_text(
        "Unreleased\n==========\n\n"
        + version
        + "\n"
        + "=" * len(version)
        + "\n"
        + released
        + "\n\n"
        + entries
        + "\n\n"
        + text[boundary.start() :]
    )
    conf = root / "docs/conf.py"
    text = conf.read_text()
    tag = version + "-scylla"
    text = replace_once(
        text,
        r"^TAGS = \[\n([\s\S]*?)^\]",
        lambda m: m.group(0)[:-1] + f"    '{tag}',\n]",
    )
    text = replace_once(
        text, r"^LATEST_VERSION = .+$", lambda m: f"LATEST_VERSION = '{tag}'"
    )
    conf.write_text(text)
    installation = root / "docs/installation.rst"
    installation.write_text(
        replace_once(
            installation.read_text(),
            r'It should print something like "[^"]+"\.',
            lambda m: f'It should print something like "{version}".',
        )
    )


def validate_candidate(info):
    stable(info["version"])
    require(
        os.environ.get("GITHUB_REPOSITORY") == REPOSITORY
        and os.environ.get("GITHUB_REF") == "refs/heads/master",
        "Unexpected release launch context.",
    )
    require(
        not os.environ.get("RELEASE_VERSION")
        or info["version"] == os.environ["RELEASE_VERSION"],
        "Retained release version differs from dispatch.",
    )
    require(
        info["baseline"] == os.environ["GITHUB_SHA"]
        and info["repository"] == REPOSITORY
        and info["run_id"] == os.environ["GITHUB_RUN_ID"],
        "Candidate launch identity changed.",
    )
    require(
        info["tag"] == info["version"] + "-scylla",
        "Candidate tag/version mismatch.",
    )
    date.fromisoformat(info["date"])


def load_candidate(directory):
    info = read_json(directory / "candidate.json")
    validate_candidate(info)
    git("bundle", "verify", str(directory / "source.bundle"))
    git(
        "fetch",
        str(directory / "source.bundle"),
        CANDIDATE_REF + ":" + CANDIDATE_REF,
    )
    require(
        git("rev-parse", CANDIDATE_REF) == info["candidate"]
        and git("rev-parse", CANDIDATE_REF + "^{tree}") == info["tree"]
        and git("rev-parse", CANDIDATE_REF + "^") == info["baseline"],
        "Candidate bundle identity differs.",
    )
    return info


def prepare(directory, version):
    require(
        os.environ.get("GITHUB_REF") == "refs/heads/master"
        and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY,
        "Launch releases from master upstream.",
    )
    stable(version)
    blockers()
    directory.mkdir(parents=True, exist_ok=True)
    retained = restore("release-state", directory.parent / "retained")
    restored = restore("release-candidate", directory)
    if restored:
        info = load_candidate(directory)
        require(info["version"] == version, "Recovery version changed.")
    else:
        require(
            not retained,
            "Signed state exists without its candidate; "
            "manual recovery "
            "required.",
        )
        baseline = git("rev-parse", "HEAD")
        require(baseline == os.environ["GITHUB_SHA"], "Launch checkout moved.")
        require(
            not git("status", "--porcelain", "--untracked-files=no"),
            "Launch checkout is dirty.",
        )
        tag = version + "-scylla"
        existing = git("tag", "--list", tag)
        require(
            not existing and not remote_ref("refs/tags/" + tag),
            "Release tag already exists.",
        )
        require(
            pypi(version) is None,
            "Release version is already published on PyPI.",
        )
        require(
            all(
                stable(version) > stable(value[:-7])
                for value in git("tag", "--list", "*-scylla").splitlines()
                if re.fullmatch(r"\d+\.\d+\.\d+-scylla", value)
            ),
            "Version must increase existing releases.",
        )
        check_branch({"baseline": baseline})
        released = date.today().isoformat()
        prepare_metadata(Path.cwd(), version, released)
        git(
            "add",
            "cassandra/__init__.py",
            "CHANGELOG.rst",
            "docs/conf.py",
            "docs/installation.rst",
        )
        tree = git("write-tree")
        candidate_env = {
            **os.environ,
            "GIT_AUTHOR_NAME": "ScyllaDB Publisher",
            "GIT_AUTHOR_EMAIL": "publish.code@scylladb.com",
            "GIT_COMMITTER_NAME": "ScyllaDB Publisher",
            "GIT_COMMITTER_EMAIL": "publish.code@scylladb.com",
            "GIT_AUTHOR_DATE": released + "T00:00:00Z",
            "GIT_COMMITTER_DATE": released + "T00:00:00Z",
        }
        candidate = git(
            "-c",
            "commit.gpgsign=false",
            "commit-tree",
            tree,
            "-p",
            baseline,
            payload=f"Release {version}\n",
            env=candidate_env,
        )
        git("update-ref", CANDIDATE_REF, candidate)
        git(
            "bundle", "create", str(directory / "source.bundle"), CANDIDATE_REF
        )
        info = dict(
            repository=REPOSITORY,
            run_id=os.environ["GITHUB_RUN_ID"],
            version=version,
            tag=tag,
            baseline=baseline,
            date=released,
            candidate=candidate,
            tree=tree,
        )
        write_json(directory / "candidate.json", info)
    output(
        version=version,
        retained=str(retained).lower(),
        restored=str(restored).lower(),
    )


def remote_ref(ref):
    value = git("ls-remote", "https://github.com/" + REPOSITORY + ".git", ref)
    require(not value or len(value.splitlines()) == 1, "Ambiguous remote ref.")
    return value.split()[0] if value else None


def check_branch(info, signed=None):
    current = remote_ref("refs/heads/master")
    require(
        current == info["baseline"]
        or (signed is not None and current == signed),
        "master advanced beyond the validated "
        "baseline; manual recovery required. Do not "
        "rebase retained "
        "history.",
    )


def package_hashes(directory, version, signed=False):
    "Read archive metadata without importing driver code or extracting files."
    files = sorted(
        path
        for path in directory.iterdir()
        if not (signed and path.name.endswith(".asc"))
    )
    require(
        files
        and all(
            p.is_file()
            and not p.is_symlink()
            and p.name.endswith((".whl", ".tar.gz"))
            for p in files
        ),
        "Unexpected or missing package files.",
    )
    require(
        sum(p.name.endswith(".tar.gz") for p in files) == 1,
        "Expected exactly one source distribution.",
    )
    require(any(p.name.endswith(".whl") for p in files), "No wheels found.")
    result = {}
    for path in files:
        if path.name.endswith(".whl"):
            with zipfile.ZipFile(path) as archive:
                names = [
                    name
                    for name in archive.namelist()
                    if name.endswith(".dist-info/METADATA")
                ]
                require(len(names) == 1, "Wheel metadata is ambiguous.")
                content = archive.read(names[0])
            require(
                path.name.startswith("scylla_driver-" + version + "-"),
                "Wheel filename version differs.",
            )
        else:
            require(
                path.name
                in (
                    f"scylla_driver-{version}.tar.gz",
                    f"scylla-driver-{version}.tar.gz",
                ),
                "Source filename version differs.",
            )
            with tarfile.open(path) as archive:
                members = [
                    m
                    for m in archive.getmembers()
                    if m.name.count("/") == 1
                    and m.name.endswith("/PKG-INFO")
                    and m.isfile()
                ]
                require(len(members) == 1, "Source metadata is ambiguous.")
                content = archive.extractfile(members[0]).read()
        metadata = BytesParser().parsebytes(content)
        require(
            metadata["Name"] == "scylla-driver"
            and metadata["Version"] == version,
            "Package metadata name/version differs.",
        )
        result[path.name] = sha256(path)
    return result


def collect(builds, destination, version):
    require(
        {path.name for path in builds.iterdir()} == BUILD_ARTIFACTS,
        "Incomplete platform artifacts.",
    )
    destination.mkdir(parents=True, exist_ok=False)
    for directory in builds.iterdir():
        files = list(directory.iterdir())
        require(
            files
            and all(
                p.is_file()
                and p.name.endswith(
                    ".tar.gz" if directory.name == "source-dist" else ".whl"
                )
                for p in files
            ),
            "Empty or unexpected build artifact.",
        )
        for path in files:
            require(
                not (destination / path.name).exists(),
                "Duplicate distribution filename.",
            )
            shutil.copyfile(path, destination / path.name)
    package_hashes(destination, version)


def create_signed(candidate_dir, builds, directory):
    info = load_candidate(candidate_dir)
    require(not directory.exists(), "Refusing to overwrite signed state.")
    directory.mkdir()
    shutil.copyfile(
        candidate_dir / "candidate.json", directory / "candidate.json"
    )
    collect(builds, directory / "dist", info["version"])
    hashes = package_hashes(directory / "dist", info["version"])
    require(
        not git("tag", "--list", info["tag"]),
        "Local release tag exists; restore signed state instead.",
    )
    with signing.signing_session() as env:
        # commit-tree signs the exact tested tree, without checking out or
        # executing candidate code.
        signed = git(
            "commit-tree",
            "-S" + signing.SIGNING + "!",
            info["tree"],
            "-p",
            info["baseline"],
            payload=f"Release {info['version']}\n",
            env=env,
        )
        git("update-ref", SIGNED_REF, signed)
        git(
            "tag",
            "-s",
            info["tag"],
            signed,
            "-m",
            f"Release {info['version']}",
            env=env,
        )
        git(
            "bundle",
            "create",
            str(directory / "signed.bundle"),
            SIGNED_REF,
            "refs/tags/" + info["tag"],
        )
        state = dict(
            info,
            commit=signed,
            tag_object=git("rev-parse", "refs/tags/" + info["tag"]),
            packages=hashes,
        )
        write_json(directory / "release.json", state)
        shutil.copyfile(signing.PUBLIC_KEY, directory / "publisher.asc")
        files = sorted((directory / "dist").iterdir())
        files += [
            directory / name
            for name in (
                "candidate.json",
                "release.json",
                "signed.bundle",
                "publisher.asc",
            )
        ]
        manifest = directory / "SHA256SUMS"
        manifest.write_text(
            "".join(f"{sha256(path)}  {path.name}\n" for path in files)
        )
        signing.sign_files(
            [
                *(path for path in files if path.name != "publisher.asc"),
                manifest,
            ],
            env,
        )
    verify_state(directory)


def verify_state(directory):
    state = read_json(directory / "release.json")
    validate_candidate(state)
    require(
        read_json(directory / "candidate.json")
        == {
            key: state[key]
            for key in (
                "repository",
                "run_id",
                "version",
                "tag",
                "baseline",
                "date",
                "candidate",
                "tree",
            )
        },
        "Retained candidate identity differs.",
    )
    require(
        (directory / "publisher.asc").read_bytes()
        == signing.PUBLIC_KEY.read_bytes(),
        "Retained certificate differs from launch certificate.",
    )
    require(
        package_hashes(directory / "dist", state["version"], signed=True)
        == state["packages"],
        "Retained distributions changed.",
    )
    expected = {
        "candidate.json",
        "release.json",
        "signed.bundle",
        "publisher.asc",
    }
    expected |= {"dist/" + name for name in state["packages"]}
    manifest = directory / "SHA256SUMS"
    lines = manifest.read_text().splitlines()
    require(len(lines) == len(expected), "Incomplete checksum manifest.")
    manifest_paths = {Path(name).name: name for name in expected}
    require(
        len(manifest_paths) == len(expected), "Duplicate checksum basename."
    )
    seen = set()
    for line in lines:
        digest, separator, name = line.partition("  ")
        require(
            separator and name in manifest_paths and name not in seen,
            "Unexpected checksum entry.",
        )
        require(
            digest == sha256(directory / manifest_paths[name]),
            "Checksum mismatch.",
        )
        seen.add(name)
    require(seen == set(manifest_paths), "Missing checksum entry.")
    expected.add("SHA256SUMS")
    signed_files = expected - {"publisher.asc"}
    actual = {
        p.relative_to(directory).as_posix()
        for p in directory.rglob("*")
        if p.is_file()
    }
    require(
        actual == expected | {name + ".asc" for name in signed_files}
        and not any(p.is_symlink() for p in directory.rglob("*")),
        "Signed artifact set is incomplete.",
    )
    with signing.public_environment() as env:
        for name in sorted(signed_files):
            signing.verify_status(
                signing.gpg(
                    "--status-fd",
                    "1",
                    "--verify",
                    directory / (name + ".asc"),
                    directory / name,
                    env=env,
                )
            )
    git("bundle", "verify", str(directory / "signed.bundle"))
    git(
        "fetch",
        str(directory / "signed.bundle"),
        SIGNED_REF + ":" + SIGNED_REF,
        "refs/tags/" + state["tag"] + ":refs/tags/" + state["tag"],
    )
    require(
        git("rev-parse", SIGNED_REF) == state["commit"]
        and git("rev-parse", SIGNED_REF + "^{tree}") == state["tree"]
        and git("rev-parse", SIGNED_REF + "^") == state["baseline"]
        and git("rev-parse", "refs/tags/" + state["tag"])
        == state["tag_object"],
        "Retained signed history differs from tested candidate.",
    )
    for field in ("%an", "%cn"):
        require(
            git("show", "-s", "--format=" + field, SIGNED_REF)
            == "ScyllaDB Publisher",
            "Unexpected Git identity.",
        )
    for field in ("%ae", "%ce"):
        require(
            git("show", "-s", "--format=" + field, SIGNED_REF)
            == "publish.code@scylladb.com",
            "Unexpected Git identity.",
        )
    signing.verify_commit(state["commit"])
    signing.verify_tag(state["tag"], state["commit"])
    return state


def missing_packages(expected, uploaded):
    found = {}
    for item in uploaded or []:
        name = item["filename"]
        require(
            name in expected and name not in found,
            "Unexpected PyPI distribution filename.",
        )
        require(
            item["digests"]["sha256"] == expected[name]
            and not item.get("yanked"),
            "PyPI distribution hash differs or file is "
            "yanked; never overwrite "
            "it.",
        )
        found[name] = item
    return sorted(set(expected) - found.keys())


def publisher_ready():
    require(
        gh("user")["login"] == "scylladb-publisher",
        "Use the scylladb-publisher account credential.",
    )
    team = gh("orgs/scylladb/teams/python-driver-publisher")
    membership = gh(
        "orgs/scylladb/teams/python-driver-publisher/me"
        "mberships/scylladb-publisher"
    )
    require(
        membership["state"] == "active",
        "Publisher must belong to its dedicated bypass team.",
    )
    members = gh(
        "orgs/scylladb/teams/python-driver-publisher/members?per_page=100"
    )
    require(
        [member["login"] for member in members] == ["scylladb-publisher"],
        "The publisher bypass team must contain only scylladb-publisher.",
    )
    rules = api("rules/branches/master")
    pr_rules = [rule for rule in rules if rule["type"] == "pull_request"]
    require(
        pr_rules,
        "Keep the master pull request requirement for other contributors.",
    )
    for rule in pr_rules:
        ruleset = api(f"rulesets/{rule['ruleset_id']}")
        require(
            any(
                actor["actor_type"] == "Team"
                and actor["actor_id"] == team["id"]
                and actor["bypass_mode"] == "always"
                for actor in ruleset["bypass_actors"]
            ),
            "Configure the publisher-only team bypass "
            "before PyPI publication.",
        )


def prepublish(directory, pending):
    state = verify_state(directory)
    blockers()
    publisher_ready()
    require(
        api("immutable-releases")["enabled"] is True,
        "Enable repository release immutability before publication.",
    )
    check_branch(state, state["commit"])
    remote_tag = remote_ref("refs/tags/" + state["tag"])
    require(
        remote_tag in (None, state["tag_object"]),
        "Remote release tag conflicts.",
    )
    missing = missing_packages(state["packages"], pypi(state["version"]))
    pending.mkdir(exist_ok=False)
    for name in missing:
        shutil.copyfile(directory / "dist" / name, pending / name)
    output(missing=str(bool(missing)).lower(), commit=state["commit"])


def verify_pypi(state):
    for attempt in range(12):
        missing = missing_packages(state["packages"], pypi(state["version"]))
        if not missing:
            return
        time.sleep(5)
    raise Invalid(
        "PyPI files are incomplete; rerun this run to "
        "upload only missing "
        "files."
    )


def push(state):
    require(
        gh("user")["login"] == "scylladb-publisher",
        "Use the scylladb-publisher account credential.",
    )
    check_branch(state, state["commit"])
    require(
        remote_ref("refs/tags/" + state["tag"]) in (None, state["tag_object"]),
        "Remote tag conflicts.",
    )
    git(
        "-c",
        "credential.helper=",
        "-c",
        "credential.helper=!gh auth git-credential",
        "push",
        "--atomic",
        "https://github.com/" + REPOSITORY + ".git",
        state["commit"] + ":refs/heads/master",
        "refs/tags/" + state["tag"] + ":refs/tags/" + state["tag"],
    )
    require(
        remote_ref("refs/tags/" + state["tag"]) == state["tag_object"],
        "Remote tag identity differs.",
    )


def github_release(directory, state):
    require(
        remote_ref("refs/tags/" + state["tag"]) == state["tag_object"],
        "Remote tag identity differs.",
    )
    existing = api("releases/tags/" + state["tag"], missing_ok=True)
    if existing is None:
        existing = gh(
            "repos/" + REPOSITORY + "/releases",
            "--method",
            "POST",
            "--input",
            "-",
            payload={
                "tag_name": state["tag"],
                "target_commitish": state["commit"],
                "name": state["tag"],
                "body": f"ScyllaDB Python driver {state['version']}.\n"
                "See CHANGELOG.rst for release notes. Verify "
                "assets with publisher.asc and "
                "SHA256SUMS.",
                "draft": True,
                "prerelease": False,
            },
        )
    require(
        existing["tag_name"] == state["tag"], "GitHub release tag differs."
    )
    assets = {p.name: p for p in directory.rglob("*") if p.is_file()}
    require(
        len(assets) == len([p for p in directory.rglob("*") if p.is_file()]),
        "Duplicate release asset basename.",
    )
    endpoint = f"releases/{existing['id']}"
    remote_assets = list(pages(endpoint + "/assets"))
    require(
        len(remote_assets) == len({item["name"] for item in remote_assets}),
        "Duplicate remote assets.",
    )
    for item in remote_assets:
        require(item["name"] in assets, "Unexpected GitHub release asset.")
        # Digest is mandatory: never replace or accept an unverified existing
        # asset.
        require(
            item.get("digest") == "sha256:" + sha256(assets[item["name"]]),
            "GitHub asset hash differs.",
        )
    missing = sorted(set(assets) - {item["name"] for item in remote_assets})
    if existing["draft"]:
        for name in missing:
            command(
                "gh",
                "release",
                "upload",
                state["tag"],
                str(assets[name]),
                "--repo",
                REPOSITORY,
            )
        uploaded = list(pages(endpoint + "/assets"))
        require(
            len(uploaded) == len(assets)
            and all(
                item["name"] in assets
                and item.get("digest")
                == "sha256:" + sha256(assets[item["name"]])
                for item in uploaded
            ),
            "Draft release assets are incomplete or changed.",
        )
        gh(
            "repos/" + REPOSITORY + "/" + endpoint,
            "--method",
            "PATCH",
            "--input",
            "-",
            payload={"draft": False},
        )
        existing = api(endpoint)
    require(
        not missing or existing["draft"] is False,
        "Published release has missing assets.",
    )
    require(
        existing.get("immutable") is True and existing["draft"] is False,
        "GitHub release must be immutable before docs deployment.",
    )
    # Recheck assets after publishing, including already-published recovery.
    uploaded = list(pages(endpoint + "/assets"))
    require(
        {item["name"] for item in uploaded} == set(assets)
        and all(
            item.get("digest") == "sha256:" + sha256(assets[item["name"]])
            for item in uploaded
        ),
        "Published release assets differ.",
    )
    require(
        remote_ref("refs/tags/" + state["tag"]) == state["tag_object"],
        "Remote tag identity differs.",
    )


def finish(directory):
    state = verify_state(directory)
    verify_pypi(state)
    blockers()
    push(state)
    github_release(directory, state)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "operation",
        choices=[
            "prepare",
            "restore",
            "collect",
            "sign",
            "verify",
            "prepublish",
            "finish",
        ],
    )
    parser.add_argument(
        "--candidate",
        type=Path,
        default=Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "candidate",
    )
    parser.add_argument(
        "--state",
        type=Path,
        default=Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "signed-state",
    )
    parser.add_argument(
        "--builds",
        type=Path,
        default=Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "builds",
    )
    parser.add_argument(
        "--pending",
        type=Path,
        default=Path(os.environ.get("RUNNER_TEMP", "/tmp")) / "pending",
    )
    parser.add_argument("--version", default=os.environ.get("RELEASE_VERSION"))
    parser.add_argument("--require-retained", action="store_true")
    args = parser.parse_args()
    try:
        if args.operation == "prepare":
            prepare(args.candidate, args.version)
        elif args.operation == "restore":
            restore_state(args.state, args.require_retained)
        elif args.operation == "collect":
            collect(args.builds, args.pending, args.version)
        elif args.operation == "sign":
            create_signed(args.candidate, args.builds, args.state)
        elif args.operation == "verify":
            verify_state(args.state)
        elif args.operation == "prepublish":
            prepublish(args.state, args.pending)
        elif args.operation == "finish":
            finish(args.state)
        return 0
    except (Invalid, signing.SigningError) as error:
        print(str(error), file=sys.stderr)
    except Exception:
        print(
            "Release operation failed; output withheld. "
            "Inspect retained artifacts before "
            "recovery.",
            file=sys.stderr,
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())
