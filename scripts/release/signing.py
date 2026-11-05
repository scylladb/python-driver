#!/usr/bin/env python3
"""Configure and verify the repository's protected release signing subkey."""

from contextlib import contextmanager
import os
from pathlib import Path
import shlex
import signal
import subprocess
import sys
import tempfile
import time

PRIMARY = "786703A4F7AC1E986C7CFB2318ED6CC7EBE7C5F9"
SIGNING = "7C0A5E8366A579484D1D352C2EC9B987C56EF1CA"
PUBLIC_KEY = Path(__file__).with_name("publisher.asc")
TERMINATION_GRACE = 2


class SigningError(ValueError):
    pass


def command_operation(command):
    # Only fixed labels enter diagnostics; argv and child output may contain
    # secrets.
    if command[0] == "gpg":
        for flag, label in [
            ("--import", "key import"),
            ("--detach-sign", "signature creation"),
            ("--verify", "signature verification"),
            ("--list-secret-keys", "secret-key inspection"),
        ]:
            if flag in command:
                return "GPG " + label
        return "GPG operation"
    if command[0] == "gpgconf":
        return "GPG agent shutdown"
    if command[0] == "git":
        for subcommand in [
            "verify-commit",
            "verify-tag",
            "check-ref-format",
            "cat-file",
            "rev-parse",
        ]:
            if subcommand in command:
                return "Git " + subcommand
        return "Git operation"
    return "command"


def invoke(command, **kwargs):
    operation = command_operation(command)
    try:
        return subprocess.run(command, **kwargs)
    except subprocess.TimeoutExpired:
        raise SigningError(operation + " timed out.") from None
    except OSError:
        raise SigningError(
            operation + " could not start (OS error)."
        ) from None


def run(command, env=None, **kwargs):
    result = invoke(
        command, env=env, capture_output=True, text=True, timeout=60, **kwargs
    )
    if result.returncode:
        raise SigningError(
            f"{command_operation(command)} failed "
            f"(exit status {result.returncode})."
        )
    # Git writes --raw verification status to stderr; ordinary callers parse
    # stdout.
    if (
        command[0] == "git"
        and "--raw" in command
        and any(
            operation in command
            for operation in ("verify-commit", "verify-tag")
        )
    ):
        return result.stdout + result.stderr
    return result.stdout


def gpg(*args, env=None):
    return run(
        [
            "gpg",
            "--no-options",
            "--batch",
            "--no-tty",
            "--no-auto-key-retrieve",
            *map(str, args),
        ],
        env,
    )


def verify_status(status):
    records = [
        line.split()
        for line in status.splitlines()
        if line.startswith("[GNUPG:] ")
    ]
    signatures = [
        row for row in records if len(row) > 1 and row[1] == "VALIDSIG"
    ]
    good = [row for row in records if len(row) > 1 and row[1] == "GOODSIG"]
    rejected = {
        "EXPKEYSIG",
        "REVKEYSIG",
        "EXPSIG",
        "BADSIG",
        "ERRSIG",
        "FAILURE",
        "ERROR",
        "KEYEXPIRED",
        "KEYREVOKED",
        "SIGEXPIRED",
    }
    if (
        len(signatures) != 1
        or len(signatures[0]) != 12
        or signatures[0][2] != SIGNING
        or signatures[0][-1] != PRIMARY
        or len(good) != 1
        or len(good[0]) < 3
        or good[0][2] != SIGNING[-16:]
        or any(len(row) > 1 and row[1] in rejected for row in records)
    ):
        raise SigningError(
            "Signature does not match the repository's release subkey."
        )


def protected_subkey(env):
    rows = [
        line.split(":")
        for line in gpg(
            "--with-colons", "--with-keygrip", "--list-secret-keys", env=env
        ).splitlines()
    ]
    fingerprints = [row[9] for row in rows if row[0] == "fpr"]
    keys = [row for row in rows if row[0] in ("sec", "ssb")]
    grips = [row[9] for row in rows if row[0] == "grp"]
    if (
        fingerprints != [PRIMARY, SIGNING]
        or len(keys) != 2
        or len(grips) != 2
        or keys[0][14] != "#"
        or keys[1][14] != "+"
    ):
        raise SigningError(
            "Import only this repository's signing-private.asc release subkey."
        )
    keydir = Path(env["GNUPGHOME"]) / "private-keys-v1.d"
    if (
        keydir / (grips[0] + ".key")
    ).exists() or b"(protected-private-key" not in (
        keydir / (grips[1] + ".key")
    ).read_bytes():
        raise SigningError(
            "Release signing requires a protected subkey "
            "without the primary "
            "secret."
        )


def write_private(path, text, executable=False):
    with open(
        path, "x", opener=lambda name, flags: os.open(name, flags, 0o600)
    ) as output:
        output.write(text)
    if executable:
        path.chmod(0o700)


@contextmanager
def public_environment():
    with tempfile.TemporaryDirectory(prefix="release-public-") as temp:
        env = {**os.environ, "GNUPGHOME": temp}
        try:
            gpg("--import", PUBLIC_KEY, env=env)
            yield env
        finally:
            run(["gpgconf", "--homedir", temp, "--kill", "gpg-agent"])


def passphrase_value(env):
    value = env.get(
        "RELEASE_GPG_PASSPHRASE",
        env.get("MAVEN_GPG_PASSPHRASE", env.get("PGP_PASSPHRASE", "")),
    )
    if "RELEASE_GPG_PASSPHRASE_FILE" in env:
        value = (
            Path(env["RELEASE_GPG_PASSPHRASE_FILE"])
            .read_text()
            .removesuffix("\n")
        )
    if (
        not value
        or "\n" in value
        or "\r" in value
        or len(value.encode()) > 1024
    ):
        raise SigningError(
            "Release signing requires a nonempty single-line passphrase."
        )
    return value


def credential_gpg(args, payload=None, capture=True, env=None):
    env = os.environ if env is None else env
    # A separate pipe preserves stdin for Git's payload. Its descriptor, never
    # its contents, appears in argv; pass_fds deliberately survives only to
    # gpg.
    reader, writer = os.pipe()
    try:
        try:
            os.write(writer, (passphrase_value(env) + "\n").encode())
        finally:
            os.close(writer)
        command = [
            "gpg",
            "--no-options",
            "--batch",
            "--no-tty",
            "--no-auto-key-retrieve",
            "--pinentry-mode",
            "loopback",
            "--passphrase-fd",
            str(reader),
            *map(str, args),
        ]
        if capture:
            return run(command, env=env, input=payload, pass_fds=(reader,))
        return invoke(
            command, input=payload, pass_fds=(reader,), env=env, timeout=60
        ).returncode
    finally:
        os.close(reader)


class SessionSignals:
    def __init__(self):
        self.signum = None

    def __call__(self, signum, frame):
        # Recording cannot interrupt resource acquisition or teardown midway.
        if self.signum is None:
            self.signum = signum

    def check(self):
        if self.signum is not None:
            raise SystemExit(128 + self.signum)


@contextmanager
def session_signals():
    signals = (signal.SIGTERM, signal.SIGHUP, signal.SIGINT)
    previous = {signum: signal.getsignal(signum) for signum in signals}
    current = previous[signal.SIGTERM]
    if isinstance(current, SessionSignals) and all(
        handler is current for handler in previous.values()
    ):
        yield current
        return
    interrupted = SessionSignals()
    try:
        for signum in signals:
            signal.signal(signum, interrupted)
        yield interrupted
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        interrupted.check()


def execute(command, env):
    with session_signals() as interrupted:
        process = None
        try:
            interrupted.check()
            try:
                process = subprocess.Popen(
                    command, env=env, start_new_session=True
                )
            except OSError:
                raise SigningError(
                    command_operation(command) + " could not start (OS error)."
                ) from None
            while True:
                interrupted.check()
                try:
                    status = process.wait(timeout=0.1)
                except subprocess.TimeoutExpired:
                    continue
                interrupted.check()
                return status if status >= 0 else 128 - status
        except BaseException as error:
            if process is None:
                raise
            signum = (
                error.code - 128
                if isinstance(error, SystemExit)
                and isinstance(error.code, int)
                else signal.SIGTERM
            )
            if signum not in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT):
                signum = signal.SIGTERM
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                process.wait()
                raise error
            deadline = time.monotonic() + TERMINATION_GRACE
            try:
                process.wait(timeout=TERMINATION_GRACE)
            except subprocess.TimeoutExpired:
                # A child that ignores graceful termination is killed below.
                pass
            # Descendants may outlive their parent, so allow the full grace
            # period.
            time.sleep(max(0, deadline - time.monotonic()))
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                # The process group may have exited during graceful teardown.
                pass
            process.wait()
            raise


@contextmanager
def signing_session():
    env = dict(os.environ)
    passphrase_value(env)
    private = env.pop("RELEASE_GPG_PRIVATE_KEY", "")
    if "RELEASE_GPG_PRIVATE_KEY_FILE" in env:
        private = Path(env.pop("RELEASE_GPG_PRIVATE_KEY_FILE")).read_text()
    if not private:
        raise SigningError(
            "Release signing requires a protected signing-subkey export."
        )
    with (
        session_signals() as interrupted,
        tempfile.TemporaryDirectory(
            prefix="release-signing-", dir=env.get("RUNNER_TEMP")
        ) as temp,
    ):
        directory = Path(temp)
        signing_home = directory / "keyring"
        signing_home.mkdir(mode=0o700)
        env["GNUPGHOME"] = str(signing_home)
        try:
            credential_gpg(["--import"], payload=private, env=env)
            interrupted.check()
            protected_subkey(env)
            wrapper = directory / "gpg-wrapper"
            write_private(
                wrapper,
                "#!/bin/sh\nexec python3 "
                + shlex.quote(str(Path(__file__).resolve()))
                + ' gpg-program "$@"\n',
                executable=True,
            )
            # Configuration is inherited only by the command and its children.
            # Runtime entries override checkout settings without changing
            # files.
            settings = {
                "user.name": "ScyllaDB Publisher",
                "user.email": "publish.code@scylladb.com",
                "user.signingkey": SIGNING + "!",
                "commit.gpgsign": "true",
                "tag.gpgSign": "true",
                "gpg.format": "openpgp",
                "gpg.program": str(wrapper),
            }
            count = int(env.get("GIT_CONFIG_COUNT", "0"))
            for index, (name, value) in enumerate(
                settings.items(), start=count
            ):
                env[f"GIT_CONFIG_KEY_{index}"] = name
                env[f"GIT_CONFIG_VALUE_{index}"] = value
            env["GIT_CONFIG_COUNT"] = str(count + len(settings))
            artifact = directory / "preflight"
            write_private(artifact, "Repository release signing preflight\n")
            # Cached credentials must not satisfy the passphrase check.
            run(
                [
                    "gpgconf",
                    "--homedir",
                    str(signing_home),
                    "--kill",
                    "gpg-agent",
                ]
            )
            credential_gpg(
                ["--local-user", SIGNING + "!", "--detach-sign", artifact],
                env=env,
            )
            # First use materializes openpgp-native imports. Before then even
            # an
            # unprotected export can appear as a protected-private-key to the
            # agent.
            protected_subkey(env)
            with public_environment() as verify_env:
                verify_status(
                    gpg(
                        "--status-fd",
                        "1",
                        "--verify",
                        str(artifact) + ".sig",
                        artifact,
                        env=verify_env,
                    )
                )
            interrupted.check()
            yield env
        finally:
            run(
                [
                    "gpgconf",
                    "--homedir",
                    str(signing_home),
                    "--kill",
                    "gpg-agent",
                ]
            )


def verify_git(kind, ref):
    with public_environment() as env:
        verify_status(
            run(
                [
                    "git",
                    "-c",
                    "gpg.program=gpg",
                    "verify-" + kind,
                    "--raw",
                    ref,
                ],
                env,
            )
        )


def verify_release(tag, baseline=None):
    baseline = baseline or os.environ.get("RELEASE_BASE_COMMIT")
    if not baseline:
        raise SigningError(
            "Release verification requires the "
            "pre-prepare RELEASE_BASE_COMMIT or an "
            "explicit baseline."
        )
    run(["git", "merge-base", "--is-ancestor", baseline, "HEAD"])
    commits = run(["git", "rev-list", baseline + "..HEAD"]).splitlines()
    if not commits:
        raise SigningError("No release commits were created.")
    with public_environment() as env:
        for commit in commits:
            verify_status(
                run(
                    [
                        "git",
                        "-c",
                        "gpg.program=gpg",
                        "verify-commit",
                        "--raw",
                        commit,
                    ],
                    env,
                )
            )
        ref = "refs/tags/" + tag
        run(["git", "check-ref-format", ref])
        if run(["git", "cat-file", "-t", ref]).strip() != "tag":
            raise SigningError("Release tags must be signed annotated tags.")
        if run(["git", "rev-parse", ref + "^{commit}"]).strip() not in commits:
            raise SigningError(
                "Release tag does not point to a newly signed release commit."
            )
        verify_status(
            run(
                ["git", "-c", "gpg.program=gpg", "verify-tag", "--raw", ref],
                env,
            )
        )


def verify_commit(commit):
    verify_git("commit", commit)


def verify_tag(tag, commit=None):
    ref = "refs/tags/" + tag
    run(["git", "check-ref-format", ref])
    if run(["git", "cat-file", "-t", ref]).strip() != "tag":
        raise SigningError("Release tags must be signed annotated tags.")
    if (
        commit is not None
        and run(["git", "rev-parse", ref + "^{commit}"]).strip() != commit
    ):
        raise SigningError("Release tag points to an unexpected commit.")
    verify_git("tag", ref)


def sign_files(files, signing_env=None):
    if not files:
        raise SigningError("At least one release artifact is required.")
    if (
        signing_env is None
        and (
            os.environ.get("RELEASE_GPG_PRIVATE_KEY")
            or "RELEASE_GPG_PRIVATE_KEY_FILE" in os.environ
        )
        and any(
            not Path(str(Path(filename).resolve()) + ".asc").exists()
            for filename in files
        )
    ):
        with signing_session() as session:
            sign_files(files, session)
            return
    with session_signals() as interrupted, public_environment() as env:
        for filename in files:
            interrupted.check()
            artifact = Path(filename).resolve()
            if not artifact.is_file() or artifact.suffix == ".asc":
                raise SigningError("Expected an unsigned release artifact.")
            # Existing signatures are immutable: never overwrite them on a
            # rerun.
            signature = Path(str(artifact) + ".asc")
            if not signature.exists():
                credential_gpg(
                    [
                        "--local-user",
                        SIGNING + "!",
                        "--armor",
                        "--detach-sign",
                        "--output",
                        signature,
                        artifact,
                    ],
                    env=signing_env,
                )
                interrupted.check()
            verify_status(
                gpg(
                    "--status-fd",
                    "1",
                    "--verify",
                    signature,
                    artifact,
                    env=env,
                )
            )


def main():
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "gpg-program":
            # Git requires stdout/stderr byte-for-byte; do not print a success
            # banner.
            return credential_gpg(sys.argv[2:], capture=False)
        if len(sys.argv) in (3, 4) and sys.argv[1] == "verify-release":
            verify_release(*sys.argv[2:])
        elif len(sys.argv) == 3 and sys.argv[1] == "verify-commit":
            verify_commit(sys.argv[2])
        elif len(sys.argv) in (3, 4) and sys.argv[1] == "verify-tag":
            verify_tag(*sys.argv[2:])
        elif len(sys.argv) > 2 and sys.argv[1] == "sign-files":
            sign_files(sys.argv[2:])
        elif len(sys.argv) > 2 and sys.argv[1] == "exec":
            with signing_session() as env:
                return execute(sys.argv[2:], env)
        else:
            raise SigningError(
                "Expected verify-release TAG [BASELINE], "
                "verify-commit COMMIT, verify-tag TAG "
                "[COMMIT], sign-files FILE..., or exec "
                "COMMAND..."
            )
        print("Release signing " + sys.argv[1] + " passed.")
        return 0
    except SigningError as error:
        print(str(error), file=sys.stderr)
    except Exception as error:
        category = (
            "OS error"
            if isinstance(error, OSError)
            else "invalid value"
            if isinstance(error, ValueError)
            else "internal error"
        )
        operation = (
            sys.argv[1]
            if len(sys.argv) > 1
            and sys.argv[1]
            in (
                "gpg-program",
                "verify-release",
                "verify-commit",
                "verify-tag",
                "sign-files",
                "exec",
            )
            else "command"
        )
        print(
            f"Release signing {operation} failed ({category}); "
            f"values withheld.",
            file=sys.stderr,
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())
