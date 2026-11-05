# Python driver releases

Dispatch **Release ScyllaDB Python Driver** (`release.yml`) on `master` with a new
canonical stable `X.Y.Z` version. The package remains `scylla-driver`; the signed
annotated tag is `X.Y.Z-scylla`. The package stays at the released version after
publication. Maintenance branches, prereleases and historical republishing are
outside this workflow.

## Validation and approval

The launch SHA and preparation date are recorded once in `release-candidate`.
Preparation updates the package version, promotes changelog entries to a dated
release section, keeps an empty `Unreleased` heading, adds the documentation tag,
updates its latest version, and updates the installation output example. A Git
bundle exports the candidate commit and tree to every builder.

The existing five-platform wheel builder retains its interpreter coverage and
runs its full configured wheel tests. Every platform artifact and the source
distribution must be present; all distribution names and archive metadata must
match the requested version. Twine checks package metadata. The integration
matrix checks out `python-driver-matrix` explicitly at
`dcb16da4c0c7f87e64fcc7751e3214343f6fc3b2`, loads the candidate bundle locally,
and runs protocols 3 and 4 against `LATEST`, `PRIOR`, `LTS-LATEST` and `LTS-PRIOR`.
A local candidate tag selects the runner's version-specific patches; it is never
pushed. The copied alias-resolution steps must be updated with the runner pin.

`skip-tests` skips project wheel tests. Release helper tests always run, and the
external matrix has its own `skip-integration-matrix` input. Open issues carrying
`release-blocker` are checked across every page before validation and immediately
before publication. Releases are serialized without cancelling an active run.

For a real release, only the `publish` job references the approval-protected
`release` environment. Preparation, builds, project and matrix tests, metadata
checks and downstream docs have no approval gate or release secrets. After
approval, that one job signs and publishes: helpers from the launch SHA sign the
exact tested tree as `ScyllaDB Publisher <publish.code@scylladb.com>`, with:

- Primary certificate: `786703A4F7AC1E986C7CFB2318ED6CC7EBE7C5F9`
- Signing subkey: `7C0A5E8366A579484D1D352C2EC9B987C56EF1CA`

The private export must contain only the protected signing subkey. The primary
secret, other fingerprints, an unprotected export and an incorrect passphrase
are rejected. Dedicated temporary keyrings and agents are cleaned on failure
and handled cancellation. Only the signing step receives signing credentials;
builds receive none. Credential jobs parse archives and Git objects without
importing the driver or running candidate build code.

Public-only verification checks the commit, annotated tag, distributions and
checksum signatures. `release-state` retains the exact packages, signatures,
public certificate, signed Git bundle, candidate baseline and publication
identity **before any upload**. Its signed manifest covers all retained payloads.
Artifacts are retained for 90 days; archive a copy before expiry when recovery
may be needed.

## Publication

The top-level `release.yml` job uses PyPI OIDC with attestations enabled. Before
upload, it verifies the dedicated publisher team bypass, repository immutability,
release blockers, the remote baseline/tag and existing PyPI file hashes. It stages
only missing packages for upload, then checks that PyPI exposes exactly the
expected filenames and SHA-256 hashes. It pushes the retained signed commit and
tag atomically using the protected publisher credential, without force.

Before dispatch, classify and verify merged PR labels as described in
[CONTRIBUTING.rst](../../CONTRIBUTING.rst#github-releases-and-release-notes).
Generated notes exclude `omit-from-release-notes`; publication does not wait for
classification automatically.

GitHub publication creates a draft with generated notes and asset-verification
instructions, attaches all packages, signatures, checksums,
state, signed bundle and public certificate, verifies asset digests, then
publishes. Docs run explicitly at the signed commit only after the release's
immutable flag and remote tag identity are verified. Automatic docs deployment
is suppressed for pushes made by `scylladb-publisher`.

`dry-run` needs no environment approval. Its signing job uses `release-dry-run`,
which is restricted to `master`, has no required reviewers and contains only
`GPG_PRIVATE_KEY` and `GPG_PASSPHRASE`. It uses the existing Python signing
certificate and has no publisher credential or OIDC permission. Real signing and
publication run together in the single approval-protected `publish` job.

`dry-run` performs preparation, builds, tests, signing and public verification,
and retains the signed checkpoint. It stops before PyPI upload, Git pushes,
GitHub release creation and docs deployment. Run a full dry run after
this workflow reaches `master`, before the first real release. PyPI OIDC itself
is exercised by the first real upload.

## Cutover

Configured during this implementation:

- `release` environment: approval by `scylladb/drivers`, self-review permitted,
  administrator bypass disabled, deployments restricted to branch `master`.
- `release-dry-run` environment: no required reviewers, deployments restricted
  to `master`, with only the two signing secrets and no publisher credential.
- Environment secrets `GPG_PRIVATE_KEY`, `GPG_PASSPHRASE`, and `PUBLISHER_TOKEN`.
  The existing `~/.repos-gpg/scylladb/python-driver/signing-private.asc` and
  passphrase were supplied through stdin; only `publisher.asc` is committed.
  The publisher credential belongs to the existing `scylladb-publisher` account.
- Python public certificate registered on `scylladb-publisher`.
- Repository release immutability enabled for future releases.

Required before the first real release:

1. An organization administrator must create the closed
   `scylladb/python-driver-publisher` team containing only `scylladb-publisher`,
   grant that team write access to this repository, and add it as an **Always**
   bypass actor on the `master` pull request ruleset (currently ruleset
   `3470144`). Retain the pull request rule and other existing rules and actors.
   The available token cannot create this team because it lacks `admin:org`.
   Publication checks team membership and every effective pull request ruleset
   before uploading to PyPI; a missing bypass blocks publication.
2. A PyPI project owner must register a GitHub trusted publisher for project
   **scylla-driver**, owner **scylladb**, repository **python-driver**, workflow
   **release.yml**, environment **release**. See
   [PyPI registration](https://docs.pypi.org/trusted-publishers/adding-a-publisher/).
   No authenticated PyPI management connection was available during implementation.
3. Merge this PR and retire the previous PyPI publisher registrations for
   `build-push.yml` and any old manual upload workflows. Their workflows retain
   builds; the PyPI upload job is removed in this PR. Verify the actual registered
   list in PyPI rather than deleting unrelated publishers.
4. Dispatch a `dry-run` on `master` with both test skips disabled.
   Inspect the build, matrix and signing results before dispatching a real release.

For key rotation, update the public certificate and fingerprints together,
register the certificate on the publisher account, replace environment secrets,
and repeat the full dry run. Do not export or upload the primary secret.

## Reruns and manual recovery

Use **Re-run failed jobs** or **Re-run all jobs** on the original workflow run.
Do not dispatch a new run for the same published version. Preparation restores
that run's candidate and original date. If `release-state` already exists,
builds and signing are skipped and the retained bytes and signed history are
verified. The credential job rechecks this run's checkpoint even when only the
failed publication job is rerun and preparation still has its original outputs.
It downloads the same signed state and never re-signs it. An expected checkpoint
that disappears or cannot be read blocks the run instead of enabling signing.
Existing PyPI files must match the original hashes; only missing files are
uploaded. GitHub draft uploads resume at missing assets without regenerating
notes, including drafts not returned by the tag lookup. Already published
releases must be immutable and complete. Existing signatures are never replaced,
and existing tags are never moved.

A failure before the signed checkpoint is retained has no external publication;
rerunning may rebuild and sign. A checkpoint upload failure prevents publication.
Once the checkpoint exists, any mismatch, expiration or ambiguous state requires
manual investigation. Never delete the recovery artifact to force regeneration.

Download a checkpoint with:

```sh
gh run download RUN_ID --repo scylladb/python-driver \
  --name release-state --dir /safe/location/release-state
```

The signed `release.json` records the baseline SHA, run ID, candidate tree, signed
commit, tag object and package hashes. For local verification, check out the
**recorded baseline** in a fresh clone, use the helpers from that checkout, and
set `GITHUB_REPOSITORY=scylladb/python-driver`, `GITHUB_REF=refs/heads/master`,
`GITHUB_SHA` to the recorded baseline and `GITHUB_RUN_ID` to the original run ID.
Then run `python3 scripts/release/release.py verify --state /safe/location/release-state`.
No signing credentials are needed. Verification imports only the public key and
loads signed Git objects from the retained bundle.

If `master` advances beyond both the original baseline and the retained signed
commit, the workflow refuses further publication. Pause release activity and
inspect the retained state, current Git history, PyPI filenames/hashes and GitHub
assets. Do not rebase, cherry-pick, amend or re-sign the retained release commit;
do not replace signatures, overwrite PyPI files, or move an existing tag. A
maintainer must decide an explicit history-recovery procedure or abandon that
version and release a new higher version. Any PyPI files already published remain
immutable evidence even if the release is abandoned. The workflow never repairs
branch history automatically.

If pushing fails after PyPI succeeds, fix the credential/ruleset or network issue
and rerun the original run while its baseline is still valid. If GitHub asset
upload fails, resume its draft. If immutability verification fails after GitHub
publication, investigate repository settings and release state before rerunning;
docs remain gated. Keep the original artifacts throughout recovery.

## Local checks

```sh
uv run pytest tests/unit/test_release_helpers.py
python3 -m unittest discover -s scripts/release -p 'test_*.py' -v
actionlint .github/workflows/release.yml .github/workflows/release-matrix.yml
```

The primary unit suite covers portable metadata and publication guards on the
supported wheel platforms. Release preparation also runs the complete dedicated
helper suite, including its disposable-key and cancellation checks.

Tests use disposable protected GPG keys and mocked remote services. They cover
metadata/version checks, blocker pagination, candidate identities, branch
movement, artifact completeness, signature verification and cleanup, partial
uploads, conflicting hashes, publication order and immutable release recovery.
Platform wheel builds and live Scylla integration run in GitHub Actions.
