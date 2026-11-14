# Releasing the Python driver

Follow the [release workflow guide](scripts/release/README.md) for preparation,
validation, approval, publication and recovery. It is the canonical release
procedure.

Start **Release ScyllaDB Python Driver** on `master` with a new stable `X.Y.Z`
version. The workflow prepares the version and documentation changes, builds and
tests the candidate, and publishes after approval. The package is
`scylla-driver`; the signed release tag is `X.Y.Z-scylla`. The workflow pushes
the release commit and tag atomically.

Before dispatch, review the guide's cutover requirements and classify merged PRs
using the [release notes policy](CONTRIBUTING.rst#github-releases-and-release-notes).
For failed runs, follow the guide's rerun and recovery instructions.
