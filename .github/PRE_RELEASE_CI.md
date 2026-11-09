# Pre-release CI

Add the `pre-release-ci` label to a PR to run full wheel builds/tests, full
coverage, the driver integration matrix, and Scylla's Python/CQL tests in GitHub
Actions. Adding the label starts the workflow; pushes and reopening the labeled
PR run it again.
Removing the label cancels its run. Unrelated label changes do not restart it.
Manual runs are also available. The driver integration matrix runs even when
the PR has the `disable-integration-tests` label. This workflow does not publish
packages.

## Scylla master source and image

Preparation resolves `scylladb/scylladb` master once to a full commit SHA. That
commit supplies the test plan, source build, submodules, and frozen toolchain
selection for the entire run. The toolchain image tag is resolved to its registry
digest before building or restoring an image.

A separate job builds an image containing that source checkout, an unstripped
Scylla server, Wasm fixtures, and the checkout's locked Python test environment.
It builds in release mode and keeps the server at
`/opt/scylla/build/RelWithDebInfo/scylla`, where the current upstream CMake test
layout expects it. The source and binary identity are recorded in
`/opt/scylla/.driver-image.json`. The candidate driver is installed during each
test job and is absent from the reusable image build inputs.

The compressed Docker image archive is cached under the exact Scylla commit,
Linux x86_64 architecture, toolchain digest, and build recipe/helper hash. A
matching cache entry is loaded and its source, toolchain, and recipe labels are
checked. A cache miss builds from a recursive checkout, saves the image, and attempts to cache
it. Cache reuse depends on Actions storage limits, eviction, branch access, and
whether master or the build inputs changed; a hit is not guaranteed. A new
driver commit can reuse an image when those inputs match.

The image job uploads one `scylla-test-image` artifact. All Scylla test.py shards
and all driver integration jobs load that exact archive. Scylla source is inside
the image; test jobs mount the candidate driver and logs without hiding it.
The pre-release driver integration matrix preserves all 11 Python/reactor
combinations and uses `SCYLLA_VERSION=/opt/scylla` and
`CASSANDRA_DIR=/opt/scylla`. Each job
checks that its installed CCM can populate a node from that source binary before
running the integration suite. Its regular standalone workflow continues to
use the configured CCM release selector.

Image creation runs independently of wheel builds. Scylla test.py jobs wait for
the image, plan, and Linux wheels; other platform wheel builds run independently.
The initial source build defaults to two compiler jobs and has a six-hour job
limit on `ubuntu-24.04`. Set the repository variable `SCYLLA_BUILD_RUNNER` to an
already enabled Linux x86_64 larger-runner label to move only the image build to
that runner. `SCYLLA_BUILD_JOBS` can raise its compiler parallelism; it must be a
positive integer. These settings do not provision runners or change the test
matrix's runner selection. Build time, peak disk use, and archive size require
measurement in Actions; the workflow reports image/archive sizes and available
disk space. A cold build may exceed runner capacity or the time limit. No
successful cold build or cache hit is implied by this configuration.

## Test jobs

Preparation discovers Python/CQL files from every eligible suite in the pinned
checkout. It splits whole modules to preserve their fixtures:

- `cluster`: eight shards.
- `cqlpy`: four shards, with Wasm fixtures built into the image.
- `alternator` and `rest_api`: two shards each.
- Other suites: one job each, including CQL approval tests, nodetool, debugger
  tests, and distribution Python tests when present.

The `scylla-test-plan` artifact records the pinned source in `source.json` and each
job's complete file list in `jobs.json`. Discovery runs again for each source
revision rather than using a fixed file list.

The shared `scylla-test-image` artifact is retained for seven days. Rerunning
failed test jobs requires that artifact; after it expires, start a new full run
so the image job can restore the cache or rebuild the pinned source image.

Jobs use Linux x86_64 runners, with up to four test jobs running at once and one
test worker per job to leave memory for multi-node tests. Each shard has a
150-minute test session limit and a 180-minute job limit. Tests marked
`non_gating` are excluded, matching upstream gating. Debugger jobs use the same
unstripped server and source included in the image.

The public source image lacks the optional KMIP client library. Parameter cases
using `KeyProvider.kmip` are excluded; other encryption providers remain covered.

The image generates cqlsh's version metadata from its pinned Git submodule.
Child Python tools use the candidate environment, and cqlsh's bundled driver
lookup is disabled so its queries exercise the candidate wheel.

Upstream auxiliary services run in nested Podman containers using its `vfs`
storage driver to avoid the default driver's overlay-on-overlay startup error.

Scope is the Python/CQL trial against a server built from master. C++ test
executables are not built. Harness/fixture directories and upstream's
manual/performance test directories are excluded. This does not reproduce
Jenkins' dev/debug or ARM build matrices.

## Results and local runs

Each `scylla-testpy-<suite>-<shard>-logs` artifact contains a tarball with candidate
identity, source/image provenance, selected test paths, console output,
JUnit/Allure reports, and test/server logs. The tarball preserves pytest
parameter names that artifact upload cannot accept as individual filenames.
Server data directories are not uploaded.

Prepare the same inputs locally:

```sh
python3 scripts/scylla_source.py resolve --output source.json
# Recursively check out scylladb/scylladb at source.json's revision into
# /path/to/scylla, then generate the plan from that checkout.
python3 scripts/scylla_source.py plan /path/to/scylla --output jobs.json
# Resolve /path/to/scylla/tools/toolchain/image to a digest as in the workflow.
SCYLLA_TOOLCHAIN="docker.io/scylladb/scylla-toolchain@sha256:<digest>" \
  scripts/build_scylla_test_image.sh /path/to/scylla scylla-driver-tests:local
```

Run `scripts/run_scylla_testpy.sh` inside that image with the workflow's Docker
privileges, network mode, and mounts, setting:

- `SCYLLA_SOURCE=/opt/scylla`.
- `SCYLLA_SOURCE_FILE` / `SCYLLA_PLAN_FILE`: mounted `source.json` / `jobs.json`.
- `SCYLLA_TEST_JOB`: one job name from the plan, such as `cluster-1`.
- `DRIVER_WHEEL_DIR`: mounted directory containing exactly one compatible
  candidate wheel for the image's Python ABI.
- `TESTPY_LOG_DIR`: mounted output directory.

## CI stages and optional platform checks

| Stage | Wheel builds and tests | Coverage and integration |
| --- | --- | --- |
| Ordinary PR, full plan | Linux x86_64 wheel smoke tests and source distribution | Full driver integration suites once on Python 3.13/libev against Scylla 2026.1.13 using the compiled driver; NumPy import checks and applicable docs builds |
| Ordinary PR, narrow plan | Wheel builds skipped | Selected unit/helper and integration modules, plus selected NumPy checks; documentation/support-only changes run no driver tests |
| Merge queue | Linux x86_64 and ARM64 wheels with configured unit tests, plus source distribution | Full unit, reactor, and driver integration coverage against Scylla 2026.1.13 using the pure-Python driver paths on the combined commit |
| Pre-release | All five platforms with configured unit tests, plus source distribution | Full coverage against stable Scylla, driver integration matrix against pinned Scylla master, and upstream Python/CQL suites |
| Release/tag build | All five platforms | Existing release validation and publication behavior |

Only ordinary PR jobs use the test plan from `scripts/ci/select_tests.py`.
Documentation/support changes select no driver tests. Standalone
unit/integration test changes select their modules, and known helper changes
select their mapped tests. Shared test infrastructure, driver source,
packaging/dependencies, deleted tests and unmapped paths keep existing full PR
validation. Mixed changes containing driver source also keep full PR validation.
Narrow PR plans skip wheel builds and unrelated integration/NumPy jobs.

The `Measure code coverage` job runs selected unit/helper tests directly on PRs,
without uploading partial coverage to Codecov. All merge-queue entries retain
full wheel/unit testing and full coverage, including documentation-only entries.
Pre-release runs (including calls from labeled PRs), protected pushes, manual
runs and release builds retain their existing suites. These runs bypass the
selection jobs entirely. See `CONTRIBUTING.rst` for local plan/run commands,
worktree selection, mappings and conservative fallbacks.

Add `ci-macos` to run both Intel and Apple Silicon wheel builds/tests on a PR,
or `ci-windows` to run Windows x86_64 wheel builds/tests. These jobs use the full
configured unit tests, and do not rebuild the source distribution. Adding a
platform label starts only that platform's jobs; subsequent pushes and reopening
the labeled PR rerun them. Unrelated labels do not restart the platform checks.
Removing a platform label disables future runs; an in-flight run can be canceled
in Actions. Platform labels explicitly opt in even with `disable-test-build`.
They also work for PRs that only change documentation or scripts.

Full coverage tests the combined merge-group commit in the merge queue. The
coverage workflow reports a skipped check on ordinary full-plan PRs so the same
check can be required for queue entry; narrow plans run their selected tests.
Pre-release calls it explicitly to run full coverage
on a labeled PR. Manual coverage runs remain available. Full-plan PR integration
runs both `tests/integration/standard/` and `tests/integration/cqlengine/` in full.
Narrow plans run only the selected integration modules. Pre-release explicitly
enables the full
integration matrix: Python 3.11 through 3.15 with libev and asyncio, plus Python
3.11 with asyncore. Manual integration runs also use that full matrix.

Pushes to `master` and `branch-**` retain full coverage runs to update Codecov's
branch baselines, including commits that bypass the merge queue. Documentation
and other existing coverage path exclusions still apply. Branch pushes do not
repeat wheel tests, integration, NumPy checks, or PR documentation builds.
Existing tag builds, documentation publication, and explicit release
workflows remain available; there is no new post-merge artifact promotion or
publication stage.

The existing `build-pre-release.yml` checks prerelease Python versions and serves
a different purpose.

To enforce the queue check, repository rules must enable the merge queue and
require `Measure code coverage`. The workflow trigger alone does not configure
those repository settings.
