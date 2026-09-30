# GitHub Actions performance review — python-driver

Branch: `ci/gha-speedups` (worktree of `origin/master`).

Status: implemented **#1, #4, #5, #7, #10** in this branch, plus the PR-side
test change of **#8** (a per-wheel import smoke test instead of the full unit
suite). **#3 was evaluated and dropped**: Cython extensions are ABI-specific,
so a single shared wheel is impossible (it would need one build job per Python
version), that restructure does not reduce wall-clock (build and test are
already sequential within a job), and `[tool.uv] cache-keys` plus `setup-uv`'s
default uv cache already reuse the compiled build across runs. The remaining
items (#2, #6, #9, and #8's matrix-trimming half) are not yet implemented.

Scope: every workflow under `.github/workflows/`. Findings are ordered by
expected impact (wall-clock and runner-minutes), highest first. The two
workflows that dominate CI cost are `lib-build.yml` (reusable wheel build, run
by three callers) and `integration-tests.yml` (a 15-job matrix).

Current inventory:

| Workflow | Trigger | Jobs / runs-on | Notes |
| --- | --- | --- | --- |
| `lib-build.yml` (reusable) | called by 3 | `prepare-matrix` + 5× `build-wheels` + `build-sdist` | cibuildwheel builds `cp3*` **and** `pp3*` and runs tests for every target |
| `build-test.yml` | PR → master | calls `lib-build` with **all** targets | full 5-platform wheel build on every PR |
| `build-push.yml` | push master/`branch-**` | calls `lib-build` + publish | builds all wheels on every merge, publishes only if ref ends `scylla` |
| `integration-tests.yml` | push/PR | 15 jobs, `ubuntu-24.04` | 7 Python versions × 3 event loops − excludes |
| `coverage.yml` | push/PR | 1 job, `ubuntu-24.04` | runs the same suite as integration-tests plus coverage |
| `docs-pr.yml` / `docs-pages.yml` | PR / push | 1 job each, `ubuntu-latest` | docs-pr checks out full history needlessly |
| `publish-manually.yml`, `build-pre-release.yml` | manual | call `lib-build` | fine |
| `call_jira_sync.yml` | PR events | reusable remote workflow | fine |

---

## 1. Add `concurrency` + `cancel-in-progress` to every PR workflow

**Where:** `integration-tests.yml`, `coverage.yml`, `docs-pr.yml`,
`build-test.yml` (top level).

**Why:** none of the workflows define a `concurrency` group, so pushing twice
to a PR leaves the first (now-stale) 15-job integration run and full wheel
build running to completion. On active PRs that is the single largest source of
wasted minutes.

```yaml
concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}
```

Never cancel on `master` (those build/publish), only on PRs.

## 2. Stop building all 5 wheel targets on every PR (`build-test.yml`)

**Where:** `build-test.yml:1-16` → `lib-build.yml` default `target`
(`linux,macos-x86,macos-arm,windows,linux-aarch64`).

**Why:** every PR that is not a docs-only change triggers a full cibuildwheel
run on 5 runners. Each runner compiles the Cython extensions for **every**
CPython `cp3*` and `pp3*` version (see `pyproject.toml` `[tool.cibuildwheel]`
`build = ["cp3*", "pp3*"]`), so this is by far the most expensive workflow in
the repo — and it duplicates the integration suite's coverage of the code.

**Change:** on PRs build only `linux` (and optionally `windows`), e.g.
`build-test.yml` passes `with: { target: "linux" }`; keep the full matrix for
`build-push.yml` (release branch) and `build-pre-release.yml`. This alone can
cut PR minutes by roughly 70–80% on the build path.

## 3. Build the driver once and share it with the 15 integration jobs

**Where:** `integration-tests.yml:82` (`uv sync` in every matrix leg),
`lib-build.yml:134` (the wheel artifact already exists for linux).

**Why:** each of the 15 jobs recompiles the whole Cython extension set from
scratch (the `uv` cache caches downloads, not compiled `.so`s). That is the
same compile repeated 15 times per run.

**Change:** add a `build-driver` job that produces a wheel (or the built venv
artifact) once, upload it, and have the matrix legs download and install it
instead of `uv sync`. Same idea applies to `coverage.yml`. Expect to remove
~15× the Cython build time from the integration workflow.

## 4. Remove the JDK setup from the test jobs

**Where:** `integration-tests.yml:65-70`, `coverage.yml:52-56`.

**Why:** `actions/setup-java` downloads and installs a JDK in all 16 jobs.
The only Java-dependent tests in the CI suites are the Java-UDF tests, and
`requires_java_udf` (`tests/integration/__init__.py:298`) **skips them whenever
`SCYLLA_VERSION` is set** — which it is (`SCYLLA_VERSION: release:2026.1.13`).
The simulacron Java helper lives in `tests/integration/simulacron/`, which
these jobs do not run (they run `tests/integration/standard` and
`tests/integration/cqlengine`).

**Caveat (verified):** ccm would start a JVM-based `scylla-jmx` whenever the
relocatable package contains a `jmx/` directory, which would require a JDK at
cluster startup. The pinned `release:2026.1.13` package has no `jmx/`
directory, so `scylla-jmx` is never started and the native `scylla nodetool` is
used. The removal is safe for this pinned version; moving to a version that
bundles `jmx/` would reintroduce the JDK requirement.

**Change:** delete both `setup-java` steps (and the single-element
`java-version: [8]` matrix axis in `integration-tests.yml:45`). Saves a
download+install per job ×16.

## 5. Replace the `ccm create … && ccm remove` "Download Scylla" warm-up

**Where:** `integration-tests.yml:92-95`, `coverage.yml:74-77`.

**Why:** the step actually creates a full 1-node Scylla cluster and then tears
it down, in every job. The stated purpose was to pre-populate the Scylla
download cache, but that cache has never worked: ccm writes downloaded
relocatable packages to `~/.ccm/scylla-repository`
(`ccmlib/scylla_repository.py:607`), while the workflow cached
`~/.ccm/repository`. Its own comment
says it is "not strictly necessary for running tests", and the timing-accounting
rationale does not apply to hosted CI. It runs on cache hits too and is a large
chunk of each job's non-test time.

**Change:** drop the warm-up entirely (the real test run populates the cache)
and fix the cache path to `~/.ccm/scylla-repository`, with
`restore-keys: scylla-${{ env.SCYLLA_VERSION }}-` so a version bump still
reuses the previous download.

## 6. Shrink the integration matrix on PRs

**Where:** `integration-tests.yml:44-60`.

**Why:** every PR runs 15 combinations: 7 Python versions × 3 event loops,
including pre-release `3.15`/`3.15t` and free-threaded `3.14t`/`3.15t`. Only
`asyncore` is pruned (and only for ≥3.12).

**Change:** run a representative matrix on `pull_request` (e.g. oldest
supported `3.11`, current `3.13`, newest stable, one loop each) and the full
matrix on `push` to `master`/`branch-**` or a nightly `schedule`. Since the
matrix is computed from the `python-version`/`event_loop_manager` lists, gate
them with an expression such as
`fromJSON(github.event_name == 'pull_request' && '["3.11","3.13"]' || '[...full...]')`.

## 7. Fix and centralize the path filters

**Where:** `integration-tests.yml:12-23` and `:27-35`, `coverage.yml:8-19` and
`:21-30`, `build-test.yml:8-14`.

**Why:** these ignore `*.rst` but not `*.md`, and the `- "*.md"` entries are
indented differently from the rest of the list (8 spaces vs 6) — they only
match markdown files in the repo root, so a `docs/foo.md` or `README` change in
a subdirectory still triggers the 15-job suite and the full wheel build.
`build-test.yml` ignores `*.rst` but not `*.md` or `.github/**` at all.

**Change:** normalize the lists and use recursive globs (`'**/*.md'`,
`'**/*.rst'`, `'docs/**'`, `'examples/**'`, `'scripts/**'`) plus
`.github/workflows/docs-*`; add the same treatment to `build-test.yml`. This
also fixes the inconsistent YAML indentation.

## 8. Narrow what cibuildwheel builds and tests

**Where:** `pyproject.toml:154-172` (`build = ["cp3*", "pp3*"]`,
`test-groups = ["dev"]`), `lib-build.yml:134-143`.

**Why:** `build = ["cp3*", "pp3*"]` builds a wheel for every CPython and PyPy
version on every platform; `test-groups = ["dev"]` makes cibuildwheel install
the dev extra and run the project's tests inside every wheel build. Wheel-build
tests overlap heavily with the dedicated integration suite.

**Change (tests: done):** PR build-test runs now pass `smoke_tests: true`
(`build-test.yml`). `lib-build.yml` then overrides cibuildwheel's per-platform
`test-command` with a one-line import check (`python -c "import cassandra,
…"`) and clears `test-groups`/`test-extras` (`CIBW_TEST_GROUPS=`,
`CIBW_TEST_EXTRAS=`). This keeps a wheel-installs-and-imports guarantee while
dropping the per-wheel full unit suite — and with it the `dev` extra install
that was failing on Windows/PyPy. Release/publish callers still run the full
suite. Building every ABI (`cp3*`/`pp3*`) is left unchanged (recommendation #2
covers trimming the matrix). This is a large multiplier on the most expensive
workflow.

## 9. Collapse the small one-purpose jobs

**Where:** `lib-build.yml:31-67` (`prepare-matrix`), `lib-build.yml:150-166`
(`build-sdist`); `docs-pages.yml:24`, `docs-pr.yml:31`.

**Why:** `prepare-matrix` spins up a whole runner just to `echo` a JSON array;
`build-sdist` spins up another runner to run `uv build --sdist` for a few
seconds. Each runner has fixed queue+startup overhead.

**Change:** replace `prepare-matrix` with a static matrix literal (or a
`fromJSON` input) and fold the sdist into an existing Linux job (e.g. append it
to the `linux` matrix leg) or into the publish job. Also right-size the docs
jobs: they are pure Python and can run on the cheaper ARM runner
(`ubuntu-24.04-arm`), and `runs-on` should be consistent (`ubuntu-latest` vs
`ubuntu-24.04`).

## 10. Caching, checkouts and timeouts

Three small, independent wins:

- **Cache the build toolchains.** On Windows the `conan install`
  (`lib-build.yml:112-117`) re-downloads every run — cache `~/.conan2`. Add a
  ccache / compiled-object cache keyed on the source hash so rebuilds are
  incremental, and enable the `uv` cache explicitly (`enable-cache: true`, as
  the docs workflows already do) in `lib-build.yml`, `integration-tests.yml`
  and `coverage.yml`.
- **Stop redundant/full checkouts.** `docs-pr.yml:34-37` does
  `fetch-depth: 0` for `make -C docs test`, which does not need history — use
  the default shallow clone (only `docs-pages.yml` needs full history for
  multiversion). `lib-build.yml:80-86` checks out twice when `target_tag` is
  set; make the first checkout conditional or move `ref:` onto a single
  checkout.
- **Add `timeout-minutes`.** No job in any workflow sets a timeout, so a hung
  test or stalled download can burn the default 300-minute job limit. Put a
  realistic cap on each job (e.g. tests 60–90 min, wheel build 120 min, sdist 15 min, docs 15-20 min).

## 11. Use the runner's full CPU count when building wheels

**Where:** `pyproject.toml` (`CASS_DRIVER_BUILD_CONCURRENCY = "2"`), `setup.py:205,294-300`.

**Why:** the wheel build compiled the C/Cython extensions with only two
threads. The runners have 3 vCPUs (`macos-14`) or 4 (`ubuntu-24.04`), so a core
sat idle, and on a smoke-test PR run the build block is the majority of the job
(e.g. ~6m15s of 8m43s on macos-arm). The build is CPU-bound, so RAM disk /
tmpfs would buy ~nothing.

**Change (done):** drop the hard-coded `2`; `lib-build.yml` now computes
`nproc` (falling back to `getconf`, then `%NUMBER_OF_PROCESSORS%`) and exports
`CASS_DRIVER_BUILD_CONCURRENCY`, which `setup.py` already reads. The global
`[tool.cibuildwheel] environment` and the Windows `environment` table no longer
pin it, and `environment-pass = ["CASS_DRIVER_BUILD_CONCURRENCY"]` forwards the
host value into the Linux build container. Deliberately *not* done (to keep PR
wheels representative of production): trimming the built ABIs, switching `-O3`
to `-O1`, or splitting ABIs into separate matrix jobs (more runners, not less
wall-clock).

---

## Suggested order of work

1. #1 concurrency (one-line, immediate effect on PR noise).
2. #2/#8 reduce PR wheel builds.
3. #4, #5, #6 cheap test-job deletions/reductions.
4. #3 share the driver build.
5. #7 filters, #9 job collapse, #10 caching/timeouts.

Items #1–#6 are low-risk, high-yield; #3 and #8 need a little more care
(artifact plumbing, supported-version policy) but carry the biggest long-term
savings.
