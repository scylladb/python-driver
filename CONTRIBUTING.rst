Contributing
============

Contributions are welcome in the form of bug reports or pull requests.

Bug Reports
-----------
Quality bug reports are welcome at the `Scylla Python Driver Github <https://github.com/scylladb/python-driver/issues>`_.

There are plenty of `good resources <http://www.drmaciver.com/2013/09/how-to-submit-a-decent-bug-report/>`_ describing how to create
good bug reports. They will not be repeated in detail here, but in general, the bug report include where appropriate:

* relevant software versions (Python runtime, driver version, cython version, server version)
* details for how to produce (e.g. a test script or written procedure)
  * any effort to isolate the issue in reproduction is much-appreciated
* stack trace from a crashed runtime

Pull Requests
-------------
If you're able to fix a bug yourself, you can `fork the repository <https://help.github.com/articles/fork-a-repo/>`_ and submit a `Pull Request <https://help.github.com/articles/using-pull-requests/>`_ with the fix.
Please include tests demonstrating the issue and fix. For examples of how to run the tests, consult the further parts of this document.

Design and Implementation Guidelines
------------------------------------
- We have integrations (notably Cassandra cqlsh) that require pure Python and minimal external dependencies. We try to avoid new external dependencies. Where compiled extensions are concerned, there should always be a pure Python fallback implementation.
- This project follows `semantic versioning <http://semver.org/>`_, so breaking API changes will only be introduced in major versions.
- Legacy ``cqlengine`` has varying degrees of overreaching client-side validation. Going forward, we will avoid client validation where server feedback is adequate and not overly expensive.
- When writing tests, try to achieve maximal coverage in unit tests (where it is faster to run across many runtimes). Integration tests are good for things where we need to test server interaction, or where it is important to test across different server versions (emulating in unit tests would not be effective).

Optional NumPy imports
----------------------
Ordinary driver imports must not import NumPy, even when it is installed.
Explicit NumPy features may load it when called or accessed. Runtime imports
of NumPy in ``cassandra/`` must go through ``cassandra.numpy_support``; do not
import NumPy directly in another driver module, including inside a function.
Use ``cassandra.numpy_support.numpy_available()`` to check availability and
``cassandra.numpy_support.get_numpy_protocol_handler()`` to request the handler.
The older ``HAVE_NUMPY`` and ``NumpyProtocolHandler`` attribute imports are
deprecated and emit ``DeprecationWarning``.
Run ``python3 scripts/check_numpy_imports.py`` and
``uv run pytest tests/unit/test_protocol_imports.py`` when changing these paths.
The NumPy import contract CI job runs both checks on pull requests.

Dev setup
=========

We recommend using `uv` tool for running tests, linters and basically everything else,
since it makes Python tooling ecosystem mostly usable.
To install it, see instructions at https://docs.astral.sh/uv/getting-started/installation/
The rest of this document assumes you have `uv` installed.

It is also strongly recommended to use C/C++-caching tool like ccache or sccache.
When modifying driver files, rebuilding Cython modules is often necessary.
Without caching, each such rebuild may take over a minute. Caching usually brings it
down to about 2-3 seconds.

**Important:** After modifying any ``.py`` file under ``cassandra/`` that is
Cython-compiled (such as ``query.py``, ``protocol.py``, ``cluster.py``, etc.),
extensions must be rebuilt before running tests. If you always use ``uv run``
(e.g. ``uv run pytest``), this is handled automatically via the ``cache-keys``
configuration in ``pyproject.toml``. If you invoke ``pytest`` directly, you can
rebuild with::

    uv sync --reinstall-package scylla-driver

Without rebuilding, Python will load the stale compiled extension (``.so`` / ``.pyd``)
instead of your modified ``.py`` source, and your changes will not actually be tested.
The test suite will emit a warning if it detects this situation.

Building the Docs
=================

To build and preview the documentation for the ScyllaDB Python driver locally, you must first manually install `python-driver`.
This is necessary for autogenerating the reference documentation of the driver.
You can find detailed instructions on how to install the driver in the `Installation guide <https://python-driver.docs.scylladb.com/stable/installation.html#manual-installation>`_.

After installing the driver, you can build the documentation:
- Make sure you have Python version compatible with docs. You can see supported version in ``docs/pyproject.toml`` - look for ``python`` in ``tool.poetry.dependencies`` section.
- Install poetry: ``pip install poetry``
- To preview docs in your browser: ``make -C docs preview``

Tests
=====

Running Unit Tests
------------------
Unit tests can be run like so::

    uv run pytest tests/unit
    EVENT_LOOP_MANAGER=asyncio CASS_DRIVER_NO_SKIP=1 uv run pytest tests/unit/io/test_asyncioreactor.py

You can run a specific test method like so::

    uv run pytest tests/unit/test_connection.py::ConnectionTest::test_bad_protocol_version

Running Integration Tests
-------------------------
In order to run integration tests, you must specify a version to run using either of:
* ``SCYLLA_VERSION`` e.g. ``release:2025.2.5``
* ``CASSANDRA_VERSION``
environment variable::

    SCYLLA_VERSION="release:2025.2.5" uv run pytest tests/integration/standard tests/integration/cqlengine/

Or you can specify a scylla/cassandra directory (to test unreleased versions)::

    SCYLLA_VERSION=/path/to/scylla uv run pytest tests/integration/standard/

Specify a Protocol Version for Tests
------------------------------------
The protocol version defaults to:
- 4 for Scylla >= 3.0 and Scylla Enterprise > 2019.
- 3 for older versions of Scylla
- 5 for Cassandra >= 4.0, 4 for Cassandra >= 2.2, 3 for Cassandra >= 2.1, 2 for Cassandra >= 2.0
You can overwrite it with the ``PROTOCOL_VERSION`` environment variable::

    PROTOCOL_VERSION=3 SCYLLA_VERSION="release:5.1.19" uv run pytest tests/integration/standard tests/integration/cqlengine/

Seeing Test Logs in Real Time
-----------------------------
Sometimes it's useful to output logs for the tests as they run::

    uv run pytest -s tests/unit/

Use tee to capture logs and see them on your terminal::

    uv run pytest -s tests/unit/ 2>&1 | tee test.log

Measuring Code Coverage
------------------------
``scripts/coverage.sh`` runs the unit suite (all event-loop reactors) and,
if a Scylla/Cassandra version is available, the integration suite, under
``coverage.py``, then combines and reports the result::

    bash scripts/coverage.sh

    # include the integration suite too
    SCYLLA_VERSION="release:2026.1.13" bash scripts/coverage.sh

Open ``htmlcov/index.html`` afterwards for a line-by-line, browsable report.
``coverage.xml`` is also produced for tooling that consumes Cobertura-style
XML.

Note that ``cluster.py``, ``connection.py``, ``protocol.py`` and the other
modules that are optionally Cython-compiled (see ``Dev setup`` above) are
measured as plain Python here, since ``coverage.py`` cannot trace
into compiled extensions -- the script sets ``CASS_DRIVER_NO_CYTHON=1`` for
this reason. Modules that are Cython-only with no pure-Python fallback
(``obj_parser``, ``numpy_parser``, ``row_parser``, and similar) are not built
at all in that mode, so they are not measured by this script.

CI runs the same script, integration suite included, for all merge-queue
entries and manual runs through ``.github/workflows/coverage.yml``. Ordinary
full-plan PR runs report a skipped coverage check without running the suite,
allowing the same check to be required for queue entry. Narrow plans run the
selected tests described below. The opt-in ``pre-release-ci`` workflow also
runs full coverage.
Merge-queue runs test the combined merge-group commit and are not disabled by
PR labels. Passing runs upload ``coverage.xml`` to
`Codecov <https://codecov.io/gh/scylladb/python-driver>`_. Codecov's coverage
threshold statuses remain informational.

Ordinary code PRs run the full driver integration suites once against
``release:2026.1.13`` using Python 3.13, libev, and the compiled driver.
Merge-queue coverage adds the pure-Python driver paths on the combined commit,
alongside Linux x86_64 and ARM64 wheel/unit tests. The full integration
interpreter/reactor matrix is reserved for pre-release and manual runs.
Branch pushes do not automatically repeat these test workflows. See
``.github/PRE_RELEASE_CI.md`` for the stages and optional platform labels.

Running tests impacted by a change
----------------------------------
The same test plan is available locally and in ordinary pull-request CI.
Automatic selection applies only to ordinary PR runs. Every merge-queue entry,
including docs-only changes, retains full wheel/unit tests and full coverage.
Pre-release, release, protected-push and manual CI retain their existing suites.
Print the plan, preview its commands, or run it::

    python3 scripts/ci/select_tests.py --base origin/master
    python3 scripts/ci/select_tests.py --base origin/master --dry-run
    python3 scripts/ci/select_tests.py --base origin/master --run

By default the comparison uses committed ``HEAD``. Add ``--worktree`` to include
staged, unstaged and untracked files. Use ``--suite unit``,
``--suite integration`` or ``--suite numpy`` to run one part of the plan.
When integration tests are
selected, set ``SCYLLA_VERSION`` or ``CASSANDRA_VERSION`` as described above.
``--full`` bypasses selection.

The comparison starts at the merge base with the requested branch. Because
dependency inspection reads local files, committed mode keeps full validation
when the working tree has changes; use ``--worktree`` to plan those changes.

Selection follows these rules:

* Documentation/support-only changes select no driver tests. This includes
  ``docs/``, Markdown files, ``CONTRIBUTING.rst``, ``CHANGELOG.rst``, ``LICENSE``,
  ``.gitignore``, ``.gitattributes``, ``.github/dependabot.yml``, and the two
  documentation workflows. Applicable docs builds and release-note
  classification continue.
* A changed standalone Python test under ``tests/unit/`` or the standard and
  cqlengine integration suites selects that file. Modules imported by other
  tests, shared fixtures, deleted tests, and Cython test modules keep full
  validation. Asyncio and asyncore unit modules run with their own reactor.
* Known helper changes select their mapped tests: the selector, Scylla source
  planner, release/signing helpers, and NumPy import checker. Release helper
  changes run ``tests/unit/test_release_helpers.py`` and ``scripts/release/``.
  A NumPy checker or contract-test change runs the NumPy check and contract tests.
* Source, packaging/build files (including ``README.rst``), dependencies,
  unmapped scripts/workflows, and unknown paths keep full driver validation.
  Mixed changes combine selected tests or expand to full validation.

``scripts/ci/select_tests.py`` owns the mappings and reports the reason for its
plan. It compares the complete Git diff, including deletions and rename sources;
missing history, missing mapped tests, and import-inspection errors select full
validation. Python imports identify shared test modules. Driver-source
dependency uncertainty falls back to full validation.

Ordinary PR CI runs only the selected unit/helper modules, integration
modules, and NumPy job. It skips wheel builds for narrow changes. The existing
``Measure code coverage`` check runs selected PR unit/helper tests directly
without uploading partial coverage to Codecov. Driver-source changes under
``cassandra/`` and mixed changes containing driver source retain the existing
full PR wheel/integration and NumPy checks. A full-plan PR keeps the existing
skipped coverage check; full coverage runs when it enters the merge queue.

Manual runs, explicit pre-release CI, release builds, and protected-branch
coverage retain their existing full suites. Optional ``ci-macos`` and
``ci-windows`` labels run the requested platform's wheel/unit jobs; they do not
expand a narrow plan's integration or coverage checks. To test the selector
without building or importing the driver::

    python3 tests/unit/test_ci_selection.py

Running the Benchmarks
======================
There needs to be a version of Scyll running locally so before running the benchmarks, if ccm is installed:

	uv run ccm create benchmark_cluster --scylla -v release:2025.2.5 -n 1 -s

To run the benchmarks, pick one of the files under the ``benchmarks/`` dir and run it::

    uv run benchmarks/future_batches.py

There are a few options.  Use ``--help`` to see them all::

    uv run benchmarks/future_batches.py --help

GitHub releases and release notes
=================================

Use the signed manual release workflow in ``.github/workflows/release.yml``.
See ``scripts/release/README.md`` for validation, approval, publication and
recovery instructions. This is the release pipeline; release-note filtering
adds no separate publication path and does not change its test-skip inputs,
signing, retained-state recovery or PyPI hash checks.

After PyPI verification and signed history publication, the pipeline creates a
GitHub draft with generated notes and the asset-verification instructions. It
uploads and verifies the signed assets before publishing the immutable release.
Reruns resume an existing draft, including one absent from the tag lookup, and
preserve its notes. Existing published releases and notes are also preserved.
PyPI publication and GitHub release creation are not atomic.

The ``Classify release notes`` workflow uses ``actions/labeler`` and the
filename rules in ``.github/labeler.yml`` to manage ``omit-from-release-notes``.
Documentation (``docs/``, ``CONTRIBUTING.rst``), tests
(``tests/``), the explicitly listed support files and workflows in ``.github/``,
``.gitignore``, ``.gitattributes``, and the coverage/import-check scripts qualify
when all changed files match this support-only union. The labeler configuration
is the source of the exact allowlist. Documentation-site publication, including
``.github/workflows/docs-pages.yml`` and ``docs/_utils/deploy.sh``, counts as
support-only even when invoked after publication by the signed release workflow.
Workflows that build or publish driver distributions or provide required release
gates (including ``integration-tests.yml``), production code, runtime
dependencies, packaging/build manifests, unknown paths, and mixed changes remain
in the notes. ``README.rst`` supplies package metadata, so README-only
documentation changes also remain in the notes.
Titles, authors and broad ``chore``/``dependencies`` labels do not determine
omission.

Labeling runs when a PR opens, changes, reopens, is edited or closes.
``sync-labels: true`` removes the omission label when filenames stop matching.
The action reads the labeler configuration from the trusted workflow revision;
it does not check out or execute code from the PR.

Before manually dispatching a release, identify the merged PRs included since
its previous release. Run ``Classify release notes`` manually on ``master`` with
``pr_number`` for each PR needing classification or backfilling. For example::

    gh workflow run label-release-notes.yml --ref master -f pr_number=123

Wait for the labeling run to succeed, then review the changed files and actual
labels. The labeler matches changed filenames; check rename origins manually
when reviewing moves out of production paths::

    gh api --paginate repos/scylladb/python-driver/pulls/123/files --jq '.[] | {filename, status, previous_filename}'
    gh api --paginate repos/scylladb/python-driver/issues/123/labels --jq '.[].name'

Confirm that production and mixed changes have no omission label. Rerun failed
classification or correct incorrect labels before releasing. The release
configuration excludes this label from generated notes, but publication does
not wait for classification automatically. Existing published notes are
preserved on reruns.

``omit-from-release-notes`` controls GitHub release-note presentation. The local
Jira caller skips add/remove events for this exact label and preserves other
events. Existing Jira copies are not cleaned, and other synchronization paths
may restore copied metadata.

Run the focused release-helper checks from the repository root::

    uv run pytest tests/unit/test_release_helpers.py
    python3 -m unittest discover -s scripts/release -p 'test_*.py' -v
    actionlint .github/workflows/label-release-notes.yml
