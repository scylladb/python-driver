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

CI runs the same script, integration suite included, in
``.github/workflows/coverage.yml`` and uploads ``coverage.xml`` from every
passing run to `Codecov <https://codecov.io/gh/scylladb/python-driver>`_,
which comments the coverage delta on the pull request. Its statuses are
informational, so a drop never blocks a merge.

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

Releases start only through manual dispatch of ``publish-manually.yml`` from
an up-to-date default branch. Supply the new version without the ``-scylla``
suffix, for example::

    gh workflow run publish-manually.yml --ref master -f version=3.29.13

Branch pushes and manual dispatch of ``build-push.yml`` build distributions
without publishing them. Tag pushes do not start release publication.

Configure the ``scylladb-releaser`` GitHub App installation for this repository
with Contents write access. Set ``RELEASE_APP_ID`` as a repository or ``release``
environment variable and ``RELEASE_APP_PRIVATE_KEY`` as a secret accessible to
the ``release`` environment. Preparation, finalization and GitHub release
creation use repository-scoped installation tokens. Version commits and annotated
tags use the App's ``scylladb-releaser[bot]`` identity. Configure any branch
protection bypass needed by that App before releasing. The existing PyPI Trusted
Publisher must authorize ``publish-manually.yml``; its publication job does not
select an environment.

The workflow validates the requested version and dispatch revision, updates
``cassandra/__init__.py`` and the documentation's release list/latest version,
and pushes one version commit to ``release-candidate/X.Y.Z-scylla``. It creates
no final release tag at this stage. That exact candidate commit must pass:

* Wheel builds and their tests across all configured platforms, plus the source
  distribution build.
* The driver's reactor/Python integration-test matrix.
* The pinned ``scylladb/python-driver-matrix`` harness against LATEST, PRIOR,
  LTS-LATEST and LTS-PRIOR Scylla releases, using protocols 3 and 4. Its existing
  compatibility exclusions for Scylla 2025.1 are retained.

Only after all gates succeed does the App atomically advance the default branch
and create the annotated ``X.Y.Z-scylla`` tag. The workflow rejects changed
candidate refs, unexpected candidate changes, mismatched build SHAs and concurrent
default-branch updates. Releasing has no test-skip input. Failed tests leave the
candidate branch available for inspection without advancing the default branch
or creating the final tag.

The same run then uploads its tested distributions to PyPI and creates a GitHub
release with generated notes. Existing published releases and their notes are
preserved; matching unpublished releases must be published before retrying.
Before uploading, the workflow checks the intended distribution filenames
against PyPI's public inventory for that version. Any filename overlap stops
the upload regardless of file contents, including same-source retries after
partial or complete prior uploads. Transport errors, non-404 HTTP errors and
malformed inventory also stop publication; a missing version (404) permits
the first upload. Duplicate skipping remains disabled. Concurrent uploads or
stale inventory can still let identical retries succeed. New GitHub notes
require a successful upload. PyPI publication and GitHub release creation are
not atomic.

For recovery, inspect the original run's candidate/build SHA, upload logs and
artifact inventory. Rerun failed test or build jobs in that original run to retain
the candidate. If preparing again is necessary before finalization, inspect and
remove only the unpublished candidate branch before dispatching from the current
default branch; preparation refuses to overwrite an existing candidate. Candidate
branches remain available after successful releases and may be removed later.
Only if the original ``publish`` job succeeded and GitHub release creation failed,
retry that failed release job in the original run, without rebuilding or rerunning
the upload. If ``publish`` failed or was cancelled, even if all files appear
present, arrange separately verified recovery. Do the same for partial uploads,
unavailable evidence or changed tags. Do not move release tags, delete published
files or enable duplicate skipping to bypass failures. This workflow does not
verify durable provenance across publication attempts.

The ``Classify release notes`` workflow uses ``actions/labeler`` and the
filename rules in ``.github/labeler.yml`` to manage ``omit-from-release-notes``.
Documentation (``docs/``, ``CONTRIBUTING.rst``), tests
(``tests/``), the explicitly listed support files and workflows in ``.github/``,
``.gitignore``, ``.gitattributes``, and the coverage/import-check scripts qualify
when all changed files match this support-only union. The labeler configuration
is the source of the exact allowlist. Workflows that build or publish shipped
artifacts, production code, runtime dependencies, packaging/build manifests,
unknown paths, and mixed changes remain in the notes. ``README.rst`` supplies
package metadata, so README-only documentation changes also remain in the notes.
Titles, authors and broad ``chore``/``dependencies`` labels do not determine
omission.

Labeling runs when a PR opens, changes, reopens, is edited or closes.
``sync-labels: true`` removes the omission label when filenames stop matching.
Trusted checks around labeling inspect rename origins and reject incomplete or
changing file listings. Failed or skipped classification attempts to remove
an unverified omission label without changing other labels. API outages can
prevent cleanup; before/after checks do not make classification atomic, and
cancellation can prevent the final cleanup step from running. The guard reads
the canonical labeler configuration using locked parser/matcher dependencies.
Unsupported configuration shapes fail visibly and require a guard update.
The workflow checks out only the trusted event revision, never the PR head.

Before manually dispatching a release, identify the merged PRs
included since the previous release. Run ``Classify release notes`` manually
with ``pr_number`` for each PR needing classification or backfilling, using a
current default branch that contains the guard. For example::

    gh workflow run label-release-notes.yml --ref master -f pr_number=123

Manual runs selecting another branch or a tag are rejected before helper
checkout. This enforces the documented dispatch procedure; it does not secure
a modified workflow on an arbitrary selected branch.

Wait for the final verification step, then compare its ``Verified`` log's
head/base SHAs, changed-file count and ``omit`` result with fresh PR metadata,
the complete file list including rename origins, and the actual labels::

    gh api repos/scylladb/python-driver/pulls/123 --jq '{head: .head.sha, base: .base.sha, count: .changed_files}'
    gh api --paginate repos/scylladb/python-driver/pulls/123/files --jq '.[] | {filename, status, previous_filename}'
    gh api --paginate repos/scylladb/python-driver/issues/123/labels --jq '.[].name'

Confirm the file count and unique names agree and that production, mixed, and
unverified changes have no omission label. Rerun classification after changed
revision evidence, failed or skipped runs, or label discrepancies, then repeat
verification. The release configuration excludes this label from generated
notes, but publication does not wait for classification automatically. Missing
this manual check can preserve omissions or unwanted support entries in the
published notes. Existing published notes are preserved on reruns; automated
release-time synchronization is outside this change.

``omit-from-release-notes`` controls GitHub release-note presentation. The local
Jira caller skips add/remove events for this exact label and preserves other
events. Existing Jira copies are not cleaned, and other synchronization paths
may restore copied metadata.

Run the focused automation checks from the repository root in a POSIX shell.
Install ``uv``, Node.js 24 (including npm), and GitHub CLI first, and authenticate
``gh`` with access to public repositories. The action tests require the unmodified
bundle pinned in ``label-release-notes.yml``; they verify its commit and SHA256::

    uv run --no-project python -m unittest discover -s .github/scripts -p 'test_*.py' -v
    npm ci --ignore-scripts --prefix .github
    labeler_ref=$(node <<'NODE'
    const config = require('./.github/node_modules/js-yaml').load(
      require('node:fs').readFileSync('.github/workflows/label-release-notes.yml', 'utf8'));
    const action = config.jobs.classify.steps.find(step => step.id === 'labeler').uses;
    const prefix = 'actions/labeler@';
    const ref = action.slice(prefix.length);
    if (!action.startsWith(prefix) || !/^[0-9a-f]{40}$/.test(ref)) {
      throw Error('Expected pinned labeler');
    }
    console.log(ref);
    NODE
    )
    export LABELER_ACTION_PATH="$(mktemp -d)/labeler.mjs"
    gh api "repos/actions/labeler/contents/dist/index.js?ref=$labeler_ref" \
      --header 'Accept: application/vnd.github.raw+json' > "$LABELER_ACTION_PATH"
    node --test .github/tests/*.test.cjs
