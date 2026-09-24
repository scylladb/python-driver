Release Preparation Guide
=========================

Follow these steps to prepare a new release (example: `3.29.6`):

1) Version bump
   - Update `cassandra/__init__.py` `__version_info__`/`__version__`.
   - Add new tag with `-scylla` postfix to `docs/conf.py` `TAGS` and set `LATEST_VERSION`.
   - Refresh any user-facing version strings (e.g. `docs/installation.rst`).

2) Changelog
   - Add a new section at the top of `CHANGELOG.rst` with the release date.
   - List each merged PR since the previous release in the format `* <description> (#<PR-ID>)`.

3) Dependency/docs metadata
   - Update `docs/conf.py` multiversion tag list if new published doc tags are required.
   - If documentation styling or assets changed, ensure `uv.lock` (or other doc assets) are refreshed.

4) Tests and validation
   - Run unit tests: `uv run pytest tests/unit`.
   - Spot-check relevant integration tests if new protocol or cluster behavior was touched.
   - Build artifacts to confirm packaging still works: `uv run python -m build`.

5) Commit and tag
   - Commit with an imperative subject, e.g. `Release 3.29.6: changelog, version and documentation`.
   - Tag the commit as `3.29.6-scylla` if publishing docs, and `3.29.6` for the driver if applicable.

6) Publish checklist
   - Push tags and commit.
   - Publish wheels/sdist to the package index.
   - Trigger docs build for the new tag/branch.
