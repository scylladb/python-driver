// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync} = require('node:fs');
const {tmpdir} = require('node:os');
const {join} = require('node:path');
const {spawnSync} = require('node:child_process');
const {test} = require('node:test');
const yaml = require('js-yaml');

const load = name => yaml.load(readFileSync(join(__dirname, '../workflows', name), 'utf8'));
const automatic = load('build-push.yml');
const manual = load('publish-manually.yml');
const matrix = load('pre-release-matrix.yml');
const needs = job => [job.needs ?? []].flat();
function ancestors(name, seen = new Set()) {
  for (const parent of needs(manual.jobs[name])) {
    if (!seen.has(parent)) {
      seen.add(parent);
      ancestors(parent, seen);
    }
  }
  return seen;
}

test('release automation runs for both version sources on PRs and master pushes', () => {
  const automation = load('release-automation-tests.yml');
  assert.deepEqual(automation.on.push.branches, ['master']);
  for (const event of ['pull_request', 'push']) {
    for (const path of ['cassandra/__init__.py', 'docs/conf.py']) {
      assert.ok(automation.on[event].paths.includes(path), `${event} must check ${path}`);
    }
  }
});

// These contracts guard remote publication boundaries, including skipped or
// failed prerequisite jobs. No status override may bypass the success gate.
test('publication can only start by manual dispatch; pushes only build', () => {
  assert.deepEqual(Object.keys(manual.on), ['workflow_dispatch']);
  assert.deepEqual(Object.keys(manual.on.workflow_dispatch.inputs), ['version']);
  assert.equal(manual.on.workflow_dispatch.inputs.version.required, true);
  assert.equal(automatic.on.push.tags, undefined);
  assert.deepEqual(Object.keys(automatic.jobs), ['build-and-publish']);
  assert.ok(!JSON.stringify(automatic).includes('pypi-publish'));
  assert.equal(manual.concurrency['cancel-in-progress'], false);
});

test('tagging and publication depend on every test gate with no bypass', () => {
  for (const name of ['finalize', 'publish', 'github-release']) {
    const parents = ancestors(name);
    for (const gate of ['prepare', 'build-and-publish', 'pre-release-integration', 'pre-release-matrix']) {
      assert.ok(parents.has(gate), `${name} must wait for ${gate}`);
    }
    assert.equal(manual.jobs[name].if, undefined);
    assert.equal(manual.jobs[name]['continue-on-error'], undefined);
  }
  assert.ok(ancestors('publish').has('finalize'));
  assert.ok(ancestors('github-release').has('publish'));
  const build = manual.jobs['build-and-publish'];
  assert.equal(build.with.ignore_tests, undefined);
  assert.equal(build.with.smoke_tests, undefined);
});

test('every build and test uses one prepared candidate and final tag checks the built SHA', () => {
  const sha = '${{ needs.prepare.outputs.source_sha }}';
  assert.equal(manual.jobs['build-and-publish'].with.target_tag, sha);
  assert.equal(manual.jobs['pre-release-integration'].with.source_sha, sha);
  assert.equal(manual.jobs['pre-release-matrix'].with.source_sha, sha);
  const final = manual.jobs.finalize.steps.at(-1);
  assert.equal(final.env.RELEASE_SOURCE_SHA, sha);
  assert.equal(final.env.RELEASE_BUILT_SHA, '${{ needs.build-and-publish.outputs.source_sha }}');
  assert.equal(manual.jobs['github-release'].with.source_sha, final.env.RELEASE_BUILT_SHA);
  assert.equal(manual.jobs['github-release'].with.tag, '${{ needs.prepare.outputs.tag }}');
  const integration = load('integration-tests.yml');
  assert.equal(integration.on.workflow_call.inputs.source_sha.required, true);
  const checkout = integration.jobs.tests.steps.find(step => step.uses?.startsWith('actions/checkout@'));
  assert.equal(checkout.with.ref, '${{ inputs.source_sha || github.sha }}');
  const build = load('lib-build.yml');
  const resolve = build.jobs['resolve-source'];
  const source = resolve.steps.find(step => step.uses?.startsWith('actions/checkout@'));
  assert.equal(source.with.ref, '${{ inputs.target_tag || github.sha }}');
  assert.equal(resolve.outputs.sha, '${{ steps.source.outputs.sha }}');
  assert.ok(resolve.steps.some(step => step.id === 'source'));
  assert.equal(build.on.workflow_call.outputs.source_sha.value, '${{ jobs.resolve-source.outputs.sha }}');
  for (const name of ['build-wheels', 'build-sdist']) {
    const job = build.jobs[name];
    assert.ok(needs(job).includes('resolve-source'));
    const checkouts = job.steps.filter(step => step.uses?.startsWith('actions/checkout@'));
    assert.ok(checkouts.length > 0, `${name} must check out the build source`);
    for (const step of checkouts) assert.equal(step.with.ref, '${{ needs.resolve-source.outputs.sha }}');
  }
});

test('downloaded distributions are checked before PyPI upload using trusted helper code', () => {
  const steps = manual.jobs.publish.steps;
  const checkout = steps.find(step => step.uses?.startsWith('actions/checkout@'));
  assert.equal(checkout.with.ref, '${{ github.sha }}');
  assert.equal(checkout.with['persist-credentials'], false);
  assert.equal(manual.jobs.publish.permissions.contents, 'read');
  assert.equal(manual.jobs.publish.environment, undefined);
  const downloads = steps.filter(step => step.uses?.startsWith('actions/download-artifact@'));
  assert.equal(downloads.length, 2);
  assert.deepEqual(downloads.map(step => step.with.pattern || step.with.name), ['wheels-*', 'source-dist']);
  assert.ok(downloads.every(step => step.with.path === 'dist'));
  const publisher = steps.find(step => step.uses?.startsWith('pypa/gh-action-pypi-publish@'));
  const guard = steps.find(step => step.run?.includes('check_pypi_release.py'));
  assert.equal(guard.env.RELEASE_VERSION, '${{ inputs.version }}');
  assert.equal(guard.if, undefined);
  assert.equal(guard['continue-on-error'], undefined);
  for (const download of downloads) {
    assert.ok(steps.indexOf(checkout) < steps.indexOf(download));
    assert.ok(steps.indexOf(download) < steps.indexOf(guard));
  }
  assert.ok(steps.indexOf(guard) < steps.indexOf(publisher));
  assert.equal(publisher.with['packages-dir'], 'dist');
  assert.equal(publisher.with['skip-existing'], false);
  assert.equal(manual.jobs.publish.permissions['id-token'], 'write');
});

test('actual publication guard command checks downloaded filenames and blocks the uploader on failure', () => {
  const guard = manual.jobs.publish.steps.find(step => step.run?.includes('check_pypi_release.py'));
  const directory = mkdtempSync(join(tmpdir(), 'python-pypi-check-'));
  const uploaded = join(directory, 'uploaded');
  const trace = join(directory, 'request');
  const wheel = 'scylla_driver-3.29.13-py3-none-any.whl';
  const sdist = 'scylla_driver-3.29.13.tar.gz';
  mkdirSync(join(directory, '.github/scripts'), {recursive: true});
  writeFileSync(join(directory, '.github/scripts/check_pypi_release.py'),
    readFileSync(join(__dirname, '../scripts/check_pypi_release.py')));
  mkdirSync(join(directory, 'dist'));
  for (const filename of [wheel, sdist]) writeFileSync(join(directory, 'dist', filename), 'artifact');
  writeFileSync(join(directory, 'decoy.whl'), 'outside upload directory');
  // Substitute HTTP only in this temporary Python process; execute the actual
  // workflow command and helper with their normal CLI and filesystem inputs.
  writeFileSync(join(directory, 'sitecustomize.py'), `
import io
import os
from pathlib import Path
from urllib.error import HTTPError
import urllib.request

def inventory(request, timeout):
    Path(os.environ['TRACE']).write_text(request.full_url)
    status = int(os.environ['INVENTORY_STATUS'])
    if status != 200:
        raise HTTPError(request.full_url, status, 'fixture', {}, None)
    response = io.BytesIO(os.environ['INVENTORY'].encode())
    response.status = status
    return response

urllib.request.urlopen = inventory
`);
  try {
    for (const [status, inventory, allowed, error] of [
      [404, '', true],
      [200, JSON.stringify({urls: [{filename: 'decoy.whl'}]}), true],
      [200, JSON.stringify({urls: [{filename: wheel}]}), false, /ValueError: Release files already exist on PyPI;/],
      [200, JSON.stringify({urls: [{filename: sdist}]}), false, /ValueError: Release files already exist on PyPI;/],
      [503, '', false, /HTTPError: HTTP Error 503:/],
      [200, '{invalid', false, /JSONDecodeError:/]
    ]) {
      rmSync(uploaded, {force: true});
      rmSync(trace, {force: true});
      const result = spawnSync('bash', ['-e', '-o', 'pipefail', '-c', `${guard.run}\nprintf uploaded > "$UPLOADED"`], {
        cwd: directory, encoding: 'utf8', timeout: 5000,
        env: {...process.env, PYTHONPATH: directory, RELEASE_VERSION: '3.29.13',
          TRACE: trace, UPLOADED: uploaded, INVENTORY_STATUS: String(status), INVENTORY: inventory}
      });
      assert.equal(result.status === 0, allowed, result.stderr);
      assert.equal(readFileSync(trace, 'utf8'), 'https://pypi.org/pypi/scylla-driver/3.29.13/json');
      if (allowed) assert.equal(readFileSync(uploaded, 'utf8'), 'uploaded');
      else {
        assert.match(result.stderr, error);
        assert.throws(() => readFileSync(uploaded), {code: 'ENOENT'});
      }
    }
  } finally { rmSync(directory, {recursive: true, force: true}); }
});

test('distribution builds keep the GitHub token read-only in every job', () => {
  const build = load('lib-build.yml');
  for (const job of Object.values(build.jobs)) {
    assert.deepEqual(job.permissions ?? build.permissions, {contents: 'read'});
  }
});

test('commit, tag and release jobs mint repository-scoped release App credentials', () => {
  for (const job of [manual.jobs.prepare, manual.jobs.finalize, load('github-release.yml').jobs.release]) {
    assert.equal(job.environment, 'release');
    const app = job.steps.find(step => step.id === 'app-token');
    assert.equal(app.uses, 'actions/create-github-app-token@bcd2ba49218906704ab6c1aa796996da409d3eb1');
    assert.equal(app.with['app-id'], '${{ vars.RELEASE_APP_ID }}');
    assert.equal(app.with['private-key'], '${{ secrets.RELEASE_APP_PRIVATE_KEY }}');
    assert.equal(app.with.repositories, '${{ github.event.repository.name }}');
    assert.equal(app.with['permission-contents'], 'write');
    assert.ok(job.steps.some(step => step.env?.GH_TOKEN === '${{ steps.app-token.outputs.token }}'));
  }
});

test('matrix covers four Scylla lines and checks out pinned harness plus exact driver SHA', () => {
  const job = matrix.jobs.matrix;
  assert.deepEqual(job.strategy.matrix.include.map(row => row.scylla), ['LATEST', 'PRIOR', 'LTS-LATEST', 'LTS-PRIOR']);
  const checkouts = job.steps.filter(step => step.uses?.startsWith('actions/checkout@'));
  const harness = checkouts.find(step => step.with.repository === 'scylladb/python-driver-matrix');
  assert.match(harness.with.ref, /^[0-9a-f]{40}$/);
  const driver = checkouts.find(step => step.with.path === 'driver');
  assert.equal(driver.with.ref, '${{ inputs.source_sha }}');
  assert.equal(job['continue-on-error'], undefined);
});

test('actual matrix launch rejects unresolved versions and forwards pinned candidate and protocols', () => {
  const step = matrix.jobs.matrix.steps.at(-1);
  const directory = mkdtempSync(join(tmpdir(), 'python-matrix-launch-'));
  const trace = join(directory, 'trace');
  mkdirSync(join(directory, 'scripts'));
  writeFileSync(join(directory, 'scripts/run_test.sh'), '#!/bin/bash\nprintf "%s\\n" "$SCYLLA_VERSION" "$PYTEST_ADDOPTS" "$@" > "$TRACE"\n', {mode: 0o700});
  try {
    for (const versions of ['["2026.1.13"]', '"2026.1.13"', '["2025.1.19"]', '[]', 'null', '["invalid"]']) {
      const result = spawnSync('bash', ['-e', '-o', 'pipefail', '-c', step.run], {
        cwd: directory, encoding: 'utf8', timeout: 5000,
        env: {...process.env, TRACE: trace, SCYLLA_VERSIONS: versions,
          DRIVER_SHA: 'a'.repeat(40), PYTHON_DRIVER_DIR: '/driver', PYTEST_ADDOPTS: ''}
      });
      if (versions.includes('2026.1.13') || versions.includes('2025.1.19')) {
        const legacy = versions.includes('2025.1.19');
        const exclusions = legacy ? [
          '--ignore=tests/integration/standard/test_tablets_routing_v2.py',
          '--deselect=tests/integration/standard/test_application_info.py::ApplicationInfoTest::test_create_session_and_check_system_views_clients',
          '--deselect=tests/integration/standard/test_tls_resumption.py::TLSSessionResumptionTests::test_every_connection_resumes_from_a_warmed_cache',
          '--deselect=tests/integration/standard/test_tls_resumption.py::TLSSessionResumptionTests::test_resumption_is_on_by_default_with_an_ssl_context'
        ].join(' ') : '';
        const version = legacy ? 'release:2025.1.19' : 'release:2026.1.13';
        assert.equal(result.status, 0, result.stderr);
        assert.deepEqual(readFileSync(trace, 'utf8').split('\n').slice(0, -1), [
          version, exclusions, 'python3', './main.py', '/driver', '--driver-type', 'scylla',
          '--versions', 'a'.repeat(40), '--protocols', '3,4', '--scylla-version', version
        ]);
      } else {
        assert.notEqual(result.status, 0, versions);
      }
    }
  } finally { rmSync(directory, {recursive: true, force: true}); }
});
