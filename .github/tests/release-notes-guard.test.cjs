// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const {test} = require('node:test');
const {spawnSync} = require('node:child_process');
const yaml = require('js-yaml');
const guard = require('../scripts/release-notes-guard.cjs');

const repo = {owner: 'scylladb', repo: 'python-driver'};
const label = 'omit-from-release-notes';
const file = (filename, extra = {}) => ({filename, status: 'modified', ...extra});
const workflowText = readFileSync(join(__dirname, '../workflows/label-release-notes.yml'), 'utf8');
const workflow = yaml.load(workflowText);
const steps = workflow.jobs.classify.steps;
const scripts = steps.filter(step => step.with?.script).map(step => step.with.script);
const collectScript = steps.find(step => step.id === 'guard').with.script;
const confirmScript = steps.find(step => step.id === 'target').with.script;
const verifyScript = steps.at(-1).with.script;
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor;

function fixture(options = {}) {
  const files = options.files ?? [file('docs/guide.rst')];
  const labels = new Set(options.labels ?? ['unrelated']);
  const calls = [];
  let gets = 0;
  let listings = 0;
  const initial = {number: 20, head: {sha: 'head'}, base: {sha: 'base'}, changed_files: files.length};
  const github = {rest: {pulls: {
    get: async params => {
      assert.deepEqual(params, {...repo, pull_number: 20});
      calls.push('get');
      gets++;
      if (options.failGetAt === gets) throw new Error('PR lookup unavailable');
      return {data: {...initial, ...options.revisions?.[gets - 1]}};
    },
    listFiles: () => {}
  }, issues: {
    listLabelsOnIssue: () => {},
    removeLabel: async params => {
      assert.deepEqual(params, {...repo, issue_number: 20, name: label});
      calls.push('remove');
      if (options.cleanupFailure) throw new Error('Removal unavailable');
      const actual = [...labels].find(name => name.toLowerCase() === label);
      if (actual === undefined) throw {status: 404};
      labels.delete(actual);
    }
  }}, paginate: async (method, params) => {
    assert.equal(params.per_page, 100);
    if (method === github.rest.pulls.listFiles) {
      assert.deepEqual(params, {...repo, pull_number: 20, per_page: 100});
      calls.push('files');
      if (options.fileFailure) throw new Error('File listing unavailable');
      return options.listings?.[listings++] ?? files;
    }
    assert.equal(method, github.rest.issues.listLabelsOnIssue);
    assert.deepEqual(params, {...repo, issue_number: 20, per_page: 100});
    calls.push('labels');
    if (options.labelFailure) throw new Error('Labels unavailable');
    return options.labelRecords ?? [...labels].map(name => ({name}));
  }};
  return {github, labels, calls};
}

async function adapter(f, hash, overrides = {}, load = () => guard) {
  const env = {PR_NUMBER: '20', CONFIRMED_PR: '20', TARGET_OUTCOME: 'success',
    CHECKOUT_OUTCOME: 'success', INSTALL_OUTCOME: 'success',
    GUARD_OUTCOME: 'success', LABELER_OUTCOME: 'success', UNSAFE_RENAME: 'false',
    SNAPSHOT_HASH: hash, ...overrides};
  const logs = [];
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', verifyScript);
  let error;
  try {
    await run(f.github, {repo}, {info: text => logs.push(text), error: text => logs.push(text)},
      load, {env});
  } catch (caught) { error = caught; }
  return {error, logs};
}

test('canonical policy matches mixed support files and keeps runtime or unknown paths visible', async () => {
  const files = ['.github/workflows/coverage.yml', '.github/workflows/docs-pages.yml',
    '.github/labeler.yml', '.github/release.yml', '.github/ISSUE_TEMPLATE/bug.md',
    'docs/a/b.md', 'docs/guide.rst', 'tests/unit/test_pool.py',
    'CONTRIBUTING.rst', '.gitignore', '.gitattributes', 'scripts/coverage.sh',
    'scripts/check_numpy_imports.py'].map(name => file(name));
  assert.equal((await guard.snapshot(fixture({files}).github, repo, 20)).omit, true);
  for (const name of ['README.rst', 'cassandra/pool.py', 'pyproject.toml', 'uv.lock', 'setup.py',
    'Makefile', 'LICENSE', 'examples/basic.py', 'scripts/release.py', 'unknown.txt',
    '.github/unknown.yml', '.github/workflows/lib-build.yml',
    '.github/workflows/build-push.yml', '.github/workflows/publish-manually.yml',
    '.github/workflows/github-release.yml']) {
    const current = await guard.snapshot(fixture({files: [...files, file(name)]}).github, repo, 20);
    assert.equal(current.omit, false, name);
  }
});

test('renames use the actual canonical policy for both prior and current paths', async () => {
  for (const [previous, omit] of [['docs/old.md', true], ['README.rst', false],
    ['cassandra/pool.py', false], ['unknown', false]]) {
    const current = await guard.snapshot(fixture({files: [file('docs/new.md', {
      status: 'renamed', previous_filename: previous})]}).github, repo, 20);
    assert.equal(current.omit, omit);
    assert.equal(current.unsafeRename, !omit);
  }
});

test('unsupported config shapes fail instead of approximating a second policy', () => {
  for (const source of ['{}', 'omit-from-release-notes: []',
    'omit-from-release-notes: [{head-branch: [main]}]',
    'omit-from-release-notes: [{changed-files: [{any-glob-to-any-file: ["**"]}]}]',
    'omit-from-release-notes: [{changed-files: [{all-globs-to-all-files: []}]}]',
    'other-label: [{changed-files: [{all-globs-to-all-files: ["**"]}]}]']) {
    assert.throws(() => guard.policy(source), /Unsupported labeler configuration/);
  }
});

test('PR input rejects malformed, ambiguous and unsafe numbers', () => {
  assert.equal(guard.parseNumber('20'), 20);
  for (const value of ['', '0', '-1', '01', '1.0', '1e2', ' 20', '20\n',
    '20,21', '${{ arbitrary }}', '9007199254740992', undefined, 20]) {
    assert.throws(() => guard.parseNumber(value), /canonical positive safe PR number/);
  }
});

test('pagination requests full metadata and accepts a complete listing beyond one page', async () => {
  const f = fixture({files: Array.from({length: 105}, (_, i) => file(`docs/${i}.md`))});
  const snapshot = await guard.snapshot(f.github, repo, 20);
  assert.equal(snapshot.count, 105);
  assert.equal(snapshot.omit, true);
  assert.match(snapshot.hash, /^[0-9a-f]{64}$/);
  assert.deepEqual(f.calls, ['get', 'files', 'get']);
});

test('zero, incomplete, truncated, duplicate and invalid file evidence fail visibly', async () => {
  const cases = [
    {files: []},
    {revisions: [{changed_files: 2}, {changed_files: 2}]},
    {files: [file('README.rst'), file('README.rst')]},
    {files: [file('README.rst', {status: 'renamed'})]},
    {files: [file('README.rst', {status: undefined})]},
    {files: [file('README.rst', {previous_filename: ''})]},
    {files: [file('README.rst')], listings: [[]]}
  ];
  for (const options of cases) await assert.rejects(guard.snapshot(fixture(options).github, repo, 20));
});

test('head, base and count changes during collection are rejected', async () => {
  for (const changed of [{head: {sha: 'new'}}, {base: {sha: 'new'}}, {changed_files: 2}]) {
    const f = fixture({revisions: [{}, changed]});
    await assert.rejects(guard.snapshot(f.github, repo, 20), /revisions changed/);
  }
});

test('verification detects before/after revisions and filename, status or prior-path changes', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [
    {revisions: [{head: {sha: 'new'}}, {head: {sha: 'new'}}]},
    {revisions: [{base: {sha: 'new'}}, {base: {sha: 'new'}}]},
    {files: [file('docs/new.md')]},
    {files: [file('docs/guide.rst', {status: 'added'})]},
    {files: [file('docs/guide.rst', {status: 'renamed', previous_filename: 'docs/old.md'})]}
  ]) {
    await assert.rejects(guard.verify(fixture(options).github, repo, 20, original.hash), /snapshot changed/);
  }
  const f = fixture({labels: [label], revisions: [{}, {}, {head: {sha: 'new'}}]});
  await assert.rejects(guard.verify(f.github, repo, 20, original.hash), /revisions changed during label/);
});

test('lookup, listing and actual-label lookup failures propagate', async () => {
  await assert.rejects(guard.snapshot(fixture({failGetAt: 1}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({failGetAt: 2}).github, repo, 20), /lookup unavailable/);
  await assert.rejects(guard.snapshot(fixture({fileFailure: true}).github, repo, 20), /listing unavailable/);
  const original = await guard.snapshot(fixture().github, repo, 20);
  await assert.rejects(guard.verify(fixture({labelFailure: true}).github, repo, 20, original.hash), /Labels unavailable/);
});

test('actual-label verification catches stale state after internal action skip', async () => {
  for (const [files, labels] of [[[file('docs/guide.rst')], []],
    [[file('README.rst')], [label]], [[file('cassandra/pool.py')], [label]]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error} = await adapter(f, original.hash);
    assert.match(error.message, /Actual omission label/);
    assert.equal(f.labels.has(label), false);
    assert.equal(f.labels.has('unrelated'), true);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('failed final snapshot, lookup or label retrieval attempts cleanup', async () => {
  const original = await guard.snapshot(fixture().github, repo, 20);
  for (const options of [{files: [file('docs/changed.md')]}, {failGetAt: 1},
    {fileFailure: true}, {labelFailure: true}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const {error} = await adapter(f, original.hash);
    assert.ok(error);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.equal(f.calls.filter(call => call === 'remove').length, 1);
  }
});

test('successful verification checks actual labels and preserves unrelated labels', async () => {
  for (const [files, labels] of [[[file('docs/guide.rst')], [label]],
    [[file('README.rst')], []], [[file('cassandra/pool.py')], []]]) {
    const f = fixture({files, labels: [...labels, 'unrelated']});
    const original = await guard.snapshot(f.github, repo, 20);
    const {error, logs} = await adapter(f, original.hash);
    assert.ifError(error);
    assert.match(logs[0], /Verified head=head base=base count=1 snapshot=.*actual label checked/);
    assert.equal(f.labels.has('unrelated'), true);
    assert.ok(!f.calls.includes('remove'));
  }
});

test('actual omission identity preserves casing and rejects runtime exclusions', async () => {
  const marker = 'Omit-from-release-notes';
  const distinct = ['unrelated', 'omit-from-release-notes-extra', 'prefix-omit-from-release-notes'];
  const support = fixture({labels: [marker, ...distinct]});
  const supportSnapshot = await guard.snapshot(support.github, repo, 20);
  const accepted = await adapter(support, supportSnapshot.hash);
  assert.ifError(accepted.error);
  assert.deepEqual([...support.labels], [marker, ...distinct]);
  assert.ok(!support.calls.includes('remove'));

  const runtime = fixture({files: [file('cassandra/pool.py')], labels: [marker, ...distinct]});
  const runtimeSnapshot = await guard.snapshot(runtime.github, repo, 20);
  const rejected = await adapter(runtime, runtimeSnapshot.hash);
  assert.match(rejected.error.message, /Actual omission label/);
  assert.deepEqual([...runtime.labels], distinct);
  assert.equal(runtime.calls.filter(call => call === 'remove').length, 1);
});

test('malformed label records fail verification and attempt cleanup', async () => {
  for (const labelRecords of [[null], [{}], [{name: null}], [{name: ''}]]) {
    const f = fixture({labels: [label, 'unrelated'], labelRecords});
    const original = await guard.snapshot(f.github, repo, 20);
    const result = await adapter(f, original.hash);
    assert.match(result.error.message, /Invalid label metadata/);
    assert.deepEqual([...f.labels], ['unrelated']);
  }
});

test('intentional unsafe-rename skip removes omission label and verifies visible state', async () => {
  const f = fixture({files: [file('docs/new.md', {status: 'renamed', previous_filename: 'cassandra/pool.py'})],
    labels: [label, 'unrelated']});
  const original = await guard.snapshot(f.github, repo, 20);
  const {error, logs} = await adapter(f, original.hash, {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.ifError(error);
  assert.match(logs[0], /omit=false; actual label checked/);
  assert.deepEqual([...f.labels], ['unrelated']);
  assert.equal(f.calls.filter(call => call === 'remove').length, 1);
});

test('failed or skipped setup, guard and labeler clean up without requiring the helper', async () => {
  for (const key of ['CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    for (const outcome of ['failure', 'skipped', 'cancelled', '']) {
      const f = fixture({labels: [label, 'unrelated']});
      const {error} = await adapter(f, '', {[key]: outcome}, () => { throw new Error('Must not require helper'); });
      assert.match(error.message, /predecessor failed or unexpectedly skipped/);
      assert.deepEqual([...f.labels], ['unrelated']);
      assert.deepEqual(f.calls, ['remove']);
    }
  }
  const f = fixture({labels: [label]});
  const {error} = await adapter(f, '', {}, () => { throw new Error('Helper missing'); });
  assert.match(error.message, /Helper missing/);
  assert.deepEqual(f.calls, ['remove']);
});

test('cleanup API failures remain visible without losing the original failure or retrying', async () => {
  const f = fixture({cleanupFailure: true});
  const result = await adapter(f, '', {INSTALL_OUTCOME: 'failure'});
  assert.match(result.error.message, /predecessor failed/);
  assert.match(result.logs[0], /cleanup failed: Removal unavailable/);
  assert.deepEqual(f.calls, ['remove']);
  const unsafe = fixture({cleanupFailure: true});
  const result2 = await adapter(unsafe, '', {UNSAFE_RENAME: 'true', LABELER_OUTCOME: 'skipped'});
  assert.match(result2.error.message, /Removal unavailable/);
  assert.deepEqual(unsafe.calls, ['remove']);
});

async function confirm(f, context = {repo, eventName: 'workflow_dispatch'}) {
  const outputs = {};
  const run = new AsyncFunction('github', 'context', 'core', 'process', confirmScript);
  let error;
  try {
    await run(f.github, context, {setOutput: (key, value) => {outputs[key] = value;}},
      {env: {PR_NUMBER: '20'}});
  } catch (caught) {error = caught;}
  return {outputs, error};
}

test('target confirmation precedes checkout and accepts manual PR or trusted PR event identity', async () => {
  const targetIndex = steps.findIndex(step => step.id === 'target');
  assert.ok(targetIndex > steps.findIndex(step => step.id === 'dispatch-ref'));
  assert.ok(targetIndex < steps.findIndex(step => step.id === 'checkout'));
  assert.ok(targetIndex < steps.findIndex(step => step.id === 'install'));
  assert.equal(workflow.jobs.classify.env.PR_NUMBER, '${{ github.event.pull_request.number || inputs.pr_number }}');
  const manual = fixture({labels: [label, 'unrelated']});
  const first = await confirm(manual);
  assert.ifError(first.error);
  assert.deepEqual(first.outputs, {'confirmed-pr': '20'});
  assert.deepEqual(manual.calls, ['get']);
  const snapshot = await guard.snapshot(manual.github, repo, 20);
  const verified = await adapter(manual, snapshot.hash, {CONFIRMED_PR: first.outputs['confirmed-pr']});
  assert.ifError(verified.error);
  assert.match(verified.logs[0], /omit=true; actual label checked/);
  const event = fixture({failGetAt: 1});
  const second = await confirm(event, {repo, eventName: 'pull_request_target',
    payload: {pull_request: {number: 20}}});
  assert.ifError(second.error);
  assert.deepEqual(second.outputs, {'confirmed-pr': '20'});
  assert.deepEqual(event.calls, []);
});

test('ordinary issue, unavailable lookup and mismatched identity never enable label mutation', async () => {
  for (const options of [{failGetAt: 1}, {revisions: [{number: 21}]}, {revisions: [{number: null}]}]) {
    const f = fixture({...options, labels: [label, 'unrelated']});
    const target = await confirm(f);
    assert.ok(target.error);
    assert.deepEqual(target.outputs, {});
    const result = await adapter(f, '', {CONFIRMED_PR: '', TARGET_OUTCOME: 'failure'},
      () => {throw new Error('Must not load helper for unconfirmed target');});
    assert.match(result.error.message, /not confirmed as a pull request/);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
    assert.ok(!f.calls.includes('remove'));
  }
  const issue = fixture({labels: [label, 'unrelated']});
  issue.github.rest.pulls.get = async () => {throw {status: 404};};
  const target = await confirm(issue);
  assert.equal(target.error.status, 404);
  const result = await adapter(issue, '', {CONFIRMED_PR: '', TARGET_OUTCOME: 'failure'});
  assert.match(result.error.message, /labels were not changed/);
  assert.deepEqual([...issue.labels], [label, 'unrelated']);
  assert.ok(!issue.calls.includes('remove'));
});

test('confirmed manual PR retains helper-independent cleanup after later failures', async () => {
  for (const key of ['CHECKOUT_OUTCOME', 'INSTALL_OUTCOME', 'GUARD_OUTCOME', 'LABELER_OUTCOME']) {
    const f = fixture({labels: [label, 'unrelated'], failGetAt: 2});
    const target = await confirm(f);
    assert.ifError(target.error);
    const result = await adapter(f, '', {CONFIRMED_PR: target.outputs['confirmed-pr'], [key]: 'failure'},
      () => {throw new Error('Must not need helper for predecessor failure');});
    assert.match(result.error.message, /predecessor failed/);
    assert.deepEqual([...f.labels], ['unrelated']);
    assert.deepEqual(f.calls, ['get', 'remove']);
  }
});

test('trusted PR event retains cleanup without a later identity read', async () => {
  const f = fixture({labels: [label, 'unrelated'], failGetAt: 1});
  const target = await confirm(f, {repo, eventName: 'pull_request_target',
    payload: {pull_request: {number: 20}}});
  assert.ifError(target.error);
  const result = await adapter(f, '', {CONFIRMED_PR: target.outputs['confirmed-pr'], GUARD_OUTCOME: 'failure'},
    () => {throw new Error('Must not load helper');});
  assert.match(result.error.message, /predecessor failed/);
  assert.deepEqual(f.calls, ['remove']);
  assert.deepEqual([...f.labels], ['unrelated']);
});

test('missing or different persisted target identity never enables cleanup', async () => {
  for (const confirmed of ['', '21', 'true']) {
    const f = fixture({labels: [label, 'unrelated']});
    const result = await adapter(f, '', {CONFIRMED_PR: confirmed, CHECKOUT_OUTCOME: 'failure'});
    assert.match(result.error.message, /not confirmed as a pull request/);
    assert.deepEqual(f.calls, []);
    assert.deepEqual([...f.labels], [label, 'unrelated']);
  }
  const finalizer = steps.at(-1);
  assert.equal(finalizer.env.CONFIRMED_PR, '${{ steps.target.outputs.confirmed-pr }}');
  assert.equal(finalizer.env.TARGET_OUTCOME, '${{ steps.target.outcome }}');
});

test('workflow adapters use environment input and emit bounded verified snapshot outputs', async () => {
  const run = new AsyncFunction('github', 'context', 'core', 'require', 'process', collectScript);
  const outputs = {};
  const f = fixture();
  await run(f.github, {repo}, {setOutput: (key, value) => { outputs[key] = value; }, info: () => {}},
    () => guard, {env: {PR_NUMBER: '20'}});
  assert.deepEqual(Object.keys(outputs), ['hash', 'unsafe-rename']);
  assert.equal(outputs['unsafe-rename'], 'false');
  const invalid = await adapter(f, '', {PR_NUMBER: '01'});
  assert.match(invalid.error.message, /canonical positive safe PR number/);
  assert.equal(steps.at(-1).if, 'always()');
  const checkout = steps.find(step => step.id === 'checkout');
  assert.equal(checkout.with.ref, '${{ github.sha }}');
  assert.equal(checkout.with['persist-credentials'], false);
  assert.equal(steps.find(step => step.id === 'install').run, 'npm ci --ignore-scripts --prefix .github');
  const action = steps.find(step => step.id === 'labeler');
  assert.equal(action.if, "success() && steps.guard.outputs.unsafe-rename == 'false'");
  assert.equal(action.with['sync-labels'], true);
  assert.equal(action.with['pr-number'], '${{ env.PR_NUMBER }}');
  for (const script of scripts) assert.ok(!script.includes('${{'));
});

test('manual backfills reject nondefault refs before loading helper code', () => {
  const check = steps.find(step => step.id === 'dispatch-ref');
  assert.equal(check.if, "github.event_name == 'workflow_dispatch'");
  assert.equal(check.env.DEFAULT_BRANCH, '${{ github.event.repository.default_branch }}');
  assert.ok(steps.indexOf(check) < steps.findIndex(step => step.id === 'checkout'));
  for (const [ref, branch, success] of [
    ['refs/heads/master', 'master', true],
    ['refs/heads/feature', 'master', false],
    ['refs/tags/v1.0.0', 'master', false],
    ['refs/heads/master', '', false]
  ]) {
    const result = spawnSync('bash', ['-c', check.run], {
      env: {...process.env, GITHUB_REF: ref, DEFAULT_BRANCH: branch}, encoding: 'utf8'
    });
    assert.equal(result.status === 0, success, ref);
    if (!success) assert.match(result.stderr, /default branch/);
  }
});
