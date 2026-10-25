// Copyright ScyllaDB, Inc. SPDX-License-Identifier: Apache-2.0
const assert = require('node:assert/strict');
const {execFile} = require('node:child_process');
const {createHash} = require('node:crypto');
const {
  chmodSync, copyFileSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync
} = require('node:fs');
const http = require('node:http');
const {tmpdir} = require('node:os');
const {basename, join} = require('node:path');
const {test} = require('node:test');
const {promisify} = require('node:util');
const yaml = require('js-yaml');
const guard = require('../scripts/release-notes-guard.cjs');

const marker = 'omit-from-release-notes';
const commit = 'bf12e9b00b37c5c0ca2b87b79b2daf7891dbda13';
const bundleHash = '7f43b049cc7d6389e192716ecbe99598cc3d72af271192ca0e7fb6171316c43a';
const policy = readFileSync(join(__dirname, '../labeler.yml'), 'utf8');
const workflow = yaml.load(
  readFileSync(join(__dirname, '../workflows/label-release-notes.yml'), 'utf8')
);
const action = workflow.jobs.classify.steps.find((step) => step.id === 'labeler');
const safeguard = yaml.load(
  readFileSync(join(__dirname, '../workflows/release-automation-tests.yml'), 'utf8')
);
const safeguardSteps = safeguard.jobs.tests.steps;
const fetchStep = safeguardSteps.find((step) => step['working-directory']);
const testStep = safeguardSteps.find((step) => step.run?.startsWith('node --test '));
assert.equal(fetchStep.env.LABELER_ACTION_PATH, testStep.env.LABELER_ACTION_PATH);
const artifactName = basename(fetchStep.env.LABELER_ACTION_PATH);
const launchSha = 'a'.repeat(40);
const file = (filename) => ({filename, status: 'modified'});

async function fixture(options, run) {
  const bundle = options.preparedBundle ?? process.env.LABELER_ACTION_PATH;
  assert.ok(bundle, 'Set LABELER_ACTION_PATH to the unmodified pinned dist/index.js');
  assert.equal(action.uses, `actions/labeler@${commit}`);
  assert.equal(createHash('sha256').update(readFileSync(bundle)).digest('hex'), bundleHash);
  const directory = mkdtempSync(join(tmpdir(), 'java-labeler-test-'));
  const executable = options.preparedBundle ?? join(directory, artifactName);
  if (options.preparedBundle) assert.equal(basename(executable), artifactName);
  else copyFileSync(bundle, executable);
  const state = {files: options.files, labels: options.labels ?? ['keep-me']};
  const calls = [];
  const errors = [];
  mkdirSync(join(directory, '.github'));
  if (options.local !== false) writeFileSync(join(directory, '.github/labeler.yml'), policy);
  writeFileSync(join(directory, 'event.json'), '{}');
  writeFileSync(join(directory, 'output'), '');
  const server = http.createServer(async (request, response) => {
    try {
      let body = '';
      for await (const chunk of request) body += chunk;
      const url = new URL(request.url, `http://127.0.0.1:${server.address().port}`);
      calls.push({method: request.method, path: url.pathname, query: url.search, body});
      let value;
      if (url.pathname === '/repos/scylladb/python-driver/pulls/1149/files') {
        const size = Number(url.searchParams.get('per_page') || 30);
        const page = Number(url.searchParams.get('page') || 1);
        value = state.files.slice((page - 1) * size, page * size);
        if (page * size < state.files.length) {
          url.searchParams.set('page', String(page + 1));
          url.searchParams.set('per_page', String(size));
          response.setHeader('link', `<${url}>; rel="next"`);
        }
      } else if (url.pathname === '/repos/scylladb/python-driver/pulls/1149') {
        value = {
          number: 1149,
          labels: state.labels.map((name) => ({name})),
          head: {ref: 'feature'},
          base: {ref: 'master'}
        };
      } else if (
        decodeURIComponent(url.pathname) ===
        '/repos/scylladb/python-driver/contents/.github/labeler.yml'
      ) {
        assert.equal(url.searchParams.get('ref'), launchSha);
        value = {encoding: 'base64', content: Buffer.from(policy).toString('base64')};
      } else if (
        url.pathname === '/repos/scylladb/python-driver/issues/1149/labels' &&
        request.method === 'PUT'
      ) {
        state.labels = JSON.parse(body).labels;
        value = state.labels.map((name) => ({name}));
      } else {
        throw Error(`Unexpected request: ${request.method} ${url}`);
      }
      response.setHeader('content-type', 'application/json');
      response.end(JSON.stringify(value));
    } catch (error) {
      errors.push(error.message);
      response.statusCode = 404;
      response.setHeader('content-type', 'application/json');
      response.end(JSON.stringify({message: error.message}));
    }
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    await run({
      state,
      calls,
      run: async () => {
        const env = {
          ...process.env,
          HTTP_PROXY: '',
          HTTPS_PROXY: '',
          ALL_PROXY: '',
          http_proxy: '',
          https_proxy: '',
          all_proxy: '',
          NO_PROXY: '127.0.0.1',
          no_proxy: '127.0.0.1',
          NODE_ENV: 'production',
          GITHUB_API_URL: `http://127.0.0.1:${server.address().port}`,
          GITHUB_REPOSITORY: 'scylladb/python-driver',
          GITHUB_EVENT_NAME: 'workflow_dispatch',
          GITHUB_EVENT_PATH: join(directory, 'event.json'),
          GITHUB_SHA: launchSha,
          GITHUB_REF: 'refs/heads/master',
          GITHUB_WORKSPACE: directory,
          GITHUB_OUTPUT: join(directory, 'output'),
          'INPUT_REPO-TOKEN': 'fixture-only',
          'INPUT_PR-NUMBER': '1149',
          INPUT_DOT: String(action.with.dot),
          'INPUT_SYNC-LABELS': String(action.with['sync-labels']),
          'INPUT_CONFIGURATION-PATH': action.with['configuration-path']
        };
        await promisify(execFile)(process.execPath, [executable], {
          cwd: directory,
          env,
          timeout: 20000,
          maxBuffer: 2 * 1024 * 1024
        });
        assert.deepEqual(errors, []);
      }
    });
  } finally {
    server.closeAllConnections();
    await new Promise((resolve) => server.close(resolve));
    rmSync(directory, {recursive: true, force: true});
  }
}

test('real labeler shares local policy and removes stale omissions', async () => {
  const files = ['docs/a.md', 'tests/unit/test_pool.py', 'docs/.hidden'].map(file);
  assert.ok(files.every((item) => guard.policy()(item.filename)));
  await fixture({files}, async (f) => {
    await f.run();
    assert.deepEqual(new Set(f.state.labels), new Set(['keep-me', marker]));
    assert.ok(!f.calls.some((call) => call.path.includes('/contents/')));
    f.state.files = [...files, file('cassandra/cluster.py')];
    await f.run();
    assert.deepEqual(f.state.labels, ['keep-me']);
  });
});

test('real action pagination sees runtime files beyond the first support-only page', async () => {
  const files = Array.from({length: 105}, (_, index) => file(`docs/${index}.md`));
  await fixture({files}, async (f) => {
    await f.run();
    assert.ok(f.state.labels.includes(marker));
    assert.equal(f.calls.filter((call) => call.path.endsWith('/files')).length, 4);
    f.state.files.push(file('cassandra/pool.py'));
    await f.run();
    assert.deepEqual(f.state.labels, ['keep-me']);
    assert.equal(f.calls.filter((call) => call.path.endsWith('/files')).length, 8);
  });
});

test('real action API fallback binds configuration to the trusted launch SHA', async () => {
  await fixture({local: false, files: [file('docs/index.rst')]}, async (f) => {
    await f.run();
    assert.ok(f.state.labels.includes(marker));
    assert.equal(f.calls.filter((call) => call.path.includes('/contents/')).length, 1);
  });
});

test('real action and guard keep build and release inputs visible', async () => {
  for (const filename of ['README.rst', 'pyproject.toml', '.github/workflows/lib-build.yml', '.github/workflows/publish-manually.yml']) {
    assert.equal(guard.policy()(filename), false);
    await fixture({files: [file(filename)], labels: ['keep-me', marker]}, async (f) => {
      await f.run();
      assert.deepEqual(f.state.labels, ['keep-me']);
    });
  }
});

test('actual CI bootstrap fetches the configured pin before action tests', async () => {
  const bundle = process.env.LABELER_ACTION_PATH;
  assert.ok(bundle);
  const step = fetchStep;
  assert.equal(step['working-directory'], '.github');
  const directory = mkdtempSync(join(tmpdir(), 'java-labeler-bootstrap-'));
  const binary = join(directory, 'gh');
  const output = join(directory, artifactName);
  const trace = join(directory, 'trace.json');
  writeFileSync(
    binary,
    `#!${process.execPath}
const fs = require('node:fs');
const args = process.argv.slice(2);
if (args[0] !== 'api' || args[1] !==
    'repos/actions/labeler/contents/dist/index.js?ref=${commit}') process.exit(1);
fs.writeFileSync(process.env.TEST_TRACE, JSON.stringify(args));
process.stdout.write(fs.readFileSync(process.env.TEST_BUNDLE));
`
  );
  chmodSync(binary, 0o700);
  try {
    await promisify(execFile)('bash', ['-e', '-o', 'pipefail', '-c', step.run], {
      cwd: join(__dirname, '..'),
      timeout: 10000,
      env: {
        ...process.env,
        PATH: directory + ':' + process.env.PATH,
        LABELER_ACTION_PATH: output,
        TEST_BUNDLE: bundle,
        TEST_TRACE: trace,
        GH_TOKEN: 'fixture-only'
      }
    });
    assert.equal(createHash('sha256').update(readFileSync(output)).digest('hex'), bundleHash);
    const args = JSON.parse(readFileSync(trace, 'utf8'));
    assert.ok(args.includes('Accept: application/vnd.github.raw+json'));
    await fixture({preparedBundle: output, files: [file('docs/bootstrap.md')]}, async (f) => {
      await f.run();
      assert.deepEqual(new Set(f.state.labels), new Set(['keep-me', marker]));
      assert.ok(f.calls.some((call) => call.path.endsWith('/files')));
    });
  } finally {
    rmSync(directory, {recursive: true, force: true});
  }
});
