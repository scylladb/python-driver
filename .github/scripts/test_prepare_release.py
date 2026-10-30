"""Exercise release preparation and promotion against disposable Git remotes.
"""

import ast
from contextlib import contextmanager
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import prepare_release as release


class RepositorySourcesTest(unittest.TestCase):
    def test_release_transforms_actual_repository_version_sources(self):
        root = Path(__file__).resolve().parents[2]
        sources = {path: (root / path).read_text() for path in (
            'cassandra/__init__.py', 'docs/conf.py')}
        current = ast.literal_eval(release.assignment(
            sources['cassandra/__init__.py'], '__version_info__').value)
        docs = sources['docs/conf.py']
        tags = ast.literal_eval(release.assignment(docs, 'TAGS').value)
        deprecated = ast.literal_eval(
            release.assignment(docs, 'DEPRECATED_VERSIONS').value)
        current_tag = '.'.join(map(str, current)) + '-scylla'
        major, minor, patch_version = current
        for requested in ((major, minor, patch_version + 1),
                          (major, minor + 1, 0), (major + 1, 0, 0)):
            with self.subTest(requested=requested):
                version = '.'.join(map(str, requested))
                tag = version + '-scylla'
                with patch.object(release, 'git_file', side_effect=(
                        lambda revision, path: sources[path])):
                    prepared = release.release_files('checkout', version)
                self.assertEqual(
                    set(prepared), {'cassandra/__init__.py', 'docs/conf.py'})
                self.assertEqual(
                    ast.literal_eval(release.assignment(
                        prepared['cassandra/__init__.py'],
                        '__version_info__').value), requested)
                expected_tags = list(tags)
                expected_deprecated = list(deprecated)
                if requested[:2] == current[:2]:
                    expected_tags[expected_tags.index(current_tag)] = tag
                else:
                    expected_tags.append(tag)
                    if current_tag not in expected_deprecated:
                        expected_deprecated.append(current_tag)
                updated_docs = prepared['docs/conf.py']
                self.assertEqual(
                    ast.literal_eval(
                        release.assignment(updated_docs, 'TAGS').value),
                    expected_tags)
                self.assertEqual(
                    ast.literal_eval(release.assignment(
                        updated_docs, 'DEPRECATED_VERSIONS').value),
                    expected_deprecated)
                self.assertEqual(
                    ast.literal_eval(release.assignment(
                        updated_docs, 'LATEST_VERSION').value), tag)


class PreparationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.remote = root / 'remote.git'
        self.work = root / 'work'
        self.work.mkdir()
        self.previous = Path.cwd()
        os.chdir(self.work)
        self.addCleanup(os.chdir, self.previous)
        self.env = patch.dict(os.environ, {
            'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null',
            'GITHUB_REPOSITORY': 'scylladb/python-driver',
            'GITHUB_EVENT_NAME': 'workflow_dispatch',
            'GITHUB_REF': 'refs/heads/master',
            'RELEASE_DEFAULT_BRANCH': 'master', 'RELEASE_VERSION': '3.29.13',
            'RELEASE_APP_SLUG': 'scylladb-releaser',
            'GITHUB_OUTPUT': str(root / 'output'),
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        self.run_git('init', '--bare', str(self.remote))
        self.run_git('init', '-b', 'master')
        self.run_git('config', 'user.name', 'Maintainer')
        self.run_git('config', 'user.email', 'maintainer@example.com')
        Path('cassandra').mkdir()
        Path('docs').mkdir()
        Path('cassandra/__init__.py').write_text(
            '"""Driver."""\n__version_info__ = (3, 29, 12)\n')
        Path('docs/conf.py').write_text(
            "TAGS = ['3.28.1-scylla', '3.29.1-scylla', '3.29.12-scylla']\n"
            "LATEST_VERSION = '3.29.12-scylla'\n"
            "DEPRECATED_VERSIONS = ['3.28.1-scylla']\n")
        self.run_git('add', '.')
        self.run_git('commit', '-m', 'Baseline')
        self.base = self.run_git('rev-parse', 'HEAD')
        os.environ['GITHUB_SHA'] = self.base
        self.run_git('remote', 'add', 'origin', str(self.remote))
        self.run_git('push', 'origin', 'HEAD:refs/heads/master')
        original = release.command

        def command(*args):
            if args[0] == 'gh':
                self.assertEqual(
                    args, ('gh', 'api', 'users/scylladb-releaser[bot]',
                           '--jq', '.id'))
                return '12345'
            return original(*args)

        self.gh = patch.object(release, 'command', side_effect=command)
        self.gh.start()
        self.addCleanup(self.gh.stop)

    @staticmethod
    def run_git(*args):
        return subprocess.check_output(
            ['git', *args], text=True, stderr=subprocess.DEVNULL).strip()

    def ref(self, ref):
        return self.run_git('--git-dir', str(self.remote), 'rev-parse', ref)

    def candidate(self):
        release.prepare()
        source = self.run_git('rev-parse', 'HEAD')
        os.environ['RELEASE_SOURCE_SHA'] = source
        os.environ['RELEASE_BUILT_SHA'] = source
        self.run_git('checkout', '--detach', self.base)
        return source

    @contextmanager
    def environment(self, **values):
        with patch.dict(os.environ, values):
            yield

    def test_manual_default_branch_and_clean_launch_required(self):
        for values in (
                {'GITHUB_EVENT_NAME': 'push'},
                {'GITHUB_REF': 'refs/tags/3.29.13-scylla'},
                {'GITHUB_REPOSITORY': 'someone/python-driver'},
                {'RELEASE_DEFAULT_BRANCH': ''}):
            with self.subTest(values=values), self.environment(**values):
                with self.assertRaises(ValueError):
                    release.check()
        Path('untracked').touch()
        with self.assertRaisesRegex(ValueError, 'clean checkout'):
            release.check()

    def test_rejected_versions_do_not_prepare_a_candidate(self):
        local_refs = self.run_git('show-ref')
        remote_refs = self.run_git('ls-remote', '--refs', 'origin')
        for version in ('3.29.13-scylla', 'v3.29.13', '03.29.13', '3.29.12',
                        '3.28.99', '3.29.13\n', '3.290.0', '3.29.14',
                        '3.31.0', '3.30.1', '4.1.0', '4.0.1', '5.0.0'):
            with (self.subTest(version=version),
                  self.environment(RELEASE_VERSION=version)):
                with self.assertRaises(ValueError):
                    release.prepare()
                self.assertEqual(self.run_git('rev-parse', 'HEAD'), self.base)
                self.assertEqual(
                    self.run_git('symbolic-ref', 'HEAD'), 'refs/heads/master')
                self.assertEqual(self.run_git('show-ref'), local_refs)
                self.assertEqual(
                    self.run_git('ls-remote', '--refs', 'origin'), remote_refs)
                self.assertEqual(self.run_git('status', '--porcelain'), '')
                self.assertFalse(Path(os.environ['GITHUB_OUTPUT']).exists())

    def test_preparation_pushes_only_bot_version_commit_and_no_final_tag(self):
        source = self.candidate()
        self.assertEqual(
            self.ref('refs/heads/release-candidate/3.29.13-scylla'), source)
        self.assertEqual(self.ref('refs/heads/master'), self.base)
        self.assertIsNone(release.remote_ref('refs/tags/3.29.13-scylla'))
        self.assertEqual(
            self.run_git('show', '-s', '--format=%an <%ae>', source),
            'scylladb-releaser[bot] '
            '<12345+scylladb-releaser[bot]@users.noreply.github.com>')
        self.assertEqual(
            set(self.run_git('diff', '--name-only', self.base,
                             source).splitlines()),
            {'cassandra/__init__.py', 'docs/conf.py'})
        self.assertIn(
            '__version_info__ = (3, 29, 13)',
            self.run_git('show', source + ':cassandra/__init__.py'))
        self.assertIn(
            "LATEST_VERSION = '3.29.13-scylla'",
            self.run_git('show', source + ':docs/conf.py'))
        docs = self.run_git('show', source + ':docs/conf.py')
        self.assertEqual(
            ast.literal_eval(release.assignment(docs, 'TAGS').value),
            ['3.28.1-scylla', '3.29.1-scylla', '3.29.13-scylla'])
        self.assertEqual(
            ast.literal_eval(
                release.assignment(docs, 'DEPRECATED_VERSIONS').value),
            ['3.28.1-scylla'])
        self.assertIn(
            'source_sha=' + source,
            Path(os.environ['GITHUB_OUTPUT']).read_text())
        with self.assertRaisesRegex(ValueError, 'already exists'):
            release.check()

    def test_minor_release_preserves_tags_and_deprecates_previous_release(
            self):
        with self.environment(RELEASE_VERSION='3.30.0'):
            source = self.candidate()
            docs = self.run_git('show', source + ':docs/conf.py')
            self.assertEqual(
                ast.literal_eval(release.assignment(docs, 'TAGS').value),
                ['3.28.1-scylla', '3.29.1-scylla', '3.29.12-scylla',
                 '3.30.0-scylla'])
            self.assertEqual(
                ast.literal_eval(
                    release.assignment(docs, 'DEPRECATED_VERSIONS').value),
                ['3.28.1-scylla', '3.29.12-scylla'])
            self.assertEqual(
                ast.literal_eval(
                    release.assignment(docs, 'LATEST_VERSION').value),
                '3.30.0-scylla')
            release.finalize()
            self.assertEqual(self.ref('refs/heads/master'), source)
            self.assertEqual(
                self.ref('refs/tags/3.30.0-scylla^{commit}'), source)

    def test_minor_release_does_not_duplicate_existing_deprecation(self):
        docs = Path('docs/conf.py')
        docs.write_text(docs.read_text().replace(
            "DEPRECATED_VERSIONS = ['3.28.1-scylla']",
            "DEPRECATED_VERSIONS = ['3.28.1-scylla', '3.29.12-scylla']"))
        self.run_git(
            'commit', '-am',
            'Baseline with current release already deprecated')
        self.base = self.run_git('rev-parse', 'HEAD')
        os.environ['GITHUB_SHA'] = self.base
        self.run_git('push', 'origin', 'HEAD:refs/heads/master')
        with self.environment(RELEASE_VERSION='3.30.0'):
            source = self.candidate()
            prepared = self.run_git('show', source + ':docs/conf.py')
            self.assertEqual(
                ast.literal_eval(
                    release.assignment(prepared, 'DEPRECATED_VERSIONS').value),
                ['3.28.1-scylla', '3.29.12-scylla'])

    def test_next_major_release_can_prepare_and_promote(self):
        with self.environment(RELEASE_VERSION='4.0.0'):
            source = self.candidate()
            self.assertEqual(
                self.ref('refs/heads/release-candidate/4.0.0-scylla'), source)
            self.assertIn(
                '__version_info__ = (4, 0, 0)',
                self.run_git('show', source + ':cassandra/__init__.py'))
            release.finalize()
            self.assertEqual(self.ref('refs/heads/master'), source)
            self.assertEqual(
                self.ref('refs/tags/4.0.0-scylla^{commit}'), source)

    def test_promotion_atomically_pushes_tested_commit_and_annotated_bot_tag(
            self):
        source = self.candidate()
        release.finalize()
        self.assertEqual(self.ref('refs/heads/master'), source)
        self.assertEqual(self.ref('refs/tags/3.29.13-scylla^{commit}'), source)
        self.assertEqual(
            self.run_git('cat-file', '-t', 'refs/tags/3.29.13-scylla'), 'tag')
        self.assertIn(
            'tagger scylladb-releaser[bot]',
            self.run_git('cat-file', '-p', 'refs/tags/3.29.13-scylla'))
        original_tag = self.ref('refs/tags/3.29.13-scylla')
        release.finalize()
        self.assertEqual(self.ref('refs/heads/master'), source)
        self.assertEqual(self.ref('refs/tags/3.29.13-scylla'), original_tag)

    def test_existing_tag_requires_matching_tag_and_default_branch(self):
        source = self.candidate()
        for branch, tag_target in ((source, self.base), (self.base, source)):
            with self.subTest(branch=branch, tag_target=tag_target):
                self.run_git(
                    '--git-dir', str(self.remote), 'update-ref',
                    'refs/heads/master', branch)
                self.run_git(
                    'tag', '-a', '-f', '3.29.13-scylla', tag_target,
                    '-m', 'Existing release tag')
                self.run_git(
                    'push', '--force', 'origin', 'refs/tags/3.29.13-scylla')
                original_tag = self.ref('refs/tags/3.29.13-scylla')
                with self.assertRaisesRegex(ValueError, 'does not match'):
                    release.finalize()
                self.assertEqual(self.ref('refs/heads/master'), branch)
                self.assertEqual(
                    self.ref('refs/tags/3.29.13-scylla'), original_tag)
                self.assertEqual(
                    self.ref('refs/heads/release-candidate/3.29.13-scylla'),
                    source)

    def test_wrong_app_cannot_prepare(self):
        local_refs = self.run_git('show-ref')
        remote_refs = self.run_git('ls-remote', '--refs', 'origin')
        with self.environment(RELEASE_APP_SLUG='another-app'):
            with self.assertRaisesRegex(ValueError, 'scylladb-releaser'):
                release.prepare()
        self.assertEqual(self.run_git('rev-parse', 'HEAD'), self.base)
        self.assertEqual(
            self.run_git('symbolic-ref', 'HEAD'), 'refs/heads/master')
        self.assertEqual(self.run_git('show-ref'), local_refs)
        self.assertEqual(
            self.run_git('ls-remote', '--refs', 'origin'), remote_refs)
        self.assertEqual(self.ref('refs/heads/master'), self.base)
        self.assertIsNone(release.remote_ref(
            'refs/heads/release-candidate/3.29.13-scylla'))
        self.assertIsNone(release.remote_ref('refs/tags/3.29.13-scylla'))
        self.assertEqual(self.run_git('status', '--porcelain'), '')
        self.assertFalse(Path(os.environ['GITHUB_OUTPUT']).exists())

    def test_mismatched_artifact_or_candidate_cannot_promote(self):
        self.candidate()
        with self.environment(RELEASE_BUILT_SHA=self.base):
            with self.assertRaisesRegex(ValueError, 'Built artifacts'):
                release.finalize()
        self.run_git('--git-dir', str(self.remote), 'update-ref',
                     'refs/heads/release-candidate/3.29.13-scylla', self.base)
        with self.assertRaisesRegex(ValueError, 'candidate ref changed'):
            release.finalize()
        self.assertEqual(self.ref('refs/heads/master'), self.base)
        self.assertIsNone(release.remote_ref('refs/tags/3.29.13-scylla'))

    def test_changed_default_branch_cannot_be_overwritten(self):
        self.candidate()
        self.run_git('commit', '--allow-empty', '-m', 'Concurrent work')
        other = self.run_git('rev-parse', 'HEAD')
        self.run_git('push', 'origin', other + ':refs/heads/master')
        self.run_git('checkout', '--detach', self.base)
        with self.assertRaisesRegex(ValueError, 'Default branch changed'):
            release.finalize()
        self.assertEqual(self.ref('refs/heads/master'), other)
        self.assertIsNone(release.remote_ref('refs/tags/3.29.13-scylla'))

    def test_tampered_candidate_cannot_promote_even_with_matching_build_sha(
            self):
        source = self.candidate()
        self.run_git('checkout', '--detach', source)
        with Path('cassandra/__init__.py').open('a') as output:
            output.write('raise RuntimeError("tampered")\n')
        self.run_git(
            'commit', '-am', 'Release 3.29.13', '--amend', '--no-edit')
        tampered = self.run_git('rev-parse', 'HEAD')
        self.run_git(
            'push', '--force', 'origin',
            tampered + ':refs/heads/release-candidate/3.29.13-scylla')
        self.run_git('checkout', '--detach', self.base)
        with self.environment(
                RELEASE_SOURCE_SHA=tampered, RELEASE_BUILT_SHA=tampered):
            with self.assertRaisesRegex(ValueError, 'expected version-only'):
                release.finalize()
        self.assertEqual(self.ref('refs/heads/master'), self.base)

    def test_candidate_with_extra_changed_file_cannot_promote(self):
        source = self.candidate()
        self.run_git('checkout', '--detach', source)
        Path('unexpected').write_text('Unrelated change\n')
        self.run_git('add', 'unexpected')
        self.run_git('commit', '--amend', '--no-edit')
        changed = self.run_git('rev-parse', 'HEAD')
        self.run_git(
            'push', '--force', 'origin',
            changed + ':refs/heads/release-candidate/3.29.13-scylla')
        self.run_git('checkout', '--detach', self.base)
        with self.environment(
                RELEASE_SOURCE_SHA=changed, RELEASE_BUILT_SHA=changed):
            with self.assertRaisesRegex(ValueError, 'unexpected files'):
                release.finalize()
        self.assertEqual(self.ref('refs/heads/master'), self.base)
        self.assertEqual(
            self.ref('refs/heads/release-candidate/3.29.13-scylla'), changed)
        self.assertIsNone(release.remote_ref('refs/tags/3.29.13-scylla'))

    def test_candidate_with_multiple_commits_cannot_promote(self):
        source = self.candidate()
        self.run_git('checkout', '--detach', source)
        self.run_git('commit', '--allow-empty', '-m', 'Extra candidate commit')
        changed = self.run_git('rev-parse', 'HEAD')
        self.run_git(
            'push', 'origin',
            changed + ':refs/heads/release-candidate/3.29.13-scylla')
        self.run_git('checkout', '--detach', self.base)
        with self.environment(
                RELEASE_SOURCE_SHA=changed, RELEASE_BUILT_SHA=changed):
            with self.assertRaisesRegex(ValueError, 'one commit'):
                release.finalize()
        self.assertEqual(self.ref('refs/heads/master'), self.base)
        self.assertEqual(
            self.ref('refs/heads/release-candidate/3.29.13-scylla'), changed)
        self.assertIsNone(release.remote_ref('refs/tags/3.29.13-scylla'))

    def test_remote_tag_race_rejects_entire_atomic_push(self):
        self.candidate()
        original = release.git

        def racing_git(*args):
            if args[:2] == ('push', '--atomic'):
                self.run_git(
                    '--git-dir', str(self.remote), 'update-ref',
                    'refs/tags/3.29.13-scylla', self.base)
            return original(*args)

        with patch.object(release, 'git', side_effect=racing_git):
            with self.assertRaises(subprocess.CalledProcessError):
                release.finalize()
        self.assertEqual(self.ref('refs/heads/master'), self.base)
        self.assertEqual(
            self.ref('refs/tags/3.29.13-scylla^{commit}'), self.base)


if __name__ == '__main__':
    unittest.main()
