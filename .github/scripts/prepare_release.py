"""Prepare a release candidate, then promote its tested commit and tag
atomically.
"""

import argparse
import ast
import os
from pathlib import Path
import re
import subprocess


VERSION_FILES = ('cassandra/__init__.py', 'docs/conf.py')
BOT = 'scylladb-releaser[bot]'


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def git(*args):
    return command('git', *args)


def git_file(revision, path):
    return subprocess.check_output(
        ['git', 'show', f'{revision}:{path}'], text=True)


def version_tuple(version):
    if not re.fullmatch(
            r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version):
        raise ValueError(
            'Expected a canonical X.Y.Z release version without -scylla')
    return tuple(map(int, version.split('.')))


def assignment(source, name):
    matches = [node for node in ast.parse(source).body
               if isinstance(node, ast.Assign)
               and any(isinstance(target, ast.Name) and target.id == name
                       for target in node.targets)]
    if len(matches) != 1:
        raise ValueError(f'Expected exactly one {name} assignment')
    return matches[0]


def replace_assignment(source, name, value):
    node = assignment(source, name)
    lines = source.splitlines(keepends=True)
    lines[node.lineno - 1:node.end_lineno] = [f'{name} = {value}\n']
    return ''.join(lines)


def release_files(base, version):
    requested = version_tuple(version)
    sources = {path: git_file(base, path) for path in VERSION_FILES}
    module = sources[VERSION_FILES[0]]
    current = ast.literal_eval(assignment(module, '__version_info__').value)
    if (not isinstance(current, tuple) or len(current) != 3
            or not all(type(part) is int for part in current)
            or requested <= current):
        raise ValueError(
            'Release version must be newer than the current driver version')
    major, minor, patch = current
    if requested not in ((major, minor, patch + 1), (major, minor + 1, 0),
                         (major + 1, 0, 0)):
        raise ValueError('Release version must be the next patch, minor, '
                         'or major version')
    sources[VERSION_FILES[0]] = replace_assignment(
        module, '__version_info__', repr(requested))
    docs = sources[VERSION_FILES[1]]
    tags = ast.literal_eval(assignment(docs, 'TAGS').value)
    tag = version + '-scylla'
    if (not isinstance(tags, list)
            or not all(isinstance(item, str) for item in tags) or tag in tags):
        raise ValueError(
            'Documentation release tags are invalid or already contain '
            'this release')
    current_tag = '.'.join(map(str, current)) + '-scylla'
    if requested[:2] == current[:2]:
        tags[tags.index(current_tag)] = tag
    else:
        tags.append(tag)
        deprecated = ast.literal_eval(
            assignment(docs, 'DEPRECATED_VERSIONS').value)
        if (not isinstance(deprecated, list)
                or not all(isinstance(item, str) for item in deprecated)):
            raise ValueError('Deprecated documentation versions are invalid')
        if current_tag not in deprecated:
            deprecated.append(current_tag)
        docs = replace_assignment(
            docs, 'DEPRECATED_VERSIONS', repr(deprecated))
    docs = replace_assignment(
        docs, 'TAGS',
        '[\n' + ''.join(f'    {item!r},\n' for item in tags) + ']')
    sources[VERSION_FILES[1]] = replace_assignment(
        docs, 'LATEST_VERSION', repr(tag))
    return sources


def remote_ref(ref):
    output = git('ls-remote', '--refs', 'origin', ref)
    if not output:
        return None
    fields = output.split()
    if (len(fields) != 2 or fields[1] != ref
            or not re.fullmatch(r'[0-9a-f]{40}', fields[0])):
        raise ValueError('Unexpected remote ref response')
    return fields[0]


def context():
    branch = os.environ['RELEASE_DEFAULT_BRANCH']
    base = os.environ['GITHUB_SHA']
    version = os.environ['RELEASE_VERSION']
    version_tuple(version)
    if (os.environ['GITHUB_REPOSITORY'] != 'scylladb/python-driver'
            or os.environ['GITHUB_EVENT_NAME'] != 'workflow_dispatch'
            or not branch or os.environ['GITHUB_REF'] != 'refs/heads/' + branch
            or not re.fullmatch(r'[0-9a-f]{40}', base)):
        raise ValueError(
            'Release must be dispatched from the repository default branch')
    if git('rev-parse', 'HEAD') != base or git('status', '--porcelain'):
        raise ValueError(
            'Release tooling requires a clean checkout of the dispatch commit')
    return (branch, base, version, version + '-scylla',
            'release-candidate/' + version + '-scylla')


def check():
    branch, base, version, tag, candidate = context()
    release_files(base, version)
    if remote_ref('refs/heads/' + branch) != base:
        raise ValueError(
            'Default branch changed; dispatch a new release from its '
            'current head')
    if remote_ref('refs/tags/' + tag) or remote_ref('refs/heads/' + candidate):
        raise ValueError(
            'Release tag or candidate already exists; inspect the original '
            'run before recovery')
    return branch, base, version, tag, candidate


def configure_bot():
    if os.environ['RELEASE_APP_SLUG'] != 'scylladb-releaser':
        raise ValueError('Release App must be scylladb-releaser')
    identifier = command('gh', 'api', 'users/' + BOT, '--jq', '.id')
    if not re.fullmatch(r'[1-9][0-9]*', identifier):
        raise ValueError('Could not resolve release bot identity')
    git('config', 'user.name', BOT)
    git('config', 'user.email', f'{identifier}+{BOT}@users.noreply.github.com')


def prepare():
    _, base, version, tag, candidate = check()
    configure_bot()
    for path, source in release_files(base, version).items():
        Path(path).write_text(source)
    git('add', '--', *VERSION_FILES)
    git('commit', '-m', f'Release {version}')
    source = git('rev-parse', 'HEAD')
    # Empty expected value reserves a new candidate ref without overwriting
    # one.
    git('push', f'--force-with-lease=refs/heads/{candidate}:', 'origin',
        f'{source}:refs/heads/{candidate}')
    with open(os.environ['GITHUB_OUTPUT'], 'a') as output:
        output.write(f'source_sha={source}\ntag={tag}\n')
    print(f'Prepared {tag} at {source}; final tag awaits all release tests')


def finalize():
    branch, base, version, tag, candidate = context()
    source = os.environ['RELEASE_SOURCE_SHA']
    if (not re.fullmatch(r'[0-9a-f]{40}', source)
            or os.environ['RELEASE_BUILT_SHA'] != source):
        raise ValueError(
            'Built artifacts must use the tested release candidate commit')
    if remote_ref('refs/heads/' + candidate) != source:
        raise ValueError('Release candidate ref changed after testing')
    if (git('rev-list', '--parents', '-n', '1', source).split()
            != [source, base]):
        raise ValueError(
            'Release candidate must be one commit on the dispatch baseline')
    if (set(git('diff', '--name-only', base, source).splitlines())
            != set(VERSION_FILES)):
        raise ValueError('Release candidate changed unexpected files')
    for path, expected in release_files(base, version).items():
        if git_file(source, path) != expected:
            raise ValueError(
                'Release candidate differs from the expected version-only '
                'changes')
    current = remote_ref('refs/heads/' + branch)
    existing = remote_ref('refs/tags/' + tag)
    if existing:
        git('fetch', '--no-tags', 'origin', 'refs/tags/' + tag)
        if (current == source
                and git('rev-parse', 'FETCH_HEAD^{commit}') == source):
            print('Tested release commit and tag already pushed; '
                  'preserving them')
            return
        raise ValueError(
            'Existing release tag or branch does not match this release')
    if current != base:
        raise ValueError(
            'Default branch changed during testing; dispatch a fresh release')
    configure_bot()
    git('tag', '-a', tag, source, '-m', f'Release {version}')
    git('push', '--atomic', f'--force-with-lease=refs/heads/{branch}:{base}',
        'origin', f'{source}:refs/heads/{branch}', 'refs/tags/' + tag)
    print(f'Published tested version commit and annotated tag {tag}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('check', 'prepare', 'finalize'))
    args = parser.parse_args()
    {'check': check, 'prepare': prepare, 'finalize': finalize}[args.command]()
