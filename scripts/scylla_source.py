"""Pin Scylla master and partition its Python/CQL suites for pre-release CI."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess


SCYLLA_REPOSITORY = 'https://github.com/scylladb/scylladb.git'
# These contain harness code, fixtures, manual/performance tests, or C++ tests.
EXCLUDED_DIRECTORIES = {
    'boost', 'unit', 'raft', 'vector_search', 'lib', 'pylib', 'resource',
    'manual', 'perf', 'pylib_test', '__pycache__',
}
SUITE_SHARDS = {'cluster': 8, 'cqlpy': 4, 'alternator': 2, 'rest_api': 2}


def master_source(refs):
    """Pin the advertised Scylla master commit before checking out tests."""
    commits = []
    for line in refs.splitlines():
        sha, ref = line.split()
        if ref == 'refs/heads/master':
            if not re.fullmatch(r'[0-9a-f]{40}', sha):
                raise ValueError('Invalid Scylla master commit')
            commits.append(sha)
    if len(commits) != 1:
        raise ValueError('Expected exactly one Scylla master ref')
    return {'ref': 'master', 'revision': commits[0]}


def test_plan(source):
    """Partition every Python/CQL test file, preserving module fixtures."""
    jobs = []
    for directory in sorted((source / 'test').iterdir()):
        if not directory.is_dir() or directory.name in EXCLUDED_DIRECTORIES:
            continue
        files = []
        for path in sorted(directory.rglob('*')):
            if any(part in EXCLUDED_DIRECTORIES
                   for part in path.relative_to(directory).parts[:-1]):
                continue
            name = path.name
            if path.is_file() and (
                (path.suffix == '.py' and (
                    name.startswith('test_')
                    or name.endswith(('_test.py', '_tests.py'))
                )) or name.endswith('_test.cql')
            ):
                files.append(path.relative_to(source).as_posix())
        if not files:
            continue
        shards = min(SUITE_SHARDS.get(directory.name, 1), len(files))
        for shard in range(shards):
            jobs.append({
                'name': f'{directory.name}-{shard + 1}',
                'suite': directory.name,
                'files': files[shard::shards],
            })
    if not jobs:
        raise ValueError('No Python/CQL tests found in the source checkout')
    return jobs


def write_outputs(values):
    if output := os.environ.get('GITHUB_OUTPUT'):
        with open(output, 'a') as stream:
            for name, value in values.items():
                stream.write(f'{name}={value}\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    resolve = commands.add_parser('resolve')
    resolve.add_argument('--output', type=Path, required=True)
    plan = commands.add_parser('plan')
    plan.add_argument('source', type=Path)
    plan.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'resolve':
        refs = subprocess.check_output(
            ['git', 'ls-remote', SCYLLA_REPOSITORY, 'refs/heads/master'],
            text=True,
        )
        source = master_source(refs)
        args.output.write_text(json.dumps(source, indent=2) + '\n')
        write_outputs(source)
        print(json.dumps(source))
        if summary := os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(summary, 'a') as stream:
                stream.write('Scylla source: **master**\n\n')
                stream.write(f'Source commit: `{source["revision"]}`\n\n')
    elif args.command == 'plan':
        jobs = test_plan(args.source.resolve())
        args.output.write_text(json.dumps(jobs, indent=2) + '\n')
        image = (args.source / 'tools/toolchain/image').read_text().strip()
        if not re.fullmatch(
            r'docker.io/scylladb/scylla-toolchain:[A-Za-z0-9_.-]+', image,
        ):
            raise ValueError(f'Unexpected source toolchain image: {image}')
        matrix = {'include': [
            {'name': job['name'], 'suite': job['suite']} for job in jobs
        ]}
        write_outputs({'matrix': json.dumps(matrix), 'toolchain': image})
        print(json.dumps(matrix, indent=2))
        if summary := os.environ.get('GITHUB_STEP_SUMMARY'):
            with open(summary, 'a') as stream:
                stream.write(f'Test plan: **{len(jobs)} jobs**, ')
                stream.write(
                    f'**{sum(len(job["files"]) for job in jobs)} files**\n\n'
                )
                for job in jobs:
                    stream.write(
                        f'- {job["name"]}: {len(job["files"])} files\n'
                    )


if __name__ == '__main__':
    main()
