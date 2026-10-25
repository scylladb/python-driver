"""Trusted GitHub release automation; no pull request code is executed."""

import argparse
import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request


class GitHub:
    def __init__(self):
        self.base = os.environ['GITHUB_API_URL'].rstrip('/')
        self.repo = os.environ['GITHUB_REPOSITORY']

    def request(self, method, path, data=None):
        request = urllib.request.Request(
            f'{self.base}/repos/{self.repo}/{path}',
            data=None if data is None else json.dumps(data).encode(),
            headers={'Authorization': f"Bearer {os.environ['GH_TOKEN']}",
                     'Accept': 'application/vnd.github+json',
                     'Content-Type': 'application/json',
                     'X-GitHub-Api-Version': '2022-11-28'},
            method=method,
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
            return json.loads(body) if body else None


def create_release(api, tag, source_sha):
    if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+-scylla', tag):
        raise ValueError('Release tag must have form X.Y.Z-scylla')
    encoded = urllib.parse.quote(tag, safe='')
    reference = api.request('GET', f'git/ref/tags/{encoded}')
    target = reference['object']
    while target['type'] == 'tag':
        target = api.request('GET', f"git/tags/{target['sha']}")['object']
    if target['type'] != 'commit' or target['sha'] != source_sha:
        raise ValueError(
            'Release tag does not match the built and published commit')
    try:
        release = api.request('GET', f'releases/tags/{encoded}')
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        # The tag endpoint does not find drafts with a pending tag. The list
        # endpoint includes drafts for authenticated users with push access.
        page = 1
        while True:
            releases = api.request('GET', f'releases?per_page=100&page={page}')
            release = next(
                (item for item in releases if item['tag_name'] == tag), None,
            )
            if release is not None or len(releases) < 100:
                break
            page += 1
    if release is not None:
        if release['draft'] or not release['published_at']:
            raise ValueError(
                'Existing release is unpublished; publish it before rerunning')
        print(f"Release already exists: {release['html_url']}")
        return
    if api.request('GET', f'git/ref/tags/{encoded}') != reference:
        raise ValueError('Release tag changed during release creation')
    release = api.request('POST', 'releases', {
        'tag_name': tag, 'name': f'Release {tag.removesuffix("-scylla")}',
        'generate_release_notes': True,
    })
    print(release['html_url'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest='command', required=True)
    release_parser = subparsers.add_parser('release')
    release_parser.add_argument('tag')
    release_parser.add_argument('source_sha')
    args = parser.parse_args()
    api = GitHub()
    create_release(api, args.tag, args.source_sha)
