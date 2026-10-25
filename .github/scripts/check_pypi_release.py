"""Reject intended release filenames already visible in PyPI's inventory."""

import argparse
import json
from pathlib import Path
import re
from urllib.error import HTTPError
from urllib.request import Request, urlopen


MAX_INVENTORY_BYTES = 4 * 1024 * 1024


def check_release(version, dist_dir):
    """Check public inventory before upload; concurrent uploads can race."""
    component = r'(0|[1-9][0-9]*)'
    if not re.fullmatch(r'\.'.join([component] * 3), version):
        raise ValueError('Expected a canonical bare X.Y.Z release version')
    artifacts = list(Path(dist_dir).iterdir())
    if not artifacts or any(not path.is_file() or path.is_symlink()
                            for path in artifacts):
        raise ValueError('Expected a nonempty directory of distribution files')
    intended = {path.name for path in artifacts}
    request = Request(
        f'https://pypi.org/pypi/scylla-driver/{version}/json',
        headers={'Accept': 'application/json'})
    try:
        with urlopen(request, timeout=30) as response:
            if response.status != 200:
                raise ValueError('Unexpected PyPI inventory HTTP status')
            data = response.read(MAX_INVENTORY_BYTES + 1)
    except HTTPError as error:
        if error.code == 404:
            return
        raise
    if len(data) > MAX_INVENTORY_BYTES:
        raise ValueError('PyPI release inventory is too large')
    inventory = json.loads(data)
    if not isinstance(inventory, dict) or not isinstance(
            inventory.get('urls'), list):
        raise ValueError('Malformed PyPI inventory: expected urls list')
    published = set()
    for entry in inventory['urls']:
        filename = entry.get('filename') if isinstance(entry, dict) else None
        if (not isinstance(filename, str) or not filename
                or filename in ('.', '..') or '/' in filename
                or '\\' in filename):
            raise ValueError('Malformed PyPI release inventory filename')
        published.add(filename)
    overlap = intended & published
    if overlap:
        raise ValueError(
            'Release files already exist on PyPI; manual recovery required: '
            + ', '.join(sorted(overlap)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', required=True)
    parser.add_argument('--dist-dir', required=True, type=Path)
    args = parser.parse_args()
    check_release(args.version, args.dist_dir)


if __name__ == '__main__':
    main()
