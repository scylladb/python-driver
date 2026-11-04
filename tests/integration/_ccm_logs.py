# Copyright ScyllaDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
CCM diagnostic copying, also runnable before driver dependencies are
built.
"""

import argparse
from itertools import chain
import logging
import os
from pathlib import Path
import shutil
from stat import S_ISDIR, S_ISREG


log = logging.getLogger(__name__)
_PRESERVED_FILE_EXTENSIONS = ('.conf', '.log', '.yaml', '.yml')


def ccm_boundary(root):
    # Resolve ancestors while keeping a symlink at the designated root
    # visible.
    return root.parent.resolve() / root.name


def directory_exists(directory, boundary):
    try:
        return (directory.resolve().is_relative_to(boundary) and
                S_ISDIR(directory.stat().st_mode))
    except FileNotFoundError:
        return False
    except (OSError, RuntimeError) as exc:
        log.warning("Failed to inspect ccm directory %s: %s", directory, exc)
        return False


def copy_cluster_logs(
        ccm_root, ccm_destination, boundary, excluded_directories=()):
    if not directory_exists(ccm_root, boundary):
        return 0
    count = 0
    # Save CCM logs/configs before cleanup removes their directories.
    sources = ccm_root.rglob('*')
    if excluded_directories:
        # Skip download caches before descending into their package trees.
        sources = chain.from_iterable(
            chain((entry,), () if entry.is_symlink() else entry.rglob('*'))
            for entry in ccm_root.iterdir()
            if entry.name not in excluded_directories)
    for source_file in chain((ccm_root,), sources):
        try:
            if not source_file.resolve().is_relative_to(boundary):
                continue
            mode = source_file.stat().st_mode
            if S_ISDIR(mode):
                # Probe access because rglob suppresses scan errors.
                with os.scandir(source_file) as entries:
                    for _ in entries:
                        pass
                continue
            if not S_ISREG(mode):
                continue
            rel_file = source_file.relative_to(ccm_root)
            preserve_all = bool({'conf', 'logs'}.intersection(rel_file.parts))
            if (not preserve_all and
                    not source_file.name.endswith(_PRESERVED_FILE_EXTENSIONS)):
                continue
            destination_file = ccm_destination / rel_file
            destination_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_file, destination_file)
            count += 1
        except (OSError, RuntimeError) as exc:
            log.warning("Failed to preserve file %s: %s", source_file, exc)

    if count:
        log.info(
            "Copied %s ccm diagnostic files to %s", count, ccm_destination)
    else:
        log.warning("No ccm diagnostic files copied from %s", ccm_root)
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('destination', type=Path)
    parser.add_argument('roots', type=Path, nargs='+')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    for index, root in enumerate(args.roots):
        try:
            boundary = ccm_boundary(root)
            copy_cluster_logs(
                root, args.destination / ('root-%d' % index), boundary,
                excluded_directories=('scylla-repository',))
        except (OSError, RuntimeError) as exc:
            log.warning(
                "Failed to preserve ccm diagnostics from %s: %s", root, exc)


if __name__ == '__main__':
    main()
