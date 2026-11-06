#!/usr/bin/env bash
# Run inside the shared image built from the pinned Scylla master commit.
set -euo pipefail

: "${SCYLLA_SOURCE:?Set SCYLLA_SOURCE to the built Scylla checkout in the image}"
: "${DRIVER_WHEEL_DIR:?Set DRIVER_WHEEL_DIR to the candidate wheel directory}"
: "${TESTPY_LOG_DIR:?Set TESTPY_LOG_DIR to the output directory}"
: "${SCYLLA_SOURCE_FILE:?Set SCYLLA_SOURCE_FILE to the pinned source JSON}"
: "${SCYLLA_PLAN_FILE:?Set SCYLLA_PLAN_FILE to the suite/shard plan JSON}"
: "${SCYLLA_TEST_JOB:?Set SCYLLA_TEST_JOB to a name from the plan}"

SCYLLA_SOURCE=$(realpath "$SCYLLA_SOURCE")
DRIVER_WHEEL_DIR=$(realpath "$DRIVER_WHEEL_DIR")
mkdir -p "$TESTPY_LOG_DIR/pytest-tmp"
TESTPY_LOG_DIR=$(realpath "$TESTPY_LOG_DIR")
# Some upstream fixtures put AF_UNIX sockets below pytest's temporary root.
# Keep that root short even when the mounted diagnostics path is long.
pytest_tmp=$(mktemp -d /tmp/tpy.XXXXXX)
save_pytest_tmp() {
    status=$?
    trap - EXIT
    cp -a "$pytest_tmp/." "$TESTPY_LOG_DIR/pytest-tmp/" || status=1
    rm -rf -- "$pytest_tmp"
    exit "$status"
}
trap save_pytest_tmp EXIT
export PYTEST_DEBUG_TEMPROOT="$pytest_tmp"
unset PYTHONPATH PYTHONHOME
cd "$SCYLLA_SOURCE"
test_python="$SCYLLA_SOURCE/build/driver-candidate-venv/bin/python"
# Child tools such as cqlsh must use the same interpreter and candidate driver.
export PATH="${test_python%/*}:$PATH"
export CQLSH_NO_BUNDLED=1
# Dependencies are synced from upstream's lockfile when the image is built.
# Bypass test.py's auto-sync after replacing its pinned upstream driver.
export DISABLE_VENV=1

"$test_python" - "$SCYLLA_SOURCE_FILE" <<'PY' | tee "$TESTPY_LOG_DIR/scylla.txt"
import hashlib
import json
from pathlib import Path
import subprocess
import sys

source = json.loads(Path(sys.argv[1]).read_text())
metadata = json.loads(Path('.driver-image.json').read_text())
assert metadata['revision'] == source['revision'], (metadata, source)
binary = Path(metadata['binary'])
assert binary.samefile('build/RelWithDebInfo/scylla'), binary
with binary.open('rb') as stream:
    assert hashlib.file_digest(stream, 'sha256').hexdigest() == metadata['binary_sha256']
assert subprocess.check_output([str(binary), '--version'], text=True).strip() == metadata['binary_version']
print(json.dumps(metadata, indent=2))
PY

candidate_wheel=$("$test_python" - "$DRIVER_WHEEL_DIR" <<'PY'
from pathlib import Path
import sys
from packaging.tags import sys_tags
from packaging.utils import canonicalize_name, parse_wheel_filename

supported = set(sys_tags())
candidates = []
for wheel in Path(sys.argv[1]).glob('*.whl'):
    name, version, build, tags = parse_wheel_filename(wheel.name)
    if canonicalize_name(name) == 'scylla-driver' and supported.intersection(tags):
        candidates.append(wheel)
if len(candidates) != 1:
    raise SystemExit(f'Expected one compatible candidate wheel, found {candidates}')
print(candidates[0])
PY
)
uv pip install --python "$test_python" --no-deps --force-reinstall "$candidate_wheel"

"$test_python" - "$candidate_wheel" <<'PY' | tee "$TESTPY_LOG_DIR/driver.txt"
from importlib.metadata import distribution
from pathlib import Path
import hashlib
import json
import sys
import cassandra
import cassandra.cluster
import cassandra.protocol
from packaging.utils import parse_wheel_filename

wheel = Path(sys.argv[1])
dist = distribution('scylla-driver')
assert dist.version == str(parse_wheel_filename(wheel.name)[1]), dist.version
direct_url = json.loads(dist.read_text('direct_url.json'))
assert direct_url['url'] == wheel.as_uri(), direct_url
for module in (cassandra, cassandra.cluster, cassandra.protocol):
    path = Path(module.__file__).resolve()
    assert path.is_relative_to(Path(sys.prefix).resolve()), path
    print(f'{module.__name__}: {path}')
print(f'Driver version: {dist.version}')
print(f'Candidate wheel: {wheel}')
with wheel.open('rb') as stream:
    print(f'SHA256: {hashlib.file_digest(stream, "sha256").hexdigest()}')
PY

test_files=$("$test_python" - "$SCYLLA_PLAN_FILE" "$SCYLLA_TEST_JOB" <<'PY'
import json
import sys

with open(sys.argv[1]) as stream:
    jobs = json.load(stream)
print('\n'.join(next(job['files'] for job in jobs if job['name'] == sys.argv[2])))
PY
)
if [[ -z "$test_files" ]]; then
    echo 'No test files assigned to this job' >&2
    exit 1
fi
mapfile -t test_paths <<< "$test_files"
printf '%s\n' "${test_paths[@]}" > "$TESTPY_LOG_DIR/test-files.txt"
# Explicit release mode preserves upstream run_in/skip_in and skip_mode rules.
# The public source image lacks the optional KMIP client library.
"$test_python" test.py --mode release --no-gather-metrics --jobs 1 \
    --timeout 1800 --session-timeout 9000 --markers 'not non_gating' \
    --skip 'KeyProvider.kmip' \
    --tmpdir "$TESTPY_LOG_DIR/tests" --save-log-on-success \
    "${test_paths[@]}" 2>&1 | tee "$TESTPY_LOG_DIR/console.log"
