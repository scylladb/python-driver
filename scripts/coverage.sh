#!/usr/bin/env bash
# Runs the unit suite (all event-loop reactors) and, if SCYLLA_VERSION or
# CASSANDRA_VERSION is set, the integration suite, all under coverage.py, then
# combines and reports.
#
# CASS_DRIVER_NO_CYTHON=1 forces cluster.py/connection.py/protocol.py/etc. to
# build as plain Python instead of Cython extensions, since coverage.py can't
# trace into compiled extensions. Cython-only modules with no .py fallback
# (obj_parser, numpy_parser, row_parser, ...) are not built at all in this mode
# and are therefore not measured by this script.
#
# Deliberately not `set -e`: a failing test must not skip report generation
# below, or a broken test leaves no coverage output at all to diagnose it
# with. Each test invocation instead records its own failure into $status,
# and the script exits with that status only after combine/report/html/xml
# have run.
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

rm -f .coverage .coverage.* || exit 1
export CASS_DRIVER_NO_CYTHON=1

# A previous plain `uv sync`/`uv run` may have left Cython-compiled .so/.pyd
# files in place from a normal (Cython-enabled) build. Python's import system
# prefers those over the .py source, so they must be removed -- otherwise
# CASS_DRIVER_NO_CYTHON=1 silently has no effect and coverage reports 0% for
# every affected module. `--reinstall-package` then rebuilds from scratch,
# producing only the extensions CASS_DRIVER_NO_CYTHON=1 actually allows
# (murmur3/libev, but none of the Cython ones). If any of this setup fails,
# there's no point running any tests, so bail out immediately -- failure
# tolerance below is scoped to test/report commands only.
find cassandra -name "*.so" -delete -o -name "*.pyd" -delete || exit 1
uv sync --reinstall-package scylla-driver || exit 1

status=0

# Unlike the gevent/eventlet/asyncio reactor tests below, tests/unit/io/
# test_asyncorereactor.py is deliberately NOT in the --ignore list: it needs
# no separate EVENT_LOOP_MANAGER run, since it self-skips via
# ASYNCCORE_AVAILABLE on Python 3.12+ (where the stdlib `asyncore` module was
# removed) and otherwise runs normally here, gaining coverage on 3.9-3.11.
uv run coverage run -m pytest tests/unit -v \
    --ignore=tests/unit/column_encryption \
    --ignore=tests/unit/io/test_geventreactor.py \
    --ignore=tests/unit/io/test_eventletreactor.py \
    --ignore=tests/unit/io/test_asyncioreactor.py \
    || status=1

EVENT_LOOP_MANAGER=gevent uv run coverage run -m pytest tests/unit/io/test_geventreactor.py -v || status=1
EVENT_LOOP_MANAGER=asyncio uv run coverage run -m pytest tests/unit/io/test_asyncioreactor.py -v || status=1
EVENT_LOOP_MANAGER=eventlet uv run coverage run -m pytest tests/unit/io/test_eventletreactor.py -v || status=1

if [[ -n "${SCYLLA_VERSION:-}" || -n "${CASSANDRA_VERSION:-}" ]]; then
    uv run coverage run -m pytest tests/integration/standard tests/integration/cqlengine/ -v || status=1
else
    echo "SCYLLA_VERSION/CASSANDRA_VERSION not set -- skipping integration coverage."
fi

uv run coverage combine || status=1
uv run coverage report -m || status=1
uv run coverage html || status=1
uv run coverage xml || status=1

exit "$status"
