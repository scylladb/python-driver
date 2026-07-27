#!/usr/bin/env bash
set -euo pipefail

stable_release_filter='^[0-9]{4}$.^[0-9]+$.^[0-9]+$ and LAST.LAST.LAST'
download_cluster_name="${SCYLLA_CCM_DOWNLOAD_CLUSTER:-scylla-driver-testpy-download}"

driver_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="${SCYLLA_SOURCE_DIR:-$driver_root/scylladb}"
tests_file="${SCYLLA_TESTPY_TESTS_FILE:-$driver_root/scripts/scylla-driver-testpy-tests.txt}"
tmpdir="${SCYLLA_TESTPY_TMPDIR:-${TMPDIR:-/tmp}/scylla-driver-testpy}"
jobs="${SCYLLA_TESTPY_JOBS:-1}"
max_failures="${SCYLLA_TESTPY_MAX_FAILURES:-1}"
timeout="${SCYLLA_TESTPY_TIMEOUT:-1800}"
session_timeout="${SCYLLA_TESTPY_SESSION_TIMEOUT:-7200}"

trim() {
    local value="$1"
    value="${value#"${value%%[![:space:]]*}"}"
    value="${value%"${value##*[![:space:]]}"}"
    printf '%s' "$value"
}

resolve_latest_stable_release() {
    local docker_version github_version

    if ! command -v get-version >/dev/null 2>&1; then
        echo "get-version is required when SCYLLA_VERSION is not set" >&2
        return 1
    fi

    docker_version="$(get-version \
        --source dockerhub-imagetag \
        --repo scylladb/scylla \
        -filters "$stable_release_filter" | tr -d '"')"
    github_version="$(get-version \
        --source github-tag \
        --repo scylladb/scylladb \
        --prefix scylla- \
        --out-no-prefix \
        --filters "$stable_release_filter" | tr -d '"')"

    if [[ -z "$docker_version" || -z "$github_version" ]]; then
        echo "failed to resolve latest stable Scylla release" >&2
        return 1
    fi
    if [[ "$docker_version" != "$github_version" ]]; then
        echo "latest Scylla release mismatch: DockerHub=$docker_version GitHub=$github_version" >&2
        return 1
    fi

    printf '%s' "$docker_version"
}

read_selected_tests() {
    local line selected
    selected=()
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%%#*}"
        line="$(trim "$line")"
        [[ -z "$line" ]] && continue
        selected+=("$line")
    done < "$tests_file"
    printf '%s\n' "${selected[@]}"
}

find_scylla_executable() {
    local version="$1"
    local candidate
    local -a candidates=(
        "$HOME/.ccm/scylla-repository/release/$version/bin/scylla"
        "$HOME/.ccm/repository/scylla-repository/release/$version/bin/scylla"
    )

    for candidate in "${candidates[@]}"; do
        if [[ -x "$candidate" ]]; then
            printf '%s' "$candidate"
            return 0
        fi
    done

    while IFS= read -r candidate; do
        if [[ -x "$candidate" ]]; then
            printf '%s' "$candidate"
            return 0
        fi
    done < <(find "$HOME/.ccm" -type f -path "*/release/$version/bin/scylla" 2>/dev/null | sort)

    return 1
}

pull_scylla_with_ccm() {
    local version="$1"
    local ccm="${CCM:-$driver_root/.venv/bin/ccm}"

    if [[ ! -x "$ccm" ]]; then
        ccm="$(command -v ccm || true)"
    fi
    if [[ -z "$ccm" || ! -x "$ccm" ]]; then
        echo "ccm is required to download Scylla release binaries" >&2
        return 1
    fi

    "$ccm" remove "$download_cluster_name" >/dev/null 2>&1 || true
    "$ccm" create "$download_cluster_name" -n 1 --scylla --version "release:$version"
    "$ccm" remove "$download_cluster_name" >/dev/null 2>&1 || true
}

validate_selected_tests() {
    local test_id test_file missing=0
    for test_id in "$@"; do
        test_file="${test_id%%::*}"
        if [[ ! -f "$source_dir/$test_file" ]]; then
            echo "missing Scylla test file for $test_id" >&2
            missing=1
        fi
    done
    return "$missing"
}

main() {
    local version scylla_exe python_bin
    local list_tests=0
    local -a tests passthrough_args pytest_args

    if [[ ! -f "$tests_file" ]]; then
        echo "test selection file does not exist: $tests_file" >&2
        return 1
    fi
    if [[ ! -f "$source_dir/test.py" ]]; then
        echo "Scylla source checkout with test.py is required at: $source_dir" >&2
        return 1
    fi

    mapfile -t tests < <(read_selected_tests)
    if [[ "${#tests[@]}" -eq 0 ]]; then
        echo "no Scylla tests selected in $tests_file" >&2
        return 1
    fi
    validate_selected_tests "${tests[@]}"

    version="${SCYLLA_VERSION:-}"
    if [[ -z "$version" || "$version" == "latest" ]]; then
        version="$(resolve_latest_stable_release)"
    fi
    version="${version#release:}"

    echo "Scylla release: $version"
    echo "Scylla source: $source_dir"
    echo "Selected Scylla tests (${#tests[@]}):"
    printf '  %s\n' "${tests[@]}"

    if [[ "${SCYLLA_TESTPY_DRY_RUN:-0}" == "1" ]]; then
        return 0
    fi

    scylla_exe="${SCYLLA_EXE:-}"
    if [[ -z "$scylla_exe" ]]; then
        if ! scylla_exe="$(find_scylla_executable "$version")"; then
            pull_scylla_with_ccm "$version"
            scylla_exe="$(find_scylla_executable "$version")"
        fi
    fi
    if [[ ! -x "$scylla_exe" ]]; then
        echo "Scylla executable is not available or executable: $scylla_exe" >&2
        return 1
    fi

    python_bin="${PYTHON:-$driver_root/.venv/bin/python}"
    if [[ ! -x "$python_bin" ]]; then
        python_bin="$(command -v python3 || command -v python)"
    fi

    echo "Scylla executable: $scylla_exe"

    passthrough_args=()
    for arg in "$@"; do
        if [[ "$arg" == "--list" ]]; then
            list_tests=1
        else
            passthrough_args+=("$arg")
        fi
    done

    pytest_args=(
        -p test.pylib.runner
        --color=yes
        --repeat=1
        --exe-path "$scylla_exe"
        --tmpdir "$tmpdir"
    )
    if [[ "$list_tests" == "1" ]]; then
        pytest_args+=(--collect-only --quiet --no-header)
    else
        pytest_args+=(
            --junit-xml "$tmpdir/report/pytest_driver_compat.xml"
            -rf
            -n "$jobs"
            --maxfail "$max_failures"
            --alluredir "$tmpdir/report/allure_driver_compat"
            --dist=worksteal
            --allure-no-capture
            --timeout "$timeout"
            --session-timeout "$session_timeout"
            --log-level=INFO
        )
    fi
    pytest_args+=(
        "${passthrough_args[@]}"
        "${tests[@]}"
    )

    cd "$source_dir"
    # Use Scylla's pytest runner plugin directly. The test.py wrapper probes
    # configured build modes before its --exe-path hook can switch to custom_exe,
    # which does not work for a source-only checkout plus a ccm-downloaded binary.
    "$python_bin" -m pytest "${pytest_args[@]}"
}

main "$@"
