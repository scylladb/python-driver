#!/usr/bin/env bash
# Build an immutable, reusable Scylla master test environment. The driver wheel
# is installed by each test run, after loading this image.
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
recipe_hash() {
    (cd "$script_dir" && sha256sum scylla-test-image.Dockerfile build_scylla_test_image.sh) |
        sha256sum | cut -d' ' -f1
}
if [[ "${1:-}" == --recipe-hash ]]; then
    recipe_hash
    exit 0
fi
scylla_source=$(realpath "${1:?Pass the recursive Scylla source checkout}")
image=${2:?Pass the image reference}
: "${SCYLLA_TOOLCHAIN:?Set the digest-pinned upstream toolchain image}"
if [[ ! "$SCYLLA_TOOLCHAIN" =~ @sha256:[[:xdigit:]]{64}$ ]]; then
    echo 'SCYLLA_TOOLCHAIN must contain an immutable sha256 digest' >&2
    exit 1
fi
git_dir=$(git -C "$scylla_source" rev-parse --absolute-git-dir)
revision=$(git -C "$scylla_source" rev-parse HEAD)
if [[ ! "$revision" =~ ^[[:xdigit:]]{40}$ ]] || [[ "${SCYLLA_REVISION:-$revision}" != "$revision" ]]; then
    echo 'The Scylla checkout does not match SCYLLA_REVISION' >&2
    exit 1
fi
if [[ -n "$(git -C "$scylla_source" status --porcelain --untracked-files=all)" ]]; then
    echo 'The Scylla checkout must be clean to build a reusable image' >&2
    exit 1
fi
submodule_status=$(git -C "$scylla_source" submodule status --recursive)
if [[ "$submodule_status" =~ (^|$'\n')[-+U] ]]; then
    echo 'Initialize the exact pinned Scylla submodules before building' >&2
    exit 1
fi
jobs=${SCYLLA_BUILD_JOBS:-2}
if [[ ! "$jobs" =~ ^[1-9][0-9]*$ ]]; then
    echo 'SCYLLA_BUILD_JOBS must be a positive integer' >&2
    exit 1
fi
recipe=$(recipe_hash)
docker build --load --progress=plain \
    --file "$script_dir/scylla-test-image.Dockerfile" \
    --build-context "scylla-source=$scylla_source" \
    --build-context "scylla-git=$git_dir" \
    --build-arg "SCYLLA_SHA=$revision" \
    --build-arg "SCYLLA_TOOLCHAIN_IMAGE=$SCYLLA_TOOLCHAIN" \
    --build-arg "SCYLLA_BUILD_JOBS=$jobs" \
    --build-arg "SCYLLA_RECIPE_SHA=$recipe" \
    --tag "$image" "$script_dir"
for field in revision toolchain recipe; do
    case "$field" in
        revision) label=org.opencontainers.image.revision; expected=$revision ;;
        toolchain) label=io.scylladb.driver-tests.toolchain; expected=$SCYLLA_TOOLCHAIN ;;
        recipe) label=io.scylladb.driver-tests.recipe; expected=$recipe ;;
    esac
    actual=$(docker image inspect --format "{{index .Config.Labels \"$label\"}}" "$image")
    if [[ "$actual" != "$expected" ]]; then
        echo "Image label $label does not match the requested build" >&2
        exit 1
    fi
done
