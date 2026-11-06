# syntax=docker/dockerfile:1.7
# The caller pins this to the digest resolved from master tools/toolchain/image.
ARG SCYLLA_TOOLCHAIN_IMAGE
FROM ${SCYLLA_TOOLCHAIN_IMAGE} AS toolchain
USER root
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
WORKDIR /opt/scylla
# Pin uv independently of the frozen compiler image, following Scylla's
# checksum-verified standalone uv release in install-dependencies.sh.
RUN case "$(uname -m)" in \
      x86_64) uv_sha=eaf842262aa1c418d8ecc5605f02ee1ebfd369124fa48548e85f9481a47831a9 ;; \
      aarch64) uv_sha=2c5d6e3092cc5223b10ff403880cc75121bf64e84644e7a0c69f643b0d89ac95 ;; \
      *) exit 1 ;; \
    esac \
    && uv_target="uv-$(uname -m)-unknown-linux-gnu" \
    && curl --fail --location --retry 3 \
       "https://github.com/astral-sh/uv/releases/download/0.12.0/${uv_target}.tar.gz" -o /tmp/uv.tar.gz \
    && echo "${uv_sha}  /tmp/uv.tar.gz" | sha256sum --check \
    && tar -xzf /tmp/uv.tar.gz --directory /tmp \
    && install -m 755 "/tmp/${uv_target}/uv" "/tmp/${uv_target}/uvx" /usr/local/bin/ \
    && rm -rf /tmp/uv.tar.gz "/tmp/${uv_target}" \
    && rpm -q libev-devel && command -v gdb && uv --version

FROM toolchain AS test-environment
COPY --from=scylla-source /test/pyproject.toml /test/uv.lock /opt/scylla/test/
ENV UV_PROJECT_ENVIRONMENT=/opt/scylla/build/driver-candidate-venv \
    UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    DISABLE_VENV=1
RUN uv sync --locked --project test --no-dev \
    && uv cache clean \
    && build/driver-candidate-venv/bin/python -c 'import cassandra, pytest; import sys; assert sys.version_info >= (3, 14)'

FROM test-environment AS cqlsh-metadata
COPY --from=scylla-source /tools/cqlsh /opt/scylla/tools/cqlsh
COPY --from=scylla-git /modules/tools/cqlsh /opt/cqlsh.git
# Generate the submodule's version before discarding its Git metadata.
RUN git config --file /opt/cqlsh.git/config core.worktree /opt/scylla/tools/cqlsh \
    && printf 'gitdir: /opt/cqlsh.git\n' > tools/cqlsh/.git \
    && cd tools/cqlsh \
    && /opt/scylla/build/driver-candidate-venv/bin/python -m setuptools_scm --force-write-version-files \
    && test -s pylib/cqlshlib/_version.py

FROM toolchain AS fixtures
ARG SCYLLA_SHA
ARG SCYLLA_BUILD_JOBS=2
# The upstream .dockerignore excludes root .git, so send it separately.
COPY --from=scylla-source / /opt/scylla
COPY --from=scylla-git / /opt/scylla/.git/
RUN test "$(git rev-parse HEAD)" = "${SCYLLA_SHA}"
# configure.py invokes ninja again for submodules; cap those invocations too.
RUN mkdir /opt/build-tools \
    && printf '#!/bin/sh\nexec /usr/bin/ninja -j%s "$@"\n' "${SCYLLA_BUILD_JOBS}" > /opt/build-tools/ninja \
    && chmod +x /opt/build-tools/ninja
ENV PATH="/opt/build-tools:${PATH}"
RUN ./configure.py --mode release --with scylla --disable-lto --debuginfo 1 \
    && ninja -j"${SCYLLA_BUILD_JOBS}" wasm

FROM fixtures AS builder
RUN ninja -j"${SCYLLA_BUILD_JOBS}" build/release/scylla
# Keep the test checkout and unstripped server, excluding intermediate objects.
# With no top-level build.ninja, test.path_to() selects the CMake layout.
RUN mkdir -p /image/opt/scylla/build/RelWithDebInfo \
    && tar --exclude=.git --exclude=./build -cf - . | tar -xf - -C /image/opt/scylla \
    && cp build/release/scylla /image/opt/scylla/build/RelWithDebInfo/scylla \
    && ln -s RelWithDebInfo /image/opt/scylla/build/release \
    && cp build/SCYLLA-* /image/opt/scylla/build/ \
    && mkdir /image/opt/scylla/build/wasm \
    && cp build/wasm/*.wat build/wasm/*.wasm /image/opt/scylla/build/wasm/ \
    && rm -f /image/opt/scylla/build.ninja

FROM test-environment AS final
ARG SCYLLA_SHA
ARG SCYLLA_TOOLCHAIN_IMAGE
ARG SCYLLA_RECIPE_SHA
COPY --from=builder /image/opt/scylla /opt/scylla
COPY --from=cqlsh-metadata /opt/scylla/tools/cqlsh/pylib/cqlshlib/_version.py /opt/scylla/tools/cqlsh/pylib/cqlshlib/_version.py
RUN build/RelWithDebInfo/scylla --version \
    && readelf --sections build/RelWithDebInfo/scylla | grep '\.debug_info' > /dev/null \
    && test build/release/scylla -ef build/RelWithDebInfo/scylla \
    && test -f conf/scylla.yaml \
    && test -f scylla-gdb.py \
    && test -f build/wasm/test_pow.wat \
    && PATH="/opt/scylla/build/driver-candidate-venv/bin:${PATH}" CQLSH_NO_BUNDLED=1 bin/cqlsh --version
RUN SCYLLA_SHA="${SCYLLA_SHA}" SCYLLA_TOOLCHAIN_IMAGE="${SCYLLA_TOOLCHAIN_IMAGE}" SCYLLA_RECIPE_SHA="${SCYLLA_RECIPE_SHA}" python3 - <<'PYMETA'
import hashlib
import json
import os
from pathlib import Path
import subprocess

binary = Path("/opt/scylla/build/RelWithDebInfo/scylla")
version = subprocess.check_output([str(binary), "--version"], text=True).strip()
assert os.environ["SCYLLA_SHA"][:12] in version, version
metadata = {
    "revision": os.environ["SCYLLA_SHA"],
    "toolchain": os.environ["SCYLLA_TOOLCHAIN_IMAGE"],
    "recipe_sha256": os.environ["SCYLLA_RECIPE_SHA"],
    "binary": str(binary),
    "binary_sha256": hashlib.file_digest(binary.open("rb"), "sha256").hexdigest(),
    "binary_version": version,
    "python": "/opt/scylla/build/driver-candidate-venv/bin/python",
}
Path("/opt/scylla/.driver-image.json").write_text(json.dumps(metadata, indent=2) + "\n")
PYMETA
LABEL org.opencontainers.image.revision="${SCYLLA_SHA}" \
    io.scylladb.driver-tests.toolchain="${SCYLLA_TOOLCHAIN_IMAGE}" \
    io.scylladb.driver-tests.recipe="${SCYLLA_RECIPE_SHA}"
ENTRYPOINT []
CMD ["/opt/scylla/build/driver-candidate-venv/bin/python", "--version"]
