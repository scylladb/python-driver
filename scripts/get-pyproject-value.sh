#!/usr/bin/env bash
set -euo pipefail

TARGET_SECTION=${1:?section name like tool.cibuildwheel}
TARGET_FIELD=${2:?field name like skip or build-frontend}

# Resolve pyproject.toml relative to script location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYPROJECT_FILE="$SCRIPT_DIR/../pyproject.toml"

awk -v sec="$TARGET_SECTION" -v field="$TARGET_FIELD" '
  /^\[/ {
    # exact match on section header, treat dots literally
    in_section = ($0 == "[" sec "]")
  }

  in_section {
    # Match "field = ..."
    if (match($0, "^[ \t]*" field "[ \t]*=")) {
      rhs = substr($0, RSTART + RLENGTH)
      gsub(/^[ \t]+/, "", rhs)

      if (rhs ~ /^\[/) {
        # Multiline or inline array
        buf = rhs
        while (buf !~ /\]/ && (getline > 0)) buf = buf " " $0
        if (match(buf, /\[(.*)\]/, m)) {
          s = m[1]
          gsub(/["'\'' ,]/, " ", s)
          gsub(/[ \t]+/, " ", s)
          gsub(/^[ \t]+|[ \t]+$/, "", s)
          print s
          exit
        }
      } else {
        # Scalar string or number, like "build[uv]"
        s = rhs
        gsub(/^["'\'' ]+|["'\'' ]+$/, "", s)
        print s
        exit
      }
    }
  }
' "$PYPROJECT_FILE"
