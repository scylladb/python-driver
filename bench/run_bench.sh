#!/usr/bin/env bash
# run_bench.sh -- Run all four ingestion benchmark variants and print comparison.
#
# Prerequisites:
#   1. Run bench/setup_bench.sh first (container, venvs, dataset)
#   2. ScyllaDB container "scylla-bench" must be running on port 9042
#
# Usage:
#   bash bench/run_bench.sh [--variants=A,B,C,D] [--batch-size=100] [--host=127.0.0.1]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BENCH_SCRIPT="$SCRIPT_DIR/bench_ingest.py"

HOST="127.0.0.1"
PORT=9042
MAX_ROWS=100000
CONCURRENCY=1000
RUNS=3
DIM=768
CLIENT_CPUSET="4-5"
DATASET_DIR="$HOME/vector_bench/dataset/cohere/cohere_medium_1m"
VARIANTS="A,A2,B,C,E"
RESULTS_FILE="$SCRIPT_DIR/results.json"

for arg in "$@"; do
    case "$arg" in
        --variants=*)    VARIANTS="${arg#*=}" ;;
        --max-rows=*)    MAX_ROWS="${arg#*=}" ;;
        --concurrency=*) CONCURRENCY="${arg#*=}" ;;
        --runs=*)        RUNS="${arg#*=}" ;;
        --client-cpuset=*) CLIENT_CPUSET="${arg#*=}" ;;
        --host=*)        HOST="${arg#*=}" ;;
        --port=*)        PORT="${arg#*=}" ;;
        --dataset-dir=*) DATASET_DIR="${arg#*=}" ;;
        *) echo "Unknown arg: $arg"; exit 1 ;;
    esac
done

# Map variant -> venv
venv_for_variant() {
    case "$1" in
        A)  echo "$SCRIPT_DIR/venv-baseline" ;;
        A2) echo "$SCRIPT_DIR/venv-baseline" ;;
        A3) echo "$SCRIPT_DIR/venv-baseline" ;;
        B)  echo "$SCRIPT_DIR/venv-enhanced" ;;
        C)  echo "$SCRIPT_DIR/venv-enhanced" ;;
        D)  echo "$SCRIPT_DIR/venv-rs-driver" ;;
        E)  echo "$SCRIPT_DIR/venv-enhanced" ;;
        F)  echo "$SCRIPT_DIR/venv-enhanced-ft" ;;
        *) echo "ERROR: unknown variant $1" >&2; exit 1 ;;
    esac
}

variant_label() {
    case "$1" in
        A)  echo "scylla-driver (master), execute_concurrent, list[float]" ;;
        A2) echo "scylla-driver (master), execute_concurrent, numpy" ;;
        A3) echo "scylla-driver (master), decoupled executor, numpy" ;;
        B)  echo "scylla-driver (enhanced), execute_concurrent, list[float]" ;;
        C)  echo "scylla-driver (enhanced), execute_concurrent, numpy" ;;
        D)  echo "python-rs-driver, asyncio.gather" ;;
        E)  echo "scylla-driver (enhanced), decoupled executor, numpy" ;;
        F)  echo "scylla-driver (enhanced), free-threaded, numpy" ;;
    esac
}

# Verify prerequisites
check_prerequisites() {
    # Check dataset
    if [ ! -f "$DATASET_DIR/shuffle_train.parquet" ]; then
        echo "ERROR: Dataset not found at $DATASET_DIR/shuffle_train.parquet"
        echo "  Run: bash bench/setup_bench.sh"
        exit 1
    fi

    # Check ScyllaDB is reachable
    if command -v podman &>/dev/null; then
        RUNTIME=podman
    elif command -v docker &>/dev/null; then
        RUNTIME=docker
    else
        echo "WARNING: No container runtime found, assuming ScyllaDB is running externally"
        return
    fi

    if ! $RUNTIME exec scylla-bench cqlsh -e "SELECT now() FROM system.local" &>/dev/null; then
        echo "ERROR: ScyllaDB container 'scylla-bench' is not responding"
        echo "  Run: bash bench/setup_bench.sh"
        exit 1
    fi

    # Check venvs exist for requested variants
    IFS=',' read -ra VARIANT_LIST <<< "$VARIANTS"
    for v in "${VARIANT_LIST[@]}"; do
        local venv
        venv=$(venv_for_variant "$v")
        if [ ! -d "$venv" ]; then
            echo "ERROR: Virtual environment not found: $venv"
            echo "  Run: bash bench/setup_bench.sh"
            exit 1
        fi
    done
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

echo "============================================================"
echo " Vector Ingestion Benchmark"
echo "============================================================"
echo " Dataset:    Cohere 768-dim, 1M vectors"
echo " Max rows:   $MAX_ROWS"
echo " Concurrency:$CONCURRENCY"
echo " Runs:       $RUNS"
echo " Client CPU: $CLIENT_CPUSET"
echo " Host:       $HOST:$PORT"
echo " Variants:   $VARIANTS"
echo "============================================================"
echo ""

check_prerequisites

IFS=',' read -ra VARIANT_LIST <<< "$VARIANTS"

# Collect results
declare -a RESULT_JSONS=()

for v in "${VARIANT_LIST[@]}"; do
    venv=$(venv_for_variant "$v")
    label=$(variant_label "$v")
    python_bin="$venv/bin/python3"

    # Free-threaded variant needs GIL disabled
    env_prefix=""
    if [ "$v" = "F" ]; then
        env_prefix="PYTHON_GIL=0"
    fi

    echo "------------------------------------------------------------"
    echo " Variant $v: $label"
    echo "------------------------------------------------------------"

    # -I (isolated mode) removes '' from sys.path so that the local
    # cassandra/ package in the repo root does not shadow the pip-installed
    # driver in site-packages.  This is critical for baseline variants (A)
    # where we want stock master code, not the enhanced branch.
    # Editable installs (enhanced venvs) use a custom path hook that
    # survives -I, so they still load from the working tree correctly.
    if ! result=$(taskset -c "$CLIENT_CPUSET" env $env_prefix $python_bin -I "$BENCH_SCRIPT" \
        --variant "$v" \
        --host "$HOST" \
        --port "$PORT" \
        --dataset-dir "$DATASET_DIR" \
        --max-rows "$MAX_ROWS" \
        --concurrency "$CONCURRENCY" \
        --runs "$RUNS" \
        --dim "$DIM" \
    ); then
        echo "  ERROR: Variant $v failed (exit code $?). Skipping."
        echo ""
        continue
    fi

    if [ -z "$result" ]; then
        echo "  ERROR: Variant $v produced no output. Skipping."
        echo ""
        continue
    fi

    echo "$result" | python3 -c "
import json, sys
r = json.load(sys.stdin)
print(f\"  => {r['rows']:,} rows in {r['elapsed_sec']:.1f}s = {r['rows_per_sec']:,.0f} rows/sec\")
"
    RESULT_JSONS+=("$result")
    echo ""
done

# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

if [ ${#RESULT_JSONS[@]} -eq 0 ]; then
    echo "ERROR: No variants completed successfully."
    exit 1
fi

echo "============================================================"
echo " RESULTS SUMMARY"
echo "============================================================"
echo ""

# Write all results to a JSON file for later analysis
echo "[" > "$RESULTS_FILE"
first=true
for r in "${RESULT_JSONS[@]}"; do
    if $first; then
        first=false
    else
        echo "," >> "$RESULTS_FILE"
    fi
    echo "  $r" >> "$RESULTS_FILE"
done
echo "]" >> "$RESULTS_FILE"

# Pretty-print the comparison table
python3 - "$RESULTS_FILE" "${RESULT_JSONS[@]}" <<'PYEOF'
import json
import sys

results_file = sys.argv[1]
results = [json.loads(arg) for arg in sys.argv[2:]]
if not results:
    print("No results to display.")
    sys.exit(0)

baseline_rps = results[0]["avg"]

print(f"{'Variant':<55} | {'Best':>8} | {'Avg':>8} | {'Worst':>8} | {'vs Baseline':>11}")
print(f"{'-'*55}-+-{'-'*8}-+-{'-'*8}-+-{'-'*8}-+-{'-'*11}")

for r in results:
    variant = f"{r['variant']}: {r['label']}"
    best = r["best"]
    avg = r["avg"]
    worst = r["worst"]
    speedup = avg / baseline_rps if baseline_rps > 0 else 0
    print(f"{variant:<55} | {best:>8,.0f} | {avg:>8,.0f} | {worst:>8,.0f} | {speedup:>9.2f}x")

print()
print(f"Results saved to: {results_file}")
PYEOF

echo ""
echo "Raw JSON results saved to: $RESULTS_FILE"
echo "Done."
