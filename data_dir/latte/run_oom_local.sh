#!/usr/bin/env bash
# Run the oom.rn latte workload against a local single-node Scylla container.
#
# Usage:
#   ./data_dir/latte/run_oom_local.sh              # quick smoke test (50 tables)
#   TABLE_COUNT=2000 ROWS=10000000 ./data_dir/latte/run_oom_local.sh  # full reproducer profile
#
# Prerequisites: docker

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OOM_RN="${SCRIPT_DIR}/oom.rn"

# Tunables — override via environment
SCYLLA_IMAGE="${SCYLLA_IMAGE:-scylladb/scylla-nightly:latest}"
LATTE_IMAGE="${LATTE_IMAGE:-scylladb/latte:0.43.1-scylladb}"
TABLE_COUNT="${TABLE_COUNT:-50}"
ROWS="${ROWS:-100000}"
ROWS_PER_PARTITION="${ROWS_PER_PARTITION:-100}"
RF="${RF:-1}"
WRITE_RATE="${WRITE_RATE:-5000}"
WRITE_DURATION="${WRITE_DURATION:-1m}"
MIXED_RATE="${MIXED_RATE:-300}"
MIXED_DURATION="${MIXED_DURATION:-1m}"
CONTAINER_NAME="${CONTAINER_NAME:-scylla-oom}"

LATTE_PARAMS="-P replication_factor=${RF} -P tablets=false -P table_count=${TABLE_COUNT} -P rows=${ROWS} -P rows_per_partition=${ROWS_PER_PARTITION}"

cleanup() {
    echo "--- Stopping Scylla container..."
    docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true
}

run_latte() {
    docker run -it --rm --network=host \
        -v "${OOM_RN}:/oom.rn:ro" \
        "${LATTE_IMAGE}" \
        "$@" /oom.rn -- 127.0.0.1
}

# ── 1. Start Scylla ──────────────────────────────────────────────────────────
echo "--- Starting Scylla (${SCYLLA_IMAGE})..."
docker rm -f "${CONTAINER_NAME}" 2>/dev/null || true
docker run --name "${CONTAINER_NAME}" --rm -d \
    -p 9042:9042 \
    "${SCYLLA_IMAGE}" \
    --smp 1 --memory 512M

trap cleanup EXIT

echo "--- Waiting for CQL to be ready..."
for i in $(seq 1 60); do
    if docker exec "${CONTAINER_NAME}" cqlsh -e "SELECT now() FROM system.local" &>/dev/null; then
        echo "    CQL ready after ~${i}s"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "ERROR: Scylla did not become ready in 60s" >&2
        exit 1
    fi
    sleep 1
done

# ── 2. Create schema ─────────────────────────────────────────────────────────
echo "--- Creating schema (${TABLE_COUNT} tables)..."
# shellcheck disable=SC2086
run_latte schema ${LATTE_PARAMS}

# ── 3. Write data ────────────────────────────────────────────────────────────
echo "--- Writing data (rows=${ROWS}, rate=${WRITE_RATE}, duration=${WRITE_DURATION})..."
# shellcheck disable=SC2086
run_latte run -f write ${LATTE_PARAMS} \
    --rate "${WRITE_RATE}" --duration="${WRITE_DURATION}" --start-cycle 0

# ── 4. Mixed read/write traffic ──────────────────────────────────────────────
echo "--- Running mixed traffic (write:1 read:3, rate=${MIXED_RATE}, duration=${MIXED_DURATION})..."
# shellcheck disable=SC2086
run_latte run -f write:1 -f read:3 ${LATTE_PARAMS} \
    --threads=4 --rate="${MIXED_RATE}" --duration="${MIXED_DURATION}" \
    --request-timeout=30

echo "--- Done."
