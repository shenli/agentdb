#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUN_ID="${RUN_ID:-agentdb-mongodb-$(date +%Y%m%d-%H%M%S)}"
BIND="${BIND:-127.0.0.1:7893}"
MONGO_BIND="${MONGO_BIND:-127.0.0.1:27018}"
BASE_URL="${BASE_URL:-http://${BIND}}"
MONGO_URI="${MONGO_URI:-mongodb://${MONGO_BIND}/?directConnection=true}"
MONGO_DB="${MONGO_DB:-agentdb_bench_probe}"
OUT="${OUT:-${ROOT}/bench-out}"
PROFILE="${PROFILE:-smoke}"
WORKLOAD="${WORKLOAD:-mixed-agent-loop}"
CONCURRENCY="${CONCURRENCY:-1,4}"
WARMUP="${WARMUP:-5s}"
DURATION="${DURATION:-15s}"
ARTIFACT_MODE="${ARTIFACT_MODE:-reference}"
MIN_FREE_GB="${MIN_FREE_GB:-5}"
BUILD_PROFILE="${BUILD_PROFILE:-release}"
LOCAL_RUNTIME_TABLETS="${LOCAL_RUNTIME_TABLETS:-1}"

STATE_ROOT="${STATE_ROOT:-${OUT}/state/${RUN_ID}}"
DATA_DIR="${DATA_DIR:-${STATE_ROOT}/rocksdb}"
CONTENT_DIR="${CONTENT_DIR:-${STATE_ROOT}/content}"
LOG_DIR="${LOG_DIR:-${OUT}/logs/${RUN_ID}}"

mkdir -p "${OUT}" "${STATE_ROOT}" "${DATA_DIR}" "${CONTENT_DIR}" "${LOG_DIR}"

free_kb="$(df -Pk "${OUT}" | awk 'NR == 2 {print $4}')"
min_free_kb="$((MIN_FREE_GB * 1024 * 1024))"
if [[ "${free_kb}" -lt "${min_free_kb}" ]]; then
  echo "not enough free disk for benchmark: have $((free_kb / 1024 / 1024)) GiB, need ${MIN_FREE_GB} GiB" >&2
  exit 1
fi

if [[ "${BUILD_PROFILE}" == "release" ]]; then
  cargo build --release -p agentdb-server -p agentdb-bench
  BIN_DIR="${ROOT}/target/release"
else
  cargo build -p agentdb-server -p agentdb-bench
  BIN_DIR="${ROOT}/target/debug"
fi

AGENTDB_LOCAL_RUNTIME_TABLETS="${LOCAL_RUNTIME_TABLETS}" "${BIN_DIR}/agentdb" serve \
  --bind "${BIND}" \
  --mongo-bind "${MONGO_BIND}" \
  --data-dir "${DATA_DIR}" \
  --content-dir "${CONTENT_DIR}" \
  >"${LOG_DIR}/agentdb-server.log" 2>&1 &
server_pid="$!"

cleanup() {
  if kill -0 "${server_pid}" 2>/dev/null; then
    kill "${server_pid}" 2>/dev/null || true
    wait "${server_pid}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

for _ in $(seq 1 100); do
  if curl -fsS "${BASE_URL}/health/ready" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

if ! curl -fsS "${BASE_URL}/health/ready" >/dev/null 2>&1; then
  echo "AgentDB did not become ready; see ${LOG_DIR}/agentdb-server.log" >&2
  exit 1
fi

prepare_args=(
  prepare
  --backend agentdb-mongodb-api
  --mongo-uri "${MONGO_URI}"
  --mongo-db "${MONGO_DB}"
  --run-id "${RUN_ID}"
  --out "${OUT}"
  --profile "${PROFILE}"
  --artifact-mode "${ARTIFACT_MODE}"
  --storage-engine rocksdb
  --content-engine localfs
  --data-dir "${DATA_DIR}"
  --content-dir "${CONTENT_DIR}"
)

[[ -n "${TENANTS:-}" ]] && prepare_args+=(--tenants "${TENANTS}")
[[ -n "${AGENTS_PER_TENANT:-}" ]] && prepare_args+=(--agents-per-tenant "${AGENTS_PER_TENANT}")
[[ -n "${SESSIONS_PER_AGENT:-}" ]] && prepare_args+=(--sessions-per-agent "${SESSIONS_PER_AGENT}")
[[ -n "${EVENTS_PER_SESSION:-}" ]] && prepare_args+=(--events-per-session "${EVENTS_PER_SESSION}")
[[ -n "${RECORDS_PER_SESSION:-}" ]] && prepare_args+=(--records-per-session "${RECORDS_PER_SESSION}")
[[ -n "${PAYLOAD_BYTES:-}" ]] && prepare_args+=(--payload-bytes "${PAYLOAD_BYTES}")
[[ -n "${ARTIFACT_RATE:-}" ]] && prepare_args+=(--artifact-rate "${ARTIFACT_RATE}")
[[ -n "${ARTIFACT_SMALL_BYTES:-}" ]] && prepare_args+=(--artifact-small-bytes "${ARTIFACT_SMALL_BYTES}")
[[ -n "${ARTIFACT_LARGE_BYTES:-}" ]] && prepare_args+=(--artifact-large-bytes "${ARTIFACT_LARGE_BYTES}")
[[ -n "${FORKS_PER_PARENT:-}" ]] && prepare_args+=(--forks-per-parent "${FORKS_PER_PARENT}")
[[ -n "${SEED:-}" ]] && prepare_args+=(--seed "${SEED}")

"${BIN_DIR}/agentdb-bench" "${prepare_args[@]}" \
  >"${LOG_DIR}/prepare.log" 2>&1

run_args=(
  run
  --backend agentdb-mongodb-api
  --mongo-uri "${MONGO_URI}"
  --mongo-db "${MONGO_DB}"
  --run-id "${RUN_ID}"
  --out "${OUT}"
  --workload "${WORKLOAD}"
  --concurrency "${CONCURRENCY}"
  --warmup "${WARMUP}"
  --duration "${DURATION}"
)

[[ -n "${FETCH_LAST_N:-}" ]] && run_args+=(--fetch-last-n "${FETCH_LAST_N}")
[[ "${NO_RAW_JSONL:-0}" == "1" ]] && run_args+=(--no-raw-jsonl)
[[ -n "${SEED:-}" ]] && run_args+=(--seed "${SEED}")

"${BIN_DIR}/agentdb-bench" "${run_args[@]}" \
  | tee "${LOG_DIR}/run.log"

echo "run_id=${RUN_ID}"
echo "report=${OUT}/runs/${RUN_ID}/report.md"
echo "logs=${LOG_DIR}"
