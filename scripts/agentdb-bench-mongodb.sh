#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUN_ID="${RUN_ID:-mongo-$(date +%Y%m%d-%H%M%S)}"
OUT="${OUT:-${ROOT}/bench-out}"
PROFILE="${PROFILE:-smoke}"
WORKLOAD="${WORKLOAD:-mixed-agent-loop}"
CONCURRENCY="${CONCURRENCY:-1,4}"
WARMUP="${WARMUP:-5s}"
DURATION="${DURATION:-15s}"
ARTIFACT_MODE="${ARTIFACT_MODE:-reference}"
BUILD_PROFILE="${BUILD_PROFILE:-release}"
DURABILITY_PROFILE="${DURABILITY_PROFILE:-single-node-wal}"
MIN_FREE_GB="${MIN_FREE_GB:-}"
if [[ -z "${MIN_FREE_GB}" ]]; then
  if [[ "${PROFILE}" == "working-set-exceeds-cache" ]]; then
    MIN_FREE_GB=80
  else
    MIN_FREE_GB=5
  fi
fi
MONGO_PORT="${MONGO_PORT:-27017}"
MONGO_DB="${MONGO_DB:-agentdb_bench}"
COMPOSE_DIR="${ROOT}/tools/agentdb-bench/baselines/mongodb"
MONGO_DATA_DIR="${MONGO_DATA_DIR:-${OUT}/state/${RUN_ID}/mongo-data}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-agentdb-bench-mongo-${RUN_ID}}"
MONGO_UID="${MONGO_UID:-$(id -u)}"
MONGO_GID="${MONGO_GID:-$(id -g)}"

compose() {
  MONGO_PORT="${MONGO_PORT}" \
    MONGO_DATA_DIR="${MONGO_DATA_DIR}" \
    MONGO_UID="${MONGO_UID}" \
    MONGO_GID="${MONGO_GID}" \
    docker compose -p "${COMPOSE_PROJECT_NAME}" -f "${COMPOSE_DIR}/docker-compose.yml" "$@"
}

cleanup() {
  compose down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

mkdir -p "${OUT}"
free_kb="$(df -Pk "${OUT}" | awk 'NR == 2 {print $4}')"
min_free_kb="$((MIN_FREE_GB * 1024 * 1024))"
if [[ "${free_kb}" -lt "${min_free_kb}" ]]; then
  echo "not enough free disk for benchmark: have $((free_kb / 1024 / 1024)) GiB, need ${MIN_FREE_GB} GiB" >&2
  exit 1
fi

if [[ -z "${MONGO_URI:-}" ]]; then
  case "${DURABILITY_PROFILE}" in
    single-node-fsync | durable-single | durable-grouped | durable-quorum | raft-quorum | strict-fsync)
      MONGO_URI="mongodb://127.0.0.1:${MONGO_PORT}/?journal=true&w=1"
      ;;
    single-node-wal | dev-fast)
      MONGO_URI="mongodb://127.0.0.1:${MONGO_PORT}"
      ;;
    *)
      echo "unknown DURABILITY_PROFILE=${DURABILITY_PROFILE}" >&2
      exit 1
      ;;
  esac
fi

mkdir -p "${MONGO_DATA_DIR}"
compose up -d

for _ in $(seq 1 100); do
  if compose exec -T mongo \
    mongosh --quiet --eval 'db.adminCommand({ping: 1}).ok' >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

if ! compose exec -T mongo \
  mongosh --quiet --eval 'db.adminCommand({ping: 1}).ok' >/dev/null 2>&1; then
  echo "MongoDB did not become ready" >&2
  exit 1
fi

if [[ "${BUILD_PROFILE}" == "release" ]]; then
  cargo build --release -p agentdb-bench
  BIN_DIR="${CARGO_TARGET_DIR:-${ROOT}/target}/release"
else
  cargo build -p agentdb-bench
  BIN_DIR="${CARGO_TARGET_DIR:-${ROOT}/target}/debug"
fi

prepare_args=(
  prepare
  --backend mongodb-event-log
  --mongo-uri "${MONGO_URI}"
  --mongo-db "${MONGO_DB}"
  --run-id "${RUN_ID}"
  --out "${OUT}"
  --profile "${PROFILE}"
  --artifact-mode "${ARTIFACT_MODE}"
  --durability-profile "${DURABILITY_PROFILE}"
  --storage-engine mongodb-event-log
  --content-engine metadata-reference
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

"${BIN_DIR}/agentdb-bench" "${prepare_args[@]}"

run_args=(
  run
  --backend mongodb-event-log
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
[[ -n "${SESSION_SELECTION:-}" ]] && run_args+=(--session-selection "${SESSION_SELECTION}")
[[ -n "${ZIPF_EXPONENT:-}" ]] && run_args+=(--zipf-exponent "${ZIPF_EXPONENT}")
[[ "${NO_RAW_JSONL:-0}" == "1" ]] && run_args+=(--no-raw-jsonl)
[[ -n "${SEED:-}" ]] && run_args+=(--seed "${SEED}")

"${BIN_DIR}/agentdb-bench" "${run_args[@]}"

echo "run_id=${RUN_ID}"
echo "report=${OUT}/runs/${RUN_ID}/report.md"
