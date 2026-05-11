#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUN_ID="${RUN_ID:-pg-$(date +%Y%m%d-%H%M%S)}"
OUT="${OUT:-${ROOT}/bench-out}"
PROFILE="${PROFILE:-smoke}"
WORKLOAD="${WORKLOAD:-mixed-agent-loop}"
CONCURRENCY="${CONCURRENCY:-1,4}"
WARMUP="${WARMUP:-5s}"
DURATION="${DURATION:-15s}"
ARTIFACT_MODE="${ARTIFACT_MODE:-reference}"
BUILD_PROFILE="${BUILD_PROFILE:-release}"
PG_PORT="${PG_PORT:-5433}"
PG_DSN="${PG_DSN:-postgres://bench:bench@127.0.0.1:${PG_PORT}/bench}"
COMPOSE_DIR="${ROOT}/benchmarks/postgres"

docker compose -f "${COMPOSE_DIR}/docker-compose.yml" up -d

for _ in $(seq 1 100); do
  if docker compose -f "${COMPOSE_DIR}/docker-compose.yml" exec -T postgres \
    pg_isready -U bench -d bench >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

if ! docker compose -f "${COMPOSE_DIR}/docker-compose.yml" exec -T postgres \
  pg_isready -U bench -d bench >/dev/null 2>&1; then
  echo "Postgres did not become ready" >&2
  exit 1
fi

if [[ "${BUILD_PROFILE}" == "release" ]]; then
  cargo build --release -p agentdb-bench
  BIN_DIR="${ROOT}/target/release"
else
  cargo build -p agentdb-bench
  BIN_DIR="${ROOT}/target/debug"
fi

prepare_args=(
  prepare
  --backend postgres-event-log
  --pg-dsn "${PG_DSN}"
  --run-id "${RUN_ID}"
  --out "${OUT}"
  --profile "${PROFILE}"
  --artifact-mode "${ARTIFACT_MODE}"
  --storage-engine postgres-event-log
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
  --backend postgres-event-log
  --pg-dsn "${PG_DSN}"
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
