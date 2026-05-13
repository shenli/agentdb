#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

MATRIX_ID="${MATRIX_ID:-value-matrix-$(date +%Y%m%d-%H%M%S)}"
OUT="${OUT:-${ROOT}/bench-out}"
BACKENDS="${BACKENDS:-agentdb,mongodb,postgres}"
WORKLOADS="${WORKLOADS:-mixed-agent-loop,append-only,append-fetch-hot,artifact-only,record-search-only}"
PROFILE="${PROFILE:-default-agent-runtime}"
CONCURRENCY="${CONCURRENCY:-32}"
WARMUP="${WARMUP:-10s}"
DURATION="${DURATION:-60s}"
DURABILITY_PROFILE="${DURABILITY_PROFILE:-single-node-wal}"
SESSION_SELECTION="${SESSION_SELECTION:-zipf}"
ZIPF_EXPONENT="${ZIPF_EXPONENT:-1.2}"
FETCH_LAST_N="${FETCH_LAST_N:-20}"
REPEATS="${REPEATS:-1}"
SEEDS="${SEEDS:-}"
SEED_BASE="${SEED_BASE:-43}"
BUILD_PROFILE="${BUILD_PROFILE:-release}"
NO_RAW_JSONL="${NO_RAW_JSONL:-1}"
ARTIFACT_MODE="${ARTIFACT_MODE:-reference}"

mkdir -p "${OUT}/matrix"
summary="${OUT}/matrix/${MATRIX_ID}.md"

IFS=',' read -r -a backend_values <<<"${BACKENDS}"
IFS=',' read -r -a workload_values <<<"${WORKLOADS}"

script_for_backend() {
  case "$1" in
    agentdb | agentdb-native-rocksdb)
      printf '%s\n' "${ROOT}/scripts/agentdb-bench-local.sh"
      ;;
    mongodb | mongodb-event-log)
      printf '%s\n' "${ROOT}/scripts/agentdb-bench-mongodb.sh"
      ;;
    postgres | postgres-event-log)
      printf '%s\n' "${ROOT}/scripts/agentdb-bench-postgres.sh"
      ;;
    agentdb-mongodb | agentdb-mongodb-api)
      printf '%s\n' "${ROOT}/scripts/agentdb-bench-agentdb-mongodb.sh"
      ;;
    *)
      echo "unknown backend: $1" >&2
      exit 1
      ;;
  esac
}

safe_name() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9._-' '-'
}

extract_report_value() {
  local report="$1"
  local query="$2"
  if command -v jq >/dev/null 2>&1; then
    jq -r "${query}" "${report}"
    return
  fi
  printf 'n/a'
}

format_number() {
  local value="$1"
  local decimals="$2"
  if [[ "${value}" == "n/a" || -z "${value}" || "${value}" == "null" ]]; then
    printf 'n/a'
    return
  fi
  awk -v value="${value}" -v decimals="${decimals}" 'BEGIN { printf "%.*f", decimals, value }'
}

find_single_report() {
  local run_id="$1"
  local report_count=0
  local report_path=""
  while IFS= read -r path; do
    report_count="$((report_count + 1))"
    report_path="${path}"
  done < <(find "${OUT}/runs/${run_id}" -maxdepth 1 -type f -name 'report-*.json' | sort)

  if [[ "${report_count}" -eq 1 ]]; then
    printf '%s\n' "${report_path}"
  fi
}

{
  echo "# AgentDB Value-Prop Benchmark Matrix"
  echo
  echo "- Matrix id: \`${MATRIX_ID}\`"
  echo "- Profile: \`${PROFILE}\`"
  echo "- Workloads: \`${WORKLOADS}\`"
  echo "- Backends: \`${BACKENDS}\`"
  echo "- Concurrency: \`${CONCURRENCY}\`"
  echo "- Warmup: \`${WARMUP}\`"
  echo "- Duration: \`${DURATION}\`"
  echo "- Durability: \`${DURABILITY_PROFILE}\`"
  echo "- Session selection: \`${SESSION_SELECTION}\`, Zipf exponent \`${ZIPF_EXPONENT}\`"
  echo "- Fetch last N: \`${FETCH_LAST_N}\`"
  echo "- Repeats: \`${REPEATS}\`"
  echo
  echo "| Backend | Workload | ops/s | p95 | p99 | Errors | Report |"
  echo "|---|---|---:|---:|---:|---:|---|"
} >"${summary}"

for backend in "${backend_values[@]}"; do
  backend="$(printf '%s' "${backend}" | xargs)"
  [[ -z "${backend}" ]] && continue
  bench_script="$(script_for_backend "${backend}")"
  backend_safe="$(safe_name "${backend}")"

  for workload in "${workload_values[@]}"; do
    workload="$(printf '%s' "${workload}" | xargs)"
    [[ -z "${workload}" ]] && continue
    workload_safe="$(safe_name "${workload}")"
    run_id="${MATRIX_ID}-${backend_safe}-${workload_safe}"

    echo "running backend=${backend} workload=${workload} run_id=${run_id}" >&2
    if [[ "${REPEATS}" -gt 1 ]]; then
      BENCH_SCRIPT="${bench_script}" \
        RUN_ID="${run_id}" \
        SUMMARY_ID="${run_id}" \
        OUT="${OUT}" \
        REPEATS="${REPEATS}" \
        SEEDS="${SEEDS}" \
        SEED_BASE="${SEED_BASE}" \
        BUILD_PROFILE="${BUILD_PROFILE}" \
        PROFILE="${PROFILE}" \
        WORKLOAD="${workload}" \
        CONCURRENCY="${CONCURRENCY}" \
        WARMUP="${WARMUP}" \
        DURATION="${DURATION}" \
        DURABILITY_PROFILE="${DURABILITY_PROFILE}" \
        SESSION_SELECTION="${SESSION_SELECTION}" \
        ZIPF_EXPONENT="${ZIPF_EXPONENT}" \
        FETCH_LAST_N="${FETCH_LAST_N}" \
        NO_RAW_JSONL="${NO_RAW_JSONL}" \
        ARTIFACT_MODE="${ARTIFACT_MODE}" \
        TENANTS="${TENANTS:-}" \
        AGENTS_PER_TENANT="${AGENTS_PER_TENANT:-}" \
        SESSIONS_PER_AGENT="${SESSIONS_PER_AGENT:-}" \
        EVENTS_PER_SESSION="${EVENTS_PER_SESSION:-}" \
        RECORDS_PER_SESSION="${RECORDS_PER_SESSION:-}" \
        PAYLOAD_BYTES="${PAYLOAD_BYTES:-}" \
        ARTIFACT_RATE="${ARTIFACT_RATE:-}" \
        ARTIFACT_SMALL_BYTES="${ARTIFACT_SMALL_BYTES:-}" \
        ARTIFACT_LARGE_BYTES="${ARTIFACT_LARGE_BYTES:-}" \
        FORKS_PER_PARENT="${FORKS_PER_PARENT:-}" \
        "${ROOT}/scripts/agentdb-bench-repeat.sh"
      echo "| \`${backend}\` | \`${workload}\` | see summary | see summary | see summary | see summary | [summary](${OUT}/summaries/${run_id}/summary.md) |" >>"${summary}"
    else
      RUN_ID="${run_id}" \
        OUT="${OUT}" \
        BUILD_PROFILE="${BUILD_PROFILE}" \
        PROFILE="${PROFILE}" \
        WORKLOAD="${workload}" \
        CONCURRENCY="${CONCURRENCY}" \
        WARMUP="${WARMUP}" \
        DURATION="${DURATION}" \
        DURABILITY_PROFILE="${DURABILITY_PROFILE}" \
        SESSION_SELECTION="${SESSION_SELECTION}" \
        ZIPF_EXPONENT="${ZIPF_EXPONENT}" \
        FETCH_LAST_N="${FETCH_LAST_N}" \
        NO_RAW_JSONL="${NO_RAW_JSONL}" \
        ARTIFACT_MODE="${ARTIFACT_MODE}" \
        "${bench_script}"

      report="$(find_single_report "${run_id}")"
      if [[ -n "${report}" ]]; then
        ops="$(format_number "$(extract_report_value "${report}" '.cells[0].ops_per_sec')" 0)"
        p95="$(format_number "$(extract_report_value "${report}" '.cells[0].latency.p95_ms')" 3)"
        p99="$(format_number "$(extract_report_value "${report}" '.cells[0].latency.p99_ms')" 3)"
        errors="$(extract_report_value "${report}" '.cells[0].errors')"
        echo "| \`${backend}\` | \`${workload}\` | ${ops} | ${p95}ms | ${p99}ms | ${errors} | [json](${report}) |" >>"${summary}"
      else
        echo "| \`${backend}\` | \`${workload}\` | n/a | n/a | n/a | n/a | report missing |" >>"${summary}"
      fi
    fi
  done
done

echo "matrix=${summary}"
