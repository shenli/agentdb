#!/usr/bin/env bash
# AUTO-GENERATED from test plan: testing-plans/agentdb-project-wide-distributed-tests.md
# Scenario: S13 benchmark_claims_remain_reproducible_under_skew_and_noisy_neighbors
# Falsifies: C7, C19
#
# REVIEW BEFORE TREATING AS A PERMANENT REGRESSION.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
SESSION_DIR="${AGENTDB_DIST_SESSION_DIR:-${ROOT}/test-sessions/agentdb-project-wide-distributed-tests/manual}"
OUT="${AGENTDB_AUTHOR_BENCH_OUT:-${HOME}/work/agentdb-author-bench}"
MODE="${AGENTDB_AUTHOR_BENCH_GRADE:-smoke}"
mkdir -p "${SESSION_DIR}/logs" "${SESSION_DIR}/artifacts" "${OUT}"

if [[ "${AGENTDB_RUN_AUTHOR_BENCH:-0}" != "1" ]]; then
  echo "INCONCLUSIVE: set AGENTDB_RUN_AUTHOR_BENCH=1 to run benchmark smoke/repeat cells"
  exit 0
fi

case "${MODE}" in
  smoke | release) ;;
  *)
    echo "FAIL: AGENTDB_AUTHOR_BENCH_GRADE must be smoke or release; got ${MODE}" >&2
    exit 1
    ;;
esac

find_report() {
  local run_id="$1"
  local report_dir="${OUT}/runs/${run_id}"
  local count=0
  local report=""
  while IFS= read -r path; do
    count="$((count + 1))"
    report="${path}"
  done < <(find "${report_dir}" -maxdepth 1 -type f -name 'report-*.json' | sort)
  if [[ "${count}" -ne 1 ]]; then
    echo "FAIL: expected one benchmark report under ${report_dir}, found ${count}" >&2
    exit 1
  fi
  printf '%s\n' "${report}"
}

record_host_context() {
  local name="$1"
  {
    echo "mode=${MODE}"
    echo "git_sha=$(git -C "${ROOT}" rev-parse HEAD)"
    echo "date_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "uname=$(uname -a)"
    df -h "${OUT}"
    ulimit -n || true
  } >"${SESSION_DIR}/artifacts/S13-${name}-host-context.txt"
}

validate_report() {
  local report="$1"
  local require_tenant_breakdown="$2"
  local max_tenant_p99_ratio="$3"
  python3 -c '
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
require_tenants = sys.argv[2] == "1"
max_tenant_ratio = float(sys.argv[3])
report = json.loads(path.read_text())
failures = []
for key in ("run_id", "backend", "workload", "host_manifest", "software_manifest", "database_config", "config", "cells"):
    if key not in report:
        failures.append(f"missing {key}")
config = report.get("config") or {}
for key in ("concurrency", "warmup", "duration", "seed", "session_selection"):
    if key not in config:
        failures.append(f"missing config.{key}")
cells = report.get("cells") or []
if not cells:
    failures.append("report has no cells")
for idx, cell in enumerate(cells):
    errors = int(cell.get("errors") or 0)
    if errors != 0:
        failures.append(f"cell {idx} has {errors} operation errors")
    if float(cell.get("ops_per_sec") or 0) <= 0:
        failures.append(f"cell {idx} has non-positive ops_per_sec")
    latency = cell.get("latency") or {}
    aggregate_p99 = float(latency.get("p99_ms") or 0)
    if aggregate_p99 <= 0:
        failures.append(f"cell {idx} has non-positive aggregate p99")
    tenants = cell.get("tenants") or []
    if require_tenants and not tenants:
        failures.append(f"cell {idx} has no tenant latency breakdown")
    tenant_p99s = [
        float((tenant.get("latency") or {}).get("p99_ms") or 0)
        for tenant in tenants
        if int(tenant.get("successful_ops") or 0) > 0
    ]
    if require_tenants and aggregate_p99 > 0 and tenant_p99s:
        worst = max(tenant_p99s)
        if worst > aggregate_p99 * max_tenant_ratio:
            failures.append(
                f"cell {idx} tenant p99 {worst:.3f}ms exceeds "
                f"{max_tenant_ratio:.2f}x aggregate p99 {aggregate_p99:.3f}ms"
            )
if failures:
    raise SystemExit("FAIL: " + "; ".join(failures))
print(f"validated report {path}")
' "${report}" "${require_tenant_breakdown}" "${max_tenant_p99_ratio}"
}

validate_repeat_summary() {
  local summary="$1"
  local expected_repeats="$2"
  local max_relative_spread="$3"
  python3 -c '
import json, pathlib, sys
path = pathlib.Path(sys.argv[1])
expected_repeats = int(sys.argv[2])
max_spread = float(sys.argv[3])
summary = json.loads(path.read_text())
failures = []
groups = summary.get("groups") or []
if not groups:
    failures.append("summary has no aggregate groups")
for group in groups:
    label = "{}/{}/c{}".format(
        group.get("backend"), group.get("workload"), group.get("concurrency")
    )
    repeats = int(group.get("repeats") or 0)
    if repeats != expected_repeats:
        failures.append(f"{label} has {repeats} repeats, expected {expected_repeats}")
    errors = int(group.get("errors") or 0)
    if errors != 0:
        failures.append(f"{label} has {errors} errors")
    ops = group.get("ops_per_sec") or {}
    p99 = group.get("p99_ms") or {}
    ops_mean = float(ops.get("mean") or 0)
    ops_min = float(ops.get("min") or 0)
    p99_mean = float(p99.get("mean") or 0)
    p99_max = float(p99.get("max") or 0)
    if ops_mean <= 0 or p99_mean <= 0:
        failures.append(f"{label} has non-positive repeat statistics")
    if ops_mean > 0 and ops_min < ops_mean * (1.0 - max_spread):
        failures.append(f"{label} ops/s min regressed by more than {max_spread:.0%}")
    if p99_mean > 0 and p99_max > p99_mean * (1.0 + max_spread):
        failures.append(f"{label} p99 max exceeded mean by more than {max_spread:.0%}")
if failures:
    raise SystemExit("FAIL: " + "; ".join(failures))
print(f"validated repeat summary {path}")
' "${summary}" "${expected_repeats}" "${max_relative_spread}"
}

run_smoke() {
  local run_id="author-s13-smoke-$(date +%Y%m%d-%H%M%S)"
  record_host_context "smoke"
  OUT="${OUT}" RUN_ID="${run_id}" BUILD_PROFILE="${BUILD_PROFILE:-debug}" PROFILE="${PROFILE:-smoke}" \
    WORKLOAD="${WORKLOAD:-mixed-agent-loop}" CONCURRENCY="${CONCURRENCY:-1,4}" \
    WARMUP="${WARMUP:-1s}" DURATION="${DURATION:-5s}" \
    SESSION_SELECTION="${SESSION_SELECTION:-zipf}" ZIPF_EXPONENT="${ZIPF_EXPONENT:-1.2}" \
    "${ROOT}/tools/agentdb-bench/scripts/agentdb-bench-local.sh" \
    2>&1 | tee "${SESSION_DIR}/logs/S13-agentdb-bench-smoke.log"
  local report
  report="$(find_report "${run_id}")"
  validate_report "${report}" 1 "${MAX_TENANT_P99_RATIO:-2.0}"
  echo "PASS: benchmark smoke produced and validated ${report}"
}

run_release_gate() {
  local release_id="author-s13-release-$(date +%Y%m%d-%H%M%S)"
  local repeats="${REPEATS:-3}"
  local spread="${MAX_REPEAT_RELATIVE_SPREAD:-0.15}"
  local tenant_ratio="${MAX_TENANT_P99_RATIO:-2.0}"
  record_host_context "release"

  OUT="${OUT}" RUN_ID="${release_id}-repeat" SUMMARY_ID="${release_id}-repeat" \
    BUILD_PROFILE="${BUILD_PROFILE:-release}" REPEATS="${repeats}" SEEDS="${SEEDS:-101,202,303}" \
    PROFILE="${PROFILE:-default-agent-runtime}" WORKLOAD="${WORKLOAD:-mixed-agent-loop}" \
    CONCURRENCY="${CONCURRENCY:-32}" WARMUP="${WARMUP:-10s}" DURATION="${DURATION:-60s}" \
    TENANTS="${TENANTS:-10}" AGENTS_PER_TENANT="${AGENTS_PER_TENANT:-2}" \
    SESSIONS_PER_AGENT="${SESSIONS_PER_AGENT:-50}" EVENTS_PER_SESSION="${EVENTS_PER_SESSION:-50}" \
    RECORDS_PER_SESSION="${RECORDS_PER_SESSION:-10}" SESSION_SELECTION="${SESSION_SELECTION:-zipf}" \
    ZIPF_EXPONENT="${ZIPF_EXPONENT:-1.2}" DURABILITY_PROFILE="${DURABILITY_PROFILE:-single-node-wal}" \
    "${ROOT}/tools/agentdb-bench/scripts/agentdb-bench-repeat.sh" \
    2>&1 | tee "${SESSION_DIR}/logs/S13-agentdb-bench-release-repeat.log"

  local summary="${OUT}/summaries/${release_id}-repeat/summary.json"
  if [[ ! -f "${summary}" ]]; then
    echo "FAIL: repeat summary not found at ${summary}" >&2
    exit 1
  fi
  validate_repeat_summary "${summary}" "${repeats}" "${spread}"

  local noisy_id="${release_id}-noisy-neighbor"
  OUT="${OUT}" RUN_ID="${noisy_id}" BUILD_PROFILE="${BUILD_PROFILE:-release}" \
    PROFILE="${NOISY_PROFILE:-default-agent-runtime}" WORKLOAD="${NOISY_WORKLOAD:-mixed-agent-loop}" \
    CONCURRENCY="${NOISY_CONCURRENCY:-32}" WARMUP="${NOISY_WARMUP:-10s}" DURATION="${NOISY_DURATION:-60s}" \
    TENANTS="${NOISY_TENANTS:-8}" AGENTS_PER_TENANT="${NOISY_AGENTS_PER_TENANT:-2}" \
    SESSIONS_PER_AGENT="${NOISY_SESSIONS_PER_AGENT:-64}" EVENTS_PER_SESSION="${NOISY_EVENTS_PER_SESSION:-50}" \
    RECORDS_PER_SESSION="${NOISY_RECORDS_PER_SESSION:-10}" SESSION_SELECTION="${NOISY_SESSION_SELECTION:-hotset-90-10}" \
    DURABILITY_PROFILE="${DURABILITY_PROFILE:-single-node-wal}" \
    "${ROOT}/tools/agentdb-bench/scripts/agentdb-bench-local.sh" \
    2>&1 | tee "${SESSION_DIR}/logs/S13-agentdb-bench-noisy-neighbor.log"
  local noisy_report
  noisy_report="$(find_report "${noisy_id}")"
  validate_report "${noisy_report}" 1 "${tenant_ratio}"

  echo "PASS: release-grade repeat and noisy-neighbor benchmark gates passed"
}

if [[ "${MODE}" == "release" ]]; then
  run_release_gate
else
  run_smoke
fi
