#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

BENCH_SCRIPT="${BENCH_SCRIPT:-${ROOT}/scripts/agentdb-bench-local.sh}"
if [[ "${BENCH_SCRIPT}" != /* ]]; then
  BENCH_SCRIPT="${ROOT}/${BENCH_SCRIPT}"
fi

RUN_ID="${RUN_ID:-repeat-$(date +%Y%m%d-%H%M%S)}"
SUMMARY_ID="${SUMMARY_ID:-${RUN_ID}}"
OUT="${OUT:-${ROOT}/bench-out}"
REPEATS="${REPEATS:-3}"
SEED_BASE="${SEED_BASE:-43}"
SEEDS="${SEEDS:-}"
BUILD_PROFILE="${BUILD_PROFILE:-release}"

if [[ ! -x "${BENCH_SCRIPT}" ]]; then
  echo "benchmark script is not executable: ${BENCH_SCRIPT}" >&2
  exit 1
fi

seed_values=()
if [[ -n "${SEEDS}" ]]; then
  IFS=',' read -r -a seed_values <<<"${SEEDS}"
  if [[ "${#seed_values[@]}" -ne "${REPEATS}" ]]; then
    echo "SEEDS must contain exactly REPEATS values" >&2
    exit 1
  fi
else
  for idx in $(seq 0 "$((REPEATS - 1))"); do
    seed_values+=("$((SEED_BASE + idx))")
  done
fi

reports=()
for idx in $(seq 1 "${REPEATS}"); do
  seed="${seed_values[$((idx - 1))]}"
  repeat_run_id="${RUN_ID}-r${idx}-s${seed}"
  echo "repeat ${idx}/${REPEATS}: run_id=${repeat_run_id} seed=${seed}"
  RUN_ID="${repeat_run_id}" OUT="${OUT}" SEED="${seed}" "${BENCH_SCRIPT}"

  report_count=0
  report_path=""
  while IFS= read -r path; do
    report_count="$((report_count + 1))"
    report_path="${path}"
  done < <(find "${OUT}/runs/${repeat_run_id}" -maxdepth 1 -type f -name 'report-*.json' | sort)

  if [[ "${report_count}" -ne 1 ]]; then
    echo "expected exactly one report JSON for ${repeat_run_id}, found ${report_count}" >&2
    exit 1
  fi
  reports+=("${report_path}")
done

if [[ "${BUILD_PROFILE}" == "release" ]]; then
  bin_dir="${ROOT}/target/release"
else
  bin_dir="${ROOT}/target/debug"
fi

"${bin_dir}/agentdb-bench" summarize-repeats \
  --out "${OUT}" \
  --summary-id "${SUMMARY_ID}" \
  "${reports[@]}"

echo "summary=${OUT}/summaries/${SUMMARY_ID}/summary.md"
