#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${OUT:-${ROOT}/bench-out}"
MATRIX_ID="${MATRIX_ID:-}"
TAIL_LINES="${TAIL_LINES:-25}"

usage() {
  cat <<'EOF'
Usage: scripts/agentdb-bench-status.sh [OPTIONS]

Shows progress for an AgentDB benchmark matrix output directory.

Options:
  --out DIR          Benchmark output root. Default: OUT or bench-out.
  --matrix-id ID     Matrix id. Default: newest Markdown file under OUT/matrix.
  --tail N           Tail N lines from OUT/logs/<matrix-id>.log. Default: 25.
  --help             Show this help.

Examples:
  scripts/agentdb-bench-status.sh --out bench-out --matrix-id benchmark-matrix-1
  OUT=bench-out MATRIX_ID=benchmark-matrix-1 scripts/agentdb-bench-status.sh
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out)
      shift
      OUT="${1:-}"
      [[ -n "${OUT}" ]] || { echo "--out requires a value" >&2; exit 2; }
      ;;
    --matrix-id)
      shift
      MATRIX_ID="${1:-}"
      [[ -n "${MATRIX_ID}" ]] || { echo "--matrix-id requires a value" >&2; exit 2; }
      ;;
    --tail)
      shift
      TAIL_LINES="${1:-}"
      [[ -n "${TAIL_LINES}" ]] || { echo "--tail requires a value" >&2; exit 2; }
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
  shift
done

if ! [[ "${TAIL_LINES}" =~ ^[0-9]+$ ]]; then
  echo "--tail must be a non-negative integer" >&2
  exit 2
fi

if [[ ! -d "${OUT}" ]]; then
  echo "benchmark output root does not exist: ${OUT}" >&2
  exit 1
fi

if [[ -z "${MATRIX_ID}" ]]; then
  latest_matrix=""
  while IFS= read -r path; do
    if [[ -z "${latest_matrix}" || "${path}" -nt "${latest_matrix}" ]]; then
      latest_matrix="${path}"
    fi
  done < <(find "${OUT}/matrix" -maxdepth 1 -type f -name '*.md' 2>/dev/null)
  if [[ -z "${latest_matrix}" ]]; then
    echo "--matrix-id is required because no matrix Markdown files exist under ${OUT}/matrix" >&2
    exit 2
  fi
  MATRIX_ID="$(basename "${latest_matrix}" .md)"
fi

matrix_md="${OUT}/matrix/${MATRIX_ID}.md"
matrix_log="${OUT}/logs/${MATRIX_ID}.log"

echo "benchmark status"
echo "matrix_id=${MATRIX_ID}"
echo "out=${OUT}"
if [[ -f "${matrix_md}" ]]; then
  echo "matrix=${matrix_md}"
else
  echo "matrix=missing:${matrix_md}"
fi
if [[ -f "${matrix_log}" ]]; then
  echo "log=${matrix_log}"
else
  echo "log=missing:${matrix_log}"
fi

echo
echo "disk"
df -h "${OUT}" | awk 'NR == 1 || NR == 2 {print}'

echo
echo "active processes"
if command -v pgrep >/dev/null 2>&1; then
  matches="$(pgrep -af "${MATRIX_ID}" 2>/dev/null || true)"
  if [[ -n "${matches}" ]]; then
    printf '%s\n' "${matches}" | grep -v 'agentdb-bench-status.sh' || true
  else
    echo "none"
  fi
else
  echo "pgrep unavailable"
fi

echo
echo "completed summaries"
summary_count=0
printed_summary_header=0
while IFS= read -r summary_json; do
  summary_count="$((summary_count + 1))"
  if command -v jq >/dev/null 2>&1; then
    rows="$(jq -r '
      .groups[] |
      [
        .backend,
        .workload,
        (.concurrency | tostring),
        (.session_selection + (if .zipf_exponent == null then "" else "-" + (.zipf_exponent | tostring) end)),
        (.repeats | tostring),
        (.ops_per_sec.mean | tostring),
        (.p95_ms.mean | tostring),
        (.p99_ms.mean | tostring),
        (.errors | tostring)
      ] | @tsv
    ' "${summary_json}" | awk -F '\t' '
      {
        printf "%-28s %-24s %3s %-12s %7s %12.2f %10.3f %10.3f %8s\n", $1, $2, $3, $4, $5, $6, $7, $8, $9
      }
    ')"
    if [[ -n "${rows}" ]]; then
      if [[ "${printed_summary_header}" -eq 0 ]]; then
        printf "%-28s %-24s %3s %-12s %7s %12s %10s %10s %8s\n" "backend" "workload" "c" "selection" "repeats" "ops_mean" "p95_ms" "p99_ms" "errors"
        printed_summary_header=1
      fi
      printf '%s\n' "${rows}"
    fi
  else
    echo "${summary_json}"
  fi
done < <(find "${OUT}/summaries" -maxdepth 2 -type f -name summary.json -path "*/${MATRIX_ID}-*/summary.json" 2>/dev/null | sort)
if [[ "${summary_count}" -eq 0 ]]; then
  echo "none"
fi

echo
echo "completed run reports"
find "${OUT}/runs" -maxdepth 2 -type f -name 'report-*.json' -path "*/${MATRIX_ID}-*/report-*.json" 2>/dev/null | wc -l | awk '{print $1}'

if [[ "${TAIL_LINES}" -gt 0 && -f "${matrix_log}" ]]; then
  echo
  echo "log tail"
  tail -n "${TAIL_LINES}" "${matrix_log}"
fi
