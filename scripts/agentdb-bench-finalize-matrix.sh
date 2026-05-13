#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${OUT:-${ROOT}/bench-out}"
MATRIX_ID="${MATRIX_ID:-}"
BASELINE_BACKEND="${BASELINE_BACKEND:-agentdb-native-rocksdb}"
TAIL_LINES="${TAIL_LINES:-0}"

usage() {
  cat <<'EOF'
Usage: scripts/agentdb-bench-finalize-matrix.sh [OPTIONS]

Builds a comparison-ready Markdown summary from repeat summary JSON files.
The command is safe to run while a matrix is still in progress; incomplete
backend/workload cells are omitted until their repeat summary exists.

Options:
  --out DIR              Benchmark output root. Default: OUT or bench-out.
  --matrix-id ID         Matrix id. Default: newest Markdown file under OUT/matrix.
  --baseline-backend ID  Backend used as numerator for ratios. Default: agentdb-native-rocksdb.
  --tail N              Include N lines from OUT/logs/<matrix-id>.log. Default: 0.
  --help                Show this help.

Output:
  Writes OUT/matrix/<matrix-id>-final.md and prints the path.

Examples:
  scripts/agentdb-bench-finalize-matrix.sh --out /home/lishen/work/agentdb-bench-runs --matrix-id longrun-value-wal-20260513-1
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
    --baseline-backend)
      shift
      BASELINE_BACKEND="${1:-}"
      [[ -n "${BASELINE_BACKEND}" ]] || { echo "--baseline-backend requires a value" >&2; exit 2; }
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
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required to finalize benchmark matrices" >&2
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
  done < <(find "${OUT}/matrix" -maxdepth 1 -type f -name '*.md' ! -name '*-final.md' 2>/dev/null)
  if [[ -z "${latest_matrix}" ]]; then
    echo "--matrix-id is required because no matrix Markdown files exist under ${OUT}/matrix" >&2
    exit 2
  fi
  MATRIX_ID="$(basename "${latest_matrix}" .md)"
fi

mkdir -p "${OUT}/matrix"
matrix_md="${OUT}/matrix/${MATRIX_ID}.md"
matrix_log="${OUT}/logs/${MATRIX_ID}.log"
output_md="${OUT}/matrix/${MATRIX_ID}-final.md"
tmp_rows="$(mktemp "${TMPDIR:-/tmp}/agentdb-bench-finalize.XXXXXX")"
trap 'rm -f "${tmp_rows}"' EXIT

summary_count=0
while IFS= read -r summary_json; do
  summary_count="$((summary_count + 1))"
  jq -r '
    .groups[] |
    [
      .backend,
      .workload,
      (.concurrency | tostring),
      (.session_selection + (if .zipf_exponent == null then "" else "-" + (.zipf_exponent | tostring) end)),
      (.repeats | tostring),
      (.ops_per_sec.mean | tostring),
      (.ops_per_sec.min | tostring),
      (.ops_per_sec.max | tostring),
      (.ops_per_sec.stddev | tostring),
      (.p95_ms.mean | tostring),
      (.p99_ms.mean | tostring),
      (.errors | tostring)
    ] | @tsv
  ' "${summary_json}" >>"${tmp_rows}"
done < <(find "${OUT}/summaries" -maxdepth 2 -type f -name summary.json -path "*/${MATRIX_ID}-*/summary.json" 2>/dev/null | sort)

run_report_count="$(find "${OUT}/runs" -maxdepth 2 -type f -name 'report-*.json' -path "*/${MATRIX_ID}-*/report-*.json" 2>/dev/null | wc -l | awk '{print $1}')"
active_matches=""
if command -v pgrep >/dev/null 2>&1; then
  active_matches="$(pgrep -af "${MATRIX_ID}" 2>/dev/null | grep -v 'agentdb-bench-finalize-matrix.sh' || true)"
fi

{
  echo "# AgentDB Benchmark Matrix Final Summary"
  echo
  echo "- Matrix id: \`${MATRIX_ID}\`"
  echo "- Output root: \`${OUT}\`"
  echo "- Baseline numerator: \`${BASELINE_BACKEND}\`"
  echo "- Repeat summaries found: \`${summary_count}\`"
  echo "- Completed run reports: \`${run_report_count}\`"
  if [[ -f "${matrix_md}" ]]; then
    echo "- Source matrix: \`${matrix_md}\`"
  fi
  if [[ -f "${matrix_log}" ]]; then
    echo "- Matrix log: \`${matrix_log}\`"
  fi
  if [[ -n "${active_matches}" ]]; then
    echo "- Status: \`running\`"
  else
    echo "- Status: \`no matching active process\`"
  fi
  echo

  if [[ -n "${active_matches}" ]]; then
    echo "## Active Processes"
    echo
    echo '```text'
    printf '%s\n' "${active_matches}"
    echo '```'
    echo
  fi

  echo "## Aggregate Results"
  echo
  if [[ ! -s "${tmp_rows}" ]]; then
    echo "No repeat summaries found yet."
  else
    echo "| Backend | Workload | c | Selection | Repeats | ops/s mean | ops/s min-max | ops/s stddev | p95 mean | p99 mean | Errors |"
    echo "|---|---|---:|---|---:|---:|---:|---:|---:|---:|---:|"
    sort -t $'\t' -k2,2 -k1,1 "${tmp_rows}" | awk -F '\t' '
      {
        printf "| `%s` | `%s` | %s | `%s` | %s | %0.2f | %0.2f-%0.2f | %0.2f | %0.3fms | %0.3fms | %s |\n",
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
      }
    '
  fi
  echo

  echo "## Throughput Ratios"
  echo
  if [[ ! -s "${tmp_rows}" ]]; then
    echo "No ratios available yet."
  else
    echo "| Workload | Comparison | Ratio | Baseline ops/s | Other ops/s |"
    echo "|---|---|---:|---:|---:|"
    awk -F '\t' -v baseline="${BASELINE_BACKEND}" '
      {
        key=$2
        backend[key, $1]=$6
        workloads[key]=1
      }
      END {
        for (workload in workloads) {
          base=backend[workload, baseline]
          if (base == "" || base == 0) {
            continue
          }
          for (pair in backend) {
            split(pair, parts, SUBSEP)
            pair_workload=parts[1]
            other_backend=parts[2]
            if (pair_workload != workload || other_backend == baseline) {
              continue
            }
            other=backend[workload, other_backend]
            if (other == "" || other == 0) {
              continue
            }
            printf "| `%s` | `%s` / `%s` | %0.2fx | %0.2f | %0.2f |\n",
              workload, baseline, other_backend, base / other, base, other
          }
        }
      }
    ' "${tmp_rows}" | sort
  fi

  if [[ "${TAIL_LINES}" -gt 0 && -f "${matrix_log}" ]]; then
    echo
    echo "## Log Tail"
    echo
    echo '```text'
    tail -n "${TAIL_LINES}" "${matrix_log}"
    echo '```'
  fi
} >"${output_md}"

echo "final_summary=${output_md}"
