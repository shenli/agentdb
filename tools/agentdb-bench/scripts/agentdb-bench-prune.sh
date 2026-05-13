#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${AGENTDB_BENCH_PRUNE_ROOT:-bench-out}"
KEEP="${AGENTDB_BENCH_PRUNE_KEEP:-20}"
TARGET_FREE_GB="${AGENTDB_BENCH_PRUNE_TARGET_FREE_GB:-0}"
DELETE=0
USE_SUDO=0
EXCLUDE_NAMES=(logs matrix runs summaries)

usage() {
  cat <<'EOF'
Usage: tools/agentdb-bench/scripts/agentdb-bench-prune.sh [OPTIONS]

Prunes benchmark output directories under a root. The script only considers
direct child directories of --root and defaults to dry-run mode.

Options:
  --root DIR             Benchmark output root. Default: AGENTDB_BENCH_PRUNE_ROOT or bench-out.
  --keep N               Keep the newest N child directories. Default: AGENTDB_BENCH_PRUNE_KEEP or 20.
  --target-free-gb N     Also prune oldest deletable dirs until the filesystem has at least N GiB free.
                         Default: AGENTDB_BENCH_PRUNE_TARGET_FREE_GB or 0, disabled.
  --delete               Actually delete selected directories. Without this, prints a dry-run plan.
  --dry-run              Explicit dry-run mode.
  --sudo                 Use passwordless sudo for sizing and deletion. Useful for Docker-owned DB data.
  --exclude NAME         Exclude a direct child directory name. Can be repeated.
                         Defaults: logs, matrix, runs, summaries.
  --help                 Show this help.

Examples:
  tools/agentdb-bench/scripts/agentdb-bench-prune.sh --root bench-out --keep 30
  tools/agentdb-bench/scripts/agentdb-bench-prune.sh --root bench-out --keep 30 --target-free-gb 120 --delete
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root)
      shift
      ROOT_DIR="${1:-}"
      [[ -n "${ROOT_DIR}" ]] || { echo "--root requires a value" >&2; exit 2; }
      ;;
    --keep)
      shift
      KEEP="${1:-}"
      [[ -n "${KEEP}" ]] || { echo "--keep requires a value" >&2; exit 2; }
      ;;
    --target-free-gb)
      shift
      TARGET_FREE_GB="${1:-}"
      [[ -n "${TARGET_FREE_GB}" ]] || { echo "--target-free-gb requires a value" >&2; exit 2; }
      ;;
    --delete)
      DELETE=1
      ;;
    --dry-run)
      DELETE=0
      ;;
    --sudo)
      USE_SUDO=1
      ;;
    --exclude)
      shift
      exclude_name="${1:-}"
      [[ -n "${exclude_name}" ]] || { echo "--exclude requires a value" >&2; exit 2; }
      EXCLUDE_NAMES+=("${exclude_name}")
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

if ! [[ "${KEEP}" =~ ^[0-9]+$ ]]; then
  echo "--keep must be a non-negative integer" >&2
  exit 2
fi
if ! [[ "${TARGET_FREE_GB}" =~ ^[0-9]+$ ]]; then
  echo "--target-free-gb must be a non-negative integer" >&2
  exit 2
fi
if [[ ! -d "${ROOT_DIR}" ]]; then
  echo "benchmark root does not exist: ${ROOT_DIR}" >&2
  exit 0
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required for portable directory mtime sorting" >&2
  exit 2
fi
if ((USE_SUDO == 1)) && ! sudo -n true >/dev/null 2>&1; then
  echo "--sudo requires passwordless sudo for this shell" >&2
  exit 2
fi

root_abs="$(cd "${ROOT_DIR}" && pwd -P)"
if [[ "${root_abs}" == "/" || "${root_abs}" == "${HOME}" ]]; then
  echo "refusing to prune unsafe root: ${root_abs}" >&2
  exit 2
fi

free_kb() {
  df -Pk "${root_abs}" | awk 'NR == 2 {print $4}'
}

du_kb() {
  if ((USE_SUDO == 1)); then
    { sudo -n du -sk -- "$1" 2>/dev/null || true; } | awk 'NR == 1 {print $1}'
  else
    { du -sk -- "$1" 2>/dev/null || true; } | awk 'NR == 1 {print $1}'
  fi
}

du_human() {
  if ((USE_SUDO == 1)); then
    { sudo -n du -sh -- "$1" 2>/dev/null || true; } | awk 'NR == 1 {print $1}'
  else
    { du -sh -- "$1" 2>/dev/null || true; } | awk 'NR == 1 {print $1}'
  fi
}

delete_path() {
  if ((USE_SUDO == 1)); then
    sudo -n rm -rf -- "$1"
  else
    rm -rf -- "$1"
  fi
}

target_free_kb="$((TARGET_FREE_GB * 1024 * 1024))"
current_free_kb="$(free_kb)"
size_errors=0

entries=()
while IFS= read -r line; do
  entries+=("${line}")
done < <(python3 - "${root_abs}" "${EXCLUDE_NAMES[@]}" <<'PY'
import os
import sys

root = sys.argv[1]
excluded = set(sys.argv[2:])
entries = []
with os.scandir(root) as it:
    for entry in it:
        try:
            if entry.name in excluded:
                continue
            if not entry.is_dir(follow_symlinks=False):
                continue
            stat = entry.stat(follow_symlinks=False)
        except FileNotFoundError:
            continue
        entries.append((stat.st_mtime_ns, entry.path))

for mtime_ns, path in sorted(entries):
    print(f"{mtime_ns}\t{path}")
PY
)

total="${#entries[@]}"
if ((total == 0)); then
  echo "no child directories under ${root_abs}"
  exit 0
fi

delete_paths=()

path_selected() {
  local needle="$1"
  local candidate
  for candidate in "${delete_paths[@]}"; do
    if [[ "${candidate}" == "${needle}" ]]; then
      return 0
    fi
  done
  return 1
}

delete_by_keep=$((total - KEEP))
if ((delete_by_keep > 0)); then
  for ((idx = 0; idx < delete_by_keep; idx++)); do
    path="${entries[$idx]#*$'\t'}"
    delete_paths+=("${path}")
  done
fi

projected_free_kb="${current_free_kb}"
for path in "${delete_paths[@]}"; do
  size_kb="$(du_kb "${path}")"
  if [[ -z "${size_kb}" ]]; then
    echo "warning: cannot size ${path}; rerun with --sudo if it contains root-owned Docker data" >&2
    size_kb=0
    size_errors=1
  fi
  projected_free_kb="$((projected_free_kb + size_kb))"
done

if ((target_free_kb > 0 && projected_free_kb < target_free_kb)); then
  for entry in "${entries[@]}"; do
    path="${entry#*$'\t'}"
    path_selected "${path}" && continue
    # Never prune into the newest KEEP entries while satisfying target-free;
    # operators can lower --keep explicitly if they want more aggressive cleanup.
    remaining_if_delete="$((total - ${#delete_paths[@]} - 1))"
    if ((remaining_if_delete < KEEP)); then
      break
    fi
    delete_paths+=("${path}")
    size_kb="$(du_kb "${path}")"
    if [[ -z "${size_kb}" ]]; then
      echo "warning: cannot size ${path}; rerun with --sudo if it contains root-owned Docker data" >&2
      size_kb=0
      size_errors=1
    fi
    projected_free_kb="$((projected_free_kb + size_kb))"
    if ((projected_free_kb >= target_free_kb)); then
      break
    fi
  done
fi

mode="dry-run"
if ((DELETE == 1)); then
  mode="delete"
fi

echo "benchmark prune mode=${mode}"
echo "root=${root_abs}"
echo "children=${total} keep=${KEEP}"
echo "excluded_names=${EXCLUDE_NAMES[*]}"
echo "current_free_gb=$((current_free_kb / 1024 / 1024)) target_free_gb=${TARGET_FREE_GB} projected_free_gb=$((projected_free_kb / 1024 / 1024))"
if ((USE_SUDO == 1)); then
  echo "sudo=enabled"
fi
if ((size_errors == 1)); then
  echo "warning: projected free space is a lower bound because some directories could not be sized" >&2
fi

if ((${#delete_paths[@]} == 0)); then
  echo "nothing to prune"
  exit 0
fi

failed_deletes=()
for path in "${delete_paths[@]}"; do
  size="$(du_human "${path}")"
  [[ -n "${size}" ]] || size="unknown-size"
  if ((DELETE == 1)); then
    if delete_path "${path}"; then
      echo "deleted ${size} ${path}"
    else
      failed_deletes+=("${path}")
      echo "failed ${size} ${path}" >&2
    fi
  else
    echo "would delete ${size} ${path}"
  fi
done

if ((DELETE == 1 && ${#failed_deletes[@]} > 0)); then
  echo "failed_deletes=${#failed_deletes[@]}" >&2
  echo "Some directories were not deleted. If they contain Docker-owned database data, rerun with --sudo after enabling passwordless sudo for this command." >&2
  exit 1
fi
