#!/usr/bin/env bash
# Testing-evidence contract rule 6: every auto acceptance script emits a
# unique positive completion marker, printed only after its assertions.
#
# Scans ops/**/tests/auto/*.sh and fails if a script:
#   - has no `PASS:` marker line at all, or
#   - shares a `PASS:` marker string with another script (a harness that
#     greps for a marker must not match the wrong scenario's success).
#
# This is a static check — it does not run the scenarios. Kept portable to
# the macOS system bash (3.2): no mapfile, no associative arrays.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

scripts="$(find ops -path '*/tests/auto/*.sh' | sort)"
if [[ -z "$scripts" ]]; then
  echo "FAIL: no auto acceptance scripts found under ops/**/tests/auto/" >&2
  exit 1
fi

fail=0
count=0
seen="$(mktemp)"
trap 'rm -f "$seen"' EXIT

while IFS= read -r script; do
  [[ -z "$script" ]] && continue
  count=$((count + 1))
  marker="$(grep -oE 'echo[[:space:]]+"PASS:[^"]*"' "$script" | head -1 \
    | sed -E 's/^echo[[:space:]]+"(PASS:[^"]*)"/\1/')"
  if [[ -z "$marker" ]]; then
    echo "FAIL: $script has no gated PASS: completion marker" >&2
    fail=1
    continue
  fi
  if grep -qF "	${marker}" "$seen" 2>/dev/null; then
    prior_script="$(grep -F "	${marker}" "$seen" | head -1 | cut -f1)"
    echo "FAIL: $script reuses PASS marker already used by ${prior_script}:" >&2
    echo "      \"$marker\"" >&2
    fail=1
    continue
  fi
  printf '%s\t%s\n' "$script" "$marker" >> "$seen"
done <<< "$scripts"

if [[ $fail -ne 0 ]]; then
  exit 1
fi
echo "PASS: ${count} auto acceptance scripts each have a unique gated PASS marker"
