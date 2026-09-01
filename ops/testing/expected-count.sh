#!/usr/bin/env bash
# Testing-evidence contract rule 1: a filtered test run declares how many
# tests it expects to select, and zero selected is a hard failure.
#
# Usage:
#   ops/testing/expected-count.sh <expected-min> -- <cargo test args...>
#
# Example:
#   ops/testing/expected-count.sh 5 -- test -p agentdb-meta store_admission
#
# Runs the given cargo invocation, sums the tests actually executed across
# every `test result:` block, and fails if the total is 0 or below
# <expected-min>. Prints the rule-8 evidence block (command, commit,
# selected count, result) so a quantified claim can cite it.

set -uo pipefail

if [[ $# -lt 3 || "$2" != "--" ]]; then
  echo "usage: $0 <expected-min> -- <cargo args...>" >&2
  exit 2
fi

expected_min="$1"
shift 2

if ! [[ "$expected_min" =~ ^[0-9]+$ ]]; then
  echo "expected-min must be a non-negative integer, got: $expected_min" >&2
  exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
commit="$(git -C "$ROOT" rev-parse --short HEAD 2>/dev/null || echo unknown)"
tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT

echo "expected-count: running: cargo $* (expecting >= ${expected_min} tests)"
cargo "$@" 2>&1 | tee "$tmp"
status=${PIPESTATUS[0]}

# Sum "N passed" across every "test result:" line (a run has several: unit,
# each integration binary, doctests). This counts tests actually executed.
selected="$(grep -oE 'test result: (ok|FAILED)\. [0-9]+ passed' "$tmp" \
  | grep -oE '[0-9]+ passed' | grep -oE '^[0-9]+' \
  | awk '{sum += $1} END {print sum + 0}')"
selected="${selected:-0}"

echo
echo "----- testing-evidence (rule 8) -----"
echo "command:  cargo $*"
echo "commit:   ${commit}"
echo "selected: ${selected}"
echo "result:   $([[ $status -eq 0 ]] && echo ok || echo FAILED) (cargo exit ${status})"
echo "-------------------------------------"

if [[ $status -ne 0 ]]; then
  echo "FAIL: cargo reported a test failure" >&2
  exit 1
fi
if [[ "$selected" -eq 0 ]]; then
  echo "FAIL: zero tests were selected — a filter that selects nothing is not a pass" >&2
  exit 1
fi
if [[ "$selected" -lt "$expected_min" ]]; then
  echo "FAIL: selected ${selected} tests, fewer than the declared minimum ${expected_min}" >&2
  exit 1
fi
echo "PASS: selected ${selected} tests (>= ${expected_min})"
