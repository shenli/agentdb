#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STRICT="${LICENSE_CHECK_STRICT:-0}"
EXIT_CODE=0

fail() {
  echo "error: $*" >&2
  EXIT_CODE=1
}

warn() {
  echo "warning: $*" >&2
}

check_agentdb_metadata() {
  echo "==> checking AgentDB-owned package metadata"
  if ! command -v rg >/dev/null 2>&1; then
    fail "ripgrep is required for package metadata checks"
    return
  fi

  local files=(
    "Cargo.toml"
    "clients/python/pyproject.toml"
    "clients/typescript/package.json"
    "adapters/mcp-agentdb/pyproject.toml"
    "adapters/langgraph-checkpoint-agentdb/pyproject.toml"
    "adapters/openai-agents-agentdb/pyproject.toml"
    "adapters/vercel-ai-sdk-agentdb-template/package.json"
  )

  local file
  for file in "${files[@]}"; do
    if [[ ! -f "$ROOT/$file" ]]; then
      fail "missing metadata file: $file"
      continue
    fi
  done

  local matches
  if matches="$(rg -n 'MIT OR Apache|Apache-2\.0|Apache Software License' \
    "${files[@]/#/$ROOT/}" 2>/dev/null)"
  then
    echo "$matches" >&2
    fail "AgentDB-owned package metadata must remain MIT-only"
  fi
}

check_rust_licenses() {
  echo "==> checking Rust dependency licenses"
  if cargo deny --version >/dev/null 2>&1; then
    (cd "$ROOT" && cargo deny --config deny.toml check licenses sources)
    return
  fi

  if [[ "$STRICT" == "1" ]]; then
    fail "cargo-deny is required when LICENSE_CHECK_STRICT=1; install with: cargo install cargo-deny"
  else
    warn "cargo-deny is not installed; skipped transitive Rust license audit"
    warn "install with: cargo install cargo-deny"
  fi
}

check_npm_lock_licenses() {
  echo "==> checking npm lockfile licenses"
  if ! command -v node >/dev/null 2>&1; then
    warn "node is not installed; skipped npm lockfile license scan"
    return
  fi

  node "$ROOT/scripts/license-check-npm-lock.mjs"
}

check_agentdb_metadata
check_rust_licenses
check_npm_lock_licenses

if [[ "$EXIT_CODE" != "0" ]]; then
  exit "$EXIT_CODE"
fi

echo "license check passed"
