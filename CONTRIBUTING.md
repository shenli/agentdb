# Contributing To AgentDB

AgentDB is alpha software. Contributions that improve correctness,
documentation, developer experience, tests, and benchmark reproducibility are
welcome.

## Development Setup

Required tools:

- Rust toolchain from `rust-toolchain.toml`
- Python 3.9+
- Node.js 20+
- `cargo-deny` for full dependency license audits:
  `cargo install cargo-deny`

Start the dev server:

```bash
cargo run -p agentdb-server -- dev
```

Run the main local checks:

```bash
cargo fmt --check
cargo clippy --workspace --tests -- -D warnings
cargo test --workspace --locked
python3 -m pytest -q clients/python/tests
(cd clients/typescript && npm test && npm run build)
scripts/license-check.sh
```

For a fuller local CI-style run:

```bash
ops/local-ci/scripts/local-ci.sh
```

## Pull Requests

- Keep PRs focused and explain the user-visible behavior change.
- Include tests for behavior changes when practical.
- Update docs when changing public APIs, SDK behavior, deployment behavior, or
  benchmark methodology.
- Do not include machine-local paths, private hostnames, API keys, secrets, or
  private benchmark run IDs in public docs.
- For benchmark claims, include workload shape, durability profile, hardware,
  database versions, and whether the result is single-node or replicated.
- Keep AgentDB-owned package metadata MIT-only. Dependency licenses remain their
  own licenses; see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
