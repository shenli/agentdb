# Release Checklist

Use this checklist before making the repository public or cutting a release.
It is intentionally biased toward commands that can run from a clean checkout.

## Public Surface

- Confirm the README quickstart reaches a stored agent session in a few minutes:

  ```bash
  cargo run -p agentdb-server -- dev
  ```

  In another terminal:

  ```bash
  pip install -e clients/python
  AGENTDB_URL=http://127.0.0.1:7878 python3 examples/agents/python/quickstart.py
  ```

- Scan for private hostnames, local paths, private run IDs, and old planning
  archive references. Replace the placeholder terms with any maintainer-local
  names that should never appear in public docs:

  ```bash
  rg -n 'PRIVATE_HOST|PRIVATE_HOME_PATH|PRIVATE_RUN_ID|PRIVATE_ARCHIVE_PATH' \
    README.md docs CONTRIBUTING.md SECURITY.md CODE_OF_CONDUCT.md THIRD_PARTY_NOTICES.md \
    .github scripts infra ops tools clients adapters examples bin crates AGENTS.md CLAUDE.md LLMS.md \
    -g '!target/**' -g '!.git/**' -g '!.devroom/**'
  ```

- Check for stale command paths after repository layout changes:

  ```bash
  rg -n '(^|[[:space:]`(])scripts/(agentdb-bench-|local-ci|real-cluster-smoke|smoke|perf_|aws/)|ops/systemd' \
    README.md docs examples .github CONTRIBUTING.md bin infra ops tools \
    -g '!docs/release-checklist.md'
  ```

## Required Validation

Run these from a clean checkout:

```bash
cargo fmt --check
cargo check -q -p agentdb-server
cargo test -q -p agentdb-api --test api_smoke create_tenant_agent_session_append_fetch_round_trip
python3 -m pytest -q examples/agents/python/test_session_first_demo.py \
  examples/agents/python/test_session_tiering_demo.py \
  examples/agents/python/test_ticket_agent_tiered_demo.py
python3 -m py_compile examples/agents/python/quickstart.py \
  examples/agents/python/openai_agents_quickstart.py \
  examples/agents/python/async_quickstart.py \
  examples/agents/python/pubsub_quickstart.py \
  examples/agents/python/session_first_demo.py \
  examples/agents/python/session_tiering_demo.py
scripts/license-check.sh
git diff --check
```

For a fuller local CI run:

```bash
ops/local-ci/scripts/local-ci.sh
```

## License And Notices

- AgentDB-owned package metadata must remain MIT-only.
- Run `scripts/license-check.sh`.
- For release validation, install `cargo-deny` and run:

  ```bash
  LICENSE_CHECK_STRICT=1 scripts/license-check.sh
  ```

- Review [../THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md) before
  distributing source archives, binaries, containers, SDK packages, or hosted
  artifacts that bundle dependencies.

## Packaging

- Verify Docker build inputs after docs/scripts moves:

  ```bash
  docker build -t agentdb:local .
  ```

- Verify release workflows and tag naming in `.github/workflows/release.yml`.
- Confirm generated artifacts are ignored and not staged:

  ```bash
  git status --short
  git status --ignored --short
  ```

## Benchmark Claims

- Keep the README benchmark framed as a single-node, localhost,
  `single-node-wal` comparison unless rerun under a different configuration.
- For new benchmark numbers, document hardware, OS/kernel, database versions,
  durability profile, working-set shape, workload mix, concurrency, warmup,
  measured duration, repeats, and caveats.
- Do not publish benchmark ratios without the detailed methodology in
  [operations/AGENTDB_BENCH.md](operations/AGENTDB_BENCH.md) and
  [../tools/agentdb-bench/README.md](../tools/agentdb-bench/README.md).
