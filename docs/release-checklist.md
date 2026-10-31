# Release Checklist

Use this checklist before making the repository public or cutting a release.
It is intentionally biased toward commands that can run from a clean checkout.

## Public Surface

- Confirm the README quickstart reaches an agent session after the initial build:

  ```bash
  cargo run -p agentdb-server -- dev
  ```

  In another terminal:

  ```bash
  pip install -e clients/python
  AGENTDB_URL=http://127.0.0.1:7878 python3 examples/agents/python/quickstart.py
  ```

- Keep the README alpha boundary explicit: local/single-node evaluation, mutable
  API/storage formats, and experimental distributed operation. Do not advertise
  Cell-level fault isolation or stable cluster support.
- Run `python3 scripts/check-secrets.py` with Gitleaks 8.30.1. It scans all local
  Git refs and a snapshot of tracked/unignored working files, with redacted output.
  Fetch any refs intended for publication before this check. Review any finding;
  exceptions must name exact demonstration values or test vectors, never whole
  directories. A pattern scan is not proof that no private data exists.
- Review historical Git content before changing repository visibility. Removing
  a file from the current tree does not remove it from history. If a real secret
  is found, revoke it before coordinating any history rewrite. The checked-in
  Gitleaks exceptions cover documentation placeholders and public test fixtures.
- Generated `test-sessions/` logs are ignored and excluded from Docker builds.
  Keep raw logs outside Git; publish reviewed summaries with commands, commit,
  assertions, and coverage limits instead.
- Confirm [private vulnerability reporting](https://github.com/shenli/agentdb/security/advisories/new)
  is enabled before making the repository public.

- Check for stale command paths after repository layout changes:

  ```bash
  rg -n '(^|[[:space:]`(])scripts/(agentdb-bench-|local-ci|real-cluster-smoke|smoke|perf_|aws/)|ops/systemd' \
    README.md docs examples .github CONTRIBUTING.md bin infra ops tools \
    -g '!docs/release-checklist.md'
  ```

## Required Validation

Run these from a clean checkout:

```bash
cargo fmt --all --check
cargo check -q -p agentdb-server
ops/testing/expected-count.sh 1 -- test -q -p agentdb-api --test api_smoke create_tenant_agent_session_append_fetch_round_trip
python3 -m pytest -q examples/agents/python/test_session_first_demo.py \
  examples/agents/python/test_session_tiering_demo.py \
  examples/agents/python/test_ticket_agent_tiered_demo.py
python3 -m py_compile examples/agents/python/quickstart.py \
  examples/agents/python/openai_agents_quickstart.py \
  examples/agents/python/async_quickstart.py \
  examples/agents/python/session_first_demo.py \
  examples/agents/python/session_tiering_demo.py
LICENSE_CHECK_STRICT=1 scripts/license-check.sh
python3 scripts/check-release-inputs.py
actionlint -shellcheck=
python3 scripts/check-secrets.py
cargo build --locked -p agentdb-server -p agentdbctl
python3 scripts/release-smoke.py
git diff --check
```

The recovery smoke uses synthetic data in a temporary directory. It appends
through the native Python SDK, kills the server, reopens the same store, and
checks exact replay, artifact bytes, idempotent retry, and fork isolation. This
is a process-crash test, not a host power-loss or cluster-availability test.

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
  docker build --target deps -t agentdb:deps-check .
  docker build -t agentdb:local .
  docker run --rm agentdb:local --version
  docker run --rm --entrypoint /usr/local/bin/agentdbctl agentdb:local --version
  python3 scripts/docker-smoke.py --image agentdb:local
  ```

  The manifest-only dependency stage must include every workspace member so a
  clean cache build succeeds. The final image must contain both `agentdb` and
  `agentdbctl`; non-loopback startup also requires
  `AGENTDB_API_AUTH_TOKEN` at runtime.
  The smoke script uses a generated token and disposable named volume, checks
  an acknowledged event after container restart, and removes its own resources.

- Verify release workflows and tag naming in `.github/workflows/release.yml`.
  For an archive matching the validation host, verify its separate checksum,
  extract it, confirm both files are executable, and run both version commands:

  ```bash
  shasum -a 256 -c agentdb-vX.Y.Z-PLATFORM.tar.gz.sha256
  tar -xzf agentdb-vX.Y.Z-PLATFORM.tar.gz
  test -x agentdb-vX.Y.Z-PLATFORM/agentdb
  test -x agentdb-vX.Y.Z-PLATFORM/agentdbctl
  agentdb-vX.Y.Z-PLATFORM/agentdb --version
  agentdb-vX.Y.Z-PLATFORM/agentdbctl --version
  ```

- Tagged workflows create draft prereleases. Validate all matrix jobs and the
  downloaded archives before publishing. The finalizer must fail when any build
  fails. The Intel macOS target uses GitHub's `macos-15-intel` runner.
- Confirm generated artifacts are ignored and not staged:

  ```bash
  git status --short
  git status --ignored --short
  ```

## Benchmark Claims

- State the actual durability and changefeed settings. Persistent RocksDB writes
  fsync by default; `AGENTDB_ROCKSDB_SYNC_WRITES=0` explicitly opts out. The
  changefeed/export ledger is disabled unless `--changefeed` is enabled.
- Do not treat results from different harnesses as a head-to-head comparison.
  Historical 8-second, sync-disabled measurements are not default throughput
  or sustained-production SLOs.
- For new benchmark numbers, document hardware, OS/kernel, database versions,
  durability profile, working-set shape, workload mix, concurrency, warmup,
  measured duration, repeats, and caveats.
- Do not publish benchmark ratios without the detailed methodology in
  [operations/agentdb-bench.md](operations/agentdb-bench.md) and
  [../tools/agentdb-bench/README.md](../tools/agentdb-bench/README.md).
