# Local CI

GitHub CI runs bounded Linux checks on pull requests and pushes to `main`,
with a manual dispatch option. It checks formatting, clippy, Rust and SDK tests,
the persistent release recovery journey, Docker workspace inputs, workflow
syntax, strict dependency license policy, and secret history. Distributed chaos
and soak tests remain on the local/nightly runner:
`ops/local-ci/scripts/local-ci.sh`.

## Run Profiles

Full local CI:

```bash
ops/local-ci/scripts/local-ci.sh
```

This runs formatting, clippy, all Rust tests, Python and TypeScript SDK tests,
and the Compose-backed real-cluster failover smoke.

Fast Rust-only validation:

```bash
ops/local-ci/scripts/local-ci.sh --cargo-only
```

Cluster-only validation:

```bash
ops/local-ci/scripts/local-ci.sh --cluster-only
```

Nightly stability validation:

```bash
ops/local-ci/scripts/local-ci-nightly.sh
```

The nightly profile runs the full local CI plus deterministic cluster hardening
and bounded random cluster chaos. The hardening stage covers persisted process
restart, checkpoint install, explicit replacement, one-AZ loss, rolling restart,
and split/merge metadata cutover. The chaos stage kills and restarts one node at
a time while the harness keeps appending and verifying session events. This
preserves quorum, but exercises leader failover, follower loss/rejoin, restart
recovery, and catch-up.

Useful chaos options:

```bash
ops/local-ci/scripts/local-ci.sh --cluster-only --chaos --chaos-iterations 48
ops/local-ci/scripts/local-ci.sh --cluster-only --chaos --chaos-seed 12345
ops/local-ci/scripts/local-ci.sh --cluster-only --hardening
```

## Results

Runs are written under:

```bash
${LOCAL_CI_RUN_ROOT:-/tmp/agentdb-local-ci}/runs/<run-id>
```

Each run contains:

- `status.env`: machine-readable status, exit code, branch, commit, duration,
  and last stage.
- `report.html`: static human-readable report with per-stage log links and log
  tails.
- `*.log`: one log file per stage.

The run root also contains:

- `summary.html`: latest run, last successful run, last failed run, and recent
  run history.
- `summary.env`: machine-readable paths for latest, last successful, and last
  failed run.
- `index.html`: dashboard entrypoint.

Check the last completed run:

```bash
ops/local-ci/scripts/local-ci-status.sh --logs
```

Check the stability summary:

```bash
ops/local-ci/scripts/local-ci-status.sh --summary
```

Serve the dashboard:

```bash
ops/local-ci/scripts/local-ci-serve.sh
```

Then open:

```text
http://127.0.0.1:8099
```

The dashboard homepage links to `summary.html`, the latest run report, and the
currently running report.

On a remote dev box, use SSH forwarding:

```bash
ssh -L 8099:127.0.0.1:8099 <host>
```

## Systemd User Timer

Install the user units:

```bash
mkdir -p ~/work
test -d ~/work/agentdb-nightly || git clone https://github.com/shenli/agentdb.git ~/work/agentdb-nightly
mkdir -p ~/.config/systemd/user
cp ops/local-ci/systemd/user/agentdb-local-ci-*.service ~/.config/systemd/user/
cp ops/local-ci/systemd/user/agentdb-local-ci-*.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now agentdb-local-ci-dashboard.service
systemctl --user enable --now agentdb-local-ci-nightly.timer
```

The timer runs nightly at 02:30 with up to 20 minutes of randomized delay. It
uses the separate clean checkout at `~/work/agentdb-nightly` and stores results
in:

```bash
~/agentdb-ci-runs
```

Inspect status:

```bash
systemctl --user status agentdb-local-ci-nightly.service
systemctl --user list-timers agentdb-local-ci-nightly.timer
ops/local-ci/scripts/local-ci-status.sh --root ~/agentdb-ci-runs --logs
ops/local-ci/scripts/local-ci-status.sh --root ~/agentdb-ci-runs --summary
```

Run immediately:

```bash
systemctl --user start agentdb-local-ci-nightly.service
```

## Distributed Test Plan Gates

The distributed test plan gates run from the same clean
`~/work/agentdb-nightly` checkout. Install the user timer on your Linux CI host:

```bash
mkdir -p ~/work
test -d ~/work/agentdb-nightly || git clone https://github.com/shenli/agentdb.git ~/work/agentdb-nightly
mkdir -p ~/.config/systemd/user
cp ops/local-ci/systemd/user/agentdb-distributed-tests-nightly.service ~/.config/systemd/user/
cp ops/local-ci/systemd/user/agentdb-distributed-tests-nightly.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now agentdb-distributed-tests-nightly.timer
```

The distributed timer runs nightly at 02:17 with up to 20 minutes of randomized
delay. It stores Markdown reports, status, logs, and per-scenario artifacts in:

```bash
~/agentdb-distributed-test-runs
```

Inspect the latest distributed run:

```bash
systemctl --user status agentdb-distributed-tests-nightly.service
systemctl --user list-timers agentdb-distributed-tests-nightly.timer
cat ~/agentdb-distributed-test-runs/latest/status.env
cat ~/agentdb-distributed-test-runs/latest/report.md
find ~/agentdb-distributed-test-runs/latest/logs -maxdepth 1 -type f -print | sort
```

Run the distributed profile immediately:

```bash
systemctl --user start agentdb-distributed-tests-nightly.service
```

For a one-off quick check from the repository checkout:

```bash
AGENTDB_DISTRIBUTED_RUN_ROOT=~/agentdb-distributed-test-runs \
  ops/local-ci/scripts/distributed-test-runner.sh --profile quick --no-pull
```

## Retention

`ops/local-ci/scripts/local-ci.sh` prunes old run directories after each run. The default is
to keep the newest 30 runs. Override it with:

```bash
LOCAL_CI_KEEP_RUNS=60 ops/local-ci/scripts/local-ci-nightly.sh
```

Manual cleanup:

```bash
ops/local-ci/scripts/local-ci-prune.sh --root ~/agentdb-ci-runs --keep 30 --dry-run
ops/local-ci/scripts/local-ci-prune.sh --root ~/agentdb-ci-runs --keep 30
```

## Dirty Working Trees

`ops/local-ci/scripts/local-ci-nightly.sh` runs `git pull --ff-only` before testing and
refuses to pull if the checkout is dirty. Use a clean checkout for scheduled CI.
For one-off testing against local changes, run `ops/local-ci/scripts/local-ci.sh` directly.

Generated test logs and machine-specific diagnostics belong outside source control.
The `test-sessions/` fallback is ignored; use `AGENTDB_DISTRIBUTED_RUN_ROOT`
for retained evidence. Share a reviewed summary with command, commit, result,
and coverage limits rather than committing raw logs.
