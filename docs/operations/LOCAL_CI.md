# Local CI

GitHub CI is manual-only to control cost. The default validation path is a local
CI runner, usually on `li-dev`, backed by `scripts/local-ci.sh`.

## Run Profiles

Full local CI:

```bash
scripts/local-ci.sh
```

This runs formatting, clippy, all Rust tests, Python and TypeScript SDK tests,
and the Compose-backed real-cluster failover smoke.

Fast Rust-only validation:

```bash
scripts/local-ci.sh --cargo-only
```

Cluster-only validation:

```bash
scripts/local-ci.sh --cluster-only
```

Nightly stability validation:

```bash
scripts/local-ci-nightly.sh
```

The nightly profile runs the full local CI plus bounded random cluster chaos:
one node is killed and restarted at a time while the harness keeps appending and
verifying session events. This preserves quorum, but exercises leader failover,
follower loss/rejoin, restart recovery, and catch-up.

Useful chaos options:

```bash
scripts/local-ci.sh --cluster-only --chaos --chaos-iterations 48
scripts/local-ci.sh --cluster-only --chaos --chaos-seed 12345
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

Check the last completed run:

```bash
scripts/local-ci-status.sh --logs
```

Serve the dashboard:

```bash
scripts/local-ci-serve.sh
```

Then open:

```text
http://127.0.0.1:8099
```

On a remote dev box, use SSH forwarding:

```bash
ssh -L 8099:127.0.0.1:8099 li-dev
```

## li-dev Systemd User Timer

Install the user units:

```bash
mkdir -p ~/.config/systemd/user
cp ops/systemd/user/agentdb-local-ci-*.service ~/.config/systemd/user/
cp ops/systemd/user/agentdb-local-ci-*.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now agentdb-local-ci-dashboard.service
systemctl --user enable --now agentdb-local-ci-nightly.timer
```

The timer runs nightly at 02:30 with up to 20 minutes of randomized delay. It
stores results in:

```bash
~/agentdb-ci-runs
```

Inspect status:

```bash
systemctl --user status agentdb-local-ci-nightly.service
systemctl --user list-timers agentdb-local-ci-nightly.timer
scripts/local-ci-status.sh --root ~/agentdb-ci-runs --logs
```

Run immediately:

```bash
systemctl --user start agentdb-local-ci-nightly.service
```

## Dirty Working Trees

`scripts/local-ci-nightly.sh` runs `git pull --ff-only` before testing and
refuses to pull if the checkout is dirty. Use a clean checkout for scheduled CI.
For one-off testing against local changes, run `scripts/local-ci.sh` directly.
