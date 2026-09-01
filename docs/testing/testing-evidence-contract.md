# Testing-Evidence Contract

A short, enforceable contract for AgentDB correctness tests. It exists so a
green result *means* something: that a specific production path was
exercised and a specific invariant held. It implements
[`distributed-safety-hardening.md`](../design/distributed-safety-hardening.md)
§6.

The eight rules below are the contract. Two are mechanically enforced (see
**Enforcement**); the rest are review criteria and authoring practice.

## The rules

1. **Declare the expected selected-test count.** A filtered test run states
   how many tests it expects to select *before* it runs, and fails if zero
   are selected. A filter typo that silently selects nothing is the most
   common false green. Enforced by `ops/testing/expected-count.sh`.

2. **A mutation experiment confirms the mutation landed and was traversed.**
   When a test claims to catch a bug, temporarily reintroduce the bug and
   confirm the test goes red for the stated reason — otherwise the test may
   be passing for an unrelated reason. Record the mutation and the red
   assertion.

3. **A red result identifies the assertion or invariant that fired**, not
   only a non-zero exit code. `assert!`/`bail!` messages name the property
   that broke and, where useful, the observed vs expected values.

4. **Cross-thread and async probes declare an observation window and await
   a durable effect.** A detached task whose panic no one awaits is not
   evidence. Poll a durable effect with a bounded deadline (as the smoke
   scenarios do), or `await` the task handle.

5. **Fixtures do not re-implement the production rule they verify.** The
   assertion consumes output from the real producer. A fixture that
   recomputes the expected value with the same logic under test proves
   nothing.

6. **Acceptance scripts emit a unique positive completion marker reachable
   only after every required assertion.** Every `ops/**/tests/auto/*.sh`
   ends with a distinctive `PASS:` line that the harness greps for, printed
   only after all checks passed; failures `exit 1` before it. Enforced by
   `ops/testing/check-acceptance-markers.sh`.

7. **Every green report states what it did not cover.** A passing run names
   the modalities, seeds, or paths it did *not* exercise, so silence is not
   mistaken for coverage. Smoke scripts that bound coverage (top-N,
   no-retry, sampling) `echo` what they dropped.

8. **Quantified claims record command, commit, selected count, result, and
   raw evidence.** A claim like "S8 passes with the statement budget at 59k"
   carries the exact command, the commit, the count, the result line, and a
   pointer to the raw log — never a bare adjective.

## Enforcement

Two rules are checked by tooling so they cannot silently rot:

- **Rule 1** — `ops/testing/expected-count.sh <expected-min> -- <cargo test args>`
  runs the tests, parses the `running N tests` / `test result:` lines, and
  fails if the total selected is `0` or below `<expected-min>`. Use it in CI
  or locally to guard a filtered run: a green from zero selected tests is a
  hard failure. It also prints the rule-8 evidence block (command, count,
  result).

- **Rule 6** — `ops/testing/check-acceptance-markers.sh` scans every
  `ops/**/tests/auto/*.sh` and fails if a script has no gated `PASS:` marker,
  or reuses a `PASS:` string another script already uses (markers must be
  unique so a harness cannot match the wrong scenario's success).

Both are cheap and run in CI (`.github/workflows/ci.yml`).

## Random chaos vs deterministic regressions

Random chaos complements deterministic tests; it does not replace them. A
failure first seen under random chaos is converted into a fixed-seed
regression before the finding is considered closed — the regression is the
durable evidence, the chaos run is how it was discovered.
