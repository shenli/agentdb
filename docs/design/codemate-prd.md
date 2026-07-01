# CodeMate PRD

## Summary

Build **CodeMate**, a third-party, local-first web app that imports coding-agent
session history into AgentDB, then helps developers review work, find recurring
issues, and distill engineering best practices from real agent sessions.

This should not be built as a core AgentDB feature. AgentDB provides the local
runtime database: sessions, events, artifacts, records, replay, and fork.
CodeMate is the developer-facing app on top.

The product is not a PR-summary generator. Codex, Claude Code, or another agent
can already write a plausible summary from the current workspace. CodeMate's
value is that it imports and preserves the recorded activity first: messages,
tool calls, command results, failures, artifacts, file-change checkpoints, human
corrections, reset boundaries, replay points, and fork points.

## Problem

AI coding agents make it cheap to produce code, but they also create new review
and learning problems:

- PR summaries can claim tests passed without showing the command or output.
- Failed attempts and human corrections disappear from the final narrative.
- The same agent mistakes repeat across sessions because feedback is not
  distilled into reusable guidance.
- Teams do not have a lightweight way to identify common issues across local
  agent runs.
- Best practices stay trapped in individual chat transcripts instead of becoming
  repo guidance, skills, checklists, or review rules.

## Product Shape

The first product should be a separate local web app:

```bash
npx codemate
```

CodeMate runs on localhost and connects to a local AgentDB server. It can offer
a one-command dev mode that starts AgentDB if needed, but the app remains
separate from the AgentDB server and repository.

Core idea:

1. User works normally in Codex, Claude Code, or a terminal.
2. User opens CodeMate.
3. CodeMate reads local session files on demand.
4. User selects a repo, session, and review window.
5. CodeMate imports selected session activity into AgentDB.
6. CodeMate can generate:
   - a PR review packet
   - a session summary
   - a recurring-issues report
   - a best-practice suggestion
7. User copies, saves, or optionally publishes selected outputs.

Pull-based import is the MVP. Real-time MCP/hooks streaming can come later for
live dashboards, policy checks, or cross-agent coordination.

## Goal

Developers can use CodeMate locally without changing how they use Codex or
Claude Code.

The first release should let a developer:

- launch a local web UI
- detect local Codex and Claude Code session files
- browse recent sessions by repo, branch, task, agent host, and time
- inspect a session timeline before importing
- choose a review window, defaulting to events after the latest reset boundary
- import selected session activity into AgentDB
- generate and edit a local Markdown PR review packet
- identify repeated mistakes, missing verification, and common failure patterns
- distill reviewed corrections into suggested repo guidance or skill updates
- export outputs to files or clipboard
- optionally publish a PR review packet to GitHub

## Non-Goals

- Do not require hosted AgentDB.
- Do not require real-time hooks or MCP streaming for MVP.
- Do not require developers to switch away from Codex, Claude Code, or their
  terminal workflow.
- Do not capture private model reasoning. Import user-visible messages, tool
  calls, command metadata, outputs, artifacts, and summaries.
- Do not capture every terminal byte by default.
- Do not build this into AgentDB core or `agentdbctl`.
- Do not claim cryptographic tamper-proof auditability.
- Do not automatically rewrite repo guidance without explicit developer review.
- Do not replace CI, code review, or security scanning.

## Target Users

- Individual developers using coding agents for local feature and bug-fix work.
- Maintainers reviewing AI-assisted PRs.
- Small teams that want reviewable session records and lightweight learning
  loops before adopting heavier agent governance.
- Agent platform builders who need a portable runtime trace format.

## User Stories

- As a developer, I can open a localhost web UI after finishing an agent task.
- As a developer, I can see recent Codex and Claude Code sessions without
  configuring hooks first.
- As a developer, I can preview a session timeline before importing anything
  into AgentDB.
- As a developer, I can generate a PR review packet from only the relevant
  window of a session.
- As a developer, I can see common issues such as missing tests, repeated
  compile failures, ignored instructions, and unverified claims.
- As a developer, I can turn recurring corrections into suggested updates for
  `AGENTS.md`, project docs, review checklists, or skills.
- As a reviewer, I can distinguish observed facts from generated explanation.
- As a team lead, I can inspect patterns across local imported sessions without
  requiring a hosted service.

## Usability Requirements

The app has to feel like a simple local developer utility, not an operations
workflow.

The happy path should be:

```bash
npx codemate
```

Then in the browser:

1. Select repo.
2. Select session.
3. Review timeline.
4. Choose output: PR packet, issues, or best practices.
5. Copy, save, or publish.

### Usability Principles

- **No setup before work.** Users should be able to use Codex or Claude Code
  normally, then import the session afterward.
- **No AgentDB jargon in the UI.** Avoid exposing tenants, agents, opaque
  session IDs, artifacts, records, replay prefixes, or lifecycle states in the
  first-run UI.
- **Local by default.** Read local session files, store imported session data in
  local AgentDB, preview locally, and publish only when explicitly requested.
- **Inspect before import.** Show what will be imported and allow trimming.
- **Reset-aware defaults.** Default packet scope to the latest meaningful reset
  boundary so the output is focused.
- **Actionable, not archival.** Every generated output should help the developer
  review, fix, or improve future work.
- **Progressive disclosure.** Advanced users can inspect AgentDB event IDs,
  artifact refs, raw JSON, and replay commands from a details panel.

## Product Views

The first web UI should have five views:

1. **Session Picker**
   - Host: Codex, Claude Code, manual import
   - Repo/worktree
   - Branch
   - Session title or first prompt
   - Start time, last event time, event count
   - Reset count

2. **Timeline Review**
   - User prompts
   - Assistant summaries
   - Tool calls and command results
   - File edits and diff summaries when available
   - Artifacts and logs
   - Reset boundaries
   - Include/exclude controls
   - Markers for "verification", "failure", "human correction", and
     "manual review needed"

3. **PR Packet**
   - Markdown preview
   - Review checklist
   - Missing verification warnings, for example "no successful test command
     found"
   - Editable notes
   - Scope selector:
     - after latest reset
     - entire session
     - from selected event
     - last N events

4. **Issues And Patterns**
   - Repeated failures
   - Missing or weak verification
   - Instructions the agent ignored
   - Commands that often fail before succeeding
   - Files or modules that repeatedly cause rework
   - Candidate review checklist items

5. **Best Practices**
   - Suggested `AGENTS.md` updates
   - Suggested docs updates
   - Suggested skill/checklist items
   - Examples from reviewed sessions that justify the suggestion
   - Explicit accept/edit/reject controls

## Reset Boundaries

Generated outputs are scoped to a selected session and review window.

- By default, CodeMate includes events after the latest reset boundary.
- Reset boundaries include host-level events such as `/clear`, `/clean`, `/new`,
  compaction, or imported markers that mean the active context was reset.
- The user can widen the review window in the UI.
- Outputs must state their scope, for example: "Showing events after reset 3;
  18 earlier events omitted."
- Reset boundaries are stored as first-class AgentDB events after import.

## Output Types

### PR Review Packet

The first packet template should include:

- PR/task summary
- repo, branch, base commit, and head commit
- review window
- changed-file summary
- timeline highlights
- failed commands
- successful verification commands
- human corrections
- unresolved manual-review notes
- artifact links or local artifact references
- replay/fork instructions

### Common Issues Report

The first report should identify:

- repeated command failures
- claimed verification without matching command output
- missing tests or lint runs
- human corrections that repeat across sessions
- patterns where the agent edited before reading relevant files
- reset boundaries that hide useful context

### Best-Practice Suggestions

Suggestions should be grounded in imported sessions and should be proposed as
patchable text, not silently applied.

Examples:

- Add a repo instruction: "When changing restore code, run `cargo test -p
  agentdbctl` and the local restore smoke before claiming completion."
- Add a checklist item: "PR packets must include the failing command and the
  successful rerun."
- Add a skill note: "For storage lifecycle work, inspect tiered-storage docs and
  artifact lifecycle tests before editing runtime code."

## MVP Architecture

```text
Codex session files         Claude Code session files
        |                              |
        +---------- importers ---------+
                         |
                    CodeMate
                         |
                  AgentDB HTTP API
                         |
            AgentDB local runtime store
```

### CodeMate

Responsibilities:

- detect host session files
- parse host-specific transcript formats
- normalize events into a review model
- provide the local web UI
- let users choose a session and review window
- import selected session activity into AgentDB
- generate PR review packets
- identify common issues
- propose best-practice updates
- optionally publish to GitHub

### AgentDB

Responsibilities:

- store imported sessions, events, artifacts, records, and reset boundaries
- provide replay and fork semantics
- provide artifact byte storage
- provide APIs/SDKs used by CodeMate

AgentDB should not need product-specific code for Codex or Claude Code session
parsing.

## MVP Scope

Ship the first demo with:

- Local web app launched by `npx codemate`.
- Codex session-file importer.
- Manual JSONL/Markdown/log importer.
- Claude Code importer if the local session format is stable enough; otherwise
  show a documented placeholder and keep manual import.
- Session picker.
- Timeline review with include/exclude selection.
- Reset-boundary detection and default "after latest reset" scope.
- Import selected session activity into AgentDB.
- PR packet builder with Markdown preview.
- Common issues report for the selected session.
- Best-practice suggestion panel with copyable/editable output.
- Copy and save outputs.
- Optional GitHub PR comment publishing.
- Replay link or local replay instructions backed by AgentDB.

Defer:

- real-time MCP/hooks ingestion
- hosted dashboard
- hosted review storage
- automatic semantic consistency checks between PR body and diff
- automatic policy scoring
- cross-session analytics beyond local imported sessions
- CI ingestion beyond imported command output artifacts
- cryptographic signing or tamper-proof logs

## First Demo

Use a real local branch with one reset boundary, one failed verification step,
and one human correction that can become a best-practice suggestion.

1. Work normally in Codex on a small bug fix.
2. Use `/clear` or another reset boundary during the session.
3. Produce one failed test run and one successful rerun.
4. Add a correction such as "you must run the restore smoke before claiming
   completion."
5. Launch `npx codemate`.
6. Select the repo and Codex session.
7. Review the timeline.
8. Generate the PR packet using the default after-reset window.
9. Open the issues report and show the failed command plus missing/late
   verification.
10. Open best-practice suggestions and show a proposed `AGENTS.md` or checklist
    update.
11. Import selected session activity into AgentDB.
12. Copy or publish selected outputs.

The demo succeeds only if the difference from an ordinary generated summary is
obvious: exact command details, reset scope, failures, reruns, recurring issues,
and best-practice suggestions are visible.

## Success Metrics

For the first local release:

- A developer can launch CodeMate locally in under 2 minutes.
- A developer can import a Codex session without configuring hooks.
- A developer can produce a PR packet from a real branch with 5 or fewer UI
  actions after launch.
- The packet identifies at least one failed command and one successful rerun in
  the demo scenario.
- The issues report identifies at least one concrete review risk from the
  imported session.
- The best-practice panel proposes at least one grounded instruction or
  checklist update from a repeated correction or failure.
- The default review window excludes events before the latest reset boundary,
  and the UI can include them.
- The outputs can be generated without network access.
- GitHub publishing remains optional.

## Risks

- **The name may not be available.** Treat CodeMate as a working name until npm,
  GitHub, domain, and trademark checks are complete.
- **Session formats may change.** Keep importers versioned and fail gracefully
  with a manual import fallback.
- **The app may feel like a PR-summary generator.** The UI must emphasize
  timeline selection, reset scope, command proof, issue detection, and
  best-practice distillation.
- **Best-practice suggestions may be too noisy.** Require explicit accept/edit
  and cite the sessions that motivated each suggestion.
- **Local session files may contain sensitive data.** Import should be explicit,
  previewed, and locally stored; exports should show redaction status.
- **AgentDB setup may create friction.** The app should offer a one-command
  local AgentDB start path.
- **Claude Code support may lag Codex.** Keep the host abstraction clean and
  ship Codex + manual import first if needed.

## Future Extensions

After local session import works, CodeMate can support:

- **Real-time MCP/hooks ingestion.** Add live capture when users want dashboards,
  policy checks, or live supervision.
- **Memory.** Derive durable project or personal memory from reviewed session
  data rather than from raw chat logs.
- **Skills.** Package repeated workflows, verification commands, and review
  conventions as reusable agent skills.
- **Team analytics.** Aggregate issue patterns across opted-in local imports or
  hosted workspaces.
- **Policy checks.** Flag missing tests, unsafe commands, skipped review notes,
  or claims without supporting recorded facts.

## Open Questions

- Is CodeMate available as an npm package, GitHub org/repo, domain, and product
  name?
- Should the app bundle a local AgentDB binary for one-command startup, or
  require an existing AgentDB server?
- Which local Codex session-file fields are stable enough to rely on?
- Where exactly does Claude Code store local sessions on each OS, and how stable
  is the format?
- What is the minimum redaction policy before users can safely import real
  sessions?
- Which best-practice suggestions are useful enough for MVP, and which should
  wait for cross-session analysis?
- What should count as a reset boundary across hosts: `/clear`, `/clean`,
  `/new`, compaction, fork, or explicit user marker?
