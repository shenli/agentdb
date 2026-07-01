# CodeMate Product Note

This note captures the product research behind **CodeMate**, a local developer
tool built on AgentDB.

The strongest near-term wedge is not "better memory" for Codex, Claude Code, or
another coding agent. The sharper need is helping developers understand,
review, and improve AI-assisted engineering work from real local sessions.

## Goal

CodeMate is a third-party local web app that developers can use without
adopting a hosted AgentDB service, changing coding agents, or moving source code
out of their machine.

The first product should be:

- **local-first**: runs as a localhost web app against a local AgentDB server
  and a local Git checkout
- **agent-agnostic**: works with Codex, Claude Code, or manual terminal imports
- **pull-based first**: imports local session files on demand instead of
  requiring real-time hooks before the work starts
- **review-focused**: helps developers verify what happened in a session
- **learning-focused**: distills recurring corrections, common issues, and
  project-specific best practices

Real-time hooks, hosted dashboards, team analytics, and managed storage can come
later. They should not be required for the first local "aha" moment.

## Research Findings

Developer pain clusters around verification cost and trust.

- A Reddit/Hacker News study of 1,154 posts describes AI-assisted development
  friction as reviewer burden, trust erosion, quality degradation, and
  productivity gains whose costs are externalized to maintainers. Source:
  ["An Endless Stream of AI Slop"](https://arxiv.org/abs/2603.27249).
- A large-scale study of 20,574 real coding-agent sessions found 16,118 visible
  misalignment episodes. The authors report that 90.50% imposed effort or trust
  costs, and 91.49% of visible resolutions required explicit developer
  correction. Common forms include constraint violations, inaccurate
  self-reporting, and faulty implementation. Source:
  ["How Coding Agents Fail Their Users"](https://arxiv.org/abs/2605.29442).
- A study of 23,247 AI-agent-authored PRs found that high message-code
  inconsistency correlated with much lower acceptance, 28.3% versus 80.0%, and
  longer merge time, 55.8 versus 16.0 hours. The most common inconsistency type
  was claiming changes that were not implemented. Source:
  ["Analyzing Message-Code Inconsistency in AI Coding Agent-Authored Pull Requests"](https://arxiv.org/abs/2601.04886).
- A study of 4,550 agentic PRs found explicit logging instructions in only 4.7%
  of cases; agents failed constructive logging requests 67% of the time, and
  humans performed 72.5% of post-generation logging repairs. Source:
  ["Do AI Coding Agents Log Like Humans?"](https://arxiv.org/abs/2604.09409).

## Product Inference

The real user question is not:

> Can AgentDB remember more than my coding agent?

The useful question is:

> Can CodeMate show me what happened, what went wrong, and what we should do
> differently next time?

Codex and Claude Code already maintain local session state and memory. CodeMate
should not compete with those as a private scratchpad. CodeMate is stronger as a
local engineering review and learning layer:

- durable session timeline
- ordered commands, tool calls, model decisions, and test results
- reset-aware review windows
- PR review packets for handoff
- common issue detection
- best-practice suggestions grounded in real sessions
- replay and fork backed by AgentDB

PR review is the first visible workflow, but not the whole product. The broader
job is to turn local agent sessions into reviewable, reusable engineering
knowledge.

## Proposed Tool

Build a local-first **CodeMate** app backed by AgentDB.

The app imports a coding-agent or terminal session into a local AgentDB
instance, then lets the developer produce several outputs:

- PR review packet
- session summary
- common issues report
- best-practice suggestions
- local replay/fork instructions

The first implementation should be a separate local web app:

- launch with `npx codemate`
- detect Codex, Claude Code, and manual session sources
- let users select a repo, session, and review window
- default to events after the latest reset boundary
- import selected session activity into AgentDB
- render and edit Markdown outputs
- copy, save, or optionally publish selected outputs

MCP/hooks can come later for real-time streaming. Pull-based import is easier
for local usability because developers can use their coding agent normally and
open CodeMate only when they want to review, summarize, or improve future work.

For a concise product requirements draft, see [`codemate-prd.md`](codemate-prd.md).

## First Demo

Use a real local branch with one reset boundary, one failed verification step,
and one human correction.

1. Work normally in Codex or Claude Code on a local branch.
2. Launch CodeMate:

   ```bash
   npx codemate
   ```

3. Select the repo and session from detected local session files.
4. Review the timeline and default post-reset window.
5. Generate a PR review packet.
6. Open the common issues report.
7. Open best-practice suggestions and show a proposed repo guidance/checklist
   update.
8. Import selected session activity into AgentDB.
9. Optionally publish selected output after local review.

## Aha Moment

The developer sees that the agent transcript is no longer just a disposable chat
log. CodeMate turns it into:

- a reviewable timeline
- a packet that supports or challenges final claims
- a list of recurring issues to fix
- concrete best-practice updates for future agent sessions

The product is worth continuing if it reduces reviewer uncertainty and helps the
next agent session perform better.
