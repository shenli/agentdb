# Project Conventions for Claude

## Workflow

- **Always use subagent-driven mode for complex tasks.** When executing a multi-task implementation plan (anything created via `superpowers:writing-plans` or otherwise spanning more than ~3 distinct steps), dispatch a fresh subagent per task with two-stage review (spec compliance, then code quality), per the `superpowers:subagent-driven-development` skill. Do not fall back to inline `executing-plans` or manual execution unless the user explicitly requests it.
