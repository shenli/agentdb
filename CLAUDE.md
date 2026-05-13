# AgentDB Coding-Agent Notes

This file gives coding agents a short project-specific orientation. For
complete instructions, use `AGENTS.md`, `CONTRIBUTING.md`, and the docs under
`docs/`.

## Default Surfaces

- Prefer the native HTTP API and SDKs for new AgentDB application code.
- Treat the MongoDB listener as a limited compatibility adapter, not a general
  MongoDB-compatible database.
- Keep generated examples aligned with `docs/reference/api-sdk.md` and
  `docs/reference/mongodb-compatibility.md`.

## Working Style

- Keep changes focused and include validation commands in the final summary.
- Do not remove or rewrite unrelated user changes in the working tree.
- Avoid adding machine-local paths, private hostnames, secrets, or private run
  IDs to public docs.
