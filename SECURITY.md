# Security Policy

AgentDB is alpha software. Please do not use public GitHub issues for security
reports.

## Reporting A Vulnerability

Use [GitHub private vulnerability reporting](https://github.com/shenli/agentdb/security/advisories/new).
If that form is unavailable, contact the maintainer through the contact details
on [the repository owner's profile](https://github.com/shenli) and request a
private reporting channel before sharing vulnerability details. Include:

- affected component or API;
- reproduction steps or proof of concept;
- expected impact;
- whether the issue is already public.

We will acknowledge valid reports as quickly as practical and coordinate a fix
before public disclosure.

## Scope

In scope:

- authentication and authorization bypasses;
- data isolation failures between tenants/workspaces;
- remote code execution or arbitrary file access;
- exposure of secrets, tokens, or private runtime data;
- unsafe default network exposure.

Out of scope:

- denial-of-service findings that require local machine access only;
- issues in intentionally experimental cluster workflows unless they also affect
  the supported single-node path;
- benchmark-only performance regressions without a security impact.
