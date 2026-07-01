# Python agent examples

Small agent loops that use AgentDB for working memory, coordination,
and durable runtime state.

## Quickstart

```bash
# 1. Start the server (one terminal)
cargo run -p agentdb-server -- dev

# 2. Run from the repo checkout (another terminal)
python3 examples/agents/python/quickstart.py

# Optional: install the SDK if you want the package in your env
pip install -e clients/python
python3 examples/agents/python/quickstart.py
```

You should see three turns of output, each showing the rolling working memory
grow by one event, then a fork that appends a child event on an alternate
timeline.

## Single-node feature demo

```bash
cargo run -p agentdb-server -- serve --config config/agentdb.toml
AGENTDB_URL=http://127.0.0.1:7878 \
  python3 examples/agents/python/single_node_feature_demo.py
```

This demo covers the local manual feature test path: create tenant, create
agent, create session, append one event, upload/fetch one artifact, and run
artifact lifecycle verification.

## OpenAI Agents SDK session adapter

```bash
cargo run -p agentdb-server -- dev
python3 examples/agents/python/openai_agents_quickstart.py
```

The default run does not require an OpenAI API key; it exercises the OpenAI
Agents SDK custom session protocol over AgentDB and prints the materialized
session items. To run a live Agents SDK call:

```bash
pip install openai-agents
OPENAI_API_KEY=... AGENTDB_OPENAI_LIVE=1 \
  python3 examples/agents/python/openai_agents_quickstart.py
```

## Session-first runtime demo

```bash
cargo run -p agentdb-server -- serve --dev
python3 examples/agents/python/session_first_demo.py

# Optional: install the SDK into your environment instead
pip install -e clients/python
python3 examples/agents/python/session_first_demo.py
```

This demo treats AgentDB as the runtime database for an agent harness. It
records user input, tool calls, tool results, artifact refs, context provenance,
model output, a simulated harness restart, replay, and resume-from-`seq` in one
ordered session timeline.

## Session tiering demo

```bash
cargo run -p agentdb-server -- serve --dev
python3 examples/agents/python/session_tiering_demo.py
```

This demo shows the first tiered-storage slice for developers:

- write to a normal warm session
- seal it into the cold tier
- compact the sealed history into the archive tier
- restore the archive into a normal cold session layout
- confirm reads are unchanged
- append again and watch a new warm head reopen on top of restored history

## Ticket agent tiered demo

```bash
cargo run -p agentdb-server -- serve --config config/agentdb.toml
python3 examples/agents/python/ticket_agent_tiered_demo.py
```

This is the best end-to-end demo app for a SlateDB + object-store deployment:

- realistic support-ticket session flow
- tool call and tool result persistence
- artifact upload
- model output and context provenance
- cold transition
- archive transition
- restore
- append after restore

## AWS automation

The repo also includes AWS deployment scaffolding for a real SlateDB + S3 run:

```bash
terraform -chdir=infra/aws/terraform init
terraform -chdir=infra/aws/terraform apply

infra/aws/scripts/deploy-demo.sh <ec2-public-ip>
infra/aws/scripts/run-tiered-storage-smoke.sh http://<ec2-public-ip>:7878
```

Terraform provisions:

- one S3 bucket
- one EC2 instance
- IAM role and instance profile
- security group

The smoke script runs:

- `session_first_demo.py`
- `session_tiering_demo.py`
- `ticket_agent_tiered_demo.py`

## What it shows

- **`session.append(event, fetch_last_n=5)`** — the AgentDB hot-
  path primitive: write one event and read the last N back in the
  same round trip. This is the canonical agent loop shape.
- **`session.fork(child_id)`** — branch a session into an
  alternate timeline. The child sees a frozen snapshot of the
  parent's events at fork time, MVCC-backed.
- **`session.context(...)` + `context_pack_built`** — persist the
  context that shaped a model response and link it back with
  `source_refs`.
- **`put_artifact(...)` + `artifact_refs`** — keep large tool outputs
  out of inline event payloads while preserving provenance.
- **`replay(from_seq=...)` / `subscribe(from_seq=...)`** — recover an
  agent harness after restart and resume without gaps.

## Customize

```bash
# Talk to a non-local server
AGENTDB_URL=http://prod.example.com:7878 python3 quickstart.py

# Use a different tenant (dev mode only — auth-mode derives it)
AGENTDB_TENANT=t-myapp python3 quickstart.py
```

## Next steps

- The full SDK reference lives at [`clients/python/README.md`](../../../clients/python/README.md).
