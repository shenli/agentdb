# AgentDB workload generator

Simulates a customer-support chatbot platform driving traffic against a
running `agentdb-server`.

## Quick start

```bash
# 1. Start the DB in another terminal:
cargo run -p agentdb-server

# 2. Run the workload:
cd workload
go run ./cmd/agentdb-workload -config config.yaml
```

The dashboard at http://127.0.0.1:7878/dashboard visualizes traffic
live.

## Demo control endpoints

The workload listens on `config.control_addr` (default `:9090`):

- `POST /demo/burst?tenant=T` — trigger a load spike on one tenant
- `POST /demo/fork?session=S` — force a fork on a running session
- `POST /demo/cold?tenant=T` — idle a tenant into the cold tier
