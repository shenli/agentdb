"""Crash-and-resume — the flagship AgentDB flow.

An agent's session is durable state that lives *outside* the process. When the
harness crashes or a worker moves, you don't reconstruct state from logs — you
keep the canonical IDs, rebuild a fresh client, and continue from durable
history.

This example:

  1. "Process A" opens a session, appends a few events, and persists ONLY the
     canonical IDs to disk (the thing a real harness would checkpoint).
  2. Process A dies. Everything in memory is gone.
  3. "Process B" starts, loads the IDs, builds a brand-new client, resumes the
     same durable session, and appends the next event — no duplication, no
     replay-from-scratch.

Run me:

    # In one terminal (persistent, so state survives the 'crash'):
    cargo run -p agentdb-server -- serve --data-dir /tmp/agentdb-resume-demo

    # In another:
    pip install -e clients/python
    python3 examples/agents/python/resume.py
"""

from __future__ import annotations

import json
import os
import tempfile

from _bootstrap import ensure_repo_python_client_on_path

ensure_repo_python_client_on_path()

from agentdb import AgentDB, Client

SERVER_URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")
TOKEN = os.environ.get("AGENTDB_TOKEN")
IDFILE = os.path.join(tempfile.gettempdir(), "agentdb-resume-ids.json")


def process_a() -> None:
    """Open a session, do some work, checkpoint the canonical IDs, then 'die'."""
    db = AgentDB(base_url=SERVER_URL, token=TOKEN)
    session = db.open_session("resume-demo", "agent", "run-1")

    for kind in ("user_turn", "model_call", "tool_result"):
        r = session.append({"note": kind}, kind=kind)
        print(f"[A] appended {kind} at seq={r.seq}")

    # The ONLY thing a restarted harness needs: the canonical IDs.
    ids = {
        "tenant_id": session.workspace.id,   # workspace .id is the tenant_id
        "agent_id": session.agent_id,
        "session_id": session.id,
    }
    with open(IDFILE, "w") as fh:
        json.dump(ids, fh)
    print(f"[A] checkpointed canonical IDs, process exiting: {ids}")


def process_b() -> None:
    """Fresh process: rebuild from the persisted IDs and continue."""
    with open(IDFILE) as fh:
        ids = json.load(fh)
    print(f"[B] restarted; loaded canonical IDs: {ids}")

    db = AgentDB(base_url=SERVER_URL, token=TOKEN)  # brand-new client
    session = (
        db.workspace(tenant_id=ids["tenant_id"])
        .agent_by_id(ids["agent_id"])
        .session_by_id(ids["session_id"])
    )
    r = session.append({"note": "resumed"}, kind="model_call")
    print(f"[B] resumed and appended at seq={r.seq}")

    history = session.replay(from_seq=0, limit=100)
    print(f"[B] durable history has {len(history)} events: {[e.kind for e in history]}")


def main() -> None:
    if not Client(base_url=SERVER_URL, token=TOKEN).health_ready():
        raise SystemExit(
            f"AgentDB not ready at {SERVER_URL}. Start a persistent server:\n"
            f"    cargo run -p agentdb-server -- serve --data-dir /tmp/agentdb-resume-demo"
        )
    process_a()
    print("--- (process A is gone; only the ID file survived) ---")
    process_b()


if __name__ == "__main__":
    main()
