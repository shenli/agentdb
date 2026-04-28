"""Quickstart agent — three turns of "thought-act-observe" with
working memory backed by AgentDB.

Each turn does the canonical agent flow:

  1. Pull the recent context (last N events) for the session.
  2. Decide an action based on that context.
  3. Append the new thought / action / observation to the session
     so the next turn — or a forked branch — sees it.

Steps 1 + 3 happen in one round trip via `append + fetch_last_n`.
That's the AgentDB hot-path primitive and the reason this whole
demo runs in <100ms against a local dev server.

Run me:

    # In one terminal:
    cargo run -p agentdb-server -- serve --dev

    # In another:
    pip install -e clients/python
    python examples/agents/python/quickstart.py

Expected output: three turns printed, each showing the working
memory growing by one event. Then a fork that branches off the
session and shows the child sees the parent's history at fork
time.
"""

from __future__ import annotations

import os

from agentdb import Client


SERVER_URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")
USER = os.environ.get("AGENTDB_USER", "demo")
PASSWORD = os.environ.get("AGENTDB_PASSWORD", "demo-secret")
SESSION_ID = "quickstart-agent"


def thought_for_turn(turn: int, recent_text: list[str]) -> dict:
    """Stand-in for an LLM call. The agent looks at recent context
    and decides what to record next. In real code this is where you'd
    call your model with `recent_text` as the prompt history."""
    if turn == 0:
        return {"role": "user", "text": "What's 2 + 2?"}
    if turn == 1:
        return {
            "role": "assistant",
            "text": "Looking back, the user asked '2 + 2'. The answer is 4.",
        }
    return {
        "role": "tool",
        "name": "calculator",
        "result": 4,
        "context_size": len(recent_text),
    }


def main() -> None:
    client = Client(base_url=SERVER_URL, auth=(USER, PASSWORD))
    if not client.health_ready():
        raise SystemExit(
            f"AgentDB not ready at {SERVER_URL}. Start it with:\n"
            f"    cargo run -p agentdb-server -- serve --dev"
        )

    sess = client.session(SESSION_ID)
    print(f"# Session: {sess}\n")

    for turn in range(3):
        # Pull the recent five events as context. Even though we
        # call this on the same line as the append, the server
        # returns the new event in `recent` so the agent sees its
        # own write.
        recent_payloads: list[str] = []
        # Build the new event off whatever happened before.
        new_event = thought_for_turn(turn, recent_payloads)
        result = sess.append(new_event, fetch_last_n=5)
        print(f"--- turn {turn} (seq={result.seq}) ---")
        for ev in result.recent:
            actor = ev.payload.get("role", "?")
            text = ev.payload.get("text") or ev.payload.get("result") or "(no text)"
            print(f"  [{ev.seq:>3}] {actor:>9}: {text}")
        print()

    # Fork: branch the agent off into an alternate timeline.
    # The child sees the parent's events at fork time, MVCC-backed.
    print("# Forking into an alternate timeline...")
    branch = sess.fork(f"{SESSION_ID}-branch-a")
    branch_result = branch.append(
        {"role": "user", "text": "What if it were 3 + 3 instead?"},
        fetch_last_n=10,
    )
    print(f"--- branch turn 0 (seq={branch_result.seq}) ---")
    for ev in branch_result.recent:
        actor = ev.payload.get("role", "?")
        text = ev.payload.get("text") or ev.payload.get("result") or "(no text)"
        print(f"  [{ev.seq:>3}] {actor:>9}: {text}")


if __name__ == "__main__":
    main()
