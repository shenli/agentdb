"""Multi-agent demo on AgentDB pub/sub.

Demonstrates the "agent A appends, agent B subscribes and reacts to
specific event kinds" pattern. Both agents connect directly to
AgentDB; no intermediate broker.

Run (in two terminals):

    # Terminal 1
    cargo run -p agentdb-server -- serve --dev

    # Terminal 2
    pip install -e clients/python
    python examples/agents/python/pubsub_quickstart.py

Expected output:

    reactor: waiting for task_done events…
    reactor: saw task_done #0 (seq=2)
    reactor: saw task_done #1 (seq=4)
    reactor: saw task_done #2 (seq=6)

The kind filter (`kind=["task_done"]`) drops the `task_started`
events server-side, so the reactor only sees what it cares about.
"""

from __future__ import annotations

import os
import threading
import time

from agentdb import Client


SERVER_URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")
USER = os.environ.get("AGENTDB_USER", "demo")
PASSWORD = os.environ.get("AGENTDB_PASSWORD", "demo-secret")
SESSION = "agent-coord"


def producer() -> None:
    """Emits 3 task_started / task_done pairs."""
    client = Client(base_url=SERVER_URL, auth=(USER, PASSWORD))
    sess = client.session(SESSION)
    # Brief delay so the reactor is attached when the first event
    # lands. (For a real workload, use `from_seq` on the reactor
    # side instead — that's gap-free even without sleep.)
    time.sleep(0.5)
    for i in range(3):
        sess.append({"kind": "task_started", "i": i})
        time.sleep(0.3)
        sess.append({"kind": "task_done", "i": i})


def reactor() -> None:
    """Subscribes to the shared session, reacts only to task_done."""
    client = Client(base_url=SERVER_URL, auth=(USER, PASSWORD))
    sess = client.session(SESSION)
    print("reactor: waiting for task_done events…")
    seen = 0
    for evt in sess.subscribe(kind=["task_done"]):
        i = evt.payload.get("i")
        print(f"reactor: saw task_done #{i} (seq={evt.seq})")
        seen += 1
        if seen >= 3:
            return


def main() -> None:
    if not Client(base_url=SERVER_URL).health_ready():
        raise SystemExit(
            f"AgentDB not ready at {SERVER_URL}. Start it with:\n"
            f"    cargo run -p agentdb-server -- serve --dev"
        )
    threading.Thread(target=producer, daemon=True).start()
    reactor()


if __name__ == "__main__":
    main()
