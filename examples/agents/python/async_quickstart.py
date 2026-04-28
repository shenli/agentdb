"""Async quickstart agent.

Same shape as `quickstart.py` but built on `AsyncClient` and
showing the async-only payoff: parallel writes to distinct
sessions via `asyncio.gather`. That's the pattern an agent
framework needs when several tools — or several agents in a
swarm — write to AgentDB concurrently.

Run me:

    # In one terminal:
    cargo run -p agentdb-server -- serve --dev

    # In another:
    pip install 'agentdb[async]'
    python examples/agents/python/async_quickstart.py
"""

from __future__ import annotations

import asyncio
import os

from agentdb import AsyncClient


SERVER_URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")
USER = os.environ.get("AGENTDB_USER", "demo")
PASSWORD = os.environ.get("AGENTDB_PASSWORD", "demo-secret")
SESSION_ID = "async-quickstart-agent"


def thought_for_turn(turn: int) -> dict:
    if turn == 0:
        return {"role": "user", "text": "What's 2 + 2?"}
    if turn == 1:
        return {
            "role": "assistant",
            "text": "Looking back, the user asked '2 + 2'. Calling tool…",
        }
    return {"role": "tool", "name": "calculator", "result": 4}


def render(label: str, recent) -> None:
    print(label)
    for ev in recent:
        actor = ev.payload.get("role", "?")
        text = ev.payload.get("text") or ev.payload.get("result") or "(no text)"
        print(f"  [{ev.seq:>3}] {actor:>9}: {text}")
    print()


async def main() -> None:
    async with AsyncClient(base_url=SERVER_URL, auth=(USER, PASSWORD)) as client:
        if not await client.health_ready():
            raise SystemExit(
                f"AgentDB not ready at {SERVER_URL}. Start it with:\n"
                f"    cargo run -p agentdb-server -- serve --dev"
            )

        sess = client.session(SESSION_ID)
        print(f"# Session: {sess}\n")

        # Sequential 3-turn loop — same flow as the sync example,
        # so the payload shape lines up if you compare side-by-
        # side.
        for turn in range(3):
            ev = thought_for_turn(turn)
            result = await sess.append(ev, fetch_last_n=5)
            render(f"--- turn {turn} (seq={result.seq}) ---", result.recent)

        # Async-only payoff: write to multiple sessions
        # concurrently. The sync client serialises these — async
        # fires them in parallel, bound by network round-trip
        # rather than client serialisation.
        print("# Fanning out 5 parallel session writes via asyncio.gather...")
        peers = [client.session(f"{SESSION_ID}-peer-{i}") for i in range(5)]
        results = await asyncio.gather(
            *(
                p.append(
                    {"role": "agent", "id": i, "text": f"hello from peer {i}"},
                    fetch_last_n=1,
                )
                for i, p in enumerate(peers)
            )
        )
        for i, r in enumerate(results):
            print(f"  peer-{i}: seq={r.seq}, recent={len(r.recent)}")
        print()

        # Fork: branch the parent into an alternate timeline.
        # Same MVCC semantics as in the sync example.
        print("# Forking into an alternate timeline...")
        branch = await sess.fork(f"{SESSION_ID}-branch-a")
        branch_result = await branch.append(
            {"role": "user", "text": "What if it were 3 + 3 instead?"},
            fetch_last_n=10,
        )
        render(
            f"--- branch turn 0 (seq={branch_result.seq}) ---",
            branch_result.recent,
        )


if __name__ == "__main__":
    asyncio.run(main())
