"""OpenAI Agents SDK session adapter quickstart.

Run locally without an OpenAI API key:

    cargo run -p agentdb-server -- dev
    python examples/agents/python/openai_agents_quickstart.py

That default path exercises the OpenAI Agents SDK session protocol over
AgentDB. To run a live Agents SDK call, install the OpenAI Agents SDK, set
`OPENAI_API_KEY`, and opt in:

    pip install openai-agents
    AGENTDB_OPENAI_LIVE=1 python examples/agents/python/openai_agents_quickstart.py
"""

from __future__ import annotations

import asyncio
import os

from _bootstrap import ensure_repo_python_client_on_path

ensure_repo_python_client_on_path()

from openai_agents_agentdb import AgentDBSessions


SERVER_URL = os.environ.get("AGENTDB_URL", "http://127.0.0.1:7878")
WORKSPACE = os.environ.get("AGENTDB_WORKSPACE", "dev")
AGENT = os.environ.get("AGENTDB_AGENT", "openai-demo-agent")
SESSION = os.environ.get("AGENTDB_SESSION", "openai-demo-session")


async def protocol_smoke() -> None:
    sessions = AgentDBSessions(url=SERVER_URL, workspace=WORKSPACE, agent=AGENT)
    session = await sessions.session(SESSION)
    await session.clear_session()
    await session.add_items(
        [
            {"role": "user", "content": "Remember that AgentDB stores session history."},
            {"role": "assistant", "content": "Stored in AgentDB as ordered events."},
        ]
    )
    items = await session.get_items()
    print("# AgentDB-backed OpenAI Agents session protocol")
    print(f"workspace={WORKSPACE} agent={AGENT} session={SESSION}")
    for item in items:
        print(f"{item.get('role')}: {item.get('content')}")


async def live_agents_run() -> None:
    try:
        from agents import Agent, Runner
    except ImportError as exc:
        raise SystemExit(
            "Install the OpenAI Agents SDK first:\n"
            "    pip install openai-agents"
        ) from exc

    sessions = AgentDBSessions(url=SERVER_URL, workspace=WORKSPACE, agent=AGENT)
    session = await sessions.session(SESSION)
    agent = Agent(
        name="AgentDB demo agent",
        instructions="Answer briefly and mention that your session is persisted in AgentDB.",
    )
    result = await Runner.run(
        agent,
        "In one sentence, what is AgentDB doing for this agent?",
        session=session,
    )
    print(result.final_output)


async def main() -> None:
    if os.environ.get("AGENTDB_OPENAI_LIVE") == "1":
        await live_agents_run()
    else:
        await protocol_smoke()


if __name__ == "__main__":
    asyncio.run(main())
