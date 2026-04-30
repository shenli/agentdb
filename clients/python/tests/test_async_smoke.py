"""End-to-end smoke test for the AsyncClient.

Same fixture pattern as test_smoke.py: spawns `agentdb serve --in-memory`
and exercises the async V2 API. Each test wraps its own coroutine
with `asyncio.run` so we don't pull in pytest-asyncio as a dep.

Skipped automatically when:
  - the server binary hasn't been built (same as the sync smoke);
  - `AGENTDB_PYTHON_SMOKE=1` is not set;
  - `httpx` isn't installed (the `[async]` extra wasn't pulled).
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import time
from pathlib import Path

import pytest
httpx = pytest.importorskip("httpx")  # gates the whole module
from agentdb import AsyncClient, AsyncSession  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_BIN = REPO_ROOT / "target" / "debug" / "agentdb"

_TENANT_COUNTER = 0


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def server():
    if not SERVER_BIN.exists():
        pytest.skip(
            f"server binary not found at {SERVER_BIN}; "
            "run `cargo build -p agentdb-server` first"
        )
    if os.environ.get("AGENTDB_PYTHON_SMOKE") != "1":
        pytest.skip("set AGENTDB_PYTHON_SMOKE=1 to run live server smoke tests")
    rest_port = _free_port()
    env = {**os.environ, "RUST_LOG": "warn"}
    proc = subprocess.Popen(
        [
            str(SERVER_BIN),
            "serve",
            "--in-memory",
            "--bind",
            f"127.0.0.1:{rest_port}",
        ],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base_url = f"http://127.0.0.1:{rest_port}"

    async def _wait_ready() -> bool:
        async with AsyncClient(base_url) as c:
            return await c.health_ready()

    deadline = time.time() + 15
    ready = False
    while time.time() < deadline:
        if asyncio.run(_wait_ready()):
            ready = True
            break
        if proc.poll() is not None:
            pytest.fail(f"server exited before ready (rc={proc.returncode})")
        time.sleep(0.1)
    if not ready:
        proc.terminate()
        proc.wait(timeout=5)
        pytest.fail("server didn't become ready within 15s")

    try:
        yield base_url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


async def _session(client: AsyncClient, label: str):
    global _TENANT_COUNTER
    _TENANT_COUNTER += 1
    tenant = await client.create_tenant(f"{label}-{_TENANT_COUNTER}")
    agent = await client.create_agent(tenant["tenant_id"], alias=f"{label}-agent")
    return await client.create_session(tenant["tenant_id"], agent["agent_id"], alias=label)


def test_async_append_round_trips(server):
    async def run():
        async with AsyncClient(server) as client:
            sess = await _session(client, "async-smoke-1")
            result = await sess.append(
                {"role": "user", "text": "hello async"},
                fetch_last_n=5,
            )
            assert result.seq >= 0
            assert len(result.recent) == 1
            assert result.recent[0].payload["text"] == "hello async"

    asyncio.run(run())


def test_async_multi_append_in_order(server):
    async def run():
        async with AsyncClient(server) as client:
            sess = await _session(client, "async-smoke-2")
            await sess.append({"i": 1})
            await sess.append({"i": 2})
            last = await sess.append({"i": 3}, fetch_last_n=10)
            seqs = [e.payload["i"] for e in last.recent]
            assert seqs == [1, 2, 3], f"expected ordered events; got {seqs}"

    asyncio.run(run())


def test_async_fork_returns_async_session(server):
    async def run():
        async with AsyncClient(server) as client:
            parent = await _session(client, "async-smoke-fork-parent")
            await parent.append({"i": 1})
            child = await parent.fork(at_seq=0, alias="async-smoke-fork-child")
            assert isinstance(child, AsyncSession)
            assert child.session_id.startswith("ses_")
            res = await child.append({"i": 99}, fetch_last_n=10)
            assert res.recent[-1].payload["i"] == 99

    asyncio.run(run())


def test_async_gather_writes_to_distinct_sessions_in_parallel(server):
    """Async payoff: write to many sessions at once via asyncio.gather."""

    async def run():
        async with AsyncClient(server) as client:
            sessions = [await _session(client, f"async-fan-{i}") for i in range(5)]
            results = await asyncio.gather(
                *(s.append({"i": i}, fetch_last_n=1) for i, s in enumerate(sessions))
            )
            for i, r in enumerate(results):
                assert r.seq >= 0, f"session {i} should accept one write"
                assert r.recent[0].payload["i"] == i

    asyncio.run(run())


def test_async_health_ready(server):
    async def run():
        async with AsyncClient(server) as client:
            assert await client.health_ready() is True

    asyncio.run(run())


def test_async_unpaired_credentials_raise():
    # token + get_token together is a programmer error
    with pytest.raises(ValueError, match="get_token"):
        AsyncClient("http://127.0.0.1:7878", token="t", get_token=lambda: "t")


def test_async_session_subscribe_replay(server):
    """Async per-session subscribe replays seeded events via
    `from_seq=1`, then exits the loop after the third event."""

    async def run():
        async with AsyncClient(server) as client:
            sess = await _session(client, "s-replay-async")
            for _ in range(3):
                await sess.append({"kind": "x"})
            seqs = []
            async for evt in sess.subscribe(from_seq=0):
                seqs.append(evt.seq)
                if len(seqs) >= 3:
                    break
            assert seqs == [0, 1, 2]

    asyncio.run(run())
