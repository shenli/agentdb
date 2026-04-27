"""End-to-end smoke test for the AsyncClient.

Same fixture pattern as test_smoke.py: spawns `agentdb serve --dev`
once per module, then exercises the async API. Each test wraps
its own coroutine with `asyncio.run` so we don't pull in
pytest-asyncio as a dep.

Skipped automatically when:
  - the server binary hasn't been built (same as the sync smoke);
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
    rest_port = _free_port()
    mongo_port = _free_port()
    env = {**os.environ, "RUST_LOG": "warn"}
    proc = subprocess.Popen(
        [
            str(SERVER_BIN),
            "serve",
            "--dev",
            "--addr",
            f"127.0.0.1:{rest_port}",
            "--mongo-addr",
            f"127.0.0.1:{mongo_port}",
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


def test_async_append_round_trips(server):
    async def run():
        async with AsyncClient(server, tenant="demo") as client:
            sess = client.session("async-smoke-1")
            result = await sess.append(
                {"role": "user", "text": "hello async"},
                fetch_last_n=5,
            )
            assert result.seq >= 1
            assert len(result.recent) == 1
            assert result.recent[0].payload["text"] == "hello async"

    asyncio.run(run())


def test_async_multi_append_in_order(server):
    async def run():
        async with AsyncClient(server, tenant="demo") as client:
            sess = client.session("async-smoke-2")
            await sess.append({"i": 1})
            await sess.append({"i": 2})
            last = await sess.append({"i": 3}, fetch_last_n=10)
            seqs = [e.payload["i"] for e in last.recent]
            assert seqs == [1, 2, 3], f"expected ordered events; got {seqs}"

    asyncio.run(run())


def test_async_fork_returns_async_session(server):
    async def run():
        async with AsyncClient(server, tenant="demo") as client:
            parent = client.session("async-smoke-fork-parent")
            await parent.append({"i": 1})
            child = await parent.fork("async-smoke-fork-child")
            assert isinstance(child, AsyncSession)
            assert child.session_id == "async-smoke-fork-child"
            res = await child.append({"i": 99}, fetch_last_n=10)
            assert res.recent[-1].payload["i"] == 99

    asyncio.run(run())


def test_async_gather_writes_to_distinct_sessions_in_parallel(server):
    """Async-only payoff: the sync client serialises N session
    writes; AsyncClient + asyncio.gather lets them fly in
    parallel, which is the actual reason an agent framework
    wants the async surface."""

    async def run():
        async with AsyncClient(server, tenant="demo") as client:
            sessions = [client.session(f"async-fan-{i}") for i in range(5)]
            results = await asyncio.gather(
                *(s.append({"i": i}, fetch_last_n=1) for i, s in enumerate(sessions))
            )
            for i, r in enumerate(results):
                assert r.seq == 1, f"session {i} should be at seq 1 after one write"
                assert r.recent[0].payload["i"] == i

    asyncio.run(run())


def test_async_session_without_tenant_or_auth_raises(server):
    async def run():
        async with AsyncClient(server) as client:
            sess = client.session("async-smoke-3")
            with pytest.raises(ValueError, match="tenant"):
                await sess.append({"x": 1})

    asyncio.run(run())


def test_async_health_ready(server):
    async def run():
        async with AsyncClient(server) as client:
            assert await client.health_ready() is True

    asyncio.run(run())


def test_async_unpaired_credentials_raise():
    with pytest.raises(ValueError, match="password"):
        AsyncClient("http://127.0.0.1:7878", user="alice")
    with pytest.raises(ValueError, match="user"):
        AsyncClient("http://127.0.0.1:7878", password="pencil")


def test_async_session_subscribe_replay(server):
    """Async per-session subscribe replays seeded events via
    `from_seq=1`, then exits the loop after the third event. Mirrors
    the sync `test_subscribe_replays_from_seq` but exercises the
    httpx-backed async path."""

    async def run():
        async with AsyncClient(server, tenant="t-async-sub") as client:
            sess = client.session("s-replay-async")
            for _ in range(3):
                await sess.append({"kind": "x"})
            seqs = []
            async for evt in sess.subscribe(from_seq=1):
                seqs.append(evt.seq)
                if len(seqs) >= 3:
                    break
            assert seqs == [1, 2, 3]

    asyncio.run(run())
