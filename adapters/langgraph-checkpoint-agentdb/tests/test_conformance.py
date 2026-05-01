import asyncio

import pytest

from langgraph_checkpoint_agentdb import AgentDBAsyncSaver
from test_checkpoint import FakeAsyncClient


def test_async_saver_passes_langgraph_base_conformance():
    initializer = pytest.importorskip("langgraph.checkpoint.conformance.initializer")
    validate_module = pytest.importorskip("langgraph.checkpoint.conformance.validate")

    @initializer.checkpointer_test(name="AgentDBAsyncSaver")
    async def agentdb_checkpointer():
        yield AgentDBAsyncSaver(FakeAsyncClient(), tenant_id="ten_1", agent_id="agt_1")

    async def run():
        report = await validate_module.validate(
            agentdb_checkpointer,
            capabilities={"put", "put_writes", "get_tuple", "list", "delete_thread"},
        )
        failures = {
            name: result.failures
            for name, result in report.results.items()
            if result.passed is False
        }
        assert failures == {}

    asyncio.run(run())
