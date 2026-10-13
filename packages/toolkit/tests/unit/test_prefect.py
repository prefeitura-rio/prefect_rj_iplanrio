from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import uuid4

import pytest

from toolkit import prefect


class FakeTaskRunContext:
    def __init__(self, flow_run_id):
        self.task_run = SimpleNamespace(flow_run_id=flow_run_id)


@pytest.fixture
def renames(monkeypatch):
    calls = []

    class FakeClient:
        async def update_flow_run(self, *, flow_run_id, name):
            calls.append((flow_run_id, name))

    @asynccontextmanager
    async def get_client():
        yield FakeClient()

    monkeypatch.setattr(prefect, "TaskRunContext", FakeTaskRunContext)
    monkeypatch.setattr(prefect, "get_client", get_client)
    return calls


async def test_renames_the_flow_run_of_the_current_task(monkeypatch, renames):
    flow_run_id = uuid4()
    monkeypatch.setattr(
        prefect, "get_run_context", lambda: FakeTaskRunContext(flow_run_id)
    )
    await prefect.rename_current_flow_run("new name")
    assert renames == [(flow_run_id, "new name")]


@pytest.mark.parametrize(
    "context",
    [
        pytest.param(object(), id="not_inside_a_task"),
        pytest.param(FakeTaskRunContext(None), id="task_without_a_flow_run"),
    ],
)
async def test_it_refuses_to_rename_without_a_flow_run(monkeypatch, renames, context):
    monkeypatch.setattr(prefect, "get_run_context", lambda: context)
    with pytest.raises(RuntimeError, match="task that has a flow run"):
        await prefect.rename_current_flow_run("x")
    assert renames == []
