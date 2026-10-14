from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml
from hypothesis import given
from hypothesis import strategies as st

from toolkit import prefect

BASE = "2025-07-15T00:00:00"


class FakeTaskRunContext:
    def __init__(self, flow_run_id):
        self.task_run = SimpleNamespace(flow_run_id=flow_run_id)


@pytest.fixture
def renames(monkeypatch):
    """Replace the Prefect client, and record the renames."""
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


async def test_the_flow_run_of_the_current_task_is_renamed(monkeypatch, renames):
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
async def test_nothing_is_renamed_without_a_flow_run(monkeypatch, renames, context):
    monkeypatch.setattr(prefect, "get_run_context", lambda: context)

    with pytest.raises(RuntimeError, match="task that has a flow run"):
        await prefect.rename_current_flow_run("x")

    assert renames == []


@given(
    slugs=st.lists(st.text(min_size=1), min_size=1, max_size=15, unique=True),
    stagger=st.integers(1, 59),
)
def test_each_schedule_starts_a_stagger_after_the_previous_one_and_keeps_its_parameters(
    slugs, stagger
):
    parameters = [{"table_id": slug, "url": f"u-{slug}"} for slug in slugs]

    text = prefect.schedules_yaml(parameters, 3600, BASE, stagger, "America/Sao_Paulo")
    schedules = yaml.safe_load(text)["schedules"]

    first = datetime.fromisoformat(BASE)
    assert [(s["slug"], s["parameters"], s["anchor_date"]) for s in schedules] == [
        (p["table_id"], p, (first + timedelta(minutes=stagger * i)).isoformat())
        for i, p in enumerate(parameters)
    ]


def test_a_parameter_set_without_the_slug_field_is_an_error():
    with pytest.raises(ValueError, match="index 1"):
        prefect.schedules_yaml(
            [{"table_id": "a"}, {"url": "u"}], 3600, BASE, 10, "America/Sao_Paulo"
        )
