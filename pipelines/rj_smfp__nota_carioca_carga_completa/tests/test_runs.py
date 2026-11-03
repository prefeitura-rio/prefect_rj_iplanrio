from uuid import UUID, uuid4

import pytest
from prefect.client.schemas.objects import FlowRun, StateType

from pipelines.rj_smfp__nota_carioca_carga_completa.utils import runs
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.chain import ChildRun


def make_flow_run(state_type: StateType, state_name: str) -> FlowRun:
    return FlowRun(id=uuid4(), flow_id=uuid4(), name="brave-owl", state_type=state_type, state_name=state_name)


def test_run_and_wait_creates_a_subflow_with_the_deployment_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: o Prefect devolve o filho concluído
    calls: list[dict[str, object]] = []
    flow_run = make_flow_run(StateType.COMPLETED, "Completed")

    def fake_run_deployment(**kwargs: object) -> FlowRun:
        calls.append(kwargs)
        return flow_run

    monkeypatch.setattr(runs, "run_deployment", fake_run_deployment)

    # When: dispara o deployment com tempo limite e tags
    child = runs.run_and_wait("flow/deployment", 14_400, ("disparo:carga-completa", "disparo:discord"))

    # Then: pede um subflow sem parâmetros (valem os do deployment), com o tempo limite e as tags, e devolve o filho
    assert calls == [
        {
            "name": "flow/deployment",
            "timeout": 14_400,
            "poll_interval": 30.0,
            "as_subflow": True,
            "tags": ["disparo:carga-completa", "disparo:discord"],
        }
    ]
    assert child == ChildRun(str(flow_run.id), "brave-owl", "COMPLETED", "Completed")


def test_to_child_run_keeps_the_state_of_a_run_still_active_after_the_timeout() -> None:
    flow_run = make_flow_run(StateType.RUNNING, "Running")

    child = runs.to_child_run(flow_run)

    assert (child.state_type, child.state_name) == ("RUNNING", "Running")


def test_own_child_filter_selects_the_subflows_of_this_run_for_the_deployment() -> None:
    deployment_id, parent_id = uuid4(), uuid4()

    flow_filter = runs.own_child_filter(deployment_id, str(parent_id))

    assert flow_filter.deployment_id is not None
    assert flow_filter.deployment_id.any_ == [deployment_id]
    assert flow_filter.parent_flow_run_id is not None
    assert flow_filter.parent_flow_run_id.any_ == [parent_id]


def test_wait_for_run_polls_until_a_final_state() -> None:
    states = iter([StateType.RUNNING, StateType.RUNNING, StateType.COMPLETED])
    run_id = str(uuid4())
    slept: list[float] = []
    clock = iter(range(0, 10_000, 30))

    def read(rid: UUID) -> FlowRun:
        assert str(rid) == run_id
        state = next(states)
        return make_flow_run(state, state.value.title())

    child = runs.wait_for_run(run_id, 3600, read=read, sleep=slept.append, clock=lambda: float(next(clock)))

    assert (child.state_type, child.state_name) == ("COMPLETED", "Completed")
    assert slept == [30.0, 30.0]


def test_wait_for_run_returns_the_active_state_at_the_timeout_without_cancelling() -> None:
    clock = iter(range(0, 10_000, 30))

    def read(_rid: UUID) -> FlowRun:
        return make_flow_run(StateType.RUNNING, "Running")

    child = runs.wait_for_run(str(uuid4()), 60, read=read, sleep=lambda _s: None, clock=lambda: float(next(clock)))

    assert (child.state_type, child.state_name) == ("RUNNING", "Running")
