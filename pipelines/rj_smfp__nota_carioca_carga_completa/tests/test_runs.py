from uuid import uuid4

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
