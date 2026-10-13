from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from prefect.client.schemas.objects import FlowRun, StateType

from pipelines.rj_smfp__nota_carioca_carga_completa.utils.guard import (
    ActiveRun,
    ActiveRunsError,
    blocking_runs,
    describe_blocking,
)
from pipelines.rj_smfp__nota_carioca_carga_completa.utils.runs import active_runs_filter, to_active_run

NOW = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)


def make_run(state_type: str, name: str = "run", expected_start: datetime | None = None) -> ActiveRun:
    return ActiveRun(
        run_id=f"id-{name}",
        name=name,
        state_type=state_type,
        state_name=state_type.title(),
        expected_start_time=expected_start,
    )


@pytest.mark.parametrize("state_type", ["RUNNING", "PENDING", "CANCELLING"])
def test_blocking_runs_blocks_when_run_is_active(state_type: str) -> None:
    # Given: um flow run em estado ativo
    runs = [make_run(state_type)]

    # When: a trava avalia as execuções
    blocking = blocking_runs(runs, NOW)

    # Then: ele impede o início
    assert blocking == runs


def test_blocking_runs_is_empty_when_deployment_is_idle() -> None:
    assert blocking_runs([], NOW) == []


@pytest.mark.parametrize("state_type", ["COMPLETED", "FAILED", "CRASHED", "CANCELLED", "PAUSED"])
def test_blocking_runs_ignores_runs_that_are_not_active(state_type: str) -> None:
    assert blocking_runs([make_run(state_type)], NOW) == []


@pytest.mark.parametrize(
    ("minutes_from_now", "blocks"),
    [(-30, True), (0, True), (9, True), (10, True), (11, False), (120, False)],
)
def test_blocking_runs_scheduled_only_blocks_inside_ten_minute_window(minutes_from_now: int, blocks: bool) -> None:
    # Given: um flow run agendado para daqui a N minutos (negativo = atrasado, ``Late``)
    run = make_run("SCHEDULED", expected_start=NOW + timedelta(minutes=minutes_from_now))

    # When / Then: só impede se começa em até 10 minutos
    assert (blocking_runs([run], NOW) == [run]) is blocks


def test_blocking_runs_scheduled_without_start_time_blocks() -> None:
    run = make_run("SCHEDULED", expected_start=None)

    assert blocking_runs([run], NOW) == [run]


def test_blocking_runs_keeps_only_the_blocking_ones_in_order() -> None:
    running = make_run("RUNNING", "a")
    far = make_run("SCHEDULED", "b", NOW + timedelta(hours=2))
    soon = make_run("SCHEDULED", "c", NOW + timedelta(minutes=5))

    assert blocking_runs([running, far, soon], NOW) == [running, soon]


def test_describe_blocking_names_runs_with_state_and_link() -> None:
    runs = [make_run("RUNNING", "alpha"), make_run("SCHEDULED", "beta")]

    text = describe_blocking(runs, lambda run_id: f"https://prefect.test/runs/flow-run/{run_id}")

    assert text == (
        "2 execuções ativas ou agendadas: alpha (Running) https://prefect.test/runs/flow-run/id-alpha; "
        "beta (Scheduled) https://prefect.test/runs/flow-run/id-beta"
    )


def test_describe_blocking_omits_link_when_ui_url_is_unknown() -> None:
    assert describe_blocking([make_run("RUNNING", "alpha")], lambda _run_id: None) == (
        "1 execução ativa ou agendada: alpha (Running)"
    )


def test_describe_blocking_lists_three_runs_and_counts_the_rest() -> None:
    runs = [make_run("RUNNING", f"r{i}") for i in range(5)]

    text = describe_blocking(runs, lambda _run_id: None)

    assert text == "5 execuções ativas ou agendadas: r0 (Running); r1 (Running); r2 (Running); e mais 2"


def test_active_runs_error_carries_the_blocking_runs() -> None:
    runs = [make_run("RUNNING")]

    error = ActiveRunsError("texto", runs)

    assert error.runs == tuple(runs)
    assert str(error) == "texto"


def test_active_runs_filter_targets_the_deployment_and_busy_state_types() -> None:
    # Given: o id de um deployment
    deployment_id = uuid4()

    # When: monta o filtro da consulta à API
    run_filter = active_runs_filter(deployment_id)

    # Then: filtra por esse deployment e por estados que podem impedir a carga (sem os finais)
    assert run_filter.deployment_id is not None
    assert run_filter.deployment_id.any_ == [deployment_id]
    assert run_filter.state is not None
    assert run_filter.state.type is not None
    assert set(run_filter.state.type.any_ or []) == {
        StateType.RUNNING,
        StateType.PENDING,
        StateType.CANCELLING,
        StateType.SCHEDULED,
    }


def test_to_active_run_reads_state_and_expected_start_from_prefect_flow_run() -> None:
    # Given: um flow run Late (agendado com início já passado), com os campos que a API do Prefect devolve
    start = NOW - timedelta(minutes=3)
    flow_run = FlowRun(
        id=uuid4(),
        flow_id=uuid4(),
        name="unruly-fox",
        state_type=StateType.SCHEDULED,
        state_name="Late",
        expected_start_time=start,
    )

    # When: converte na visão da trava
    run = to_active_run(flow_run)

    # Then: tipo e nome do estado e o início previsto são preservados
    assert (run.name, run.state_type, run.state_name, run.expected_start_time) == (
        "unruly-fox",
        "SCHEDULED",
        "Late",
        start,
    )
