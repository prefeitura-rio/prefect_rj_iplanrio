"""Cancela contra um servidor Prefect local (temporário), sem rede externa."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from prefect import flow
from prefect.client.orchestration import SyncPrefectClient, get_client
from prefect.client.schemas.objects import StateType
from prefect.states import Pending, Running, Scheduled, State
from prefect.testing.utilities import prefect_test_harness

from pipelines.rj_iplanrio__cancela_flow_runs_presos import tasks

SIX_HOURS = timedelta(hours=6)


@flow
def dummy_flow() -> None:
    """Flow vazio só para criar flow runs."""


@pytest.fixture(scope="module")
def client() -> Iterator[SyncPrefectClient]:
    with prefect_test_harness(server_startup_timeout=90), get_client(sync_client=True) as sync_client:
        yield sync_client


def test_only_the_run_stuck_in_pending_goes_to_cancelling(client: SyncPrefectClient) -> None:
    stuck = client.create_flow_run(dummy_flow, state=Scheduled())
    client.set_flow_run_state(stuck.id, State(type=StateType.PENDING, name="InfrastructurePending"), force=True)
    client.update_flow_run(stuck.id, infrastructure_pid="prefect-server:small-puma-mrkwr")
    running = client.create_flow_run(dummy_flow, state=Scheduled())
    client.set_flow_run_state(running.id, Pending(), force=True)
    client.set_flow_run_state(running.id, Running(), force=True)
    scheduled = client.create_flow_run(dummy_flow, state=Scheduled())

    # O servidor grava o horário do estado; o "agora" de referência avança 7 h.
    later = datetime.now(UTC) + timedelta(hours=7)
    assert tasks.cancel_stuck_runs(client, now=datetime.now(UTC), max_pending=SIX_HOURS, dry_run=False) == []
    assert tasks.cancel_stuck_runs(client, now=later, max_pending=SIX_HOURS, dry_run=True) == [str(stuck.id)]
    assert client.read_flow_run(stuck.id).state.type == StateType.PENDING

    assert tasks.cancel_stuck_runs(client, now=later, max_pending=SIX_HOURS, dry_run=False) == [str(stuck.id)]
    assert client.read_flow_run(stuck.id).state.type == StateType.CANCELLING
    assert client.read_flow_run(running.id).state.type == StateType.RUNNING
    assert client.read_flow_run(scheduled.id).state.type == StateType.SCHEDULED
    assert tasks.cancel_stuck_runs(client, now=later, max_pending=SIX_HOURS, dry_run=False) == []
