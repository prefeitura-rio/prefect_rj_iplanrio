from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
from prefect.client.schemas.objects import FlowRun, State, StateType
from prefect.client.schemas.responses import OrchestrationResult, SetStateStatus, StateAbortDetails

from pipelines.rj_iplanrio__cancela_flow_runs_presos import tasks

NOW = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
SIX_HOURS = timedelta(hours=6)


def make_flow_run(
    state_type: StateType = StateType.PENDING,
    state_name: str = "InfrastructurePending",
    pending_for: timedelta = timedelta(days=9),
    start_time: datetime | None = None,
) -> FlowRun:
    return FlowRun(
        id=uuid4(),
        name="small-puma",
        flow_id=uuid4(),
        deployment_id=uuid4(),
        state=State(type=state_type, name=state_name, timestamp=NOW - pending_for),
        start_time=start_time,
        infrastructure_pid="prefect-server:small-puma-mrkwr",
    )


class FakeClient:
    """Cliente falso com a parte da API do Prefect que as tasks usam."""

    def __init__(self, listed: list[FlowRun], reject: SetStateStatus | None = None) -> None:
        self.listed = listed
        self.current: dict[UUID, FlowRun] = {run.id: run for run in listed}
        self.reject = reject
        self.cancelled: list[UUID] = []
        self.list_calls: list[dict[str, Any]] = []

    def read_flow_runs(self, **kwargs: Any) -> list[FlowRun]:
        self.list_calls.append(kwargs)
        offset, limit = kwargs["offset"], kwargs["limit"]
        return self.listed[offset : offset + limit]

    def read_flow_run(self, flow_run_id: UUID) -> FlowRun:
        return self.current[flow_run_id]

    def set_flow_run_state(self, flow_run_id: UUID, state: State) -> OrchestrationResult[Any]:
        if self.reject is not None:
            current_state = self.current[flow_run_id].state if self.reject == SetStateStatus.REJECT else None
            return OrchestrationResult(state=current_state, status=self.reject, details=StateAbortDetails(reason="no"))
        self.cancelled.append(flow_run_id)
        return OrchestrationResult(state=state, status=SetStateStatus.ACCEPT, details=StateAbortDetails())


def run_cancel(client: FakeClient, dry_run: bool = False) -> list[str]:
    return tasks.cancel_stuck_runs(client, now=NOW, max_pending=SIX_HOURS, dry_run=dry_run)


def test_run_stuck_for_days_is_cancelled() -> None:
    stuck = make_flow_run()
    client = FakeClient([stuck])
    assert run_cancel(client) == [str(stuck.id)]
    assert client.cancelled == [stuck.id]


def test_listing_filters_pending_state_type() -> None:
    client = FakeClient([])
    run_cancel(client)
    flow_run_filter = client.list_calls[0]["flow_run_filter"]
    assert flow_run_filter.state.type.any_ == [StateType.PENDING]


def test_recent_pending_run_is_kept() -> None:
    recent = make_flow_run(pending_for=timedelta(minutes=30))
    client = FakeClient([recent])
    assert run_cancel(client) == []
    assert client.cancelled == []


def test_run_that_started_after_listing_is_not_cancelled() -> None:
    stuck = make_flow_run()
    client = FakeClient([stuck])
    client.current[stuck.id] = make_flow_run(state_type=StateType.RUNNING, state_name="Running", start_time=NOW)
    client.current[stuck.id].id = stuck.id
    assert run_cancel(client) == []
    assert client.cancelled == []


def test_dry_run_only_reports() -> None:
    stuck = make_flow_run()
    client = FakeClient([stuck])
    assert run_cancel(client, dry_run=True) == [str(stuck.id)]
    assert client.cancelled == []


@pytest.mark.parametrize("status", [SetStateStatus.ABORT, SetStateStatus.REJECT])
def test_refused_cancellation_is_not_reported_as_cancelled(status: SetStateStatus) -> None:
    client = FakeClient([make_flow_run()], reject=status)
    assert run_cancel(client) == []


def test_listing_walks_every_page(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "PAGE_SIZE", 2)
    runs = [make_flow_run(pending_for=timedelta(days=day)) for day in range(1, 6)]
    client = FakeClient(runs)
    assert len(run_cancel(client)) == 5
    assert [call["offset"] for call in client.list_calls] == [0, 2, 4]


def test_run_without_state_is_never_selected() -> None:
    run = make_flow_run()
    run.state = None
    client = FakeClient([run])
    assert run_cancel(client) == []
