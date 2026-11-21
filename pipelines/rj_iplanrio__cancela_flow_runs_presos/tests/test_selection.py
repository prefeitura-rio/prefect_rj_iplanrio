from datetime import UTC, datetime, timedelta

import pytest

from pipelines.rj_iplanrio__cancela_flow_runs_presos.utils import selection

NOW = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
SIX_HOURS = timedelta(hours=6)


def make_run(
    run_id: str = "run-1",
    state_type: str = "PENDING",
    state_name: str = "InfrastructurePending",
    pending_for: timedelta = timedelta(days=9),
    started: bool = False,
) -> selection.PendingRun:
    return selection.PendingRun(
        id=run_id,
        name="small-puma",
        deployment_id="deployment-1",
        state_name=state_name,
        state_type=state_type,
        state_since=NOW - pending_for,
        started=started,
        infrastructure_pid="prefect-server:small-puma-mrkwr",
    )


def test_run_pending_for_days_is_stuck() -> None:
    assert selection.is_stuck(make_run(), now=NOW, max_pending=SIX_HOURS)


def test_run_pending_below_limit_is_not_stuck() -> None:
    assert not selection.is_stuck(make_run(pending_for=timedelta(hours=5, minutes=59)), now=NOW, max_pending=SIX_HOURS)


def test_run_pending_exactly_at_limit_is_not_stuck() -> None:
    assert not selection.is_stuck(make_run(pending_for=SIX_HOURS), now=NOW, max_pending=SIX_HOURS)


@pytest.mark.parametrize("state_type", ["RUNNING", "SCHEDULED", "CANCELLING", "COMPLETED", ""])
def test_run_outside_pending_is_never_stuck(state_type: str) -> None:
    assert not selection.is_stuck(make_run(state_type=state_type), now=NOW, max_pending=SIX_HOURS)


def test_run_that_already_started_is_never_stuck() -> None:
    assert not selection.is_stuck(make_run(started=True), now=NOW, max_pending=SIX_HOURS)


def test_naive_datetimes_are_rejected() -> None:
    with pytest.raises(ValueError, match="fuso"):
        selection.is_stuck(make_run(), now=NOW.replace(tzinfo=None), max_pending=SIX_HOURS)


@pytest.mark.parametrize(
    "max_pending", [timedelta(0), timedelta(minutes=-1), timedelta(minutes=1), timedelta(minutes=59)]
)
def test_limit_below_floor_is_rejected(max_pending: timedelta) -> None:
    with pytest.raises(ValueError, match="pelo menos 60 minutos"):
        selection.is_stuck(make_run(), now=NOW, max_pending=max_pending)
    with pytest.raises(ValueError, match="pelo menos 60 minutos"):
        selection.select_stuck_runs([], now=NOW, max_pending=max_pending)


def test_limit_at_floor_is_accepted() -> None:
    assert selection.is_stuck(make_run(), now=NOW, max_pending=timedelta(minutes=60))


def test_select_stuck_runs_keeps_only_stuck_ones_oldest_first() -> None:
    runs = [
        make_run("recent", pending_for=timedelta(minutes=10)),
        make_run("two-days", pending_for=timedelta(days=2)),
        make_run("running", state_type="RUNNING"),
        make_run("nine-days", pending_for=timedelta(days=9)),
    ]
    selected = selection.select_stuck_runs(runs, now=NOW, max_pending=SIX_HOURS)
    assert [run.id for run in selected] == ["nine-days", "two-days"]


def test_describe_run_shows_hours_and_job() -> None:
    text = selection.describe_run(make_run(pending_for=timedelta(hours=30)), now=NOW)
    assert "pending_hours=30.0" in text
    assert "job=prefect-server:small-puma-mrkwr" in text
