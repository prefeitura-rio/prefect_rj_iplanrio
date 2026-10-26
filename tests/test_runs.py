from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import final
from uuid import UUID

import pytest
from prefect.client.schemas.objects import State

from utils import runs
from utils.runs import PrefectRuns, start_deployment_run

RUN_ID = "7f1d6c1e-6f4b-4a53-9b1e-3b7d1d6c1e00"


@final
class FakeClient:
    """A Prefect client that gives one flow run and records the state it is set to."""

    def __init__(self, state: object | None) -> None:
        """Store the arguments."""
        self.state = state
        self.set_to: list[tuple[UUID, str, bool]] = []

    def read_flow_run(self, run_id: UUID) -> SimpleNamespace:
        return SimpleNamespace(id=run_id, state=self.state, end_time=datetime(2026, 7, 17, tzinfo=UTC))

    def set_flow_run_state(self, run_id: UUID, state: State, *, force: bool) -> None:
        self.set_to.append((run_id, str(state.name), force))


def use_client(monkeypatch: pytest.MonkeyPatch, client: FakeClient) -> None:
    """Make `get_client` give the fake client."""

    @contextmanager
    def get_client(*, sync_client: bool):
        assert sync_client
        yield client

    monkeypatch.setattr(runs, "get_client", get_client)


def test_a_started_run_has_its_link_when_the_ui_url_is_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start the deployment as a subflow with no wait, and build the link well.

    A slash is never doubled.
    """
    calls: list[dict[str, object]] = []

    def run_deployment(**arguments: object) -> SimpleNamespace:
        calls.append(arguments)

        return SimpleNamespace(id=RUN_ID, name="happy-fox")

    monkeypatch.setattr(runs, "run_deployment", run_deployment)
    monkeypatch.setattr(runs, "PREFECT_UI_URL", SimpleNamespace(value=lambda: "https://ui.example/"))

    started = start_deployment_run("flow/deployment", {"a": 1})

    assert calls == [{"name": "flow/deployment", "parameters": {"a": 1}, "timeout": 0, "as_subflow": True}]
    assert (started.id, started.name, started.url) == (
        RUN_ID,
        "happy-fox",
        f"https://ui.example/runs/flow-run/{RUN_ID}",
    )


def test_a_started_run_has_no_link_without_a_ui_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Leave the link out when `PREFECT_UI_URL` is empty."""
    monkeypatch.setattr(runs, "run_deployment", lambda **_: SimpleNamespace(id=RUN_ID, name="n"))
    monkeypatch.setattr(runs, "PREFECT_UI_URL", SimpleNamespace(value=lambda: ""))

    assert start_deployment_run("f/d", {}).url is None


def test_the_status_of_a_run_is_read_from_its_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give the type, the name and the message of the state, and the end of the run."""
    state = SimpleNamespace(type=SimpleNamespace(value="COMPLETED"), name="Completed", message="done")
    use_client(monkeypatch, FakeClient(state))

    status = PrefectRuns().read_status(RUN_ID)

    assert (status.state_type, status.state_name, status.message) == ("COMPLETED", "Completed", "done")
    assert status.end_time == datetime(2026, 7, 17, tzinfo=UTC)


def test_a_run_with_no_state_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise `ValueError` when the run has no state."""
    use_client(monkeypatch, FakeClient(None))

    with pytest.raises(ValueError, match="no state"):
        PrefectRuns().read_status(RUN_ID)


def test_a_cancelled_run_is_moved_to_the_cancelling_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the state `Cancelling`."""
    client = FakeClient(None)
    use_client(monkeypatch, client)

    PrefectRuns().cancel(RUN_ID)

    assert client.set_to == [(UUID(RUN_ID), "Cancelling", True)]
