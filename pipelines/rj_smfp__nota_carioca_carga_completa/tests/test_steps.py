from collections.abc import Mapping, Sequence

import pytest

from pipelines.rj_smfp__nota_carioca_carga_completa.helpers import steps
from pipelines.rj_smfp__nota_carioca_carga_completa.types import (
    ActiveRun,
    ActiveRunsError,
    ChainStoppedError,
    ChildRun,
    Step,
)

FIRST = Step("Oracle → BigQuery", "flow/first", 3600)
SECOND = Step("BigQuery → Oracle", "flow/second", 7200)


class Recorder:
    """What the fake tasks saw: the lines of Discord and the runs that started."""

    def __init__(self) -> None:
        """Start with nothing."""
        self.lines: list[str] = []
        self.started: list[tuple[str, int]] = []


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    """Replace the notice task by one that records its lines."""
    seen = Recorder()

    async def notify(message: str, *, enabled: bool) -> None:
        """Record the line, if Discord is on."""
        if enabled:
            seen.lines.append(message)

    monkeypatch.setattr(steps, "notify_task", notify)

    return seen


def children(*states: str) -> list[ChildRun]:
    """Return child runs in the given states."""
    return [ChildRun(f"id{n}", f"table{n}", state, state.title()) for n, state in enumerate(states)]


def fake_runner(monkeypatch: pytest.MonkeyPatch, seen: Recorder, states: Sequence[str]) -> None:
    """Replace the task that runs the children by one that gives the states."""

    async def run(
        deployment: str, parameter_sets: Sequence[Mapping[str, object]], timeout_seconds: int, tags: Sequence[str]
    ) -> list[ChildRun]:
        """Record the start and give the children."""
        del tags
        seen.started.append((deployment, len(parameter_sets)))
        assert timeout_seconds > 0

        return children(*states)

    monkeypatch.setattr(steps, "run_children_task", run)


async def test_a_busy_deployment_stops_the_chain_with_a_line_for_discord(
    monkeypatch: pytest.MonkeyPatch, recorder: Recorder
) -> None:
    """Raise `ChainStoppedError` and tell Discord that the deployment has a run."""

    async def busy(deployment: str) -> None:
        """Say that the deployment is busy."""
        del deployment
        raise ActiveRunsError("1 execução ativa", (ActiveRun("i", "r", "RUNNING", "Running", None),))

    monkeypatch.setattr(steps, "ensure_idle_task", busy)

    with pytest.raises(ChainStoppedError, match="Carga completa não iniciada: Oracle → BigQuery"):
        await steps.guard(FIRST, enabled=True)

    assert recorder.lines == ["⛔ Carga completa não iniciada: Oracle → BigQuery já tem execução ativa"]


async def test_an_idle_deployment_lets_the_chain_go_on(monkeypatch: pytest.MonkeyPatch, recorder: Recorder) -> None:
    """Say nothing when the deployment is free."""

    async def idle(deployment: str) -> None:
        """Give no error."""
        del deployment

    monkeypatch.setattr(steps, "ensure_idle_task", idle)

    await steps.guard(FIRST, enabled=True)

    assert recorder.lines == []


async def test_a_step_with_every_child_completed_goes_on(monkeypatch: pytest.MonkeyPatch, recorder: Recorder) -> None:
    """Start one child for each set of parameters, and stop for nothing."""
    fake_runner(monkeypatch, recorder, ["COMPLETED", "COMPLETED"])

    await steps.run_step(FIRST, [{"a": 1}, {"a": 2}], [], SECOND, enabled=True)

    assert recorder.started == [("flow/first", 2)]
    assert recorder.lines == []


@pytest.mark.parametrize(
    ("enabled", "lines"),
    [pytest.param(True, 1, id="discord_on"), pytest.param(False, 0, id="discord_off")],
)
async def test_a_failed_child_stops_the_chain_before_the_next_step(
    monkeypatch: pytest.MonkeyPatch, recorder: Recorder, enabled: bool, lines: int
) -> None:
    """Raise with the child that failed and the step that was skipped."""
    fake_runner(monkeypatch, recorder, ["COMPLETED", "FAILED"])

    with pytest.raises(ChainStoppedError, match="table1 terminou em Failed; BigQuery → Oracle não foi disparado"):
        await steps.run_step(FIRST, [{}, {}], [], SECOND, enabled=enabled)

    assert len(recorder.lines) == lines
