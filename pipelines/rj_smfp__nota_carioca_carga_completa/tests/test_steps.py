from collections.abc import Mapping, Sequence

import pytest

from pipelines.rj_smfp__nota_carioca_carga_completa.helpers import steps
from pipelines.rj_smfp__nota_carioca_carga_completa.types import (
    ActiveRun,
    ActiveRunsError,
    ChainStoppedError,
    ChildRun,
    Plan,
    Step,
)

EXTRACTION = Step("Extração XML", "flow/extraction", 7200, done="concluída")
FIRST = Step("Oracle → BigQuery", "flow/first", 3600, per_schedule=True)
SECOND = Step("BigQuery → Oracle", "flow/second", 7200)
ROLES = ("111", "222")
PLAN = Plan((FIRST, SECOND), ("disparo:carga-completa",), 30, "parent-run", ROLES)
FULL = Plan((EXTRACTION, FIRST, SECOND), ("disparo:carga-completa",), 30, "parent-run", ROLES)


class Recorder:
    """What the fake tasks saw: the lines of Discord, the runs and the waits."""

    def __init__(self) -> None:
        """Start with nothing."""
        self.lines: list[str] = []
        self.roles: list[Sequence[str]] = []
        self.started: list[tuple[str, int, int]] = []
        self.guarded: list[str] = []
        self.paused: list[int] = []


def children(*states: str, position: int = 1) -> list[ChildRun]:
    """Return child runs in the given states, with positions from `position`."""
    return [
        ChildRun(f"id{n}", f"table{n}", state, state.title(), n + position - 1)
        for n, state in enumerate(states, start=1)
    ]


class World:
    """The tasks of the chain, replaced by fakes with the answers that a test sets."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Replace every task that the chain uses."""
        self.seen: Recorder = Recorder()
        self.own: dict[str, list[ChildRun]] = {}
        self.outcomes: dict[str, list[str]] = {}
        self.busy: set[str] = set()
        self.schedules: int = 2
        monkeypatch.setattr(steps, "notify_task", self.notify)
        monkeypatch.setattr(steps, "ensure_idle_task", self.ensure_idle)
        monkeypatch.setattr(steps, "find_children_task", self.find)
        monkeypatch.setattr(steps, "read_parameter_sets_task", self.read_sets)
        monkeypatch.setattr(steps, "run_children_task", self.run)
        monkeypatch.setattr(steps, "pause_task", self.pause)

    async def notify(self, message: str, *, enabled: bool, roles: Sequence[str] = ()) -> None:
        """Record the line, if Discord is on."""
        if enabled:
            self.seen.lines.append(message)
            self.seen.roles.append(roles)

    async def ensure_idle(self, deployment: str) -> None:
        """Raise for a deployment that the test made busy."""
        self.seen.guarded.append(deployment)

        if deployment in self.busy:
            raise ActiveRunsError("1 execução ativa", (ActiveRun("i", "r", "RUNNING", "Running", None),))

    async def find(self, step: Step, parent_run_id: str | None) -> list[ChildRun]:
        """Give the children that the test says were started before."""
        assert parent_run_id == "parent-run"

        return self.own.get(step.deployment, [])

    async def read_sets(self, deployment: str) -> list[dict[str, object]]:
        """Give one set of parameters for each schedule that the test sets."""
        del deployment

        return [{"n": n} for n in range(self.schedules)]

    async def run(
        self,
        step: Step,
        parameter_sets: Sequence[Mapping[str, object]],
        tags: Sequence[str],
        existing: Sequence[ChildRun],
    ) -> list[ChildRun]:
        """Record the start and give the outcome that the test sets for the step."""
        self.seen.started.append((step.deployment, len(parameter_sets), len(existing)))
        assert tags == ("disparo:carga-completa",)

        return children(*self.outcomes.get(step.deployment, ["COMPLETED"] * len(parameter_sets)))

    async def pause(self, seconds: int) -> None:
        """Record the wait."""
        self.seen.paused.append(seconds)


@pytest.fixture
def world(monkeypatch: pytest.MonkeyPatch) -> World:
    """Return the fake world of the chain."""
    return World(monkeypatch)


async def test_a_busy_deployment_stops_the_chain_with_a_line_for_discord(world: World) -> None:
    """Raise `ChainStoppedError`, and tell Discord with the alert roles."""
    world.busy.add(FIRST.deployment)

    with pytest.raises(ChainStoppedError, match="Carga completa não iniciada: Oracle → BigQuery"):
        await steps.guard(FIRST, "Carga completa não iniciada", PLAN, enabled=True)

    assert world.seen.lines == ["⛔ Carga completa não iniciada: Oracle → BigQuery já tem execução ativa (r (Running))"]
    assert world.seen.roles == [ROLES]


async def test_an_idle_deployment_lets_the_chain_go_on(world: World) -> None:
    """Say nothing when the deployment is free."""
    await steps.guard(FIRST, "x", PLAN, enabled=True)

    assert world.seen.lines == []


async def test_a_step_with_every_child_completed_goes_on(world: World) -> None:
    """Start one child for each set of parameters, and stop for nothing."""
    await steps.run_step(FIRST, PLAN, [{"a": 1}, {"a": 2}], [], enabled=True)

    assert world.seen.started == [("flow/first", 2, 0)]
    assert world.seen.lines == []


@pytest.mark.parametrize(
    ("enabled", "lines"),
    [pytest.param(True, 1, id="discord_on"), pytest.param(False, 0, id="discord_off")],
)
async def test_a_failed_child_stops_the_chain_before_the_next_step(world: World, enabled: bool, lines: int) -> None:
    """Raise with the child that failed and the step that was skipped."""
    world.outcomes[FIRST.deployment] = ["COMPLETED", "FAILED"]

    with pytest.raises(ChainStoppedError, match="table2 terminou em Failed; BigQuery → Oracle não foi disparado"):
        await steps.run_step(FIRST, PLAN, [{}, {}], [], enabled=enabled)

    assert len(world.seen.lines) == lines


async def test_a_new_chain_runs_every_step_in_order_with_a_wait_between(world: World) -> None:
    """Guard each deployment, start each step once, and tell Discord the start."""
    await steps.run_chain(FULL, enabled=True)

    assert [item[0] for item in world.seen.started] == ["flow/extraction", "flow/first", "flow/second"]
    assert world.seen.paused == [30, 30]
    assert (
        world.seen.lines[0]
        == "🔗 Carga completa iniciada: Extração XML, depois Oracle → BigQuery, depois BigQuery → Oracle"
    )
    assert world.seen.lines[1] == "⏭️ Extração XML concluída; Oracle → BigQuery começa em 30 s"
    assert world.seen.lines[-1].startswith("✅ Carga completa concluída em ")


async def test_a_restart_does_not_repeat_the_steps_that_completed(world: World) -> None:
    """Skip the extraction that completed, wait for the load, start the last."""
    world.own = {EXTRACTION.deployment: children("COMPLETED"), FIRST.deployment: children("COMPLETED", "RUNNING")}

    await steps.run_chain(FULL, enabled=True)

    assert world.seen.started == [("flow/first", 2, 2), ("flow/second", 1, 0)]
    assert world.seen.guarded == ["flow/second", "flow/second"]
    assert world.seen.lines[0].startswith("🔁 Carga completa retomada depois de um reinício do orquestrador: ")
    assert not any("Extração XML concluída" in line for line in world.seen.lines)


async def test_a_step_that_stopped_half_way_starts_only_the_missing_children(world: World) -> None:
    """Pass the child that exists, so that the task does not start it twice."""
    world.own = {FIRST.deployment: children("COMPLETED")}

    await steps.run_chain(PLAN, enabled=False)

    assert world.seen.started[0] == ("flow/first", 2, 1)


async def test_a_busy_deployment_stops_a_new_chain_before_any_step_starts(world: World) -> None:
    """Refuse to start when a later deployment has a run."""
    world.busy.add(SECOND.deployment)

    with pytest.raises(ChainStoppedError, match="Carga completa não iniciada: BigQuery → Oracle"):
        await steps.run_chain(PLAN, enabled=True)

    assert world.seen.started == []
