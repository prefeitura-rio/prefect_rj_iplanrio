from datetime import datetime
from typing import final

import pytest

from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.wait import (
    filter_blocking,
    finished_successfully,
    finished_without_success,
    wait_for_task,
    wait_until_idle,
)
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import (
    Clock,
    ExtractionFailedError,
    ExtractionServices,
    ExtractionTimedOutError,
    Limits,
    RoutineResponse,
    TaskRow,
)

END = datetime(2026, 10, 20, 13, 0, 0)
HOUR = 3600


def row(status: int = 1, *, ended: bool = False, seconds: int | None = 60, summary: str | None = None) -> TaskRow:
    """Return a task of the extraction with the status, ended or not."""
    return TaskRow(9, 46, status, None, END if ended else None, 10, summary, seconds)


@final
class FakeClock:
    """A clock that moves only when someone waits."""

    def __init__(self) -> None:
        """Start at time zero."""
        self.moment: float = 0.0

    def now(self) -> float:
        """Return the time now."""
        return self.moment

    async def sleep(self, seconds: float) -> None:
        """Move the time."""
        self.moment += seconds


class FakeStore:
    """A store that answers from lists, and repeats the last answer."""

    def __init__(
        self, tasks: list[TaskRow | None] | None = None, running: list[tuple[TaskRow, ...]] | None = None
    ) -> None:
        """Keep the answers of `get` and of `running`."""
        self.tasks: list[TaskRow | None] = tasks or []
        self.runs: list[tuple[TaskRow, ...]] = running or []

    async def get(self, task_id: int) -> TaskRow | None:
        """Return the next task."""
        del task_id
        return self.tasks.pop(0) if len(self.tasks) > 1 else self.tasks[0]

    async def running(self) -> tuple[TaskRow, ...]:
        """Return the next list of running tasks."""
        return self.runs.pop(0) if len(self.runs) > 1 else self.runs[0]

    async def extraction_in_progress(self) -> TaskRow | None:
        """Return nothing."""
        return None


@final
class Notices:
    """The lines that the follow-up posts."""

    def __init__(self) -> None:
        """Start with no line."""
        self.lines: list[str] = []

    async def __call__(self, message: str) -> None:
        """Keep the line."""
        self.lines.append(message)


LIMITS = Limits(poll_seconds=60, timeout_hours=1, stale_hours=6, idle_checks=2, progress_minutes=5)


def services_of(store: FakeStore, clock: FakeClock, notices: Notices) -> ExtractionServices:
    """Return services with a fake store, clock and notices, and no web service."""

    async def refuse() -> RoutineResponse:
        """Fail the test if the web service is called."""
        raise AssertionError("the wait never calls the service")

    return ExtractionServices(refuse, store, notices, Clock(clock.now, clock.sleep))


@pytest.mark.parametrize(
    ("task", "success", "failure"),
    [
        pytest.param(row(2, ended=True), True, False, id="success"),
        pytest.param(row(2), False, False, id="success_status_without_end"),
        pytest.param(row(1), False, False, id="running"),
        pytest.param(row(3), False, True, id="error_status"),
        pytest.param(row(5), False, True, id="interrupted_status"),
        pytest.param(row(1, ended=True), False, True, id="ended_with_another_status"),
    ],
)
def test_a_task_ends_in_success_or_failure_by_status_and_end_date(task: TaskRow, success: bool, failure: bool) -> None:
    """Call it a success only with status 2 and an end date."""
    assert finished_successfully(task) is success
    assert finished_without_success(task) is failure


def test_a_task_that_started_long_ago_does_not_block() -> None:
    """Keep the recent tasks, and drop those past the limit or with no start."""
    rows = [row(seconds=10), row(seconds=7 * HOUR), row(seconds=None), row(seconds=6 * HOUR)]

    assert [item.running_seconds for item in filter_blocking(rows, 6 * HOUR)] == [10, 6 * HOUR]


async def test_the_wait_returns_the_task_that_ends_in_success() -> None:
    """Read until status 2 and an end date, with a progress line on the way."""
    clock, notices = FakeClock(), Notices()
    store = FakeStore([row(), None, row(), row(), row(), row(), row(), row(2, ended=True)])

    done = await wait_for_task(services_of(store, clock, notices), 9, LIMITS, deadline=HOUR)

    assert done.status == 2
    assert len(notices.lines) == 1
    assert notices.lines[0].endswith("há 1min 00s · progresso 10%")


async def test_the_wait_fails_with_the_summary_of_a_failed_task() -> None:
    """Raise with the status, the end and the summary of the task."""
    store = FakeStore([row(3, ended=True, summary="ORA-1")])

    with pytest.raises(ExtractionFailedError, match=r"status 3 \(concluída com erro\).*ORA-1") as error:
        await wait_for_task(services_of(store, FakeClock(), Notices()), 9, LIMITS, deadline=HOUR)

    assert error.value.announcement.startswith("🚨 ")


async def test_the_wait_gives_up_at_the_deadline_and_leaves_the_task() -> None:
    """Raise a timeout that says the task still runs in Oracle."""
    with pytest.raises(ExtractionTimedOutError, match="não terminou dentro do limite de 1h 00min") as error:
        await wait_for_task(services_of(FakeStore([row()]), FakeClock(), Notices()), 9, LIMITS, deadline=HOUR)

    assert "não foi interrompida" in error.value.announcement


async def test_the_release_needs_idle_checks_in_a_row() -> None:
    """Reset the count when a task runs again, and say once that it waits."""
    clock, notices = FakeClock(), Notices()
    store = FakeStore(running=[(), (row(),), (), ()])

    await wait_until_idle(services_of(store, clock, notices), LIMITS, deadline=HOUR)

    assert clock.moment == 3 * 60
    assert len(notices.lines) == 1


async def test_a_stuck_task_does_not_hold_the_release() -> None:
    """Count a task past `stale_hours` as idle."""
    store = FakeStore(running=[(row(seconds=9 * HOUR),)])

    await wait_until_idle(services_of(store, FakeClock(), Notices()), LIMITS, deadline=HOUR)


async def test_the_release_gives_up_at_the_deadline() -> None:
    """Raise a timeout that lists the task that still runs."""
    with pytest.raises(ExtractionTimedOutError, match="ainda em execução no limite de 1h 00min: tarefa 9"):
        await wait_until_idle(services_of(FakeStore(running=[(row(),)]), FakeClock(), Notices()), LIMITS, deadline=HOUR)
