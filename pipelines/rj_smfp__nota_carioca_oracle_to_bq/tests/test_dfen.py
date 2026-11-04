from collections.abc import Awaitable, Callable
from datetime import datetime
from types import SimpleNamespace

import oracledb
import pytest

from pipelines.rj_smfp__nota_carioca_oracle_to_bq import tasks
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.helpers.dfen import (
    check_no_extraction,
    classify_tasks,
    describe_blocking,
    describe_stale,
    describe_task,
    format_elapsed,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.types import Dfen, DfenExtractionError, RunningTask

STARTED = datetime(2026, 10, 20, 12, 0, 0)
HOUR = 3600


def running(task_id: int = 12, kind: int = 46, seconds: int | None = HOUR + 123) -> RunningTask:
    """Return a running task from `STARTED`, with no start if `seconds` is none."""
    return RunningTask(task_id, kind, STARTED if seconds is not None else None, seconds)


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        pytest.param(0, "0s", id="zero"),
        pytest.param(3, "3s", id="seconds"),
        pytest.param(123, "2m03s", id="minutes"),
        pytest.param(3723, "1h02m03s", id="hours"),
    ],
)
def test_a_duration_is_written_in_its_largest_units(seconds: int, expected: str) -> None:
    """Write hours, minutes and seconds, without the zero units in front."""
    assert format_elapsed(seconds) == expected


@pytest.mark.parametrize(
    ("seconds", "blocks"),
    [
        pytest.param(60, True, id="a_recent_task"),
        pytest.param(6 * HOUR, True, id="at_the_limit"),
        pytest.param(6 * HOUR + 1, False, id="past_the_limit"),
        pytest.param(None, False, id="no_start"),
    ],
)
def test_only_a_recent_task_blocks_the_run(seconds: int | None, blocks: bool) -> None:
    """Block for a task that runs for at most the limit, and call the rest stuck."""
    status = classify_tasks([running(seconds=seconds)], stale_hours=6)

    assert bool(status.blocking) is blocks
    assert bool(status.stale) is not blocks


def test_the_order_of_the_tasks_stays() -> None:
    """Keep the order that the database gave, in each group."""
    tasks_ = [running(1, seconds=10), running(2, seconds=99 * HOUR), running(3, seconds=20), running(4, seconds=None)]

    status = classify_tasks(tasks_, stale_hours=6)

    assert [task.task_id for task in status.blocking] == [1, 3]
    assert [task.task_id for task in status.stale] == [2, 4]


def test_a_task_is_described_by_its_number_its_type_and_its_time() -> None:
    """Name the task, its type and for how long it runs, or say that it has no start."""
    assert describe_task(running()) == (
        "tarefa 12 (tipo 46, extração de documentos fiscais nacionais) "
        "desde 20/10/2026 12:00:00 (horário do banco, há 1h02m03s)"
    )
    assert (
        describe_task(running(kind=99, seconds=None))
        == "tarefa 12 (tipo 99, tipo desconhecido) sem data de início registrada"
    )


def test_the_refusal_names_every_task_and_says_how_to_force() -> None:
    """List the blocking tasks and the way out."""
    message = describe_blocking([running(1), running(2, kind=48)])

    assert "tarefa 1 (tipo 46" in message
    assert "tarefa 2 (tipo 48, pós-processamento" in message
    assert "dfen.check=false" in message
    assert "stuck" not in describe_stale(running(), 6)
    assert "parecer travada" in describe_stale(running(), 6)


def fetching(*found: RunningTask) -> Callable[[], Awaitable[tuple[RunningTask, ...]]]:
    """Return a `fetch` that gives the tasks."""

    async def fetch() -> tuple[RunningTask, ...]:
        """Give the tasks."""
        return found

    return fetch


async def test_the_run_goes_on_when_nothing_extracts() -> None:
    """Return the status, with no blocking task."""
    status = await check_no_extraction(fetching(), stale_hours=6)

    assert status is not None
    assert not status.blocking


async def test_the_run_stops_for_a_task_that_extracts() -> None:
    """Raise with the task that blocks."""
    with pytest.raises(DfenExtractionError, match="tarefa 12"):
        await check_no_extraction(fetching(running()), stale_hours=6)


async def test_a_stuck_task_does_not_stop_the_run() -> None:
    """Warn and go on for a task that runs for too long."""
    status = await check_no_extraction(fetching(running(seconds=99 * HOUR)), stale_hours=6)

    assert status is not None
    assert len(status.stale) == 1


async def test_the_stale_hours_must_be_at_least_one() -> None:
    """Refuse a limit of less than one hour."""
    with pytest.raises(ValueError, match="pelo menos 1"):
        await check_no_extraction(fetching(), stale_hours=0)


def failing(message: str) -> Callable[[], Awaitable[tuple[RunningTask, ...]]]:
    """Return a `fetch` that fails with an Oracle error."""

    async def fetch() -> tuple[RunningTask, ...]:
        """Raise the error."""
        raise oracledb.DatabaseError(SimpleNamespace(code=0, message=message))

    return fetch


@pytest.mark.parametrize(
    "message",
    [
        pytest.param("ORA-00942: table or view does not exist", id="a_missing_table"),
        pytest.param("ORA-01031: insufficient privileges", id="a_missing_privilege"),
    ],
)
async def test_a_table_that_the_login_cannot_read_lets_the_run_go_on(message: str) -> None:
    """Warn and return nothing: the guard is a protection, not a dependency."""
    assert await check_no_extraction(failing(message), stale_hours=6) is None


async def test_any_other_oracle_error_is_raised() -> None:
    """Raise an error that is not about access."""
    with pytest.raises(oracledb.DatabaseError):
        await check_no_extraction(failing("ORA-03113: end-of-file on communication channel"), stale_hours=6)


async def test_a_run_with_the_check_off_reads_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Skip the read of the table when `dfen.check` is false."""

    def refuse(database: object) -> None:
        """Fail the test if the database is read."""
        del database
        raise AssertionError("the check is off")

    monkeypatch.setattr(tasks, "connect_database", refuse)

    await tasks.check_dfen_extraction_task.fn(
        {"type": "oracle", "host": "h", "port": 1521, "database": "d"}, Dfen(check=False)
    )
