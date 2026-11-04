from typing import final, override

import pytest

from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.run import run_extraction
from pipelines.rj_smfp__nota_carioca_extracao_xml.tests.test_wait import FakeClock, FakeStore, Notices, row
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import (
    Clock,
    ExtractionFailedError,
    ExtractionRefusedError,
    ExtractionServices,
    Limits,
    RoutineResponse,
    TaskRow,
    WebserviceError,
)

LIMITS = Limits(poll_seconds=60, timeout_hours=1, stale_hours=6, idle_checks=1, progress_minutes=30)


@final
class ConflictStore(FakeStore):
    """A store that shows an extraction in progress, or none."""

    def __init__(self, in_progress: TaskRow | None, tasks: list[TaskRow | None]) -> None:
        """Keep the extraction in progress, and the tasks to read."""
        super().__init__(tasks, [()])
        self.in_progress: TaskRow | None = in_progress

    @override
    async def extraction_in_progress(self) -> TaskRow | None:
        """Return the extraction in progress."""
        return self.in_progress


def build(answers: list[RoutineResponse | Exception], store: FakeStore, notices: Notices) -> ExtractionServices:
    """Return the services, with a call that gives the answers in order."""

    async def call() -> RoutineResponse:
        """Give the next answer, or raise it."""
        answer = answers.pop(0)

        if isinstance(answer, Exception):
            raise answer

        return answer

    clock = FakeClock()

    return ExtractionServices(call, store, notices, Clock(clock.now, clock.sleep))


async def test_a_started_extraction_is_followed_and_released() -> None:
    """Follow the task to its end, wait for the release and tell Discord each step."""
    notices = Notices()
    services = build([RoutineResponse("Accepted", "", 9)], FakeStore([row(2, ended=True)], [()]), notices)

    summary = await run_extraction(services, LIMITS)

    assert summary == "Extração XML concluída: tarefa 9 em 0s"
    assert notices.lines == ["📥 Extração XML iniciada: tarefa 9", f"✅ {summary}"]


async def test_nothing_pending_still_waits_for_the_release() -> None:
    """Skip the follow-up of a task, and check the tasks 46 and 48."""
    notices = Notices()
    services = build([RoutineResponse("OK", "", 0)], FakeStore([None], [()]), notices)

    assert await run_extraction(services, LIMITS) == "Extração XML: nada pendente para extrair"
    assert len(notices.lines) == 2


async def test_a_conflict_follows_the_extraction_that_runs() -> None:
    """Follow the task of the extraction that is already in progress."""
    running = row(1)
    notices = Notices()
    store = ConflictStore(running, [row(2, ended=True)])
    services = build([RoutineResponse("Conflict", "", 0)], store, notices)

    assert (await run_extraction(services, LIMITS)).startswith("Extração XML concluída: tarefa 9")
    assert "já em execução por outro disparo" in notices.lines[0]


async def test_a_conflict_with_no_extraction_in_the_table_is_refused() -> None:
    """Call three times, then refuse, and tell Discord that it did not start."""
    notices = Notices()
    services = build([RoutineResponse("Conflict", "", 0)] * 3, ConflictStore(None, [None]), notices)

    with pytest.raises(ExtractionRefusedError, match="Conflict"):
        await run_extraction(services, LIMITS)

    assert notices.lines[0].startswith("🚨 Extração XML não iniciada")


async def test_a_web_service_error_is_told_and_raised() -> None:
    """Tell Discord that the extraction did not start, and raise the error again."""
    notices = Notices()
    services = build([WebserviceError("down")], FakeStore([None], [()]), notices)

    with pytest.raises(WebserviceError):
        await run_extraction(services, LIMITS)

    assert notices.lines == ["🚨 Extração XML não iniciada: down"]


async def test_a_failed_task_is_told_and_raised() -> None:
    """Tell Discord the failure, and raise it again so the full load stops."""
    notices = Notices()
    services = build([RoutineResponse("Accepted", "", 9)], FakeStore([row(3, ended=True)], [()]), notices)

    with pytest.raises(ExtractionFailedError):
        await run_extraction(services, LIMITS)

    assert notices.lines[-1].startswith("🚨 Extração XML falhou: tarefa 9")


async def test_a_conflict_with_no_extraction_visible_calls_again_after_the_interval() -> None:
    """Call the service again once, then follow the extraction that shows up."""
    notices = Notices()
    store = ConflictStore(None, [row(2, ended=True)])
    answers: list[RoutineResponse | Exception] = [
        RoutineResponse("Conflict", "", 0),
        RoutineResponse("Accepted", "", 9),
    ]
    services = build(answers, store, notices)

    assert (await run_extraction(services, LIMITS)).startswith("Extração XML concluída: tarefa 9")
    assert answers == []


async def test_a_refused_answer_stops_before_reading_oracle() -> None:
    """Raise the refusal without any read of `NFSE_OWNER.TAREFAS`."""
    store = FakeStore([], [])
    services = build([RoutineResponse("Unauthorized", "Token inválido", 0)], store, Notices())

    with pytest.raises(ExtractionRefusedError, match="Unauthorized: Token inválido"):
        await run_extraction(services, LIMITS)
