from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

import pytest

from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.tasks_db import TaskRow
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.wait import (
    Clock,
    ExtractionFailedError,
    ExtractionTimedOutError,
    Limits,
    wait_for_task,
    wait_until_idle,
)

TASK = 134191
STARTED = datetime(2026, 10, 29, 17, 0, 5)
FINISHED = datetime(2026, 10, 29, 18, 2, 0)
LIMITS = Limits(poll_seconds=60, deadline_seconds=8 * 3600, stale_hours=6, idle_checks=2, progress_every_seconds=1800)


def row(  # noqa: PLR0913, PLR0917
    status: int,
    finished: datetime | None = None,
    kind: int = 46,
    task_id: int = TASK,
    progress: int | None = None,
    summary: str | None = None,
    running_seconds: int | None = 60,
) -> TaskRow:
    return TaskRow(
        task_id=task_id,
        kind=kind,
        status=status,
        started_at=STARTED,
        finished_at=finished,
        progress=progress,
        summary=summary,
        running_seconds=running_seconds,
    )


@dataclass
class FakeStore:
    """Devolve, a cada leitura, o próximo estado da sequência (o último se repete)."""

    tasks: list[TaskRow | None] = field(default_factory=list)
    running_sets: list[Sequence[TaskRow]] = field(default_factory=list)
    reads: int = 0
    running_reads: int = 0

    def get(self, task_id: int) -> TaskRow | None:
        assert task_id == TASK
        value = self.tasks[min(self.reads, len(self.tasks) - 1)]
        self.reads += 1
        return value

    def running(self) -> tuple[TaskRow, ...]:
        value = self.running_sets[min(self.running_reads, len(self.running_sets) - 1)]
        self.running_reads += 1
        return tuple(value)

    def extraction_in_progress(self) -> TaskRow | None:
        return None


@dataclass
class FakeClock:
    now: float = 0.0
    slept: list[float] = field(default_factory=list)

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds

    def clock(self) -> Clock:
        return Clock(now=lambda: self.now, sleep=self.sleep)


@dataclass
class Sink:
    notes: list[str] = field(default_factory=list)
    logs: list[str] = field(default_factory=list)


def wait(store: FakeStore, clock: FakeClock, sink: Sink, limits: Limits = LIMITS) -> TaskRow:
    return wait_for_task(
        store, TASK, limits, clock.clock(), sink.notes.append, sink.logs.append, limits.deadline_seconds
    )


def test_wait_for_task_returns_when_finished_with_success_and_polls_at_the_interval() -> None:
    store = FakeStore(tasks=[row(0), row(1), row(1), row(2, FINISHED)])
    clock = FakeClock()

    done = wait(store, clock, Sink())

    assert done.status == 2
    assert store.reads == 4
    assert clock.slept == [60, 60, 60]


def test_wait_for_task_keeps_waiting_while_success_has_no_end_date() -> None:
    store = FakeStore(tasks=[row(2, None), row(2, FINISHED)])

    assert wait(store, FakeClock(), Sink()).finished_at == FINISHED
    assert store.reads == 2


def test_wait_for_task_keeps_waiting_while_the_task_is_not_visible_yet() -> None:
    store = FakeStore(tasks=[None, row(1), row(2, FINISHED)])

    assert wait(store, FakeClock(), Sink()).status == 2


@pytest.mark.parametrize(
    ("final", "described"),
    [
        (row(3, FINISHED, summary="Falha ao gravar notas"), "concluída com erro"),
        (row(5, FINISHED), "interrompida"),
        (row(5, None), "interrompida"),
        (row(3, None), "concluída com erro"),
        (row(1, FINISHED), "em execução"),
    ],
)
def test_wait_for_task_fails_when_the_task_ends_without_success(final: TaskRow, described: str) -> None:
    store = FakeStore(tasks=[row(1), final])
    sink = Sink()

    with pytest.raises(ExtractionFailedError) as error:
        wait(store, FakeClock(), sink)

    assert f"tarefa {TASK}" in str(error.value)
    assert f"status {final.status} ({described})" in str(error.value)
    assert f"tarefa {TASK}" in error.value.announcement


def test_wait_for_task_failure_carries_the_task_summary() -> None:
    store = FakeStore(tasks=[row(3, FINISHED, summary="Falha ao gravar notas: ORA-00001")])

    with pytest.raises(ExtractionFailedError, match="Falha ao gravar notas: ORA-00001"):
        wait(store, FakeClock(), Sink())


def test_wait_for_task_times_out_at_the_deadline_without_touching_the_task() -> None:
    store = FakeStore(tasks=[row(1)])
    clock = FakeClock()
    limits = Limits(poll_seconds=60, deadline_seconds=300, stale_hours=6, idle_checks=2, progress_every_seconds=1800)

    with pytest.raises(ExtractionTimedOutError) as error:
        wait(store, clock, Sink(), limits)

    assert clock.now <= 300
    assert f"tarefa {TASK}" in str(error.value)
    assert "segue no Oracle" in error.value.announcement


def test_wait_for_task_announces_progress_every_interval() -> None:
    store = FakeStore(tasks=[row(1, progress=10)] * 70 + [row(2, FINISHED, progress=100)])
    sink = Sink()

    wait(store, FakeClock(), sink)

    progress_notes = [note for note in sink.notes if note.startswith("⏳")]
    assert len(progress_notes) == 2
    assert "tarefa 134191" in progress_notes[0]
    assert "10%" in progress_notes[0]


def idle(store: FakeStore, clock: FakeClock, sink: Sink, limits: Limits = LIMITS) -> None:
    wait_until_idle(store, limits, clock.clock(), sink.notes.append, sink.logs.append, limits.deadline_seconds)


def test_wait_until_idle_waits_for_post_processing_and_needs_consecutive_idle_checks() -> None:
    post = row(1, kind=48, task_id=134200)
    store = FakeStore(running_sets=[[post], [post], [], [post], [], []])
    clock = FakeClock()

    idle(store, clock, Sink())

    assert store.running_reads == 6
    assert clock.slept == [60] * 5


def test_wait_until_idle_ignores_stale_tasks_and_logs_them() -> None:
    stale = row(1, kind=48, task_id=99, running_seconds=7 * 3600)
    store = FakeStore(running_sets=[[stale]])
    sink = Sink()

    idle(store, FakeClock(), sink)

    assert store.running_reads == 2
    assert any("tarefa 99" in line and "travada" in line for line in sink.logs)


def test_wait_until_idle_times_out_while_something_keeps_running() -> None:
    post = row(1, kind=48, task_id=134200)
    store = FakeStore(running_sets=[[post]])
    limits = Limits(poll_seconds=60, deadline_seconds=180, stale_hours=6, idle_checks=2, progress_every_seconds=1800)

    with pytest.raises(ExtractionTimedOutError, match="tarefa 134200"):
        idle(store, FakeClock(), Sink(), limits)
