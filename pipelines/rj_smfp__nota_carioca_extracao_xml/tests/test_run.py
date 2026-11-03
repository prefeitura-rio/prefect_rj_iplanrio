from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime

import pytest

from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.run import ExtractionServices, run_extraction
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.soap import ExtractionRefusedError, RoutineResponse
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.tasks_db import TaskRow
from pipelines.rj_smfp__nota_carioca_extracao_xml.utils.wait import Clock, ExtractionFailedError, Limits

STARTED = datetime(2026, 10, 29, 17, 0, 5)
FINISHED = datetime(2026, 10, 29, 18, 2, 0)
LIMITS = Limits(poll_seconds=60, deadline_seconds=8 * 3600, stale_hours=6, idle_checks=2, progress_every_seconds=1800)


def row(task_id: int, status: int, finished: datetime | None = None, kind: int = 46) -> TaskRow:
    return TaskRow(task_id, kind, status, STARTED, finished, None, "resumo da tarefa", 60)


@dataclass
class World:
    answers: list[RoutineResponse]
    tasks: dict[int, list[TaskRow]] = field(default_factory=dict)
    running_sets: list[Sequence[TaskRow]] = field(default_factory=lambda: [[]])
    in_progress: list[TaskRow | None] = field(default_factory=list)
    now: float = 0.0
    calls: int = 0
    events: list[str] = field(default_factory=list)
    reads: dict[int, int] = field(default_factory=dict)
    running_reads: int = 0

    def call(self) -> RoutineResponse:
        self.events.append("call")
        answer = self.answers[min(self.calls, len(self.answers) - 1)]
        self.calls += 1
        return answer

    def get(self, task_id: int) -> TaskRow | None:
        self.events.append(f"get {task_id}")
        sequence = self.tasks[task_id]
        index = self.reads.get(task_id, 0)
        self.reads[task_id] = index + 1
        return sequence[min(index, len(sequence) - 1)]

    def running(self) -> tuple[TaskRow, ...]:
        self.events.append("running")
        value = self.running_sets[min(self.running_reads, len(self.running_sets) - 1)]
        self.running_reads += 1
        return tuple(value)

    def extraction_in_progress(self) -> TaskRow | None:
        self.events.append("in_progress")
        return self.in_progress.pop(0) if self.in_progress else None

    def sleep(self, seconds: float) -> None:
        self.now += seconds

    def services(self, notes: list[str]) -> ExtractionServices:
        return ExtractionServices(
            call=self.call,
            store=self,
            notify=notes.append,
            log=lambda _text: None,
            clock=Clock(now=lambda: self.now, sleep=self.sleep),
        )


def test_started_extraction_is_followed_until_success_and_idle() -> None:
    world = World(
        answers=[RoutineResponse("Accepted", "iniciado", 134191)],
        tasks={134191: [row(134191, 1), row(134191, 2, FINISHED)]},
    )
    notes: list[str] = []

    summary = run_extraction(world.services(notes), LIMITS)

    assert world.events[:3] == ["call", "get 134191", "get 134191"]
    assert world.events[-1] == "running"
    assert "tarefa 134191" in summary
    assert any(note.startswith("📥") and "tarefa 134191" in note for note in notes)
    assert any(note.startswith("✅") and "tarefa 134191" in note for note in notes)


def test_nothing_pending_skips_the_task_and_still_waits_for_idle() -> None:
    world = World(answers=[RoutineResponse("OK", "", 0)])
    notes: list[str] = []

    summary = run_extraction(world.services(notes), LIMITS)

    assert not any(event.startswith("get") for event in world.events)
    assert "running" in world.events
    assert "nada pendente" in summary
    assert any("nada pendente" in note for note in notes)


def test_conflict_follows_the_extraction_already_running() -> None:
    world = World(
        answers=[RoutineResponse("Conflict", "já em execução", 0)],
        tasks={555: [row(555, 1), row(555, 2, FINISHED)]},
        in_progress=[row(555, 1)],
    )
    notes: list[str] = []

    run_extraction(world.services(notes), LIMITS)

    assert world.calls == 1
    assert "get 555" in world.events
    assert any("já em execução" in note and "tarefa 555" in note for note in notes)


def test_conflict_without_visible_task_calls_again_after_the_interval() -> None:
    world = World(
        answers=[RoutineResponse("Conflict", "já em execução", 0), RoutineResponse("Accepted", "iniciado", 9)],
        tasks={9: [row(9, 2, FINISHED)]},
        in_progress=[None],
    )

    run_extraction(world.services([]), LIMITS)

    assert world.calls == 2
    assert world.now >= LIMITS.poll_seconds


def test_refused_answer_stops_before_reading_the_oracle() -> None:
    world = World(answers=[RoutineResponse("Unauthorized", "Token inválido", 0)])
    notes: list[str] = []

    with pytest.raises(ExtractionRefusedError, match="Token inválido"):
        run_extraction(world.services(notes), LIMITS)

    assert world.events == ["call"]
    assert any(note.startswith("🚨") and "Token inválido" in note for note in notes)


def test_failed_task_is_announced_and_raised_without_waiting_for_idle() -> None:
    world = World(
        answers=[RoutineResponse("Accepted", "iniciado", 134191)],
        tasks={134191: [row(134191, 3, FINISHED)]},
    )
    notes: list[str] = []

    with pytest.raises(ExtractionFailedError):
        run_extraction(world.services(notes), LIMITS)

    assert "running" not in world.events
    assert any(note.startswith("🚨") and "tarefa 134191" in note for note in notes)
