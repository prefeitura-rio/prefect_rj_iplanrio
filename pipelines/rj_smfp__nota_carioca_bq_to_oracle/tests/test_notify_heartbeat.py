import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import final, override

import pytest

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers import discord as discord_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers import notify as notify_module
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord import DiscordStatusMessage
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_embed import build_payload
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.discord_view import RunStatus
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.notify import LoadNotifier, NotifierConfig
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.notify_view import RunState, TableState, build_view
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.stages import Step, TableStep

NOW = datetime(2026, 10, 9, 15, 0, 0, tzinfo=UTC)
INTERVAL = 0.05
WAIT_SECONDS = 2.0
THREAD_NAME = "discord-heartbeat"


@dataclass(frozen=True)
class Publish:
    payload: Mapping[str, object]
    force: bool


class RecordingMessage(DiscordStatusMessage):
    """A fake client that records each `publish` and `announce`, with no HTTP."""

    def __init__(self, on_publish: Callable[[bool], None] | None = None) -> None:
        """Store the arguments."""
        super().__init__("https://discord.example/api/webhooks/1/t")
        self.publishes: list[Publish] = []
        self.announcements: list[Mapping[str, object]] = []
        self.on_publish: Callable[[bool], None] | None = on_publish
        self.changed: threading.Condition = threading.Condition()

    @override
    def publish(self, payload: Mapping[str, object], force: bool = False) -> None:
        with self.changed:
            self.publishes.append(Publish(payload, force))
            self.changed.notify_all()
        if self.on_publish is not None:
            self.on_publish(force)

    @override
    def announce(self, payload: Mapping[str, object]) -> None:
        self.announcements.append(payload)

    def wait_for(self, count: int) -> bool:
        """Wait up to a limit for `count` publications, or return `False`."""
        with self.changed:
            return self.changed.wait_for(lambda: len(self.publishes) >= count, WAIT_SECONDS)


@final
class Clock:
    """Replace the `time` module of the notifier to control the monotonic clock."""

    def __init__(self) -> None:
        """Store the arguments."""
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now


def make(
    message: DiscordStatusMessage | None, heartbeat_seconds: float = INTERVAL, tables: tuple[str, ...] = ("A",)
) -> LoadNotifier:
    config = NotifierConfig(
        "nota_carioca_staging", None, "nota_carioca_staging", tables, 2, False, "full", True, heartbeat_seconds
    )
    return LoadNotifier(config, message)


def heartbeat_threads() -> set[threading.Thread]:
    return {thread for thread in threading.enumerate() if thread.name == THREAD_NAME and thread.is_alive()}


def fields_by_name(payload: Mapping[str, object]) -> dict[str, str]:
    embeds = payload["embeds"]
    assert isinstance(embeds, list)
    fields = embeds[0]["fields"]
    assert isinstance(fields, list)
    return {field["name"]: field["value"] for field in fields}


def etapa_atual(notifier: LoadNotifier) -> str:
    return fields_by_name(build_payload(build_view(notifier.state)))["🧭 Etapa atual"]


@pytest.fixture
def warnings(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []
    monkeypatch.setattr(discord_module, "warn", seen.append)
    return seen


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Clock:
    fake = Clock()
    monkeypatch.setattr(notify_module, "time", fake)
    return fake


# --- heartbeat thread


def test_heartbeat_republishes_the_view_while_nothing_else_happens(warnings: list[str]) -> None:
    # Given a live notifier that is begun and then goes idle (no step change, no
    # progress)
    """Check that heartbeat republishes the view while nothing else happens."""
    before = heartbeat_threads()
    message = RecordingMessage()
    notifier = make(message)
    # When it sits idle for a few intervals
    with notifier.guard():
        notifier.begin()
        reached = message.wait_for(4)
        started = heartbeat_threads() - before
    # Then the creation was forced and every later publish is a plain (throttled) one
    # from a daemon thread
    assert reached
    assert [publish.force for publish in message.publishes[:4]] == [True, False, False, False]
    assert len(started) == 1
    assert all(thread.daemon for thread in started)
    assert warnings == []


def test_begin_twice_keeps_a_single_heartbeat() -> None:
    # Given a notifier begun twice
    """Check that begin twice keeps a single heartbeat."""
    before = heartbeat_threads()
    notifier = make(RecordingMessage())
    with notifier.guard():
        notifier.begin()
        notifier.begin()
        # Then there is still one heartbeat thread
        assert len(heartbeat_threads() - before) == 1


def succeed(notifier: LoadNotifier) -> None:
    notifier.succeed()


def fail_with_error(notifier: LoadNotifier) -> None:
    notifier.fail(RuntimeError("falhou"))


def fail_with_interrupt(notifier: LoadNotifier) -> None:
    notifier.fail(KeyboardInterrupt())


@pytest.mark.parametrize(
    "finish",
    [
        pytest.param(succeed, id="succeed"),
        pytest.param(fail_with_error, id="fail"),
        pytest.param(fail_with_interrupt, id="cancel"),
    ],
)
def test_heartbeat_stops_when_the_run_is_finalized(finish: Callable[[LoadNotifier], None]) -> None:
    # Given a notifier with a heartbeat that already beat at least once
    """Check that heartbeat stops when the run is finalized."""
    before = heartbeat_threads()
    message = RecordingMessage()
    notifier = make(message)
    notifier.begin()
    assert message.wait_for(2)
    # When the run is finalized
    finish(notifier)
    # Then the thread is gone, the last publish is the forced final one, and nothing is
    # published afterwards
    assert not heartbeat_threads() - before
    assert message.publishes[-1].force
    assert len(message.announcements) == 1
    published = len(message.publishes)
    time.sleep(2 * INTERVAL)
    assert len(message.publishes) == published


def run_until_beat(notifier: LoadNotifier, message: RecordingMessage, *, fails: bool) -> None:
    with notifier.guard():
        notifier.begin()
        assert message.wait_for(2)
        if fails:
            raise RuntimeError("sqlldr exited 1")


@pytest.mark.parametrize("fails", [False, True], ids=["normal exit", "exception"])
def test_heartbeat_stops_when_the_guard_exits(*, fails: bool) -> None:
    # Given a notifier that begins inside the guard and beats at least once
    """Check that heartbeat stops when the guard exits."""
    before = heartbeat_threads()
    message = RecordingMessage()
    notifier = make(message)
    # When the guard block ends, normally or by an exception
    if fails:
        with pytest.raises(RuntimeError, match="sqlldr"):
            run_until_beat(notifier, message, fails=fails)
    else:
        run_until_beat(notifier, message, fails=fails)
    # Then the thread is gone and the message stopped receiving publishes
    assert not heartbeat_threads() - before
    published = len(message.publishes)
    time.sleep(2 * INTERVAL)
    assert len(message.publishes) == published


def test_no_heartbeat_when_discord_is_off_or_the_interval_is_zero() -> None:
    # Given a notifier without a client, one created with the notifications disabled,
    # and one with interval 0
    """Check that no heartbeat when discord is off or the interval is zero."""
    before = heartbeat_threads()
    without_client = make(None)
    disabled = LoadNotifier.create(
        NotifierConfig("ds", None, "ds", ("A",), 2, False, "full", enabled=False, heartbeat_seconds=INTERVAL)
    )
    no_interval = make(RecordingMessage(), heartbeat_seconds=0)
    # When each begins and finishes
    for notifier in (without_client, disabled, no_interval):
        with notifier.guard():
            notifier.begin()
            # Then no thread was started
            assert not heartbeat_threads() - before


def test_a_failing_publish_neither_kills_the_heartbeat_nor_raises(warnings: list[str]) -> None:
    # Given a client whose plain publishes blow up (the forced ones, at the start and
    # the end, work)
    """Check that a failing publish neither kills the heartbeat nor raises."""

    def explode(force: bool) -> None:
        if not force:
            raise RuntimeError("boom")

    message = RecordingMessage(on_publish=explode)
    notifier = make(message)
    # When the heartbeat tries several times and the run still finishes
    with notifier.guard():
        notifier.begin()
        survived = message.wait_for(5)
    # Then the thread kept beating, nothing reached the flow, and the failure was warned
    # once
    assert survived
    assert notifier.state.status is RunStatus.SUCCESS
    assert warnings == ["Batimento do Discord falhou: RuntimeError: boom"]


# --- elapsed time


def test_heartbeat_makes_the_elapsed_and_the_step_elapsed_advance(clock: Clock, warnings: list[str]) -> None:
    # Given a client that makes 30 s pass on every publish, so each heartbeat sees a
    # later clock
    """Check that heartbeat makes the elapsed and the step elapsed advance."""

    def advance(force: bool) -> None:
        del force
        clock.now += 30

    message = RecordingMessage(on_publish=advance)
    notifier = make(message, tables=())
    # When the run sits in its first step (export) across the creation and three beats
    with notifier.guard():
        notifier.begin()
        reached = message.wait_for(4)
    # Then the beats show the run elapsed growing and, once past a minute, how long the
    # step has been going
    assert reached
    assert warnings == []
    beats = [fields_by_name(publish.payload) for publish in message.publishes if not publish.force]
    assert [fields["⏱️ Decorrido"] for fields in beats[:3]] == ["30s", "1min 00s", "1min 30s"]
    assert [fields["🧭 Etapa atual"] for fields in beats[:3]] == [
        "Exportação do BigQuery",
        "Exportação do BigQuery · há 1min 00s",
        "Exportação do BigQuery · há 1min 30s",
    ]


def test_step_elapsed_shows_in_the_current_stage_and_resets_when_the_step_changes(clock: Clock) -> None:
    # Given a run in the export step for 2 minutes
    """Show the step time in the current stage, and reset it when the step changes."""
    notifier = make(None)
    notifier.begin()
    notifier.enter(Step.EXPORT)
    clock.now += 120
    assert etapa_atual(notifier) == "Exportação do BigQuery · há 2min 00s"
    # When the tables step starts and table A prepares for 3min 05s
    notifier.enter(Step.TABLES)
    notifier.table_step("A", TableStep.PREPARATION)
    clock.now += 185
    assert etapa_atual(notifier) == "A: Preparação · há 3min 05s"
    # Then a slot update or a progress tick of the same stage does not reset it
    notifier.table_step("A", TableStep.PREPARATION, slot="B")
    notifier.set_physical_table("A", "BQLOAD_A_B")
    assert etapa_atual(notifier) == "A: Preparação · há 3min 05s"
    # and moving to the next table step starts the count again, silent until a minute
    # has passed
    notifier.table_step("A", TableStep.LOAD)
    clock.now += 59
    assert etapa_atual(notifier) == "A: Carga SQL*Loader"
    clock.now += 1
    assert etapa_atual(notifier) == "A: Carga SQL*Loader · há 1min 00s"


def test_step_elapsed_is_only_shown_for_a_running_stage() -> None:
    # Given a state 5 minutes into the same stage
    """Check that step elapsed is only shown for a running stage."""
    running = RunState(
        dataset_id="ds",
        run_name="ds",
        url=None,
        status=RunStatus.RUNNING,
        mode="full",
        run_dbt=False,
        step=Step.TABLES,
        tables={"A": TableState("A", step=TableStep.INDEXES)},
        sessions=2,
        elapsed_seconds=900.0,
        updated_at=NOW,
        current_table="A",
        step_elapsed_seconds=300.0,
    )
    done = replace(running, tables={"A": TableState("A", step=TableStep.DONE)})
    failed = replace(running, status=RunStatus.FAILED, failed_step=Step.TABLES)
    # Then only the running one carries the suffix; a finished table and a failed run do
    # not
    assert build_view(running).stage == "A: Índices · há 5min 00s"
    assert build_view(done).stage == "A: Concluída"
    assert build_view(failed).stage == "A: Índices"
    assert build_view(failed).failed_stage == "A: Índices"
