"""Types of rj_smfp__nota_carioca_extracao_xml."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Protocol

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import STATUS_DESCRIPTIONS, TASK_KINDS
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.format import format_duration


@dataclass(frozen=True)
class Limits:
    """The pace and the limits of the follow-up.

    `poll_seconds` is the wait between two reads of `NFSE_OWNER.TAREFAS`.
    `timeout_hours` is the longest time from the call to the release: at the limit the
    run fails and the task stays in Oracle. `stale_hours` is the time after which a task
    46 or 48 counts as stuck, as in the guard of `oracle_to_bq`. `idle_checks` is the
    reads in a row with no task 46 or 48 that mean the release. `progress_minutes` is
    the wait between two progress lines in Discord.
    """

    poll_seconds: int = 60
    timeout_hours: int = 8
    stale_hours: int = 6
    idle_checks: int = 2
    progress_minutes: int = 30

    def __post_init__(self) -> None:
        """Refuse a limit that makes the wait stop or never end."""
        if min(self.poll_seconds, self.timeout_hours, self.progress_minutes, self.stale_hours) <= 0:
            raise ValueError("poll_seconds, timeout_hours, stale_hours and progress_minutes must be positive")
        if self.idle_checks < 1:
            raise ValueError("idle_checks must be at least 1")


@dataclass(frozen=True)
class TaskRow:
    """One row of `NFSE_OWNER.TAREFAS`.

    `kind` is `TIPO` (46 extraction, 48 post-processing). `started_at` is `DATA_INICIO`
    in the clock of the database, and `finished_at` is `DATA_TERMINO`, empty until it
    ends. `progress` is the percent of `EXECUCAO_PROGRESSO`. `running_seconds` are the
    seconds since the start, counted by the database.
    """

    task_id: int
    kind: int
    status: int
    started_at: datetime | None
    finished_at: datetime | None
    progress: int | None
    summary: str | None
    running_seconds: int | None

    @property
    def status_description(self) -> str:
        """Return `status N (description)`."""
        return f"status {self.status} ({STATUS_DESCRIPTIONS.get(self.status, 'desconhecido')})"

    def describe(self) -> str:
        """Return `tarefa N (tipo 46, ...)`, with how long it runs until it ends."""
        kind = TASK_KINDS.get(self.kind, "tipo desconhecido")
        text = f"tarefa {self.task_id} (tipo {self.kind}, {kind})"

        if self.running_seconds is not None and self.finished_at is None:
            text += f" há {format_duration(max(self.running_seconds, 0))}"

        return text


@dataclass(frozen=True)
class RoutineResponse:
    """The `RotinaAutomaticaRetorno` that the web service answers.

    `status_code` is the name of the .NET `HttpStatusCode` (`Accepted`, `OK`,
    `Conflict`...). `message` is `ErrorMessage`, which also holds text on a success.
    `task_id` is `TarefaId`, and it is 0 if no task was made.
    """

    status_code: str
    message: str
    task_id: int


class Outcome(StrEnum):
    """What the answer means for the load."""

    STARTED = "iniciada"
    NOTHING_PENDING = "nada pendente"
    ALREADY_RUNNING = "já em execução"


@dataclass(frozen=True)
class Decision:
    """The outcome of the call, and the task to follow (0 if there is none)."""

    outcome: Outcome
    task_id: int


@dataclass(frozen=True)
class Clock:
    """A monotonic clock and a wait, injected so that a test uses a fake one."""

    now: Callable[[], float]
    sleep: Callable[[float], Awaitable[None]]


class TaskStore(Protocol):
    """What the follow-up reads from `NFSE_OWNER.TAREFAS`."""

    async def get(self, task_id: int) -> TaskRow | None:
        """Return the task by its number, or nothing if it is not visible yet."""
        ...

    async def running(self) -> tuple[TaskRow, ...]:
        """Return the tasks of the types 46 and 48 with `STATUS = 1`."""
        ...

    async def extraction_in_progress(self) -> TaskRow | None:
        """Return the newest extraction (type 46) that waits, runs or is stopping."""
        ...


type Notify = Callable[[str], Awaitable[None]]


@dataclass(frozen=True)
class ExtractionServices:
    """What the extraction does in the world, each action injected.

    `call` calls the web service and gives the answer. `store` reads
    `NFSE_OWNER.TAREFAS`. `notify` posts a line to Discord and never raises. `clock` is
    the clock and the wait.
    """

    call: Callable[[], Awaitable[RoutineResponse]]
    store: TaskStore
    notify: Notify
    clock: Clock


class WebserviceError(RuntimeError):
    """The web service could not be called, or its answer is not a readable one."""


class ExtractionRefusedError(RuntimeError):
    """The web service answered, but refused or did not start the extraction."""


class ExtractionFailedError(RuntimeError):
    """The task of the extraction ended with no success.

    `announcement` is the line for Discord.
    """

    def __init__(self, message: str, announcement: str) -> None:
        """Keep the announcement, with the message."""
        super().__init__(message)
        self.announcement: str = announcement


class ExtractionTimedOutError(RuntimeError):
    """The extraction, or the release of the tasks 46 and 48, passed the limit.

    Nothing is cancelled in Oracle. `announcement` is the line for Discord.
    """

    def __init__(self, message: str, announcement: str) -> None:
        """Keep the announcement, with the message."""
        super().__init__(message)
        self.announcement: str = announcement
