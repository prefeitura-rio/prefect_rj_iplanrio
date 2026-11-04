"""Types of rj_smfp__nota_carioca_oracle_to_bq."""

from dataclasses import dataclass
from datetime import datetime

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import DEFAULT_STALE_HOURS


@dataclass(frozen=True)
class Dfen:
    """The guard against the extraction of the national notes.

    With `check`, the run stops before it reads anything while the Nota Carioca
    application extracts the national notes to the DFEN schema. A task that runs for
    more than `stale_hours` hours counts as stuck, and it does not stop the run. Turn
    `check` off to force a run.
    """

    check: bool = True
    stale_hours: int = DEFAULT_STALE_HOURS


@dataclass(frozen=True)
class RunningTask:
    """A task of extraction that is running (`STATUS = 1`) in `NFSE_OWNER.TAREFAS`.

    `task_id` is the column `TAREFA`, and `kind` is 46 (extraction) or 48
    (post-processing). `started_at` is `DATA_INICIO` in the clock of the database, and
    `running_seconds` are the seconds since then, counted by the database.
    """

    task_id: int
    kind: int
    started_at: datetime | None
    running_seconds: int | None


@dataclass(frozen=True)
class ExtractionStatus:
    """The result of the check of the running tasks.

    `blocking` are the tasks that started at most `stale_hours` hours ago, and so they
    stop the run. `stale` are those that run for longer, or have no start: they look
    stuck, and they only get a warning.
    """

    blocking: tuple[RunningTask, ...]
    stale: tuple[RunningTask, ...]


class DfenExtractionError(RuntimeError):
    """The application extracts national notes to the DFEN, so the run must wait."""
