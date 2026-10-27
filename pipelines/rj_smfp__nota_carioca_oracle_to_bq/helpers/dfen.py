"""The guard against the extraction of the national notes.

The load does not start while the Nota Carioca application runs it. The application
registers its tasks in `NFSE_OWNER.TAREFAS`: type 46 extracts the national tax
documents to the DFEN schema, and type 48 post-processes the national notes, in
sequence. A task that runs (`STATUS = 1`) for more than `stale_hours` hours counts as
stuck: it does not block, and it gets a warning. The decision is pure.
"""

from collections.abc import Awaitable, Callable, Sequence

import oracledb
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import (
    ACCESS_HINT,
    NO_ACCESS,
    SECONDS_PER_HOUR,
    TASK_KINDS,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.types import DfenExtractionError, ExtractionStatus, RunningTask


def classify_tasks(tasks: Sequence[RunningTask], stale_hours: int) -> ExtractionStatus:
    """Split the running tasks between those that block the run and the stuck ones.

    A task blocks if it runs for at most `stale_hours` hours. With no `DATA_INICIO`
    there is no way to know for how long it runs, so it counts as stuck. The order of
    the tasks stays.
    """
    limit = stale_hours * SECONDS_PER_HOUR
    blocking: list[RunningTask] = []
    stale: list[RunningTask] = []

    for task in tasks:
        recent = task.running_seconds is not None and task.running_seconds <= limit
        (blocking if recent else stale).append(task)

    return ExtractionStatus(blocking=tuple(blocking), stale=tuple(stale))


def format_elapsed(seconds: int) -> str:
    """Write a duration as `1h02m03s`, `2m03s` or `3s`."""
    hours, rest = divmod(seconds, SECONDS_PER_HOUR)
    minutes, secs = divmod(rest, 60)

    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"

    return f"{minutes}m{secs:02d}s" if minutes else f"{secs}s"


def describe_task(task: RunningTask) -> str:
    """Describe a task: its number, its type, since when it runs and for how long.

    ```text
    tarefa 12 (tipo 46, ...) desde 20/10/2026 12:00:00 (horário do banco, há 1h02m03s)
    ```
    """
    kind = TASK_KINDS.get(task.kind, "tipo desconhecido")
    head = f"tarefa {task.task_id} (tipo {task.kind}, {kind})"

    if task.started_at is None or task.running_seconds is None:
        return f"{head} sem data de início registrada"

    elapsed = format_elapsed(max(task.running_seconds, 0))

    return f"{head} desde {task.started_at:%d/%m/%Y %H:%M:%S} (horário do banco, há {elapsed})"


def describe_blocking(blocking: Sequence[RunningTask]) -> str:
    """Write the message of the refusal: what runs, since when, and what to do."""
    listed = "; ".join(describe_task(task) for task in blocking)

    return (
        f"Extração das notas nacionais em andamento no Oracle (schema DFEN): {listed}. A carga foi recusada antes "
        "da leitura porque essas tabelas estão sendo atualizadas. Rode a carga de novo depois que terminarem ou, "
        "para forçar, rode com dfen.check=false."
    )


def describe_stale(task: RunningTask, stale_hours: int) -> str:
    """Write the warning of a stuck task, which does not block the run."""
    return (
        f"Extração DFEN ignorada por parecer travada (STATUS = 1 há mais de {stale_hours}h ou sem início): "
        f"{describe_task(task)}"
    )


async def check_no_extraction(
    fetch: Callable[[], Awaitable[tuple[RunningTask, ...]]], stale_hours: int
) -> ExtractionStatus | None:
    """Refuse the run if the application extracts the national notes to the DFEN.

    The stuck tasks get a warning and are ignored. With no privilege to read
    `NFSE_OWNER.TAREFAS` (`ORA-00942` or `ORA-01031`) the guard warns and lets the run
    go on, because it is a protection and not a dependency. Return the status, or
    nothing if the table could not be read. Raise `ValueError` if `stale_hours` is less
    than 1, `DfenExtractionError` if a task that blocks runs, and
    `oracledb.DatabaseError` for any other Oracle error.
    """
    if stale_hours < 1:
        raise ValueError(f"dfen.stale_hours deve ser pelo menos 1, recebido {stale_hours}")

    try:
        tasks = await fetch()
    except oracledb.DatabaseError as error:
        first_line = str(error).split("\n", 1)[0]

        if not NO_ACCESS.search(first_line):
            raise

        logger.warning(
            "Não foi possível consultar NFSE_OWNER.TAREFAS (%s); a carga segue sem esta conferência. %s",
            first_line,
            ACCESS_HINT,
        )

        return None

    status = classify_tasks(tasks, stale_hours)

    for task in status.stale:
        logger.warning(describe_stale(task, stale_hours))

    if status.blocking:
        raise DfenExtractionError(describe_blocking(status.blocking))

    return status
