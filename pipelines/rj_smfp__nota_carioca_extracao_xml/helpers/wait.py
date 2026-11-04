"""Wait for the task of the extraction, and for the release of the tasks 46 and 48.

The rule to end, the same as by hand: `DATA_TERMINO` filled and `STATUS = 2` is a
success. `DATA_TERMINO` filled with another status, or status 3 (error) or 5 (stopped),
is a failure. After the success the load goes on only when no task 46 or 48 runs in
`idle_checks` reads in a row. It is the condition of the guard of `oracle_to_bq`, which
refuses to start while an extraction or a post-processing runs. A task that runs for
more than `stale_hours` hours counts as stuck, and it does not block, as in the guard.
Nothing here changes Oracle.
"""

from collections.abc import Sequence

from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import (
    LABEL,
    SECONDS_PER_HOUR,
    SECONDS_PER_MINUTE,
    STATUS_FAILED,
    STATUS_INTERRUPTED,
    STATUS_SUCCESS,
    SUMMARY_LIMIT,
)
from pipelines.rj_smfp__nota_carioca_extracao_xml.helpers.format import format_duration
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import (
    ExtractionFailedError,
    ExtractionServices,
    ExtractionTimedOutError,
    Limits,
    TaskRow,
)


def finished_successfully(row: TaskRow) -> bool:
    """Tell whether the task has `STATUS = 2` and an end date."""
    return row.status == STATUS_SUCCESS and row.finished_at is not None


def finished_without_success(row: TaskRow) -> bool:
    """Tell whether the task ended with another status, or has status 3 or 5."""
    return row.status in {STATUS_FAILED, STATUS_INTERRUPTED} or (
        row.finished_at is not None and row.status != STATUS_SUCCESS
    )


def build_failure(row: TaskRow) -> ExtractionFailedError:
    """Return the error of a task that ended with no success."""
    end = f", término {row.finished_at:%d/%m/%Y %H:%M:%S} (horário do banco)" if row.finished_at else ""
    summary = f": {row.summary[:SUMMARY_LIMIT]}" if row.summary else ""
    text = f"{LABEL} falhou: tarefa {row.task_id} terminou com {row.status_description}{end}{summary}"

    return ExtractionFailedError(text, f"🚨 {text}")


def filter_blocking(rows: Sequence[TaskRow], stale_seconds: int) -> list[TaskRow]:
    """Return the tasks that started at most `stale_seconds` ago, not the stuck ones."""
    return [row for row in rows if row.running_seconds is not None and row.running_seconds <= stale_seconds]


async def wait_for_task(services: ExtractionServices, task_id: int, limits: Limits, deadline: float) -> TaskRow:
    """Read the task every `poll_seconds` until it ends, with progress lines.

    Return the task that ended with success. Raise `ExtractionFailedError` if it ends
    with no success, and `ExtractionTimedOutError` if the deadline comes first.
    """
    clock, store, notify = services.clock, services.store, services.notify
    last_progress = clock.now()

    while True:
        row = await store.get(task_id)

        if row is None:
            logger.info(
                "Tarefa %d ainda não aparece em NFSE_OWNER.TAREFAS; nova consulta em %ds", task_id, limits.poll_seconds
            )
        elif finished_successfully(row):
            return row
        elif finished_without_success(row):
            raise build_failure(row)
        else:
            progress = row.progress if row.progress is not None else "?"
            logger.info("%s: %s, progresso %s%%", row.describe(), row.status_description, progress)

            if clock.now() - last_progress >= limits.progress_minutes * SECONDS_PER_MINUTE:
                suffix = f" · progresso {row.progress}%" if row.progress is not None else ""
                await notify(f"⏳ {LABEL} em andamento: {row.describe()}{suffix}")
                last_progress = clock.now()

        if clock.now() + limits.poll_seconds > deadline:
            limit = format_duration(limits.timeout_hours * SECONDS_PER_HOUR)
            text = f"{LABEL}: tarefa {task_id} não terminou dentro do limite de {limit}"

            raise ExtractionTimedOutError(text, f"🚨 {text}; a tarefa segue no Oracle e não foi interrompida")

        await clock.sleep(limits.poll_seconds)


async def wait_until_idle(services: ExtractionServices, limits: Limits, deadline: float) -> None:
    """Wait until no task 46 or 48 runs in `idle_checks` reads in a row.

    Raise `ExtractionTimedOutError` if the deadline comes with a task still running.
    """
    clock, store, notify = services.clock, services.store, services.notify
    stale_seconds = limits.stale_hours * SECONDS_PER_HOUR
    idle_in_a_row = 0
    announced = False
    blocking: list[TaskRow] = []

    while True:
        rows = await store.running()
        blocking = filter_blocking(rows, stale_seconds)

        for row in rows:
            if row not in blocking:
                logger.warning(
                    "%s parece travada (STATUS = 1 há mais de %dh ou sem início); não bloqueia",
                    row.describe(),
                    limits.stale_hours,
                )

        if blocking:
            idle_in_a_row = 0
            listed = "; ".join(row.describe() for row in blocking)
            logger.info("Aguardando terminar: %s", listed)

            if not announced:
                await notify(f"⏳ {LABEL} concluída; aguardando terminar: {listed}")
                announced = True
        else:
            idle_in_a_row += 1

            if idle_in_a_row >= limits.idle_checks:
                logger.info("Nenhuma extração ou pós-processamento em execução em %d consultas seguidas", idle_in_a_row)

                return

        if clock.now() + limits.poll_seconds > deadline:
            listed = "; ".join(row.describe() for row in blocking) or "liberação não confirmada"
            limit = format_duration(limits.timeout_hours * SECONDS_PER_HOUR)
            text = f"{LABEL}: rotinas da aplicação ainda em execução no limite de {limit}: {listed}"

            raise ExtractionTimedOutError(text, f"🚨 {text}")

        await clock.sleep(limits.poll_seconds)
