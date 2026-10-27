"""Tasks of rj_smfp__nota_carioca_oracle_to_bq."""

from datetime import datetime
from decimal import Decimal
from typing import cast

from prefect import task
from toolkit.executor.sources.protocols import DBConn
from toolkit.oracle import Oracle
from toolkit.settings import DatabaseSettings
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import TASK_EXTRACTION, TASK_POST_PROCESSING, TASK_RUNNING
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.helpers.dfen import check_no_extraction
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.types import Dfen, RunningTask
from utils.origins import expand_database
from utils.types import Database
from utils.utils import render_query


def connect_database(database: Database) -> DBConn:
    """Return the connection of the database, with the login of the settings."""
    credentials = DatabaseSettings()
    expanded = expand_database(database)

    return DBConn(
        expanded["host"],
        expanded["port"],
        expanded["database"],
        credentials.DB_USERNAME,
        credentials.DB_PASSWORD.get_secret_value(),
    )


def to_int(value: object) -> int:
    """Return a number of the database as an `int`."""
    return int(Decimal(str(value)))


async def read_running_tasks(conn: DBConn) -> tuple[RunningTask, ...]:
    """Read the tasks of extraction (types 46 and 48) that run, by the database clock.

    Return those with `STATUS = 1`, from the oldest to the newest.
    """
    sql = render_query(__file__, "list_running_extractions")
    params = [TASK_EXTRACTION, TASK_POST_PROCESSING, TASK_RUNNING]

    async with Oracle.connect(conn) as oracle:
        rows = [
            cast("dict[str, object]", row)
            async for frame in oracle.query(sql, params=params)
            for row in frame.iter_rows(named=True)
        ]

    return tuple(
        RunningTask(
            task_id=to_int(row["tarefa"]),
            kind=to_int(row["tipo"]),
            started_at=cast("datetime | None", row["data_inicio"]),
            running_seconds=None if row["segundos_em_execucao"] is None else to_int(row["segundos_em_execucao"]),
        )
        for row in rows
    )


@task
async def check_dfen_extraction_task(database: Database, dfen: Dfen) -> None:
    """Stop the run if the Nota Carioca application extracts national notes to the DFEN.

    It reads `NFSE_OWNER.TAREFAS` with the login of the dump. A task that runs for more
    than `dfen.stale_hours` hours is a stuck one: it gets a warning and is ignored.
    Raise `DfenExtractionError` if an extraction runs, with the tasks and what to do.
    """
    if not dfen.check:
        logger.warning("Conferência da extração DFEN desligada (dfen.check=false)")
        return

    conn = connect_database(database)
    status = await check_no_extraction(lambda: read_running_tasks(conn), dfen.stale_hours)

    if status is not None:
        logger.info(
            "Nenhuma extração DFEN em andamento no Oracle (%d tarefa(s) travada(s) ignorada(s))", len(status.stale)
        )
