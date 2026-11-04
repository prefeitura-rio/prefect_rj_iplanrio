"""Read `NFSE_OWNER.TAREFAS`: the task of the extraction, and the tasks that run.

It is the same table, with the same login, that the guard of `oracle_to_bq` reads. Each
read opens a short connection: the follow-up lasts hours, and it holds no session
between two reads. Nothing here writes to Oracle.
"""

from datetime import datetime
from decimal import Decimal
from typing import cast, final

from toolkit.executor.sources.protocols import DBConn
from toolkit.oracle import Oracle

from pipelines.rj_smfp__nota_carioca_extracao_xml.constants import QUERIES_ANCHOR, TASK_EXTRACTION, TASK_POST_PROCESSING
from pipelines.rj_smfp__nota_carioca_extracao_xml.types import TaskRow
from utils.utils import render_query


def to_int(value: object) -> int | None:
    """Return a number of the database as an `int`, or nothing for a null."""
    if value is None:
        return None

    if isinstance(value, int | float | Decimal | str):
        return int(Decimal(str(value)))

    raise TypeError(f"Número esperado, recebido {value!r}")


def to_datetime(value: object) -> datetime | None:
    """Return a date of the database, or nothing for a null."""
    if value is None or isinstance(value, datetime):
        return value

    raise TypeError(f"Data esperada, recebido {value!r}")


def build_row(record: dict[str, object]) -> TaskRow:
    """Turn a row that was read (lower case columns) into a `TaskRow`.

    Raise `TypeError` if a column has an unexpected type, or the task has no number,
    type or status.
    """
    task_id = to_int(record["tarefa"])
    kind = to_int(record["tipo"])
    status = to_int(record["status"])

    if task_id is None or kind is None or status is None:
        raise TypeError(f"Tarefa sem número, tipo ou status: {record!r}")

    summary = record.get("execucao_resumo")

    return TaskRow(
        task_id=task_id,
        kind=kind,
        status=status,
        started_at=to_datetime(record.get("data_inicio")),
        finished_at=to_datetime(record.get("data_termino")),
        progress=to_int(record.get("execucao_progresso")),
        summary=None if summary is None else str(summary).strip() or None,
        running_seconds=to_int(record.get("segundos_em_execucao")),
    )


@final
class OracleTaskStore:
    """The `TaskStore` over Oracle, with one connection for each read."""

    def __init__(self, conn: DBConn) -> None:
        """Keep the connection data."""
        self.conn = conn

    async def fetch(self, query: str, params: list[int]) -> list[TaskRow]:
        """Run `queries/<query>.sql` with the binds, and return the rows."""
        sql = render_query(QUERIES_ANCHOR, query)

        async with Oracle.connect(self.conn) as oracle:
            return [
                build_row(cast("dict[str, object]", row))
                async for frame in oracle.query(sql, params=params)
                for row in frame.iter_rows(named=True)
            ]

    async def get(self, task_id: int) -> TaskRow | None:
        """Return the task by its number, or nothing if it is not visible yet."""
        rows = await self.fetch("get_task", [task_id])

        return rows[0] if rows else None

    async def running(self) -> tuple[TaskRow, ...]:
        """Return the tasks 46 and 48 with `STATUS = 1`, from the oldest start."""
        return tuple(await self.fetch("list_running", [TASK_EXTRACTION, TASK_POST_PROCESSING]))

    async def extraction_in_progress(self) -> TaskRow | None:
        """Return the newest extraction (type 46) with `STATUS` 0, 1 or 4."""
        rows = await self.fetch("extraction_in_progress", [TASK_EXTRACTION])

        return rows[0] if rows else None
