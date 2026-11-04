"""The In-Memory of the loaded tables.

The physical table is born `NO INMEMORY`. After the load, only the partitions of the
window (the current month, the `months` before it and the future ones) get
`LOADED_TABLE_INMEMORY` and are populated before the swap. The rest of the history
stays on disk. After the swap, the table that left use goes back to `NO INMEMORY` and
frees its memory.

The `ALTER` follows the job of the DBA (`DFEN.PRC_ROLL_IMCS_NACIONAL`): a short
`DDL_LOCK_TIMEOUT` for each attempt, and a new attempt with a wait after `ORA-00054`. It
never kills a session.
"""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from time import monotonic, sleep
from typing import cast

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.constants import (
    DDL_ATTEMPTS,
    DDL_LOCK_TIMEOUT_SECONDS,
    DDL_RETRY_SECONDS,
    RESOURCE_BUSY,
    UPPER_BOUND_PATTERN,
)
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import OracleConfig, connect
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_ddl import assert_managed_existing_table
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_query import QUERIES_ANCHOR, to_int
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_read import fetch_rows
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import LOADED_TABLE_INMEMORY
from utils.utils import render_query


def oracle_message(error: oracledb.DatabaseError) -> str:
    """Summarize the message of an Oracle error, with all its lines, in one line.

    The result has at most 300 characters.
    """
    return " ".join(str(error).split())[:300]


@dataclass(frozen=True)
class InMemoryStatus:
    """The progress of the population of a table in the In-Memory.

    `expected_segments` are the segments with rows (the partitions, or the table if it
    is not partitioned). `populated_segments` are those that already show in the
    In-Memory. `bytes_not_populated` is what is still out of it, and
    `incomplete_segments` are the segments whose population is going on.
    """

    table: str
    expected_segments: int
    populated_segments: int
    bytes_not_populated: int
    incomplete_segments: int

    @property
    def complete(self) -> bool:
        """Indica se todos os segmentos com linhas estão inteiros no In-Memory."""
        return (
            self.populated_segments >= self.expected_segments
            and self.bytes_not_populated == 0
            and self.incomplete_segments == 0
        )

    @property
    def description(self) -> str:
        """Descreve o andamento em uma linha."""
        pending = self.bytes_not_populated / 1024**3
        return (
            f"{self.table}: {self.populated_segments}/{self.expected_segments} segmento(s) no In-Memory, "
            + f"{pending:.2f} GB por popular".replace(".", ",")
        )


def start_population(config: OracleConfig, table: str, partitioned: bool) -> list[str]:
    """Ask Oracle to populate the table in the In-Memory now.

    With `PRIORITY HIGH`, Oracle would populate it alone in a few minutes. The request
    only brings the start forward, while the other tables still load. In a partitioned
    table the request goes by partition that has rows, because the table has no segment
    itself (`ORA-03211`). Return the Oracle errors as text, empty if every request was
    accepted. Raise `PermissionError` if the table does not exist or does not belong to
    the pipeline.
    """
    binds = {"owner": config.schema, "table_name": table}
    errors = []
    with connect(config) as connection, connection.cursor() as cursor:
        assert_managed_existing_table(cursor, config.schema, table)
        if partitioned:
            segments = [str(row["partition_name"]) for row in fetch_rows(cursor, "get_partitions_with_rows", binds)]
        else:
            segments = [None]
        for partition in segments:
            try:
                cursor.execute(
                    render_query(QUERIES_ANCHOR, "populate_inmemory"), {**binds, "partition_name": partition}
                )
            except oracledb.DatabaseError as error:
                errors.append(f"{partition or table}: {oracle_message(error)}")
    return errors


def read_status(cursor: oracledb.Cursor, owner: str, table: str) -> InMemoryStatus:
    """Read the progress of the population of a table.

    The expected segments come from the statistics, which are gathered right after the
    load: a partition with no rows does not enter the In-Memory. Raise
    `oracledb.DatabaseError` if the session cannot read `gv$im_segments`.
    """
    binds = {"owner": owner, "table_name": table}
    expected = fetch_rows(cursor, "get_inmemory_expected_segments", binds)
    segments = fetch_rows(cursor, "get_inmemory_segments", binds)[0]
    return InMemoryStatus(
        table=table,
        expected_segments=to_int(expected[0]["expected_segments"]) if expected else 0,
        populated_segments=to_int(segments["populated_segments"]),
        bytes_not_populated=to_int(segments["bytes_not_populated"]),
        incomplete_segments=to_int(segments["incomplete_segments"]),
    )


def wait_for_population(
    config: OracleConfig,
    tables: list[str],
    timeout_seconds: float,
    report: Callable[[str], None],
    poll_seconds: float = 30.0,
) -> bool:
    """Wait for the tables to be whole in the In-Memory, up to the time limit.

    It does not wait if the In-Memory is off in the database or if the session cannot
    read the `gv$` views, and then it reports why. `timeout_seconds` is the longest
    wait, summed over all the tables. `report` gets the progress messages. Return
    whether all the tables became whole in the In-Memory.
    """
    deadline = monotonic() + timeout_seconds
    with connect(config) as connection, connection.cursor() as cursor:
        try:
            (area,) = fetch_rows(cursor, "get_inmemory_area", {})[0].values()
        except oracledb.DatabaseError as error:
            report(f"Sem acesso às views do In-Memory ({oracle_message(error)}); a troca não espera a população.")
            return False
        if not area:
            report("In-Memory desligado no banco (INMEMORY_SIZE = 0); a troca não espera a população.")
            return False
        while True:
            statuses = [read_status(cursor, config.schema, table) for table in tables]
            for status in statuses:
                report(status.description)
            if all(status.complete for status in statuses):
                return True
            if monotonic() >= deadline:
                report(
                    f"In-Memory não terminou de popular em {timeout_seconds / 60:.0f} min; a troca segue assim mesmo."
                )
                return False
            sleep(poll_seconds)


def window_start(today: date, months: int) -> date:
    """Return the first day of the In-Memory window: `months` before `today`'s month.

    `months` is how many months before the current one stay in the In-Memory, and `0`
    keeps only the current month. Raise `ValueError` if it is negative.
    """
    if months < 0:
        raise ValueError(f"A janela de In-Memory não pode ser negativa: {months}")
    index = today.year * 12 + today.month - 1 - months

    return date(index // 12, index % 12 + 1, 1)


def partition_upper_bound(high_value: str) -> date | None:
    """Read the date of the upper bound of a `RANGE` partition.

    Return nothing for `MAXVALUE` or a bound that is not a date.
    """
    found = UPPER_BOUND_PATTERN.search(high_value)

    return date(int(found[1]), int(found[2]), int(found[3])) if found else None


def in_window(high_value: str, start: date) -> bool:
    """Tell whether a partition has data from `start` on, so it enters the In-Memory.

    A partition whose bound is not a date (`MAXVALUE`, a key that is not a time) enters,
    so that data that may be recent never stays out of the In-Memory.
    """
    bound = partition_upper_bound(high_value)

    return bound is None or bound > start


def oracle_code(error: oracledb.DatabaseError) -> int | None:
    """Return the number of an Oracle error, such as 54 for `ORA-00054`, if any."""
    return cast("int | None", getattr(cast("object", error.args[0]), "code", None))


def execute_ddl(
    cursor: oracledb.Cursor,
    statement: str,
    report: Callable[[str], None],
    attempts: int = DDL_ATTEMPTS,
    retry_seconds: float = DDL_RETRY_SECONDS,
) -> None:
    """Run a `DDL`, and try again while the object is busy (`ORA-00054`).

    Each attempt waits for the lock only for the `DDL_LOCK_TIMEOUT` of the session.
    Between attempts the queue is free and the application goes on. `report` gets the
    messages of a new attempt. Raise `oracledb.DatabaseError` for any other error, or
    when the attempts end.
    """
    for attempt in range(1, attempts + 1):
        try:
            cursor.execute(statement)
        except oracledb.DatabaseError as error:
            if oracle_code(error) != RESOURCE_BUSY or attempt == attempts:
                raise
            report(
                f"Objeto ocupado (ORA-00054), tentativa {attempt}/{attempts}; nova tentativa em {retry_seconds:.0f}s"
            )
            sleep(retry_seconds)
        else:
            return


def prepare_ddl(cursor: oracledb.Cursor, schema: str, table: str) -> None:
    """Check that the table belongs to the pipeline and set the lock timeout."""
    assert_managed_existing_table(cursor, schema, table)
    cursor.execute(render_query(QUERIES_ANCHOR, "set_ddl_lock_timeout", seconds=DDL_LOCK_TIMEOUT_SECONDS))


def disable(config: OracleConfig, table: str, report: Callable[[str], None]) -> None:
    """Take the whole table out of the In-Memory, the table and all its partitions.

    It frees the memory of the table. `report` gets the messages of a new attempt.
    Raise `PermissionError` if the table does not exist or does not belong to the
    pipeline, and `oracledb.DatabaseError` if the `ALTER` fails.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        prepare_ddl(cursor, config.schema, table)
        statement = render_query(
            QUERIES_ANCHOR, "alter_table_inmemory", schema=config.schema, table=table, clause="NO INMEMORY"
        )
        execute_ddl(cursor, statement, report)


def select_window(partitions: Sequence[dict[str, object]], start: date) -> list[str]:
    """Pick, among the partitions of the table, those that stay in the In-Memory.

    The rows are from `all_tab_partitions`, with `partition_name` and `high_value`.
    Return the names in the order received.
    """
    return [str(row["partition_name"]) for row in partitions if in_window(str(row["high_value"]), start)]


def enable_window(
    config: OracleConfig,
    table: str,
    start: date,
    report: Callable[[str], None],
    clause: str = LOADED_TABLE_INMEMORY,
) -> list[str]:
    """Put only the partitions of the window in the In-Memory, and leave the rest out.

    A table with no partitions goes in whole. `table` is the physical table, just
    loaded and not yet in use, and `clause` is the `INMEMORY` clause of the partitions.
    Return the partitions that went in, or the table itself if it has no partitions.
    Raise `PermissionError` if the table does not exist or does not belong to the
    pipeline, and `oracledb.DatabaseError` if an `ALTER` fails.
    """
    binds = {"owner": config.schema, "table_name": table}
    with connect(config) as connection, connection.cursor() as cursor:
        prepare_ddl(cursor, config.schema, table)
        partitions = fetch_rows(cursor, "get_table_partitions", binds)
        if not partitions:
            statement = render_query(
                QUERIES_ANCHOR, "alter_table_inmemory", schema=config.schema, table=table, clause=clause
            )
            execute_ddl(cursor, statement, report)

            return [table]
        chosen = select_window(partitions, start)
        for partition in chosen:
            statement = render_query(
                QUERIES_ANCHOR,
                "alter_partition_inmemory",
                schema=config.schema,
                table=table,
                partition=partition,
                clause=clause,
            )
            execute_ddl(cursor, statement, report)

        return chosen
