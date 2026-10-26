"""Populate the In-Memory of the table just loaded, so the swap finds data in memory."""

import time
from collections.abc import Callable
from dataclasses import dataclass

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import OracleConfig, connect
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_ddl import assert_managed_existing_table
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_query import QUERIES_ANCHOR, to_int
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle_read import fetch_rows
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
    deadline = time.monotonic() + timeout_seconds
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
            if time.monotonic() >= deadline:
                report(
                    f"In-Memory não terminou de popular em {timeout_seconds / 60:.0f} min; a troca segue assim mesmo."
                )
                return False
            time.sleep(poll_seconds)
