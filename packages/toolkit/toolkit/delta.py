"""Write windows into a Delta table, and know which ones did not change.

The ledger of the table holds one fingerprint for each window and config. A window whose
fingerprint is the same is skipped, and a window that changed is replaced in one atomic
commit. The table lives in a bucket and is read through ADC, or on a local path.
"""

from collections.abc import Collection, Sequence
from hashlib import md5
from pathlib import Path
from time import sleep
from typing import cast

from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
from deltalake.exceptions import CommitFailedError
from psycopg.sql import Literal
from pyarrow.dataset import dataset

from .constants import BATCH_COLUMN, COMMIT_ATTEMPTS, VACUUM_RETENTION_HOURS
from .executor.executor import Executor
from .telemetry import logger
from .types import Fingerprint


def fingerprint_of(rows: int, low: str, high: str) -> int:
    """Turn a content summary into the 60 bit number that the ledger stores.

    ```python
    fingerprint_of(3, "1", "2")  # 810314219720917701
    ```
    """
    digest = md5(f"{rows}:{low}:{high}".encode(), usedforsecurity=False)
    return int(digest.hexdigest()[:15], 16)


async def fingerprint(executor: Executor, directory: Path) -> int:
    """Return the fingerprint of the Parquet files of a folder.

    It follows the content and ignores the order of the rows, and it changes when a row
    is added, removed, repeated or changed.
    """
    data = await executor.query(
        "duckdb/fingerprint",
        {"source": Literal(f"{directory}/*.parquet")},
        expect=Fingerprint,
    )
    row = data.frame.row(0, named=True)
    return fingerprint_of(
        cast("int", row["n"]), cast("str", row["low"]), cast("str", row["high"])
    )


def known_fingerprints(uri: str, ids: Sequence[str]) -> dict[str, int]:
    """Return the fingerprint that the ledger holds for each of the ids that it knows.

    A table that does not exist yet knows nothing.
    """
    if not DeltaTable.is_deltatable(uri):
        return {}

    table = DeltaTable(uri)
    return {
        name: version
        for name in ids
        if (version := table.transaction_version(name)) is not None
    }


def write_window(
    uri: str,
    directory: Path,
    key: str,
    ledger: str,
    number: int,
    *,
    overwrite_schema: bool = False,
) -> None:
    """Replace the rows of one window with the Parquet files of a folder.

    It runs in one commit that also records `number` in the ledger as `ledger`. An empty
    folder removes the rows of the window. A commit conflict, such as two writers that
    create the table, is tried again. A schema that differs from the table raises
    `SchemaMismatchError`, unless `overwrite_schema` replaces the schema of the table.
    """
    properties = CommitProperties(app_transactions=[Transaction(ledger, number)])

    for attempt in range(1, COMMIT_ATTEMPTS + 1):
        reader = dataset(directory, format="parquet").scanner().to_reader()
        try:
            write_deltalake(
                uri,
                reader,
                mode="overwrite",
                predicate=f"{BATCH_COLUMN} = '{key}'",
                partition_by=[BATCH_COLUMN],
                schema_mode="overwrite" if overwrite_schema else None,
                commit_properties=properties,
            )
        except CommitFailedError:
            if attempt == COMMIT_ATTEMPTS:
                raise
            logger.info("Delta commit conflict: window=%s attempt=%d", key, attempt)
            sleep(attempt * 0.2)
        else:
            return


def sweep(uri: str, keep: Collection[str]) -> None:
    """Delete every window not in `keep`. A table that does not exist is left."""
    if not DeltaTable.is_deltatable(uri):
        return

    keys = ", ".join(f"'{key}'" for key in keep)
    DeltaTable(uri).delete(f"{BATCH_COLUMN} NOT IN ({keys})")


def maintain(uri: str) -> None:
    """Write a checkpoint and remove the files that no version reads any more.

    BigQuery replays the log of the table, so a checkpoint keeps its queries fast.
    """
    table = DeltaTable(uri)
    table.create_checkpoint()
    table.vacuum(retention_hours=VACUUM_RETENTION_HOURS, dry_run=False)
