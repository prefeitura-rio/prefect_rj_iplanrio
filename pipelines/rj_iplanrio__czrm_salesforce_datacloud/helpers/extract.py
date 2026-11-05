"""Data Cloud Query API extraction helpers.

The API has no cursor-based pagination.  The only option is to re-send the SQL
with `ORDER BY <key> LIMIT 1000 OFFSET n`.  The actual server page ceiling
varies between ~1 400 and ~1 700 rows (payload limit, not row count), so a
page size of 1 000 provides a safe margin.

Termination condition: an empty page is the only reliable signal that all rows
have been read.  Using `len(page) < PAGE_SIZE` would silently truncate the
extraction when the payload limit is hit mid-page (observed on `Case`: 989 of
1 000 rows with 3 659 total).

Memory note: all pages accumulate in a `list[list]` before the DataFrame is
built.  For 5 M rows this is substantial.  The safety limit (`MAX_ROWS`) guards
against runaway pagination rather than memory exhaustion; for tables approaching
that limit use `reprocessar=True` to process day by day.
"""

import polars as pl
from httpx2 import AsyncClient
from prefect.logging import get_logger

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import (
    API_VERSION,
    MAX_ROWS,
    PAGE_SIZE,
    TIMEOUT,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.auth import Session
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.queries import load_query

logger = get_logger("toolkit")


async def execute_query(session: Session, sql: str) -> dict:
    """Execute a SQL query against the Data Cloud and return the JSON response.

    Raise `httpx2.HTTPStatusError` if the API responds with an error.
    """
    url = (
        f"{session['instance_url']}/services/data/{API_VERSION}/ssot/query-sql"
        f"?dataspace={session['dataspace']}&workloadName=BatchQuery"
    )
    async with AsyncClient(timeout=TIMEOUT) as client:
        response = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {session['access_token']}",
                "Content-Type": "application/json",
            },
            json={"sql": sql},
        )
    response.raise_for_status()
    return response.json()


async def discover_columns(session: Session, dlo: str, table: str = "") -> list[dict]:
    """Discover column names and types via `SELECT * LIMIT 0`.

    Raise `RuntimeError` if the response contains no metadata (DLO not found).
    """
    sql = load_query("extract", dlo=dlo, filtro="").rstrip() + "\nLIMIT 0"
    body = await execute_query(session, sql)
    metadata = body.get("metadata") or []
    if not metadata:
        raise RuntimeError(
            f"[DC] '{table or dlo}' returned no column metadata — does the DLO exist?"
        )
    logger.info("Schema discovered: table=%s columns=%d", table or dlo, len(metadata))
    return [{"name": c["name"], "type": c["type"]} for c in metadata]


async def extract_table(
    session: Session,
    dlo: str,
    filter_clause: str,
    order_by: str,
    table: str = "",
) -> pl.DataFrame:
    """Extract all rows from a DLO using LIMIT/OFFSET pagination.

    Return an empty DataFrame if no rows match the filter.
    Raise `RuntimeError` if the safety row limit is reached.
    """
    base = load_query("extract", dlo=dlo, filtro=filter_clause).rstrip().rstrip(";")
    rows: list[list] = []
    columns: list[str] = []
    offset = 0
    page = 0

    while True:
        page += 1
        body = await execute_query(session, f"{base} ORDER BY {order_by} LIMIT {PAGE_SIZE} OFFSET {offset}")
        if not columns:
            columns = [c["name"] for c in body.get("metadata") or []]
        new_rows = body.get("data") or []
        if not new_rows:
            break
        rows.extend(new_rows)
        offset += len(new_rows)
        if offset >= MAX_ROWS:
            raise RuntimeError(
                f"[DC] {table or dlo}: safety limit of {MAX_ROWS} rows reached — "
                "extraction would be incomplete. "
                "Use `reprocessar=True` with `reprocessar_de`/`reprocessar_ate` to process day by day."
            )

    logger.info("Extraction complete: table=%s rows=%d pages=%d", table or dlo, len(rows), page)

    if not rows:
        return pl.DataFrame(schema={c: pl.String for c in columns})
    return pl.DataFrame(dict(zip(columns, zip(*rows, strict=True), strict=True))  # type: ignore[arg-type]
                        if columns else {})
