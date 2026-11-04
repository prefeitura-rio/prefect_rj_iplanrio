"""Async view of an Oracle connection that reads a table or query as Arrow batches.

The `OracleEngine` of `executor/engines/oracle.py` wraps it.

The reader uses `oracledb` with the Oracle Instant Client (thick mode), and falls back
to thin mode if the client libraries are missing. A number never loses digits: it is
an integer, a `decimal128` or exact text. A type that Arrow cannot hold, such as an
object type, raises and names the column.
"""

from asyncio import to_thread
from collections.abc import AsyncGenerator, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Self, cast

from oracledb import Connection, DatabaseError, init_oracle_client
from oracledb import connect as oracle_connect
from polars import DataFrame, from_arrow
from psycopg.sql import Composable, Identifier
from pyarrow import ChunkedArray, DataType, Schema, Table, decimal128
from pyarrow import Scalar as arrow_Scalar
from pyarrow import field as arrow_field
from pyarrow import float64 as arrow_float64
from pyarrow import int64 as arrow_int64
from pyarrow import schema as arrow_schema
from pyarrow import string as arrow_string
from pyarrow import table as arrow_table
from pyarrow import timestamp as arrow_timestamp

from .chunks import NULL_CHUNK, Chunk
from .constants import (
    ORACLE_EXPRESSIONS,
    ORACLE_KEEPALIVE_MINUTES,
)
from .executor.constants import DEFAULT_BATCH_SIZE
from .executor.render import render_template
from .executor.requests import Query, UnsupportedRequestError
from .executor.requests import Table as TableOrigin
from .executor.sources.protocols import DBConn
from .executor.types import Origin
from .settings import OracleSettings
from .telemetry import logger
from .types import ArrowColumn, Bind, Binder, Predicate, QueryParams

BARE_SCALE = -127
"""The scale that `oracledb` reports for a `NUMBER` that has none."""

INTEGER_DIGITS = 18
"""The digits that always fit a 64 bit integer."""

DECIMAL_DIGITS = 38
"""The digits of the widest `decimal128`."""


@dataclass(frozen=True, slots=True)
class OracleColumn:
    """One column of a read: its name, expression kind and Arrow types."""

    name: str
    expression: str
    fetch: DataType
    target: DataType


def build_number_column(
    precision: int | None, scale: int | None
) -> tuple[str, DataType]:
    """Return the expression kind and the Arrow type of a `NUMBER`.

    An integer of up to 18 digits is `int64`, and any other number with a precision and
    a scale that fit is a `decimal128`. A `FLOAT` is a double. A `NUMBER` with no
    precision, or one that no `decimal128` holds, is read as exact text.

    ```python
    build_number_column(10, 0)  # ("plain", int64())
    build_number_column(20, 6)  # ("plain", decimal128(20, 6))
    build_number_column(0, -127)  # ("number_text", string())
    ```
    """
    if precision is None or scale is None:
        return "number_text", arrow_string()

    match precision, scale:
        case 0, _ if scale == BARE_SCALE:
            return "number_text", arrow_string()
        case _, _ if scale == BARE_SCALE:
            return "plain", arrow_float64()
        case 0, 0:
            return "plain", decimal128(DECIMAL_DIGITS, 0)
        case digits, 0 if digits <= INTEGER_DIGITS:
            return "plain", arrow_int64()
        case digits, places if 0 <= places <= digits <= DECIMAL_DIGITS:
            return "plain", decimal128(digits, places)
        case _:
            return "number_text", arrow_string()


def plan_column(
    name: str, type_name: str, precision: int | None, scale: int | None
) -> OracleColumn:
    """Return how to read one column, from the description of its cursor.

    Raise `NotImplementedError` for a type with no conversion, such as an object type,
    and name the column so that the query can cast it.

    ```python
    plan_column("QUANDO", "DB_TYPE_TIMESTAMP_TZ", 0, 6)
    # OracleColumn("QUANDO", "utc", timestamp("us"), timestamp("us", tz="UTC"))
    ```
    """
    if type_name == "DB_TYPE_NUMBER":
        expression, arrow = build_number_column(precision, scale)
    elif type_name in ORACLE_EXPRESSIONS:
        expression, arrow = ORACLE_EXPRESSIONS[type_name]
    else:
        raise NotImplementedError(
            f"The type {type_name} of the Oracle column {name} has no conversion: cast it in a query"
        )

    target = arrow_timestamp("us", tz="UTC") if expression == "utc" else arrow
    return OracleColumn(name, expression, arrow, target)


@lru_cache
def init_client() -> None:
    """Load the Oracle Instant Client once, or stay in thin mode if it is not there."""
    try:
        init_oracle_client(lib_dir=OracleSettings().ORACLE_CLIENT_LIB_DIR or None)
    except DatabaseError as error:
        logger.warning(
            "Oracle Instant Client not loaded, so oracledb runs in thin mode: %s",
            str(error).splitlines()[0],
        )


def build_source_sql(origin: Origin) -> tuple[str, bool]:
    r"""Return what a read selects from, and whether it is a table.

    A table is `OWNER.TABLE`, each part quoted as written. A query loses its trailing
    `;` and goes in parentheses, so that a chunk can filter it. Raise
    `UnsupportedRequestError` for any other origin.

    ```python
    build_source_sql(TableOrigin("APP.ORDERS"))  # ('"APP"."ORDERS"', True)
    build_source_sql(Query("SELECT 1 FROM dual;"))  # ("(SELECT 1 FROM dual\n)", False)
    ```
    """
    match origin:
        case TableOrigin(name):
            return Identifier(*name.split(".")).as_string(None), True
        case Query(text):
            return f"({text.strip().removesuffix(';').rstrip()}\n)", False
        case _:
            raise UnsupportedRequestError("OracleEngine", origin)


def build_predicates(
    column: str | None, chunk: Chunk | None, known: Sequence[str]
) -> tuple[list[Predicate], dict[str, Bind]]:
    """Return the chunk as predicates with binds, and the binds.

    The column must be one of `known`. A value is always a bind, never part of the SQL
    text. Raise `ValueError` for an unknown column.

    ```python
    found, binds = predicates("DIA", chunk, ["STATUS", "DIA"])
    # found: "DIA" >= :p0, "DIA" < :p1
    # binds: {"p0": date(2024, 3, 1), "p1": date(2024, 3, 2)}
    ```
    """
    binds: dict[str, Bind] = {}

    def bind(value: Bind) -> str:
        """Add a value to the binds and return its placeholder, such as `:p0`."""
        name = f"p{len(binds)}"
        binds[name] = value
        return f":{name}"

    if column and column not in known:
        raise ValueError(f"Unknown columns: {[column]}")

    found = bind_chunk_predicates(column, chunk, bind) if column and chunk else []

    return found, binds


def bind_chunk_predicates(column: str, chunk: Chunk, bind: Binder) -> list[Predicate]:
    """Return the filter of one chunk: its range, or `IS NULL` for the NULL chunk."""
    name = Identifier(column)
    if chunk == NULL_CHUNK:
        return [{"column": name, "op": "is null"}]
    return [
        {"column": name, "op": ">=", "value": bind(chunk.start)},
        {"column": name, "op": "<", "value": bind(chunk.stop)},
    ]


@dataclass(frozen=True, slots=True)
class ChunkRead:
    """One read of a chunk: its source, filters, chunk and the values of its binds."""

    source: str
    is_table: bool
    filters: Sequence[str]
    found: Sequence[Predicate]
    binds: Mapping[str, Bind]
    names: Sequence[Composable] = ()


def convert(table: Table, columns: Sequence[OracleColumn]) -> Table:
    """Give a batch the target types.

    A naive instant becomes UTC by a cast, which needs no time zone database. A text
    gets its plain type.
    """
    arrays: list[ArrowColumn | ChunkedArray[arrow_Scalar[DataType]]] = [
        array.cast(column.target)
        for column, array in zip(columns, table.columns, strict=True)
    ]
    return Table.from_arrays(arrays, schema=build_target_schema(columns))


def build_target_schema(columns: Sequence[OracleColumn]) -> Schema:
    """Return the schema of the dataframes of a read."""
    return arrow_schema([arrow_field(column.name, column.target) for column in columns])


@dataclass(frozen=True, slots=True)
class Oracle:
    """An async view of one synchronous Oracle connection, for SQL text.

    A query comes back in the types that Delta and BigQuery hold: the engine describes
    the statement, and wraps it in a select list that reads each column in its form.
    """

    connection: Connection

    @classmethod
    @asynccontextmanager
    async def connect(cls, conn: DBConn) -> AsyncGenerator[Self]:
        """Open a connection to the service and close it off the event loop.

        A probe keeps a firewall from dropping an idle connection.
        """

        def open_connection() -> Connection:
            """Load the client and connect."""
            init_client()
            return oracle_connect(
                user=conn.user,
                password=conn.password,
                dsn=f"{conn.hostname}:{conn.port}/{conn.database}?expire_time={ORACLE_KEEPALIVE_MINUTES}",
            )

        async with AsyncExitStack() as stack:
            connection = await to_thread(open_connection)
            stack.push_async_callback(to_thread, connection.close)
            yield cls(connection=connection)

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one SQL statement. The params bind by position."""

        def run() -> None:
            """Execute the statement on a cursor."""
            with self.connection.cursor() as cursor:
                cursor.execute(sql, list(params or []))

        await to_thread(run)

    async def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncGenerator[DataFrame]:
        """Run one SQL query and yield Polars dataframes of up to `batch_size` rows.

        The params bind by position. A query with no rows still yields one dataframe,
        with the columns.

        ```python
        async with Oracle.connect(conn) as oracle:
            sql = "SELECT id FROM orders WHERE id > :1"
            async for dataframe in oracle.query(sql, params=[7]):
                ...
        ```
        """

        def plan() -> list[OracleColumn]:
            """Describe the statement without running it, and plan each column."""
            with self.connection.cursor() as cursor:
                cursor.parse(sql)
                return [
                    plan_column(
                        column.name, column.type.name, column.precision, column.scale
                    )
                    for column in cursor.description or []
                ]

        columns = await to_thread(plan)
        batches = self.connection.fetch_df_batches(
            render_template(
                "oracle/convert",
                {
                    "sql": sql,
                    "columns": [
                        {
                            "name": Identifier(column.name),
                            "expression": column.expression,
                        }
                        for column in columns
                    ],
                },
            ),
            list(params or []),
            size=batch_size,
            requested_schema=arrow_schema(
                [arrow_field(column.name, column.fetch) for column in columns]
            ),
        )

        def read_next_batch() -> Table | None:
            """Fetch the next batch in the target types, or return `None` at the end."""
            dataframe = next(batches, None)
            return (
                None if dataframe is None else convert(arrow_table(dataframe), columns)
            )

        yielded = False
        while (table := await to_thread(read_next_batch)) is not None:
            yielded = True
            yield cast("DataFrame", from_arrow(table))

        if not yielded:
            yield cast(
                "DataFrame", from_arrow(build_target_schema(columns).empty_table())
            )
