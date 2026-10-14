"""Async view of a synchronous Oracle connection that reads a table or query as Arrow batches.

The types are the ones that Delta and BigQuery hold.

The reader uses `oracledb` with the Oracle Instant Client (thick mode), and falls back to
thin mode if the client libraries are missing. A number never loses digits: it is an
integer, a `decimal128` or exact text. A type that Arrow cannot hold, such as an object
type, raises and names the column.
"""

from asyncio import to_thread
from collections.abc import AsyncGenerator, Callable, Mapping, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import date
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

from .batches import NULL_WINDOW, Window
from .constants import (
    FILTER_OPERATORS,
    ORACLE_EXPRESSIONS,
    ORACLE_KEEPALIVE_MINUTES,
)
from .executor.constants import DEFAULT_BATCH_SIZE
from .executor.render import render_template
from .executor.sources.protocols import DBConn
from .settings import settings
from .telemetry import logger
from .types import ArrowColumn, Filter, QueryParams, Scalar

type Bind = Scalar | date
type Predicate = dict[str, Composable | str]
type Binder = Callable[[Bind], str]

BARE_SCALE = -127
"""The scale that `oracledb` reports for a `NUMBER` that has none."""

INTEGER_DIGITS = 18
"""The digits that always fit a 64 bit integer."""

DECIMAL_DIGITS = 38
"""The digits of the widest `decimal128`."""


@dataclass(frozen=True, slots=True)
class OracleColumn:
    """One column of a read: its name, how a select list reads it, and its Arrow types."""

    name: str
    expression: str
    fetch: DataType
    target: DataType


def number_column(precision: int | None, scale: int | None) -> tuple[str, DataType]:
    """Return the expression kind and the Arrow type of a `NUMBER`.

    An integer of up to 18 digits is `int64`, and any other number with a precision and
    a scale that fit is a `decimal128`. A `FLOAT` is a double. A `NUMBER` with no
    precision, or one that no `decimal128` holds, is read as exact text.

    ```python
    number_column(10, 0)  # ("plain", int64())
    number_column(20, 6)  # ("plain", decimal128(20, 6))
    number_column(0, -127)  # ("number_text", string())
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
    """
    if type_name == "DB_TYPE_NUMBER":
        expression, arrow = number_column(precision, scale)
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
        init_oracle_client(lib_dir=settings.ORACLE_CLIENT_LIB_DIR or None)
    except DatabaseError as error:
        logger.warning(
            "Oracle Instant Client not loaded, so oracledb runs in thin mode: %s",
            str(error).splitlines()[0],
        )


def source_sql(values: Mapping[str, object]) -> tuple[str, bool]:
    """Return what a read selects from, and whether it is a table.

    A table is `OWNER.TABLE`, each part quoted as written. A query loses its trailing
    `;` and goes in parentheses, so that a window can filter it.
    """
    match values.get("table"), values.get("query"):
        case str() as table, None:
            return Identifier(*table.split(".")).as_string(None), True
        case None, str() as query:
            return f"({query.strip().removesuffix(';').rstrip()}\n)", False
        case _:
            raise ValueError("Give exactly one of table and query")


def predicates(
    filters: Sequence[Filter],
    column: str | None,
    window: Window | None,
    known: Sequence[str],
) -> tuple[list[Predicate], dict[str, Bind]]:
    """Return the filters and the window as predicates with binds, and the values of the binds.

    Every column must be one of `known`, and every operator one of the list. A value is
    always a bind, never part of the SQL text. Raise `ValueError` for anything else.
    """
    binds: dict[str, Bind] = {}

    def bind(value: Bind) -> str:
        name = f"p{len(binds)}"
        binds[name] = value
        return f":{name}"

    unknown = {item["column"] for item in filters} | ({column} if column else set())
    if unknown - set(known):
        raise ValueError(f"Unknown columns: {sorted(unknown - set(known))}")

    found = [filter_predicate(item, bind) for item in filters]
    if column and window:
        found += window_predicates(column, window, bind)

    return found, binds


def window_predicates(column: str, window: Window, bind: Binder) -> list[Predicate]:
    """Return the filter of one window: its range, or `IS NULL` for the NULL window."""
    name = Identifier(column)
    if window == NULL_WINDOW:
        return [{"column": name, "op": "is null"}]
    return [
        {"column": name, "op": ">=", "value": bind(window.start)},
        {"column": name, "op": "<", "value": bind(window.stop)},
    ]


def filter_predicate(item: Filter, bind: Binder) -> Predicate:
    """Return one filter as `column`, `op` and, if the operator takes one, the bind text."""
    op = item["op"]
    if op not in FILTER_OPERATORS:
        raise ValueError(f"Not a filter operator: {op!r}")

    column = Identifier(item["column"])
    match op, item.get("value"):
        case "is null" | "is not null", None:
            return {"column": column, "op": op}
        case "is null" | "is not null", _:
            raise ValueError(f"The operator {op!r} takes no value")
        case "in" | "not in", [_, *_] as values:
            binds = ", ".join(bind(value) for value in values)
            return {"column": column, "op": op, "value": f"({binds})"}
        case "in" | "not in", _:
            raise ValueError(f"The operator {op!r} needs a list of values")
        case _, str() | int() | float() as value:
            return {"column": column, "op": op, "value": bind(value)}
        case _, list():
            raise ValueError(f"The operator {op!r} needs one value, not a list")
        case _:
            raise ValueError(f"The operator {op!r} needs a value")


@dataclass(frozen=True, slots=True)
class WindowRead:
    """One read of a window: what it selects from, its filters and the values of their binds."""

    source: str
    is_table: bool
    found: Sequence[Predicate]
    binds: Mapping[str, Bind]


def convert(table: Table, columns: Sequence[OracleColumn]) -> Table:
    """Give a batch the target types.

    A naive instant becomes UTC by a cast, which needs no time zone database. A text
    gets its plain type.
    """
    arrays: list[ArrowColumn | ChunkedArray[arrow_Scalar[DataType]]] = [
        array.cast(column.target)
        for column, array in zip(columns, table.columns, strict=True)
    ]
    return Table.from_arrays(arrays, schema=target_schema(columns))


def target_schema(columns: Sequence[OracleColumn]) -> Schema:
    """Return the schema of the frames of a read."""
    return arrow_schema([arrow_field(column.name, column.target) for column in columns])


@dataclass(frozen=True, slots=True)
class Oracle:
    """An async view of one synchronous Oracle connection, as an `Engine`.

    A query comes back in the types that Delta and BigQuery hold: the engine describes the
    statement, and wraps it in a select list that reads each column in its form.
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
        """Run one statement. The params bind by position."""

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
        """Run one query and yield Polars frames of up to `batch_size` rows.

        The params bind by position. A query with no rows still yields one frame, with
        the columns.
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

        def next_batch() -> Table | None:
            """Fetch the next batch and give it the target types, or return `None` at the end."""
            frame = next(batches, None)
            return None if frame is None else convert(arrow_table(frame), columns)

        yielded = False
        while (table := await to_thread(next_batch)) is not None:
            yielded = True
            yield cast("DataFrame", from_arrow(table))

        if not yielded:
            yield cast("DataFrame", from_arrow(target_schema(columns).empty_table()))
