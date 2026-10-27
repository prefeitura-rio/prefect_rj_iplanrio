"""Async reader of a MySQL, from 5.0 up, that yields Arrow tables.

The DuckDB `mysql_scanner` forces `utf8mb4`, which MySQL 5.0 and 5.1 do not know.
This reader uses `aiomysql` with the character set of the connection, and reads a
table or a query in batches. The `MySQLEngine` of `executor/engines/mysql.py` wraps
it.

A column of an old database may declare `utf8` and hold bytes of Latin-1, written by an
application that did not convert them. A strict `utf8` or `latin1` connection fails on
such a row. The character set `legacy` reads each text as UTF-8 and, if that fails, as
Windows-1252 and then Latin-1, so no row fails.
"""

from codecs import CodecInfo, register
from collections.abc import AsyncGenerator, Buffer, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from re import sub
from typing import Self, cast

import pyarrow as pa
from aiomysql import Connection, SSCursor
from aiomysql import connect as mysql_connect

from .chunks import NULL_CHUNK, Chunk
from .constants import (
    MYSQL_BLOB_TYPES,
    MYSQL_DATE_TYPES,
    MYSQL_DECIMAL_TYPES,
    MYSQL_FLOAT_TYPES,
    MYSQL_INTEGER_TYPES,
    MYSQL_LEGACY_CHARSET,
    MYSQL_LEGACY_CODEC,
    MYSQL_TIMESTAMP_TYPES,
)
from .executor.constants import DEFAULT_BATCH_SIZE
from .executor.requests import Query, UnsupportedRequestError
from .executor.requests import Table as TableOrigin
from .executor.sources.protocols import DBConn
from .executor.types import Origin
from .telemetry import logger
from .types import Bind, QueryParams, QueryValue

DECIMAL_DIGITS = 38
"""The digits of the widest `decimal128`."""

type Row = Sequence[object]


@dataclass(frozen=True, slots=True)
class ChunkRead:
    """One read of a chunk: its source, names, filters, conditions and parameters.

    `empty` reads no row, only the columns.
    """

    source: str
    names: Sequence[str] = ()
    filters: Sequence[str] = ()
    predicates: Sequence[str] = ()
    params: QueryParams = ()
    empty: bool = False


@dataclass(frozen=True, slots=True)
class MySQLColumn:
    """One column of a read: its name, MySQL field type and Arrow type."""

    name: str
    type_code: int
    arrow: pa.DataType


def decode_legacy(data: Buffer, errors: str = "strict") -> tuple[str, int]:
    """Decode a text that may be UTF-8, Windows-1252 or Latin-1, in that order.

    Latin-1 holds every byte, so this never fails. `errors` is ignored, as the decoding
    does not fail.

    ```python
    decode_legacy("ação".encode())  # ("ação", 6)
    decode_legacy("ação".encode("latin-1"))  # ("ação", 4)
    ```
    """
    del errors
    raw = bytes(data)

    for encoding in ("utf-8", "cp1252"):
        try:
            return raw.decode(encoding), len(raw)
        except UnicodeDecodeError:
            continue

    return raw.decode("latin-1"), len(raw)


def find_legacy_codec(name: str) -> CodecInfo | None:
    """Give the codec `MYSQL_LEGACY_CODEC` to Python, and nothing for another name."""
    if name != MYSQL_LEGACY_CODEC:
        return None

    def encode(text: str, errors: str = "strict") -> tuple[bytes, int]:
        """Encode as UTF-8."""
        return text.encode("utf-8", errors), len(text)

    return CodecInfo(encode, decode_legacy, name=MYSQL_LEGACY_CODEC)


register(find_legacy_codec)


def quote(name: str) -> str:
    r"""Return a name between backticks, with a backtick inside doubled.

    ```python
    quote("app")  # "`app`"
    quote("a`b")  # "`a``b`"
    ```
    """
    return "`" + name.replace("`", "``") + "`"


def unquote(identifier: str) -> str:
    """Return the name inside a double-quoted identifier, such as `"id"` to `id`."""
    return identifier.removeprefix('"').removesuffix('"').replace('""', '"')


def build_source_sql(origin: Origin) -> tuple[str, bool]:
    r"""Return what a read selects from, and whether it is a table.

    A table is `database.table`, each part quoted. A query loses its trailing `;` and
    goes in parentheses with an alias, so that a chunk can filter it. Raise
    `UnsupportedRequestError` for any other origin.

    ```python
    build_source_sql(TableOrigin("app.orders"))  # ("`app`.`orders`", True)
    build_source_sql(Query("SELECT 1;"))  # ("(SELECT 1\n) AS `source`", False)
    ```
    """
    match origin:
        case TableOrigin(name):
            return ".".join(quote(part) for part in name.split(".")), True
        case Query(text):
            return f"({text.strip().removesuffix(';').rstrip()}\n) AS `source`", False
        case _:
            raise UnsupportedRequestError("MySQLEngine", origin)


def bind_params(
    sql: str, params: QueryParams | None
) -> tuple[str, list[QueryValue] | None]:
    """Return the SQL and the parameters in the form that `aiomysql` takes.

    With no parameters the driver must not format the SQL, so a `LIKE 'a%'` in a native
    query stays as it is, and the parameters are `None`. With parameters it formats the
    SQL with `%`, so each `%` that is not a `%s` placeholder is doubled.

    ```python
    bind_params("SELECT 'a%'", None)  # ("SELECT 'a%'", None)
    bind_params("n LIKE 'a%' AND id = %s", [1])  # ("n LIKE 'a%%' AND id = %s", [1])
    ```
    """
    if not params:
        return sql, None

    return sub(r"%(?!s)", "%%", sql), list(params)


def build_predicates(
    column: str | None, chunk: Chunk | None, known: Sequence[str]
) -> tuple[list[str], QueryParams]:
    """Return the chunk as conditions with `%s` placeholders, and their values.

    The column must be one of `known`. A value is always a parameter, never part of the
    SQL text. Raise `ValueError` for an unknown column.

    ```python
    build_predicates("dia", chunk, ["dia"])
    # (["`dia` >= %s", "`dia` < %s"], [date(2024, 3, 1), date(2024, 3, 2)])
    ```
    """
    if column and column not in known:
        raise ValueError(f"Unknown columns: {[column]}")

    if not column or not chunk:
        return [], []

    if chunk == NULL_CHUNK:
        return [f"{quote(column)} IS NULL"], []

    values: list[Bind] = [chunk.start, chunk.stop]

    return [f"{quote(column)} >= %s", f"{quote(column)} < %s"], values


def plan_column(
    name: str, type_code: int, precision: int | None, scale: int | None
) -> MySQLColumn:
    """Return how to read one column, from the description of its cursor.

    An integer is `int64`, a float `float64`, a decimal a `decimal128` when it fits and
    exact text when it does not, a date `date32`, and a date and time a microsecond
    timestamp. A blob is text or bytes, and every other type is text.

    ```python
    plan_column("preco", 246, 10, 2)  # MySQLColumn("preco", 246, decimal128(10, 2))
    ```
    """
    match type_code:
        case code if code in MYSQL_INTEGER_TYPES:
            arrow = pa.int64()
        case code if code in MYSQL_FLOAT_TYPES:
            arrow = pa.float64()
        case code if code in MYSQL_DECIMAL_TYPES:
            digits, places = precision or 0, scale or 0
            fits = 0 < digits <= DECIMAL_DIGITS and 0 <= places <= digits
            arrow = pa.decimal128(digits, places) if fits else pa.string()
        case code if code in MYSQL_DATE_TYPES:
            arrow = pa.date32()
        case code if code in MYSQL_TIMESTAMP_TYPES:
            arrow = pa.timestamp("us")
        case _:
            arrow = pa.string()

    return MySQLColumn(name, type_code, arrow)


def convert_value(column: MySQLColumn, value: object) -> object:
    """Return a value in the form of its Arrow type, or `None` if it has none.

    The server keeps dates such as `0000-00-00`, which no date holds, and the driver
    gives them as text. They become nulls.
    """
    if value is None:
        return None

    match column.arrow:
        case arrow if pa.types.is_date32(arrow):
            return (
                value
                if isinstance(value, date) and not isinstance(value, datetime)
                else None
            )
        case arrow if pa.types.is_timestamp(arrow):
            return value if isinstance(value, datetime) else None
        case arrow if pa.types.is_decimal(arrow):
            return value if isinstance(value, Decimal) else None
        case arrow if pa.types.is_string(arrow):
            return value if isinstance(value, str) else str(value)
        case _:
            return value


def convert(rows: Sequence[Row], columns: Sequence[MySQLColumn]) -> pa.Table:
    """Give a batch of rows the Arrow types of the columns.

    A blob that holds bytes makes its column binary, and a number that no `int64`
    holds makes its column exact text. A zero date becomes a null, with one warning.
    """
    arrays: list[pa.Array[pa.Scalar[pa.DataType]]] = []
    fields: list[pa.Field[pa.DataType]] = []

    for position, column in enumerate(columns):
        values = [row[position] for row in rows]
        arrow = column.arrow

        if column.type_code in MYSQL_BLOB_TYPES and any(
            isinstance(value, bytes | bytearray) for value in values
        ):
            arrow = pa.binary()
            converted: list[object] = [
                None if value is None else bytes(cast("bytes", value))
                for value in values
            ]
        else:
            converted = [convert_value(column, value) for value in values]

            lost = sum(
                1
                for before, after in zip(values, converted, strict=True)
                if after is None and before is not None
            )
            if lost:
                logger.warning(
                    "Values with no date or number were read as null: column=%s rows=%d",
                    column.name,
                    lost,
                )

        try:
            array = pa.array(converted, type=arrow)
        except pa.ArrowInvalid, OverflowError:
            arrow = pa.string()
            array = pa.array(
                [None if value is None else str(value) for value in converted],
                type=arrow,
            )

        arrays.append(array)
        fields.append(pa.field(column.name, arrow))

    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


@dataclass(frozen=True, slots=True)
class MySQL:
    """An async connection to a MySQL that reads SQL text as Arrow tables."""

    connection: Connection

    @classmethod
    @asynccontextmanager
    async def connect(cls, conn: DBConn, charset: str) -> AsyncGenerator[Self]:
        """Open a connection with the character set of the data, and close it.

        With `legacy` the server sends UTF-8, and each text is decoded with
        `decode_legacy`.
        """
        legacy = charset == MYSQL_LEGACY_CHARSET
        connection = await mysql_connect(
            host=conn.hostname,
            port=conn.port,
            user=conn.user,
            password=conn.password,
            db=conn.database,
            charset="utf8" if legacy else charset,
        )

        if legacy:
            connection._encoding = (
                MYSQL_LEGACY_CODEC  # the only way to set the codec of the rows
            )

        try:
            yield cls(connection=connection)
        finally:
            connection.close()

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one SQL statement. The params bind by position with `%s`."""
        async with self.connection.cursor() as cursor:
            await cursor.execute(*bind_params(sql, params))

    async def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncGenerator[pa.Table]:
        """Run one SQL query and yield Arrow tables of up to `batch_size` rows.

        The params bind by position with `%s`. A query with no rows still yields one
        table, with the columns. The rows stream from the server, so read the batches
        before the next call.
        """
        async with self.connection.cursor(SSCursor) as cursor:
            await cursor.execute(*bind_params(sql, params))
            columns = [
                plan_column(item[0], item[1], item[4], item[5])
                for item in cursor.description or []
            ]
            yielded = False

            while rows := await cursor.fetchmany(batch_size):
                yielded = True
                yield convert(rows, columns)

            if not yielded:
                yield convert([], columns)
