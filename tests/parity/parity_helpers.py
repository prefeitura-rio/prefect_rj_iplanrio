"""Helpers shared by the parity tests."""

from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

from parity_dataset import Order
from parity_dataset import rows as dataset_rows
from parity_lab import build_source
from pyarrow import Table
from pyarrow.parquet import read_table
from toolkit.chunks import Rows
from toolkit.executor.engines.base import Engine

SQL = ["postgres", "mysql", "mssql", "oracle"]
DOCUMENTS = ["couchdb", "mongodb"]

COLUMN = {"oracle": "ID"}
DIA = {"oracle": "DIA"}

FILTER_LT_10 = {"mongodb": '{"id": {"$lt": 10}}', "couchdb": '{"id": {"$lt": 10}}'}
FILTER_GE_5 = {"mongodb": '{"id": {"$gte": 5}}', "couchdb": '{"id": {"$gte": 5}}'}
FILTER_OR = {
    "mongodb": '{"$or": [{"id": 1}, {"id": 2}]}',
    "couchdb": '{"$or": [{"id": 1}, {"id": 2}]}',
}


def column(engine: str) -> str:
    """Return the name of the id column in an engine."""
    return COLUMN.get(engine, "id")


def dia(engine: str) -> str:
    """Return the name of the date column in an engine."""
    return DIA.get(engine, "dia")


def sql_filter(engine: str, text: str) -> str:
    """Return a SQL predicate with the column names of the engine."""
    return text.replace("id", column(engine)) if engine == "oracle" else text


def filters_for(engine: str, kind: str) -> list[str]:
    """Return the filter of a kind in the dialect of an engine."""
    sql = {"lt10": "id < 10", "ge5": "id >= 5", "or12": "id = 1 OR id = 2"}
    documents = {"lt10": FILTER_LT_10, "ge5": FILTER_GE_5, "or12": FILTER_OR}

    return [documents[kind][engine]] if engine in DOCUMENTS else [sql_filter(engine, sql[kind])]


async def stage(name: str, rows: Rows, columns=(), **options: object) -> Table:
    """Write a chunk of `orders` through the engine of `name`, and return the table."""
    async with Engine.from_source(build_source(name, **options)) as engine:
        with TemporaryDirectory() as folder:
            target = Path(folder) / "out.parquet"
            await engine.materialize(target, rows, columns)

            return read_table(target)


def canonical(table: Table) -> list[tuple[int, str | None, date | None, Decimal]]:
    """Return the rows of a staged table in one form, sorted by id, for any engine."""
    found = []

    for row in table.to_pylist():
        values = {key.lower(): value for key, value in row.items()}
        when = values["dia"]

        if isinstance(when, str):
            when = date.fromisoformat(when)
        elif isinstance(when, datetime):
            when = when.date()

        found.append((int(values["id"]), values["nome"], when, Decimal(str(values["valor"]))))

    return sorted(found)


def keep_all(row: Order) -> bool:
    """Keep every row."""
    return bool(row)


def expected(keep: Callable[[Order], bool] = keep_all) -> list[tuple[int, str | None, date | None, Decimal]]:
    """Return the rows of the dataset that pass `keep`, in the canonical form."""
    return sorted((row["id"], row["nome"], row["dia"], row["valor"]) for row in dataset_rows() if keep(row))
