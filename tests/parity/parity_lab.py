"""The sources of the lab: one for each real engine, built from env.json."""

import json
from datetime import datetime
from os import environ
from pathlib import Path

from pyarrow import schema, string
from pymongoarrow.api import Schema
from pymongoarrow.types import Decimal128Type, ObjectIdType
from toolkit.executor.requests import Table
from toolkit.executor.sources.backends.couchdb import CouchDBSource
from toolkit.executor.sources.backends.files import FilesSource
from toolkit.executor.sources.backends.mongodb import MongoDBSource
from toolkit.executor.sources.backends.mssql import MSSQLSource
from toolkit.executor.sources.backends.mysql import MySQLSource
from toolkit.executor.sources.backends.oracle import OracleSource
from toolkit.executor.sources.backends.postgres import PostgresSource
from toolkit.executor.sources.protocols import DBConn, Source
from toolkit.executor.sources.shapes import Shape
from toolkit.executor.types import Origin

ENV_FILE = Path(environ.get("PARITY_LAB", "/tmp/toolkit-parity")) / "env.json"
ENGINES = ["postgres", "mysql", "mssql", "oracle", "couchdb", "mongodb"]

MONGO_SHAPE = Schema({"_id": ObjectIdType(), "id": int, "nome": str, "dia": datetime, "valor": Decimal128Type()})


def lab_row(item) -> dict[str, object]:
    """Return a CouchDB document as the columns of `orders`."""
    document = item.doc or {}

    return {
        "id": item.id,
        "nome": document.get("nome"),
        "dia": document.get("dia"),
        "valor": document.get("valor"),
    }


COUCH_SHAPE = Shape(
    schema([("id", string()), ("nome", string()), ("dia", string()), ("valor", string())]),
    lab_row,
)


def environment() -> dict[str, dict[str, object]]:
    """Return the connection data of the engines that are up."""
    return json.loads(ENV_FILE.read_text()) if ENV_FILE.exists() else {}


def available() -> list[str]:
    """Return the names of the engines that are up."""
    known = environment()

    return [name for name in ENGINES if name in known]


def build_conn(name: str) -> DBConn:
    """Return the connection of an engine."""
    entry = environment()[name]

    return DBConn(
        str(entry["host"]), int(entry["port"]), str(entry["database"]), str(entry["user"]), str(entry["password"])
    )


def build_source(name: str, origin: Origin | None = None, **options: object) -> Source:
    """Return the source of an engine, reading `orders` unless another origin is given."""
    entry = environment()[name]
    conn = build_conn(name)
    origin = origin or Table(str(entry["table"]))

    match name:
        case "postgres":
            source = PostgresSource(conn, origin=origin)
        case "mysql":
            source = MySQLSource(conn, origin=origin)
        case "mssql":
            source = MSSQLSource(conn, origin=origin)
        case "oracle":
            source = OracleSource(conn, origin=origin, **options)
        case "couchdb":
            source = CouchDBSource(conn, shape=COUCH_SHAPE, key_width=11, secure=False, origin=origin, **options)
        case "mongodb":
            source = MongoDBSource(
                conn, shape=MONGO_SHAPE, auth_source=str(entry["auth_source"]), tls=False, origin=origin, **options
            )
        case "files":
            source = FilesSource(origin=origin)
        case _:
            raise KeyError(name)

    return source
