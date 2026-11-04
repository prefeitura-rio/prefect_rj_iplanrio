"""The MongoDB source: the configuration that the `MongoDBEngine` reads with."""

from importlib import import_module
from typing import Annotated, ClassVar
from urllib.parse import quote_plus, urlencode

from pydantic import BeforeValidator, InstanceOf, PositiveInt
from pydantic.dataclasses import dataclass
from pymongoarrow.api import Schema

from ....constants import MONGO_BATCH_ROWS
from ...constants import DEFAULT_CONCURRENCY, SOURCE_CONFIG
from ..base import Registered
from ..protocols import DBConn


def load_schema(value: str | Schema) -> Schema:
    """Return the PyMongoArrow schema that a path `module:name` names.

    Any other value is returned as it is. Raise `ValueError` if the path does not lead
    to a schema.
    """
    if not isinstance(value, str):
        return value

    path = value
    module, _, name = path.partition(":")

    if not module or not name:
        raise ValueError(f"The shape {path!r} is not a path of the form module:name")

    try:
        found: object = getattr(import_module(module), name, None)
    except ImportError as error:
        raise ValueError(f"The shape {path!r} cannot be imported: {error}") from error

    match found:
        case Schema():
            return found
        case _:
            raise ValueError(f"The shape {path!r} is not a PyMongoArrow Schema")


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class MongoDBSource(Registered):
    """A MongoDB server that the `MongoDBEngine` reads in pages of `_id`.

    `shape` is the PyMongoArrow schema of what a read returns, as a `Schema` or the path
    `module:name` of one. A paged read of a collection needs the field `_id` in it.
    `batch_size` is the documents of one page, `auth_source` the database that holds
    the user, `tls` turns TLS on or off, and `concurrency` is the chunks that a dump
    reads at the same time.
    """

    name: ClassVar[str] = "mongodb"
    probe_kind: ClassVar[str] = "mongodb-range-1"
    range_column_needed: ClassVar[bool] = True

    conn: DBConn
    shape: Annotated[InstanceOf[Schema], BeforeValidator(load_schema)]
    batch_size: PositiveInt = MONGO_BATCH_ROWS
    auth_source: str | None = None
    tls: bool | None = None
    concurrency: PositiveInt = DEFAULT_CONCURRENCY

    @property
    def uri(self) -> str:
        """Return the connection string, with the login quoted and the options set."""
        settings: dict[str, str] = {}

        if self.auth_source:
            settings["authSource"] = self.auth_source
        if self.tls is not None:
            settings["tls"] = "true" if self.tls else "false"

        login = f"{quote_plus(self.conn.user)}:{quote_plus(self.conn.password)}"
        host = f"{self.conn.hostname}:{self.conn.port}"

        return f"mongodb://{login}@{host}/?{urlencode(settings)}"
