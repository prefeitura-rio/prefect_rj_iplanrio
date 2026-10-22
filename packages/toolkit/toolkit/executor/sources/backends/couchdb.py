"""The CouchDB source: the configuration that the `CouchDBEngine` reads with."""

from dataclasses import field
from importlib import import_module
from typing import Annotated, ClassVar

from httpx2 import AsyncClient, Timeout
from pyarrow import json_, schema, string
from pydantic import BeforeValidator, InstanceOf, PositiveInt
from pydantic.dataclasses import dataclass

from ....settings import CouchDBSettings
from ...constants import COUCHDB_PLAIN_PORT, DEFAULT_CONCURRENCY, SOURCE_CONFIG
from ..base import Registered
from ..protocols import DBConn
from ..shapes import Shape, build_document_row


def load_shape(value: str | Shape) -> Shape:
    """Return the shape that a path `module:name` names, and any other value as it is.

    Raise `ValueError` for a path that leads to no `Shape`.
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
        case Shape():
            return found
        case _:
            raise ValueError(f"The shape {path!r} is not a Shape")


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class CouchDBSource(Registered):
    """A CouchDB server that the `CouchDBEngine` reads in pages.

    `batch_size` is the documents of one page, `key_width` the digits of the id that cut
    the chunks, and `concurrency` the chunks that a dump reads at the same time.
    `shape` is the columns of a row, as a `Shape` or the path `module:name` of one.
    """

    name: ClassVar[str] = "couchdb"

    conn: DBConn
    shape: Annotated[InstanceOf[Shape], BeforeValidator(load_shape)] = field(
        default_factory=lambda: Shape(
            schema([("id", string()), ("doc", json_())]), build_document_row
        )
    )
    batch_size: PositiveInt = field(
        default_factory=lambda: CouchDBSettings().COUCHDB_PAGE_SIZE
    )
    verify_tls: bool = field(
        default_factory=lambda: CouchDBSettings().COUCHDB_VERIFY_TLS
    )
    secure: bool | None = field(
        default_factory=lambda: CouchDBSettings().COUCHDB_SECURE
    )
    path_prefix: str = field(
        default_factory=lambda: CouchDBSettings().COUCHDB_PATH_PREFIX
    )
    timeout: int = field(
        default_factory=lambda: CouchDBSettings().COUCHDB_TIMEOUT_SECONDS
    )
    key_width: int | None = None
    concurrency: PositiveInt = DEFAULT_CONCURRENCY

    @property
    def base_url(self) -> str:
        """Return the URL of the server, with its path prefix and without the login."""
        secure = (
            self.conn.port != COUCHDB_PLAIN_PORT if self.secure is None else self.secure
        )
        prefix = (
            f"/{self.path_prefix.strip('/')}" if self.path_prefix.strip("/") else ""
        )

        return f"{'https' if secure else 'http'}://{self.conn.hostname}:{self.conn.port}{prefix}"

    def build_client(self) -> AsyncClient:
        """Return a client with the login in the header and the TLS setting."""
        return AsyncClient(
            base_url=self.base_url,
            auth=(self.conn.user, self.conn.password),
            verify=self.verify_tls,
            timeout=Timeout(self.timeout),
        )
