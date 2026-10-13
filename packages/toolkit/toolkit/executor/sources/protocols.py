"""The contract of a source: where the data comes from."""

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Protocol, cast, runtime_checkable

from psycopg.sql import Composable

from ...partitions import Selection
from ..engines.duckdb import Extension


@dataclass(frozen=True, slots=True)
class DBConn:
    """Connection parameters of one source database."""

    hostname: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class SourceContext:
    """Everything a source may need when it is built. Each source reads what it uses."""

    conn: DBConn | None
    workdir: Path
    batch_size: int

    def require_conn(self, name: str) -> DBConn:
        """Return the connection data.

        :param name: The source name, for the error message.
        :raises ValueError: If the context has no connection data.
        """
        if self.conn is None:
            raise ValueError(f"Source {name} needs connection data")
        return self.conn


class Source(Protocol):
    """A remote system a dump reads from. It describes the system; it never runs SQL.

    The ``Executor`` makes the engine read it: it loads ``extensions``, runs
    ``setup`` and reads ``scan``. The toolkit provides postgres, mysql, mssql,
    bigquery and oracle. Any class that follows this protocol also works.
    """

    name: ClassVar[str]
    """The source name, also the template folder of its window query."""

    extensions: ClassVar[tuple[Extension, ...]]
    """The DuckDB extensions the engine must load to read the source."""

    def window(self, query: str, selection: Selection) -> str:
        """Return the remote query restricted to the selection, in the source dialect."""
        ...

    def setup(self) -> str | None:
        """Return the SQL to run on the engine before the scan, or ``None``."""
        ...

    def scan(self, query: str) -> Composable:
        """Return the ``FROM`` expression that reads the query result.

        :param query: The remote query, already restricted by :meth:`window`.
        """
        ...


@runtime_checkable
class Fetcher(Protocol):
    """A source the engine cannot read directly: it copies the data to local files."""

    async def fetch(self, query: str) -> list[Path]:
        """Copy the query result to local files.

        :returns: The files. Empty when the query has no rows.
        """
        ...


def as_fetcher(source: object) -> Fetcher | None:
    """Return the source as a ``Fetcher`` when it has ``fetch``, and ``None`` otherwise.

    The check is on the method, so a source needs no base class: any object with
    ``fetch`` is used as a ``Fetcher``.

    :param source: A source, or any other object.
    """
    return cast(Fetcher, source) if hasattr(source, "fetch") else None


type SourceFactory = Callable[[SourceContext], Source]
