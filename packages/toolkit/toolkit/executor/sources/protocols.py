"""The contract of a source: the system the statements run in."""

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar, Protocol, runtime_checkable

from ...batches import Rows
from ...types import TemplateValue
from ..engines.protocol import Engine


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
    """Everything a source may need when it is built.

    Each source reads what it uses.
    """

    conn: DBConn | None

    def require_conn(self, name: str) -> DBConn:
        """Return the connection data of the context.

        Raise `ValueError`, naming `name`, if it has none.
        """
        if self.conn is None:
            raise ValueError(f"Source {name} needs connection data")
        return self.conn


class Source(Protocol):
    """A system the statements run in.

    It provides the client that the `Executor` runs SQL on.
    """

    name: ClassVar[str]
    """The source name."""

    def open(self, engine: Engine) -> AbstractAsyncContextManager[Engine]:
        """Open the client that runs statements in this system.

        Close the client on exit. A system that DuckDB attaches yields the `engine`
        itself. One that DuckDB cannot read, such as Oracle, yields an engine of its
        own.
        """
        ...

    async def setup(self, client: Engine) -> None:
        """Run the statements that prepare the client, such as `ATTACH`."""
        ...


@runtime_checkable
class Stager(Protocol):
    """A source whose client is not DuckDB: it stages rows as Parquet for DuckDB."""

    async def stage(
        self,
        client: Engine,
        values: Mapping[str, TemplateValue],
        rows: Rows,
        folder: Path,
    ) -> Mapping[str, TemplateValue]:
        """Copy the rows to Parquet in `folder`, and return the values that read it.

        The copy has the filters and the window already applied, and it keeps the
        schema when there are no rows.
        """
        ...
