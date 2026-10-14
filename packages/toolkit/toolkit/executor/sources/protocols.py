"""The contract of a source: the system the statements run in."""

from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from typing import ClassVar, Protocol

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
    """Everything a source may need when it is built. Each source reads what it uses."""

    conn: DBConn | None

    def require_conn(self, name: str) -> DBConn:
        """Return the connection data.

        :param name: The source name, for the error message.
        :raises ValueError: If the context has no connection data.
        """
        if self.conn is None:
            raise ValueError(f"Source {name} needs connection data")
        return self.conn


class Source(Protocol):
    """A system the statements run in. It provides the client; the ``Executor`` runs SQL.

    The toolkit provides postgres, mysql, mssql, bigquery and oracle. Any class that
    follows this protocol also works.
    """

    name: ClassVar[str]
    """The source name."""

    def open(self, engine: Engine) -> AbstractAsyncContextManager[Engine]:
        """Open the client that runs statements in this system, and close it on exit.

        A system that DuckDB attaches yields the ``engine`` itself, after loading its
        extensions. A system that DuckDB cannot attach, such as Oracle, ignores the
        ``engine`` and yields a client of its own. The client follows ``Engine``.
        """
        ...

    async def setup(self, client: Engine) -> None:
        """Run the statements that prepare the client, such as the secret and the ATTACH."""
        ...
