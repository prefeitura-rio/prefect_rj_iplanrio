"""The source for databases that DuckDB scans with an extension."""

from dataclasses import dataclass
from typing import ClassVar, LiteralString, Self, override

from psycopg.sql import SQL, Composable, Identifier, Literal

from ...partitions import Selection
from ...types import TemplateValue
from ..constants import ALIAS, SECRET
from ..engines.duckdb import Extension
from ..templates import render_template
from ..utils import window_mapping
from .base import SourceBase
from .protocols import DBConn, SourceContext


@dataclass(frozen=True, slots=True)
class DuckDBSource(SourceBase, skip=True):
    """Base class of the databases that DuckDB scans with an extension.

    A backend subclasses it and only sets the class attributes:
    ``attach_type``, ``query_function``, and optionally ``extensions`` and
    ``read_only``. The behaviour lives here.
    """

    attach_type: ClassVar[str]
    query_function: ClassVar[LiteralString]
    extensions: ClassVar[tuple[Extension, ...]] = ()
    read_only: ClassVar[bool] = True

    conn: DBConn

    @classmethod
    @override
    def from_context(cls, context: SourceContext) -> Self:
        """Build the source from the connection data of the context.

        :raises ValueError: If the context has no connection data.
        """
        return cls(context.require_conn(cls.name))

    @override
    def window(self, query: str, selection: Selection) -> str:
        """Return the remote query restricted to the selection."""
        return render_template(f"{self.name}/window", window_mapping(query, selection))

    @override
    def setup(self) -> str:
        """Return the SQL that creates the secret and attaches the database as ``src``."""
        connection = self.conn
        values: dict[str, TemplateValue] = {
            "secret": Identifier(SECRET),
            "alias": Identifier(ALIAS),
            "type": self.attach_type,
            "host": Literal(connection.hostname),
            "port": Literal(connection.port),
            "database": Literal(connection.database),
            "user": Literal(connection.user),
            "password": Literal(connection.password),
            "read_only": self.read_only,
        }

        return render_template("duckdb/attach", values)

    @override
    def scan(self, query: str) -> Composable:
        """Return the ``FROM`` expression that runs the remote query."""
        return SQL("{}({}, {})").format(
            SQL(self.query_function), Literal(ALIAS), Literal(query)
        )
