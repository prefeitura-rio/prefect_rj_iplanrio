"""The source for databases that DuckDB attaches with an extension."""

from dataclasses import dataclass
from typing import ClassVar, Self, override

from psycopg.sql import Identifier, Literal

from ...types import TemplateValue
from ..constants import ALIAS, SECRET
from ..engines.duckdb import Extension
from ..engines.protocol import Engine
from ..render import render_template
from .base import SourceBase
from .protocols import DBConn, SourceContext


@dataclass(frozen=True, slots=True)
class DuckDBSource(SourceBase, skip=True):
    """Base class of the databases that DuckDB attaches with an extension.

    A backend subclasses it and only sets the class attributes: ``attach_type``, and
    optionally ``extensions`` and ``read_only``. The behaviour lives here.
    """

    attach_type: ClassVar[str]
    extensions: ClassVar[tuple[Extension, ...]] = ()
    read_only: ClassVar[bool] = True

    conn: DBConn

    @classmethod
    def from_context(cls, context: SourceContext) -> Self:
        """Build the source from the connection data of the context.

        :raises ValueError: If the context has no connection data.
        """
        return cls(context.require_conn(cls.name))

    @override
    async def setup(self, client: Engine) -> None:
        """Create the secret and attach the database as ``src``.

        The password is a bound value, so it never enters the statement text.
        """
        connection = self.conn
        values: dict[str, TemplateValue] = {
            "secret": Identifier(SECRET),
            "alias": Identifier(ALIAS),
            "type": self.attach_type,
            "host": Literal(connection.hostname),
            "port": Literal(connection.port),
            "database": Literal(connection.database),
            "user": Literal(connection.user),
            "read_only": self.read_only,
        }
        await client.execute(
            render_template("duckdb/create_source_secret", values),
            params=[connection.password],
        )
        await client.execute(render_template("duckdb/attach", values))
