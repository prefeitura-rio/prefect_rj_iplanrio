"""Oracle, read through the community DuckDB `oracle_scanner` extension."""

from dataclasses import dataclass
from typing import ClassVar, Self, override

from psycopg.sql import Identifier, Literal

from ....types import TemplateValue
from ...constants import SECRET
from ...engines.duckdb import Extension
from ...engines.protocol import Engine
from ...render import render_template
from ..base import SourceBase
from ..protocols import DBConn, SourceContext


@dataclass(frozen=True, slots=True)
class OracleSource(SourceBase):
    """Oracle, read through the community DuckDB `oracle_scanner` extension.

    The extension needs no Oracle client. A template reads with `oracle_query`.
    """

    extensions: ClassVar[tuple[Extension, ...]] = (
        Extension("oracle_scanner", community=True),
    )

    conn: DBConn

    @override
    @classmethod
    def from_context(cls, context: SourceContext) -> Self:
        """Build the Oracle source from the connection data.

        Raise `ValueError` if the context has none.
        """
        return cls(context.require_conn(cls.name))

    @override
    async def setup(self, client: Engine) -> None:
        """Create the secret that `oracle_query` reads.

        The password is a bound value. `database` is the Oracle service name.
        """
        connection = self.conn
        values: dict[str, TemplateValue] = {
            "secret": Identifier(SECRET),
            "host": Literal(connection.hostname),
            "port": Literal(connection.port),
            "service": Literal(connection.database),
            "user": Literal(connection.user),
        }
        await client.execute(
            render_template("duckdb/create_oracle_secret", values),
            params=[connection.password],
        )
