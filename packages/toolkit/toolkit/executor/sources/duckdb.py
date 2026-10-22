"""The sources that DuckDB reads in place: databases it attaches, files and services."""

from dataclasses import dataclass
from typing import ClassVar, override

from psycopg.sql import Identifier, Literal
from pydantic.dataclasses import dataclass as pydantic_dataclass

from ...types import TemplateValue
from ..constants import ALIAS, SECRET, SOURCE_CONFIG
from ..engines.base import Engine
from ..render import render_template
from .base import Registered
from .protocols import DBConn


@dataclass(frozen=True, slots=True)
class Extension:
    """One DuckDB extension to install and load."""

    name: str
    community: bool = False


@pydantic_dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class DuckDBSource(Registered):
    """Base of the sources that DuckDB reads in place.

    The `DuckDBEngine` loads `extensions`, then runs `setup` to attach or to log in. A
    backend sets `name` and, if needed, `extensions`, and overrides `setup`.
    """

    extensions: ClassVar[tuple[Extension, ...]] = ()

    async def setup(self, engine: Engine) -> None:
        """Prepare the DuckDB engine for the source. The default does nothing."""


@pydantic_dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class AttachedSource(DuckDBSource):
    """Base of the databases that DuckDB attaches with an extension.

    A backend only sets `name`, which is the attach type, and if needed `extensions` and
    `read_only`.
    """

    read_only: ClassVar[bool] = True

    conn: DBConn

    @override
    async def setup(self, engine: Engine) -> None:
        """Create the secret and attach the database as `src`.

        The password is a bound value.
        """
        connection = self.conn
        values: dict[str, TemplateValue] = {
            "secret": Identifier(SECRET),
            "alias": Identifier(ALIAS),
            "type": self.name,
            "host": Literal(connection.hostname),
            "port": Literal(connection.port),
            "database": Literal(connection.database),
            "user": Literal(connection.user),
            "read_only": self.read_only,
        }
        await engine.execute(
            render_template("duckdb/create_source_secret", values),
            params=[connection.password],
        )
        await engine.execute(render_template("duckdb/attach", values))
