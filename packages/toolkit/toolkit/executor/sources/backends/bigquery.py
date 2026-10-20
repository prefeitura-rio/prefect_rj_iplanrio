"""BigQuery, attached to DuckDB through the community `bigquery` extension (ADC)."""

from dataclasses import dataclass, field
from typing import ClassVar, override

from psycopg.sql import Identifier, Literal

from ....settings import settings
from ...constants import ALIAS
from ...engines.duckdb import Extension
from ...engines.protocol import Engine
from ...render import render_template
from ..base import SourceBase


@dataclass(frozen=True, slots=True)
class BigQuerySource(SourceBase):
    """Read BigQuery with workload identity and no password.

    The project is attached read-only as `src`.
    """

    extensions: ClassVar[tuple[Extension, ...]] = (
        Extension("bigquery", community=True),
    )

    project: str = field(default_factory=lambda: settings.BIGQUERY_PROJECT)

    @override
    async def setup(self, client: Engine) -> None:
        """Attach the project.

        Raise `ValueError` if there is no project.
        """
        if not self.project:
            raise ValueError(
                "BigQuerySource needs a project: pass it or set BIGQUERY_PROJECT"
            )

        sql = render_template(
            "duckdb/attach_bigquery",
            {
                "alias": Identifier(ALIAS),
                "project": Literal(f"project={self.project}"),
            },
        )
        await client.execute(sql)
