"""BigQuery, attached to DuckDB through the community `bigquery` extension (ADC)."""

from dataclasses import field
from typing import ClassVar, override

from psycopg.sql import Identifier, Literal
from pydantic.dataclasses import dataclass

from ....settings import BigQuerySettings
from ...constants import ALIAS, SOURCE_CONFIG
from ...engines.base import Engine
from ...render import render_template
from ..duckdb import DuckDBSource, Extension


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class BigQuerySource(DuckDBSource):
    """Read BigQuery with workload identity and no password.

    The project is attached read-only as `src`. The `BIGQUERY_PROJECT` setting is the
    project without one.
    """

    name: ClassVar[str] = "bigquery"
    extensions: ClassVar[tuple[Extension, ...]] = (
        Extension("bigquery", community=True),
    )

    project: str = field(default_factory=lambda: BigQuerySettings().BIGQUERY_PROJECT)

    @override
    async def setup(self, engine: Engine) -> None:
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
        await engine.execute(sql)
