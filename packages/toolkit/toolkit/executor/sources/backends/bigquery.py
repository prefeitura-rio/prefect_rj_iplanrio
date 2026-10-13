"""BigQuery, read through the community DuckDB ``bigquery`` extension (ADC)."""

from dataclasses import dataclass
from typing import ClassVar, override

from psycopg.sql import SQL, Composable, Literal

from ....partitions import Selection
from ....settings import settings
from ...engines.duckdb import Extension
from ...templates import render_template
from ...utils import window_mapping
from ..base import SourceBase


@dataclass(frozen=True, slots=True)
class BigQuerySource(SourceBase):
    """Read BigQuery with workload identity. It needs no connection data."""

    extensions: ClassVar[tuple[Extension, ...]] = (
        Extension("bigquery", community=True),
    )

    project: str | None = None

    @override
    def window(self, query: str, selection: Selection) -> str:
        """Return the BigQuery query restricted to the selection."""
        return render_template("bigquery/window", window_mapping(query, selection))

    @override
    def setup(self) -> None:
        """Return ``None``: ``bigquery_query`` needs no ATTACH."""

    @override
    def scan(self, query: str) -> Composable:
        """Return the ``bigquery_query`` expression for the project."""
        project = self.project or settings.BIGQUERY_PROJECT
        return SQL("bigquery_query({}, {})").format(Literal(project), Literal(query))
