"""ArcGIS REST layers, read through the core DuckDB `spatial` extension."""

from typing import ClassVar

from pydantic.dataclasses import dataclass

from ...constants import SOURCE_CONFIG
from ..duckdb import DuckDBSource, Extension


@dataclass(frozen=True, slots=True, config=SOURCE_CONFIG)
class ArcGISSource(DuckDBSource):
    """Read ArcGIS layers with the `spatial` extension.

    A template reads a layer with the `arcgis_layer` macro of `duckdb/macros.sql`.
    """

    name: ClassVar[str] = "arcgis"
    extensions: ClassVar[tuple[Extension, ...]] = (Extension("spatial"),)
