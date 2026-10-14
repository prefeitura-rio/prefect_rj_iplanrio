"""ArcGIS REST layers, read through the core DuckDB `spatial` extension."""

from dataclasses import dataclass
from typing import ClassVar

from ...engines.duckdb import Extension
from ..base import SourceBase


@dataclass(frozen=True, slots=True)
class ArcGISSource(SourceBase):
    """Read ArcGIS layers with the `spatial` extension.

    A template reads a layer with the `arcgis_layer` macro of `duckdb/macros.sql`.
    """

    extensions: ClassVar[tuple[Extension, ...]] = (Extension("spatial"),)
