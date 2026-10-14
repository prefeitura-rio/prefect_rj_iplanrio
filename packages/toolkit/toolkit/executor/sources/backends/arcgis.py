"""ArcGIS REST layers, read through the core DuckDB ``spatial`` extension."""

from dataclasses import dataclass
from typing import ClassVar

from ...engines.duckdb import Extension
from ..base import SourceBase


@dataclass(frozen=True, slots=True)
class ArcGISSource(SourceBase):
    """Read ArcGIS layers with ``ST_Read``, which pages through the layer by itself.

    It only loads the extension. A template reads a layer with the ``arcgis_layer``
    macro of ``duckdb/macros.sql``. The geometry keeps the CRS of the layer.
    """

    extensions: ClassVar[tuple[Extension, ...]] = (Extension("spatial"),)
