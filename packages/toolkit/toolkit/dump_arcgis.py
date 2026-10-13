"""Download an ArcGIS REST layer and stage it with geometries in EPSG:4326."""

import json
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path

import polars as pl
import requests
from asyncer import asyncify
from psycopg.sql import Literal
from pydantic import BaseModel, Field, JsonValue

from .dump import StageRequest, stage_scan
from .executor.engines.duckdb import DuckDBEngine, Extension
from .executor.executor import Executor
from .extraction import parquet_scan
from .log import logger

SPATIAL = Extension("spatial")
PAGE_TIMEOUT_SECONDS = 30


class Page(BaseModel):
    """One page of an ArcGIS ``query`` response."""

    features: list[dict[str, JsonValue]] = Field(default_factory=list)
    exceededTransferLimit: bool = False  # noqa: N815


def esri_to_geojson(geometry: Mapping[str, JsonValue]) -> dict[str, JsonValue] | None:
    """Convert an ESRI JSON geometry to GeoJSON.

    :param geometry: ESRI geometry with ``rings``, ``x`` and ``y``, or ``paths``.
    :returns: The GeoJSON geometry, or ``None`` when the shape is not supported.
    """
    match geometry:
        case {"rings": list() as rings} if rings:
            return {"type": "Polygon", "coordinates": rings}
        case {"x": int() | float() as x, "y": int() | float() as y}:
            return {"type": "Point", "coordinates": [x, y]}
        case {"paths": [list() as path, *_]}:
            return {"type": "LineString", "coordinates": path}
        case _:
            return None


def query_url(url: str) -> str:
    """Return the layer ``/query`` URL."""
    return url.rstrip("/").removesuffix("/query") + "/query"


def fetch_features(url: str) -> list[dict[str, JsonValue]]:
    """Fetch every feature of a layer, page by page.

    :param url: Layer URL, with or without ``/query``.
    :returns: The raw ESRI features.
    :raises ValueError: If the server fails or returns no features.
    """
    params: dict[str, str | int] = {
        "where": "1=1",
        "outFields": "*",
        "returnGeometry": "true",
        "f": "json",
        "resultOffset": 0,
    }
    features: list[dict[str, JsonValue]] = []
    while True:
        try:
            response = requests.get(
                query_url(url), params=params, timeout=PAGE_TIMEOUT_SECONDS
            )
            response.raise_for_status()
            page = Page.model_validate(response.json())
        except requests.RequestException as error:
            raise ValueError("ArcGIS request failed") from error
        if not page.features:
            break
        features.extend(page.features)
        params["resultOffset"] = len(features)
        if not page.exceededTransferLimit:
            break
    if not features:
        raise ValueError("ArcGIS layer returned no features")
    logger.info("ArcGIS layer downloaded: features=%d", len(features))
    return features


def json_mapping(value: JsonValue) -> Mapping[str, JsonValue]:
    """Return the value when it is a JSON object, and an empty mapping otherwise."""
    match value:
        case dict():
            return value
        case _:
            return {}


def features_to_frame(features: Sequence[Mapping[str, JsonValue]]) -> pl.DataFrame:
    """Build a frame of attributes plus a ``geometry`` GeoJSON string column.

    Features without a supported geometry are dropped.

    :param features: Raw ESRI features.
    """
    rows: list[dict[str, JsonValue]] = []
    for feature in features:
        geometry = json_mapping(feature.get("geometry"))
        geojson = esri_to_geojson(geometry) if geometry else None
        if geojson is not None:
            attributes = json_mapping(feature.get("attributes"))
            rows.append({**attributes, "geometry": json.dumps(geojson)})
    return pl.from_dicts(rows, infer_schema_length=None)


async def reproject_to_parquet(frame: pl.DataFrame, crs: str, directory: Path) -> Path:
    """Write the frame, then reproject its geometry to EPSG:4326 with DuckDB spatial.

    :param frame: Output of :func:`features_to_frame`.
    :param crs: Source CRS, for example ``EPSG:31983``.
    :param directory: Existing directory for the files.
    :returns: A Parquet file with WKT geometries, plus latitude and longitude when
        the layer has points.
    """
    raw = directory / "raw.parquet"
    out = directory / "data.parquet"
    frame.write_parquet(raw)
    async with DuckDBEngine.connect((SPATIAL,)) as engine:
        executor = Executor(engine)
        await executor.execute(
            "duckdb/reproject_geometry",
            {
                "source": Literal(str(raw)),
                "dest": Literal(str(out)),
                "crs": Literal(crs),
                "has_points": bool(frame["geometry"].str.contains('"Point"').any()),
            },
        )
    return out


async def dump_arcgis(url: str, crs: str, request: StageRequest) -> None:
    """Download a layer, reproject it and stage it.

    :param url: Layer URL.
    :param crs: Source CRS, for example ``EPSG:31983``.
    :param request: Destination and format.
    """
    features = await asyncify(fetch_features)(url)
    frame = features_to_frame(features)
    with tempfile.TemporaryDirectory() as directory:
        path = await reproject_to_parquet(frame, crs, Path(directory))
        await stage_scan(request, parquet_scan(str(path)))
