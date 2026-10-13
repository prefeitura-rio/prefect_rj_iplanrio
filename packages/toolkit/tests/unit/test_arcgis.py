import json

import polars as pl
import pytest
from pydantic import JsonValue

from toolkit.dump_arcgis import (
    esri_to_geojson,
    features_to_frame,
    query_url,
    reproject_to_parquet,
)


@pytest.mark.parametrize(
    ("geometry", "expected"),
    [
        pytest.param(
            {"rings": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
            {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
            id="polygon",
        ),
        pytest.param(
            {"x": 1.5, "y": 2.5},
            {"type": "Point", "coordinates": [1.5, 2.5]},
            id="point",
        ),
        pytest.param(
            {"paths": [[[0, 0], [1, 1]], [[2, 2], [3, 3]]]},
            {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
            id="first_path_only",
        ),
        pytest.param({}, None, id="empty"),
        pytest.param({"rings": "x"}, None, id="rings_not_a_list"),
        pytest.param({"rings": []}, None, id="empty_rings"),
        pytest.param({"x": "1", "y": 2}, None, id="point_with_a_string_coordinate"),
        pytest.param({"paths": []}, None, id="empty_paths"),
        pytest.param({"paths": ["x"]}, None, id="path_not_a_list"),
    ],
)
def test_esri_to_geojson(geometry, expected):
    assert esri_to_geojson(geometry) == expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param("https://h/layer/0", "https://h/layer/0/query", id="plain"),
        pytest.param(
            "https://h/layer/0/", "https://h/layer/0/query", id="trailing_slash"
        ),
        pytest.param(
            "https://h/layer/0/query", "https://h/layer/0/query", id="already_query"
        ),
    ],
)
def test_query_url(url, expected):
    assert query_url(url) == expected


def test_features_without_geometry_are_dropped():
    features: list[dict[str, JsonValue]] = [
        {"attributes": {"id": 1}, "geometry": {"x": 1, "y": 2}},
        {"attributes": {"id": 2}, "geometry": {}},
    ]
    frame = features_to_frame(features)
    assert frame["id"].to_list() == [1]
    assert json.loads(frame["geometry"][0]) == {"type": "Point", "coordinates": [1, 2]}


@pytest.mark.extensions
async def test_reproject_points_to_wgs84(tmp_path):
    # EPSG:31983 (SIRGAS 2000 / UTM 23S) near Rio de Janeiro
    frame = features_to_frame(
        [{"attributes": {"id": 1}, "geometry": {"x": 683000.0, "y": 7460000.0}}]
    )
    out = await reproject_to_parquet(frame, "EPSG:31983", tmp_path)
    row = pl.read_parquet(out).row(0, named=True)
    assert row["geometry"].startswith("POINT (")
    assert -44 < row["longitude"] < -42
    assert -24 < row["latitude"] < -22
