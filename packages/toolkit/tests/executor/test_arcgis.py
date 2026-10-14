import json
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from typing import TypedDict
from urllib.parse import parse_qs, urlparse

import pytest
from psycopg.sql import Literal

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.executor.sources.backends.arcgis import ArcGISSource
from toolkit.types import Geometry

FEATURES = [
    {"attributes": {"id": 1, "name": "a"}, "geometry": {"x": 1.0, "y": 2.0}},
    {"attributes": {"id": 2, "name": "b"}, "geometry": {"x": 3.0, "y": 4.0}},
    {"attributes": {"id": 3, "name": "c"}, "geometry": {"x": 5.0, "y": 6.0}},
]
PAGE_SIZE = 2


@pytest.fixture
def layer() -> Iterator[tuple[str, list[dict[str, list[str]]]]]:
    """Serve one ArcGIS layer of three points with a page size of two.

    It yields the layer URL and the list that logs the query string of every
    ``/query`` request.
    """
    queries: list[dict[str, list[str]]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            if parsed.path.endswith("/query"):
                queries.append(query)
                offset = int(query.get("resultOffset", ["0"])[0])
                page = FEATURES[offset : offset + PAGE_SIZE]
                body = {
                    "objectIdFieldName": "id",
                    "geometryType": "esriGeometryPoint",
                    "spatialReference": {"wkid": 4326},
                    "fields": [
                        {"name": "id", "type": "esriFieldTypeInteger"},
                        {"name": "name", "type": "esriFieldTypeString"},
                    ],
                    "features": page,
                    "exceededTransferLimit": offset + PAGE_SIZE < len(FEATURES),
                }
            else:
                body = {
                    "name": "points",
                    "type": "Feature Layer",
                    "geometryType": "esriGeometryPoint",
                    "maxRecordCount": PAGE_SIZE,
                    "fields": [
                        {"name": "id", "type": "esriFieldTypeInteger"},
                        {"name": "name", "type": "esriFieldTypeString"},
                    ],
                }
            payload = json.dumps(body).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    Thread(target=server.serve_forever, daemon=True).start()
    url = (
        f"http://127.0.0.1:{server.server_port}/arcgis/rest/services/s/FeatureServer/0"
    )
    yield url, queries
    server.shutdown()


class Point(TypedDict):
    id: int
    name: str
    geom: Geometry


async def read_layer(url: str, directory: Path, where: str = "1=1") -> list[int]:
    """Read the ids of the layer with the macro, through the executor."""
    (directory / "points.sql").write_text(
        '{% from "duckdb/macros.sql" import arcgis_layer %}'
        "SELECT id, name, geom FROM {{ arcgis_layer(url, where) }} ORDER BY id\n"
    )
    source = ArcGISSource()

    async with DuckDBEngine.connect(source.extensions) as engine:
        dataset = await Executor(engine, directory).query(
            "points", {"url": Literal(url), "where": Literal(where)}, expect=Point
        )

    assert dataset.frame.schema["geom"].is_extension()
    return dataset.frame["id"].to_list()


@pytest.mark.extensions
async def test_a_layer_is_read_through_every_page(layer, tmp_path: Path):
    url, queries = layer

    assert await read_layer(url, tmp_path) == [1, 2, 3]
    assert len(queries) > 1


@pytest.mark.extensions
async def test_the_where_clause_reaches_the_server_intact(layer, tmp_path: Path):
    url, queries = layer
    where = "id > 1 AND name <> 'x'"

    await read_layer(url, tmp_path, where)

    assert {query["where"][0] for query in queries} == {where}
