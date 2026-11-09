import json
from pathlib import Path
from typing import Literal, get_args

import polars as pl
import pytest

from pipelines.rj_smct__mit import utils
from pipelines.rj_smct__mit.flow import rj_smct__mit


def test_flow_table_ids_match_configured_tables() -> None:
    annotation = rj_smct__mit.fn.__annotations__["table_ids"]
    literal = next(arg for arg in get_args(get_args(annotation)[0]) if get_args(arg))

    assert Literal[get_args(literal)] == Literal[tuple(utils.TABLES)]


def test_get_table_config_rejects_unknown_table() -> None:
    with pytest.raises(ValueError, match="não suportado"):
        utils.get_table_config("lidar_mare")


def test_clean_column_name() -> None:
    assert utils.clean_column_name("pm1.0_avg") == "pm1_0_avg"
    assert utils.clean_column_name("PM2.5_cal") == "pm2_5_cal"
    assert utils.clean_column_name(" Valor (m) ") == "valor_m"
    assert utils.clean_column_name("10x") == "_10x"


def test_parse_capture_name() -> None:
    assert utils.parse_capture_name("OM_1_20251201_evening_12durmin") == {
        "route_id": "OM_1",
        "capture_date": "2025-12-01",
        "capture_period": "evening",
        "capture_duration_min": "12",
    }


def test_parse_capture_name_returns_nulls_for_unexpected_name() -> None:
    assert set(utils.parse_capture_name("qualquer_coisa").values()) == {None}


def test_reference_column_order_uses_most_common_order() -> None:
    dataframes = {
        Path("a.csv"): pl.DataFrame({"x": ["1"], "y": ["2"]}),
        Path("b.csv"): pl.DataFrame({"x": ["1"], "y": ["2"]}),
        Path("c.csv"): pl.DataFrame({"y": ["2"], "x": ["1"]}),
    }

    assert utils.reference_column_order(dataframes) == ["x", "y"]


def test_reference_column_order_rejects_different_columns() -> None:
    dataframes = {
        Path("a.csv"): pl.DataFrame({"x": ["1"]}),
        Path("b.csv"): pl.DataFrame({"z": ["1"]}),
    }

    with pytest.raises(ValueError, match="conjunto de colunas"):
        utils.reference_column_order(dataframes)


def test_read_route_metadata_flattens_edges(tmp_path: Path) -> None:
    route = {
        "route_id": "OM_1",
        "edges": [{"u": 1, "v": 2, "key": 0, "edge_order": 0}, {"u": 2, "v": 3, "key": 0, "edge_order": 1}],
        "edge_geometry": {"(1,2,0)": {"geometry": "LINESTRING (0 0, 1 1)", "length": 1.5}},
    }
    path = tmp_path / "route.json"
    path.write_text(json.dumps(route), encoding="utf-8")

    rows = utils.read_route_metadata(path).to_dicts()

    assert rows[0] == {
        "route_id": "OM_1",
        "edge_order": "0",
        "u": "1",
        "v": "2",
        "key": "0",
        "geometry": "LINESTRING (0 0, 1 1)",
        "length": "1.5",
    }
    assert rows[1]["geometry"] is None


def test_convert_to_parquet_adds_capture_columns_and_standardizes_schema(tmp_path: Path) -> None:
    (tmp_path / "OM_1_20251201_evening_12durmin.csv").write_text("Edge ID,Valor (m)\n1,\n", encoding="utf-8")
    (tmp_path / "OM_2_20251202_morning_8durmin.csv").write_text("Valor (m),Edge ID\n2.5,2\n", encoding="utf-8")

    output_dir = utils.convert_to_parquet(tmp_path, "matched")

    frames = [pl.read_parquet(path) for path in sorted(output_dir.glob("*.parquet"))]
    assert len({tuple(frame.schema.items()) for frame in frames}) == 1
    combined = pl.concat(frames)
    assert combined.columns == [
        "edge_id",
        "valor_m",
        "source_file",
        "route_id",
        "capture_date",
        "capture_period",
        "capture_duration_min",
    ]
    assert combined["route_id"].to_list() == ["OM_1", "OM_2"]
    assert combined["valor_m"].to_list() == [None, "2.5"]
