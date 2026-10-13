from pathlib import Path

import polars as pl
import pytest
from psycopg.sql import SQL

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor
from toolkit.extraction import (
    ColumnInfo,
    ExtractionPlan,
    build_copy_query,
    run_extraction,
)

SCAN = SQL(
    '(SELECT \'Ana\' AS "Nome Completo", DATE \'2024-01-15\' AS "Data Início", 1 AS "2024"'
    " UNION ALL SELECT 'Bia', DATE '2024-02-20', 2)"
)


async def stage(tmp_path: Path, **plan_options) -> None:
    (tmp_path / "out").mkdir()
    plan = ExtractionPlan(scan=SCAN, target_uri=str(tmp_path / "out"), **plan_options)
    async with DuckDBEngine.connect() as db:
        await run_extraction(Executor(db), plan)


async def test_unpartitioned_parquet_has_normalized_columns(tmp_path):
    await stage(tmp_path)
    files = list((tmp_path / "out").glob("data_*.parquet"))
    assert len(files) == 1
    frame = pl.read_parquet(files[0])
    assert frame.columns == ["nome_completo", "data_inicio", "_2024"]
    assert frame["nome_completo"].to_list() == ["Ana", "Bia"]


async def test_date_partitions_follow_hive_layout(tmp_path):
    await stage(tmp_path, date_column="data_inicio")
    folders = sorted(
        str(path.relative_to(tmp_path / "out"))
        for path in (tmp_path / "out").rglob("data_particao=*")
    )
    assert folders == [
        "ano_particao=2024/mes_particao=1/data_particao=2024-01-15",
        "ano_particao=2024/mes_particao=2/data_particao=2024-02-20",
    ]


async def test_ingestion_timestamp_column_is_added(tmp_path):
    await stage(tmp_path, add_timestamp=True)
    frame = pl.read_parquet(next((tmp_path / "out").glob("data_*.parquet")))
    assert frame.columns[-1] == "_prefect_extracted_at"
    assert frame["_prefect_extracted_at"].null_count() == 0


async def test_csv_format_writes_header(tmp_path):
    await stage(tmp_path, file_format="csv")
    file = next((tmp_path / "out").glob("data_*.csv"))
    assert file.read_text().splitlines()[0] == "nome_completo,data_inicio,_2024"


async def test_text_columns_lose_nul_and_line_breaks(tmp_path):
    (tmp_path / "out").mkdir()
    scan = SQL("(SELECT 'a' || chr(10) || 'b' || chr(0) AS t)")
    plan = ExtractionPlan(scan=scan, target_uri=str(tmp_path / "out"))
    async with DuckDBEngine.connect() as db:
        await run_extraction(Executor(db), plan)
    frame = pl.read_parquet(next((tmp_path / "out").glob("data_*.parquet")))
    assert frame["t"].to_list() == ["a b"]


def test_unknown_partition_column_is_rejected():
    plan = ExtractionPlan(scan=SCAN, target_uri="gs://b/p", date_column="missing")
    columns = [ColumnInfo(column_name="a", column_type="VARCHAR")]
    with pytest.raises(ValueError, match="not a source column"):
        build_copy_query(plan, columns, "t")
