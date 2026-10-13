"""Build and run the statement that copies a source to the staging bucket."""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal as TypingLiteral
from uuid import uuid4

from psycopg.sql import SQL, Composable, Identifier, Literal
from pydantic import BaseModel

from .columns import normalize_columns
from .constants import HIVE_DATE, HIVE_MONTH, HIVE_YEAR, INGESTION_COLUMN
from .executor.executor import Executor
from .types import TemplateValue

type FileFormat = TypingLiteral["parquet", "csv"]

FORMATS: Mapping[FileFormat, tuple[str, str]] = {
    "parquet": ("PARQUET", "parquet"),
    "csv": ("CSV, HEADER", "csv"),
}


class ColumnInfo(BaseModel):
    """One column returned by ``DESCRIBE``."""

    column_name: str
    column_type: str


def parquet_scan(pattern: str) -> Composable:
    """Return the DuckDB ``FROM`` expression that reads Parquet files.

    :param pattern: A file path or glob.
    """
    return SQL("read_parquet({})").format(Literal(pattern))


@dataclass(frozen=True, slots=True)
class ExtractionPlan:
    """What to copy and where."""

    scan: Composable
    target_uri: str
    file_format: FileFormat = "parquet"
    date_column: str | None = None
    add_timestamp: bool = False


def build_copy_query(
    plan: ExtractionPlan, columns: list[ColumnInfo], token: str
) -> tuple[str, dict[str, TemplateValue]]:
    """Return the copy template path and its values.

    :param plan: What to copy and where.
    :param columns: The source columns from ``DESCRIBE``.
    :param token: Unique token for the output file names.
    :raises ValueError: If ``plan.date_column`` is not a source column, or two
        columns collide after normalization.
    """
    sources = [column.column_name for column in columns]
    aliases = normalize_columns(sources)
    items: list[TemplateValue] = [
        {
            "source": Identifier(column.column_name),
            "alias": Identifier(alias),
            "clean": column.column_type == "VARCHAR",
        }
        for column, alias in zip(columns, aliases, strict=True)
    ]
    date_column = plan.date_column
    if date_column is not None and date_column not in aliases:
        raise ValueError(f"Partition column {date_column!r} is not a source column")

    options, extension = FORMATS[plan.file_format]
    partitioned = date_column is not None
    uri = plan.target_uri.rstrip("/")
    path = f"{uri}/" if partitioned else f"{uri}/data_{token}.{extension}"
    mapping: dict[str, TemplateValue] = {
        "source": plan.scan,
        "columns": items,
        "date_column": Identifier(date_column) if date_column else False,
        "year_column": Identifier(HIVE_YEAR),
        "month_column": Identifier(HIVE_MONTH),
        "day_column": Identifier(HIVE_DATE),
        "add_timestamp": plan.add_timestamp,
        "timestamp_column": Identifier(INGESTION_COLUMN),
        "path": Literal(path),
        "format": options,
        "partition_columns": (
            ", ".join(
                Identifier(name).as_string(None)
                for name in (HIVE_YEAR, HIVE_MONTH, HIVE_DATE)
            )
            if partitioned
            else False
        ),
        "pattern": Literal(f"data_{token}_{{i}}"),
    }
    return "duckdb/copy_to_staging", mapping


async def run_extraction(executor: Executor, plan: ExtractionPlan) -> None:
    """Describe the source, then copy it to the staging bucket.

    :param executor: Executor on a connection with the source attached.
    :param plan: What to copy and where.
    """
    columns = await executor.query(
        "duckdb/describe_source", {"source": plan.scan}, expect=ColumnInfo
    )
    template, mapping = build_copy_query(plan, columns, uuid4().hex)
    await executor.execute(template, mapping)
