"""Transformation helpers applied to the raw DataFrame before loading.

Operations:
- Rename columns using `normalize_column_name` + the per-table `renomear` map.
- Deduplicate columns (two DLO names can normalise to the same final name).
- Drop columns not in the schema (with a warning — schema drift).
- Parse timestamp columns as UTC.
- Parse date columns as `datetime.date`.
- Convert boolean columns from text (`"true"` / `"false"`).
- Warn when `created_date` / `system_modstamp` are absent (fallback to today).
- Add control columns: `_loaded_at`, `data_criacao`, `data_atualizacao`.
- Reorder columns to match the schema exactly.
"""

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import polars as pl
from prefect.logging import get_logger

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.schema import (
    TableSchema,
    normalize_column_name,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.tabelas import Table

logger = get_logger("toolkit")

SP = ZoneInfo("America/Sao_Paulo")
CONTROL = {"_loaded_at", "data_criacao", "data_atualizacao"}


def to_sp_date(series: pl.Series, fallback: date) -> pl.Series:
    """Convert a UTC timestamp series to São Paulo calendar dates.

    Null values are filled with `fallback`.
    """
    return (
        series.dt.convert_time_zone("America/Sao_Paulo")
        .dt.date()
        .fill_null(pl.lit(fallback))
    )


def convert_boolean_columns(df: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    """Convert text `'true'`/`'false'` columns to Boolean, preserving nulls."""
    mapping = {"true": True, "false": False, "1": True, "0": False}
    exprs = []
    for col in columns:
        if col in df.columns:
            exprs.append(
                pl.col(col)
                .cast(pl.String)
                .str.to_lowercase()
                .replace(mapping)
                .cast(pl.Boolean)
                .alias(col)
            )
    return df.with_columns(exprs) if exprs else df


def apply_transform(
    df: pl.DataFrame,
    table: Table,
    schema: TableSchema,
    now: datetime | None = None,
) -> pl.DataFrame:
    """Normalise the raw DataFrame to match the target schema.

    Return `df` unchanged if it is empty.
    Raise `ValueError` if columns required by the schema are missing after transformation.
    """
    if df.is_empty():
        return df

    now = now or datetime.now(tz=timezone.utc)

    # Rename columns using DLO → snake_case mapping.
    rename_map = {c: normalize_column_name(c, table.renomear) for c in df.columns}
    df = df.rename(rename_map)

    # Deduplicate: two DLO names may normalise to the same final name.
    seen: set[str] = set()
    keep: list[str] = []
    for col in df.columns:
        if col not in seen:
            seen.add(col)
            keep.append(col)
    df = df.select(keep)

    # Drop columns outside the schema (schema drift) with a warning.
    allowed = (set(schema.all_columns) - CONTROL) | {"created_date", "system_modstamp"}
    unknown = [c for c in df.columns if c not in allowed]
    if unknown:
        logger.warning(
            "Transform skipped columns: table=%s columns=%s — rerun to rebuild schema.",
            table.nome,
            unknown,
        )
        df = df.drop(unknown)

    # Parse timestamp columns as UTC.
    for col in schema.timestamp_columns:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).str.to_datetime(format=None, time_unit="us", time_zone="UTC", strict=False).alias(col)
            )

    # Parse date columns.
    for col in schema.date_columns:
        if col in df.columns:
            df = df.with_columns(
                pl.col(col).str.to_date(format=None, strict=False).alias(col)
            )

    df = convert_boolean_columns(df, table.booleanas)

    fallback = now.astimezone(SP).date()

    # Warn when standard temporal columns are absent.
    if "created_date" not in df.columns:
        logger.warning("Transform using fallback date: table=%s column=created_date fallback=%s", table.nome, fallback)
    if "system_modstamp" not in df.columns:
        logger.warning("Transform using fallback date: table=%s column=system_modstamp fallback=%s", table.nome, fallback)

    if "created_date" in df.columns:
        df = df.with_columns(to_sp_date(df["created_date"], fallback).alias("data_criacao"))
    else:
        df = df.with_columns(pl.lit(fallback).alias("data_criacao"))

    if "system_modstamp" in df.columns:
        df = df.with_columns(to_sp_date(df["system_modstamp"], fallback).alias("data_atualizacao"))
    else:
        df = df.with_columns(pl.lit(fallback).alias("data_atualizacao"))

    df = df.with_columns(pl.lit(now).alias("_loaded_at"))
    df = df.with_columns(
        pl.when(pl.col(pl.String).eq("")).then(None).otherwise(pl.col(pl.String)).name.keep()
    )

    missing = [c for c in schema.all_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Table '{table.nome}': schema columns missing from DataFrame: {missing}")

    return df.select(list(schema.all_columns))
