import polars as pl
import pytest

from pipelines.rj_cor__precipitacao_alertario_sftp import tasks

TABLE = "rj-iplanrio.clima_pluviometro.taxa_precipitacao_alertario_5min"


async def test_a_missing_table_has_no_watermark(monkeypatch: pytest.MonkeyPatch) -> None:
    """Start from the beginning when the destination does not exist yet."""

    async def read_nothing(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(tasks, "read_bq_if_exists", read_nothing)

    assert await tasks.read_watermark_task.fn(TABLE, "rj-iplanrio") is None


async def test_the_watermark_is_the_latest_measurement(monkeypatch: pytest.MonkeyPatch) -> None:
    """Read the latest `data_medicao` of the table."""

    async def read_bq(*_args: object, **_kwargs: object) -> pl.DataFrame:
        return pl.DataFrame({"max_data_medicao": ["2026-07-17 12:00:00"]})

    monkeypatch.setattr(tasks, "read_bq_if_exists", read_bq)

    assert await tasks.read_watermark_task.fn(TABLE, "rj-iplanrio") == "2026-07-17 12:00:00"
