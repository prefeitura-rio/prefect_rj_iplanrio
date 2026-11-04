from collections.abc import Awaitable, Callable

import polars as pl
import pytest
from duckdb import BinderException
from google.api_core.exceptions import NotFound

from pipelines.rj_cor__precipitacao_alertario_sftp import tasks

TABLE = "rj-iplanrio.clima_pluviometro.taxa_precipitacao_alertario_5min"


def raising(error: Exception) -> Callable[..., Awaitable[pl.DataFrame]]:
    """Return a `read_bq` that raises `error`."""

    async def read_bq(*_args: object, **_kwargs: object) -> pl.DataFrame:
        raise error

    return read_bq


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(NotFound("table"), id="a_missing_table_in_the_client"),
        pytest.param(
            BinderException("Binder Error: Not found: Dataset rj-iplanrio:x was not found"),
            id="a_missing_dataset_in_duckdb",
        ),
    ],
)
async def test_a_missing_table_has_no_watermark(monkeypatch: pytest.MonkeyPatch, error: Exception) -> None:
    """Start from the beginning when the destination does not exist yet."""
    monkeypatch.setattr(tasks, "read_bq", raising(error))

    assert await tasks.read_watermark_task.fn(TABLE, "rj-iplanrio") is None


async def test_another_binder_error_is_not_hidden(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise a failure that is not a missing table."""
    monkeypatch.setattr(tasks, "read_bq", raising(BinderException("Binder Error: bad column")))

    with pytest.raises(BinderException):
        await tasks.read_watermark_task.fn(TABLE, "rj-iplanrio")


async def test_the_watermark_is_the_latest_measurement(monkeypatch: pytest.MonkeyPatch) -> None:
    """Read the latest `data_medicao` of the table."""

    async def read_bq(*_args: object, **_kwargs: object) -> pl.DataFrame:
        return pl.DataFrame({"max_data_medicao": ["2026-07-17 12:00:00"]})

    monkeypatch.setattr(tasks, "read_bq", read_bq)

    assert await tasks.read_watermark_task.fn(TABLE, "rj-iplanrio") == "2026-07-17 12:00:00"
