from collections.abc import Awaitable, Callable
from datetime import date
from pathlib import Path
from zipfile import ZipFile

import polars as pl
import pytest
from duckdb import BinderException
from google.api_core.exceptions import NotFound

from pipelines.rj_smfp__crf import helpers, tasks
from pipelines.rj_smfp__crf.constants import EXTRA_COLUMNS, LAYOUTS, PERIODS_LAYOUT
from pipelines.rj_smfp__crf.helpers import (
    build_rows,
    date_from_name,
    parse_date,
    read_fwf,
    read_member,
    reference_date,
    select_zip_names,
)

PERIOD = "12345678" + "20200101" + "20201231" + "X" + "000000042"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, None, id="nothing"),
        pytest.param(date(2021, 1, 3), date(2021, 1, 3), id="a_date"),
        pytest.param("2021-01-03", date(2021, 1, 3), id="a_text"),
    ],
)
def test_a_date_is_read_from_a_date_or_a_text(value: date | str | None, expected: date | None) -> None:
    """Accept a date, an ISO text, and nothing."""
    assert parse_date(value, "data_inicio") == expected


def test_a_text_that_is_not_a_date_names_the_parameter() -> None:
    """Raise with the name of the parameter."""
    with pytest.raises(ValueError, match="data_fim"):
        parse_date("03/01/2021", "data_fim")


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("PERIODOS_EVENTOS/BX-22518948-EVE-03012021.zip", date(2021, 1, 3), id="day_month_year"),
        pytest.param("BX-1-PER-31122020.ZIP", date(2020, 12, 31), id="upper_case_extension"),
        pytest.param("BX-1-PER-32132020.zip", None, id="a_date_that_does_not_exist"),
        pytest.param("BX-1-PER.zip", None, id="no_date"),
    ],
)
def test_the_date_of_a_zip_is_at_the_end_of_its_name(name: str, expected: date | None) -> None:
    """Read `DDMMYYYY`, and give nothing for what is not a date."""
    assert date_from_name(name) == expected


NAMES = [
    "P/BX-1-EVE-01012021.zip",
    "P/BX-1-EVE-03012021.zip",
    "P/BX-1-EVE-05012021.zip",
    "P/BX-1-EVE-XX012021.zip",
    "P/OTHER-1-EVE-04012021.zip",
    "P/BX-1-EVE-04012021.txt",
]


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        pytest.param(None, None, ["01012021", "03012021", "05012021", "XX012021"], id="no_limit_keeps_every_zip"),
        pytest.param(date(2021, 1, 1), None, ["03012021", "05012021"], id="the_start_is_excluded"),
        pytest.param(date(2021, 1, 1), date(2021, 1, 3), ["03012021"], id="the_end_is_included"),
        pytest.param(None, date(2021, 1, 3), ["01012021", "03012021"], id="only_an_end"),
    ],
)
def test_the_zip_files_of_the_window_are_after_the_start_and_up_to_the_end(
    start: date | None, end: date | None, expected: list[str]
) -> None:
    """Keep the `BX-` ZIPs in `(start, end]`, and one with no date only if no limit."""
    chosen = select_zip_names(NAMES, start, end)

    assert [name[-12:-4] for name in chosen] == expected


def test_a_line_is_cut_into_the_columns_of_the_layout() -> None:
    """Strip each field, make an empty one nothing, and skip a blank line."""
    line = "12345678" + "20200101" + "        " + " " + "000000042"

    assert read_fwf(f"{line}\n\n   \n", PERIODS_LAYOUT) == [
        {
            "cnpj": "12345678",
            "data_inicio": "20200101",
            "data_fim": None,
            "identificador_cancelamento": None,
            "numero_opcao": "000000042",
        }
    ]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("00-PER-20210103.txt", date(2021, 1, 3), id="a_period_file"),
        pytest.param("00-EVEMEI-20201231.TXT", date(2020, 12, 31), id="an_event_file"),
    ],
)
def test_the_reference_date_ends_the_name_of_the_file(name: str, expected: date) -> None:
    """Read `YYYYMMDD` after the last hyphen."""
    assert reference_date(name) == expected


def test_a_name_with_no_date_part_is_refused() -> None:
    """Raise for a name that does not follow the pattern."""
    with pytest.raises(ValueError, match="XX-XXX-AAAAMMDD"):
        reference_date("periodos.txt")


def test_the_rows_have_the_file_and_the_day_of_reference() -> None:
    """Add the origin and the partition columns to each row."""
    [row] = build_rows("00-PER-20210103.txt", PERIOD, PERIODS_LAYOUT)

    assert row["cnpj"] == "12345678"
    assert row["arquivo_origem"] == "00-PER-20210103.txt"
    assert (row["data_referencia"], row["ano_particao"], row["mes_particao"], row["data_particao"]) == (
        "20210103",
        "2021",
        "01",
        "2021-01-03",
    )


def make_zip(path: Path, members: dict[str, str]) -> Path:
    """Write a ZIP file with the given members."""
    with ZipFile(path, "w") as archive:
        for name, text in members.items():
            archive.writestr(name, text)

    return path


@pytest.mark.parametrize(
    ("members", "error"),
    [
        pytest.param({"other.txt": "x"}, FileNotFoundError, id="no_file_of_the_layout"),
        pytest.param({"00-PER-20210103.txt": "x", "sub/00-PER-20210104.txt": "y"}, ValueError, id="two_files"),
    ],
)
def test_the_zip_must_hold_one_file_of_the_layout(
    tmp_path: Path, members: dict[str, str], error: type[Exception]
) -> None:
    """Raise when the ZIP has no file of the layout, or has more than one."""
    with ZipFile(make_zip(tmp_path / "z.zip", members)) as archive, pytest.raises(error):
        read_member(archive, LAYOUTS["periodos_simples"], "periodos_simples")


def test_a_zip_becomes_a_parquet_file_of_text_columns(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Write the rows of the file of the table, with every column as text."""
    archive = make_zip(tmp_path / "BX-1-PER-03012021.zip", {"00-PER-20210103.txt": PERIOD + "\n"})

    def create_store(bucket: str) -> str:
        """Give the name of the bucket as the store."""
        return bucket

    def download(store: str, key: str, path: Path) -> Path:
        """Write the test ZIP where the real download would."""
        del store, key
        path.write_bytes(archive.read_bytes())

        return path

    monkeypatch.setattr(helpers, "create_store", create_store)
    monkeypatch.setattr(helpers, "download", download)
    folder = tmp_path / "out"
    folder.mkdir()

    rows = helpers.process_zip("b", "P/BX-1-PER-03012021.zip", "periodos_simples", folder)

    frame = pl.read_parquet(folder / "BX-1-PER-03012021.parquet")
    assert rows == 1
    assert frame["data_particao"].to_list() == ["2021-01-03"]
    assert set(frame.schema.values()) == {pl.String}
    assert frame.columns == [*PERIODS_LAYOUT.names, *EXTRA_COLUMNS]


def raising(error: Exception) -> Callable[..., Awaitable[pl.DataFrame]]:
    """Return a `read_bq` that raises `error`."""

    async def read_bq(*_args: object, **_kwargs: object) -> pl.DataFrame:
        raise error

    return read_bq


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(NotFound("table"), id="a_missing_table_in_the_client"),
        pytest.param(BinderException("Binder Error: Not found: Table x was not found"), id="a_missing_table_in_duckdb"),
    ],
)
async def test_a_missing_table_has_no_watermark(monkeypatch: pytest.MonkeyPatch, error: Exception) -> None:
    """Load every ZIP file when the table does not exist yet."""
    monkeypatch.setattr(tasks, "read_bq", raising(error))

    assert await tasks.read_watermark_task.fn("p.d.t", "p") is None


async def test_another_binder_error_is_not_hidden(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise a failure that is not a missing table."""
    monkeypatch.setattr(tasks, "read_bq", raising(BinderException("Binder Error: bad column")))

    with pytest.raises(BinderException):
        await tasks.read_watermark_task.fn("p.d.t", "p")


async def test_the_watermark_is_the_latest_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    """Read the latest `data_particao` as a date."""

    async def read_bq(*_args: object, **_kwargs: object) -> pl.DataFrame:
        return pl.DataFrame({"max_data_particao": [date(2021, 1, 3)]})

    monkeypatch.setattr(tasks, "read_bq", read_bq)

    assert await tasks.read_watermark_task.fn("p.d.t", "p") == date(2021, 1, 3)
