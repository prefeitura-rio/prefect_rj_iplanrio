from collections.abc import Callable
from json import loads

import polars as pl
import pytest

from pipelines.rj_crm__disparo_template.helpers.frames import (
    apply_filters,
    build_csv,
    build_log,
    drop_dispatched,
    drop_duplicates,
    fill_phones,
    has_alternatives,
    normalize_columns,
    quarantine,
    retry_frame,
    split_frame,
    validate_frame,
)
from pipelines.rj_crm__disparo_template.tests.conftest import NOW
from pipelines.rj_crm__disparo_template.types import Audience

type Frames = Callable[..., pl.DataFrame]


def test_the_locale_is_added_and_an_old_cpf_name_is_renamed() -> None:
    """Add `LOCALE` as `BR` and rename `SubscriberKey` to `cpf`."""
    frame = normalize_columns(pl.DataFrame({"SubscriberKey": ["1"], "telefone": ["2"]}))

    assert frame.columns == ["cpf", "telefone", "LOCALE"]
    assert frame["LOCALE"].to_list() == ["BR"]


def test_a_cpf_column_and_a_locale_are_left_as_they_are() -> None:
    """Do not rename when `cpf` exists, and do not overwrite `LOCALE`."""
    frame = pl.DataFrame({"cpf": ["1"], "SubscriberKey": ["x"], "LOCALE": ["PT"], "telefone": ["2"]})

    assert normalize_columns(frame).equals(frame)


@pytest.mark.parametrize(
    ("data", "campaign", "message"),
    [
        pytest.param({"telefone": ["1"]}, "c", "Columns missing", id="no_cpf"),
        pytest.param({"cpf": ["1"]}, "c", "Columns missing", id="no_phone"),
        pytest.param({"cpf": ["1"], "telefone": ["2"]}, "  ", "campaign name", id="no_campaign"),
        pytest.param({"cpf": ["1", "  "], "telefone": ["2", "3"]}, "c", "row 1", id="an_empty_cpf"),
        pytest.param({"cpf": ["1"], "telefone": [None]}, "c", "row 0", id="a_null_phone"),
    ],
)
def test_a_frame_that_the_log_cannot_take_is_refused(
    data: dict[str, list[str | None]], campaign: str, message: str
) -> None:
    """Raise `ValueError` for a missing column, unnamed campaign or empty value.

    The empty value is checked in the first rows.
    """
    with pytest.raises(ValueError, match=message):
        validate_frame(pl.DataFrame(data), campaign)


def test_a_good_frame_is_accepted() -> None:
    """Accept a frame whose first rows have a CPF and a phone."""
    validate_frame(pl.DataFrame({"cpf": ["1"], "telefone": ["2"]}), "c")


def test_the_first_row_of_each_value_stays_when_duplicates_are_dropped(make_frame: Frames) -> None:
    """Keep the order, and keep the first row of a repeated value."""
    frame = make_frame(("1", "a", None), ("2", "b", None), ("1", "c", None))

    assert drop_duplicates(frame, "cpf", enabled=True)["telefone"].to_list() == ["a", "b"]
    assert drop_duplicates(frame, "cpf", enabled=False).height == 3
    assert drop_duplicates(frame, "missing", enabled=True).height == 3


@pytest.mark.parametrize(
    ("retries", "phones"),
    [
        pytest.param(False, ["ok"], id="without_retries_the_row_is_dropped"),
        pytest.param(True, ["ok", None], id="with_retries_the_phone_is_emptied"),
    ],
)
def test_the_phones_that_failed_are_dropped_or_emptied(
    make_frame: Frames, retries: bool, phones: list[str | None]
) -> None:
    """Empty the phone for a retry to fill, and drop the row when there is no retry."""
    frame = make_frame(("1", "ok", None), ("2", "bad", ["alt"]))

    assert quarantine(frame, {"bad"}, retries=retries)["telefone"].to_list() == phones


def test_nothing_changes_when_no_phone_failed(make_frame: Frames) -> None:
    """Leave the frame alone for an empty set."""
    frame = make_frame(("1", "a", None))

    assert quarantine(frame, set(), retries=False).equals(frame)


def test_a_row_with_no_phone_takes_the_first_other_or_goes_away(make_frame: Frames) -> None:
    """Use the first of `others`, and drop a row with no phone and no other."""
    frame = make_frame(("1", "a", ["x"]), ("2", None, ["y", "z"]), ("3", None, None), ("4", None, []))

    assert fill_phones(frame)["telefone"].to_list() == ["a", "y"]


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [
        pytest.param(1, True, id="one_other"),
        pytest.param(2, True, id="two_others"),
        pytest.param(3, False, id="not_enough_others"),
    ],
)
def test_a_retry_needs_a_row_with_enough_other_phones(make_frame: Frames, attempt: int, expected: bool) -> None:
    """Tell whether any row has `attempt` alternative phones."""
    frame = make_frame(("1", "a", ["x"]), ("2", "b", ["y", "z"]))

    assert has_alternatives(frame, attempt) is expected


def test_a_frame_with_no_others_column_always_has_alternatives() -> None:
    """Do not stop the loop of retries for a frame that has no `others`, as before."""
    assert has_alternatives(pl.DataFrame({"cpf": ["1"]}), 1) is True


def test_the_retry_takes_the_failed_cpfs_with_the_next_phone(make_frame: Frames) -> None:
    """Pick the CPFs that failed and give each the phone `attempt` of its list.

    CPFs with none are dropped.
    """
    frame = make_frame(("1", "a", ["x1", "x2"]), ("2", "b", ["y1"]), ("3", "c", ["z1", "z2"]))

    second = retry_frame(frame, {"1", "2"}, 2)
    first = retry_frame(frame, {"1", "2", "3"}, 1)

    assert second["cpf"].to_list() == ["1"]
    assert second["telefone"].to_list() == ["x2"]
    assert first["telefone"].to_list() == ["x1", "y1", "z1"]


def test_a_retry_without_others_is_refused() -> None:
    """Raise `ValueError`."""
    with pytest.raises(ValueError, match="others"):
        retry_frame(pl.DataFrame({"cpf": ["1"], "telefone": ["a"]}), {"1"}, 1)


@pytest.mark.parametrize(
    ("column", "kept"),
    [
        pytest.param("cpf", ["3"], id="by_cpf"),
        pytest.param("telefone", ["2"], id="by_phone"),
        pytest.param("SubscriberKey", ["3"], id="the_old_name_of_cpf"),
    ],
)
def test_who_was_dispatched_is_dropped_by_the_column(
    make_frame: Frames, make_dispatched: Frames, column: str, kept: list[str]
) -> None:
    """Drop by CPF, or by phone."""
    frame = make_frame(("1", "a", None), ("2", "b", None), ("3", "c", None))
    dispatched = make_dispatched(("2", "a", "c", "2026-07-17"), ("1", "c", "c", "2026-07-17"))

    assert drop_dispatched(frame, dispatched, column)["cpf"].to_list() == kept


def test_the_filter_of_the_dispatched_does_nothing_for_an_empty_table_or_a_missing_column(
    make_frame: Frames, make_dispatched: Frames
) -> None:
    """Leave the frame as it is."""
    frame = make_frame(("1", "a", None))

    assert drop_dispatched(frame, make_dispatched(), "cpf").equals(frame)
    assert drop_dispatched(frame, make_dispatched(("1", "a", "c", "2026-07-17")).drop("cpf"), "cpf").equals(frame)


@pytest.mark.parametrize(
    ("audience", "attempt", "kept"),
    [
        pytest.param(Audience(), 0, ["4"], id="the_first_try_drops_the_cpfs_of_the_campaign_and_of_today"),
        pytest.param(Audience(dispatched=None), 0, ["2", "3", "4"], id="no_filter_of_today_keeps_those_of_today"),
        pytest.param(Audience(dispatched="telefone"), 0, ["2", "4"], id="by_phone_today"),
        pytest.param(Audience(), 1, ["2", "3", "4"], id="a_retry_filters_the_campaign_by_phone_and_never_by_cpf"),
        pytest.param(Audience(dispatched="telefone"), 1, ["2", "4"], id="a_retry_by_phone_today"),
    ],
)
def test_the_filters_of_a_try(
    make_frame: Frames, make_dispatched: Frames, audience: Audience, attempt: int, kept: list[str]
) -> None:
    """Drop who got the campaign before, and who got any campaign today.

    The rule of the try decides.
    """
    frame = make_frame(("1", "p1", None), ("2", "p2", None), ("3", "p3", None), ("4", "p4", None))
    dispatched = make_dispatched(
        ("1", "p1", "campaign", "2026-07-15"),
        ("3", "p3", "other", "2026-07-17"),
        ("2", "p9", "other", "2026-07-17"),
    )

    assert apply_filters(frame, dispatched, audience, "campaign", attempt)["cpf"].to_list() == kept


def test_no_filter_applies_when_nothing_was_dispatched(make_frame: Frames, make_dispatched: Frames) -> None:
    """Keep every row."""
    frame = make_frame(("1", "a", None))

    assert apply_filters(frame, make_dispatched(), Audience(), "c", 0).equals(frame)


@pytest.mark.parametrize(
    ("size", "sizes"),
    [
        pytest.param(None, [5], id="no_size_keeps_it_whole"),
        pytest.param(2, [2, 2, 1], id="the_last_is_smaller"),
        pytest.param(5, [5], id="exact"),
    ],
)
def test_the_frame_is_cut_in_batches(size: int | None, sizes: list[int]) -> None:
    """Cut by size, or keep the frame whole with no size."""
    frame = pl.DataFrame({"telefone": list("abcde")})

    assert [batch.height for batch in split_frame(frame, size)] == sizes


def test_the_file_of_a_batch_has_the_columns_that_were_asked_for(make_frame: Frames) -> None:
    """Keep `telefone`, `SubscriberKey` and the asked columns, and add `LOCALE`.

    The control columns are dropped.
    """
    frame = make_frame(("111", "5521999", ["x"]), nome=["Ana"], interno=["x"])

    csv = build_csv(frame, "whatsapp_teste", ["nome"], ";", NOW)

    assert csv.name == "whatsapp_teste_20260717093005.csv"
    assert csv.dispatch_date == "2026-07-17 09:30:05"
    assert csv.text == "telefone;SubscriberKey;nome;LOCALE\n5521999;111;Ana;BR\n"


def test_the_file_without_columns_drops_only_others(make_frame: Frames) -> None:
    """Keep everything but `others`."""
    csv = build_csv(make_frame(("111", "5521999", ["x"]), nome=["Ana"]), "stem", None, ",", NOW)

    assert csv.text == "SubscriberKey,telefone,nome,LOCALE\n111,5521999,Ana,BR\n"


def test_the_file_can_have_its_own_name_as_a_column(make_frame: Frames) -> None:
    """Fill `NOME_ARQUIVO` with the name of the file."""
    csv = build_csv(make_frame(("111", "5521999", None), NOME_ARQUIVO=["x"]), "stem", ["NOME_ARQUIVO"], ";", NOW)

    assert csv.text == f"telefone;SubscriberKey;NOME_ARQUIVO;LOCALE\n5521999;111;{csv.name};BR\n"


def test_a_column_of_lists_is_written_as_json_in_the_file() -> None:
    """Write a nested value as JSON text, not as a polars error."""
    frame = pl.DataFrame({"cpf": ["1"], "telefone": ["2"], "itens": [["a", "b"]]})

    csv = build_csv(frame, "stem", ["itens"], ";", NOW)

    assert '"[""a"", ""b""]"' in csv.text


def test_the_log_has_the_cpf_as_subscriber_key_and_the_other_columns_as_json(make_frame: Frames) -> None:
    """Write one row for each destination, with the partition of the dispatch day."""
    frame = make_frame(("111", "5521999", ["x"]), nome=["Ana"])

    rows = build_log(frame, "2026-07-17 09:30:05", "campanha")

    assert len(rows) == 1
    assert {key: rows[0][key] for key in ("dispatch_date", "campaign_name", "SubscriberKey", "telefone")} == {
        "dispatch_date": "2026-07-17 09:30:05",
        "campaign_name": "campanha",
        "SubscriberKey": "111",
        "telefone": "5521999",
    }
    assert (rows[0]["ano_particao"], rows[0]["mes_particao"], rows[0]["data_particao"]) == ("2026", "07", "2026-07-17")
    assert loads(str(rows[0]["data"])) == {"others": ["x"], "nome": "Ana"}


def test_a_row_with_no_cpf_or_phone_is_left_out_of_the_log(make_frame: Frames) -> None:
    """Drop the invalid rows, and keep the valid ones."""
    frame = make_frame(("111", "5521999", None), (" ", "5521888", None), ("222", None, None))

    assert [row["SubscriberKey"] for row in build_log(frame, "2026-07-17 09:30:05", "c")] == ["111"]


def test_a_log_with_no_valid_row_is_refused(make_frame: Frames) -> None:
    """Raise `ValueError` when every row is left out."""
    with pytest.raises(ValueError, match="No valid row"):
        build_log(make_frame((" ", "1", None)), "2026-07-17 09:30:05", "c")
