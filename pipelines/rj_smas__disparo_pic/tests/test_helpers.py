from pathlib import Path

import polars as pl
import pytest

from pipelines.rj_smas__disparo_pic.helpers import build_query, read_event_date, read_test_texts, read_texts
from pipelines.rj_smas__disparo_pic.types import Campaign
from utils.hsm import Hsm

FOLDER = Path(__file__).parent.parent


def approval(*statuses: str | None, dates: tuple[str, ...] = ("2026-07-20",)) -> pl.DataFrame:
    """Build the table of approvals with a status for each row."""
    return pl.DataFrame(
        {
            "APROVACAO_DISPARO_AVISO": list(statuses),
            "DATA_ENTREGA": [dates[min(number, len(dates) - 1)] for number in range(len(statuses))],
        },
        schema={"APROVACAO_DISPARO_AVISO": pl.String, "DATA_ENTREGA": pl.String},
    )


@pytest.mark.parametrize(
    ("table", "expected"),
    [
        pytest.param(approval("aprovado"), "2026-07-20", id="approved"),
        pytest.param(approval(" Aprovado "), "2026-07-20", id="approved_in_another_case"),
        pytest.param(approval("pendente"), None, id="pending"),
        pytest.param(approval("aprovado", "negado"), "2026-07-20", id="the_first_status_in_order_counts"),
        pytest.param(approval("alterado", "aprovado"), None, id="a_status_that_sorts_before_approved"),
        pytest.param(approval(None), None, id="no_status"),
        pytest.param(approval(), None, id="an_empty_table"),
    ],
)
def test_the_event_date_comes_only_with_an_approval(table: pl.DataFrame, expected: str | None) -> None:
    """Give the date of the event for an approved dispatch, and nothing otherwise."""
    assert read_event_date(table) == expected


def test_the_query_has_the_event_date_and_the_hsm() -> None:
    """Fill the placeholders of `queries/destinations.sql`."""
    query = build_query(FOLDER, Campaign(id_hsm=184), "2026-07-20")

    assert "date('2026-07-20')" in query
    assert "cast(184 as int64)" in query
    assert "placeholder" not in query


def test_the_first_column_of_the_query_has_the_destinations() -> None:
    """Read the JSON texts of the first column, and give none for an empty result."""
    frame = pl.DataFrame({"destination_data": ['{"to": "1"}']})

    assert read_texts(frame) == ['{"to": "1"}']
    assert read_texts(pl.DataFrame({"destination_data": []}, schema={"destination_data": pl.String})) == []


def test_the_test_destinations_come_from_the_setting() -> None:
    """Read a JSON list as one JSON text for each destination."""
    assert read_test_texts('[{"to": "5521999", "externalId": "1"}]') == ['{"to": "5521999", "externalId": "1"}']


@pytest.mark.parametrize("setting", [None, "", "  "])
def test_a_test_without_destinations_is_refused(setting: str | None) -> None:
    """Raise, so a test never falls back to the people of the query."""
    with pytest.raises(ValueError, match="DESTINATIONS"):
        read_test_texts(setting)


@pytest.mark.parametrize(
    ("test_mode", "name"),
    [
        pytest.param(True, "teste-smas-cartaopic-disparoevento", id="a_test_has_a_prefix"),
        pytest.param(False, "smas-cartaopic-disparoevento", id="a_real_run_has_the_name"),
    ],
)
def test_the_campaign_name_of_a_test_has_a_prefix(test_mode: bool, name: str) -> None:
    """Send a test as another campaign, so its results do not mix with the real ones."""
    assert Campaign(test_mode=test_mode).as_hsm() == Hsm(184, name, 71, 1000)
