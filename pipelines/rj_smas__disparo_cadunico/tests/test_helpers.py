from datetime import date
from pathlib import Path

import polars as pl
import pytest

from pipelines.rj_smas__disparo_cadunico.helpers import build_query, read_test_texts, read_texts
from pipelines.rj_smas__disparo_cadunico.types import Audience, Campaign
from utils.hsm import Hsm, parse_recipients

FOLDER = Path(__file__).parent.parent
WEDNESDAY = date(2026, 7, 15)
THURSDAY = date(2026, 7, 16)
SATURDAY = date(2026, 7, 18)


def test_the_query_has_the_hsm_and_the_days_ahead() -> None:
    """Fill the two placeholders of `queries/destinations.sql`."""
    query = build_query(FOLDER, Campaign(id_hsm=101), Audience(days_ahead=2), WEDNESDAY)

    assert query is not None
    assert "placeholder" not in query
    assert "CAST(2 AS int64)" in query
    assert "id_hsm = cast(101 as string)" in query


def test_the_query_looks_past_the_weekend_and_skips_it() -> None:
    """Look two days ahead when the day is a weekend, and give nothing on a weekend."""
    thursday = build_query(FOLDER, Campaign(), Audience(days_ahead=2), THURSDAY)

    assert thursday is not None
    assert "CAST(4 AS int64)" in thursday
    assert build_query(FOLDER, Campaign(), Audience(), SATURDAY) is None


def test_the_first_column_of_the_query_has_the_destinations() -> None:
    """Read the JSON texts of the first column, and give none for an empty result."""
    frame = pl.DataFrame({"destination_data": ['{"to": "1"}', '{"to": "2"}'], "other": ["x", "y"]})

    assert read_texts(frame) == ['{"to": "1"}', '{"to": "2"}']
    assert read_texts(pl.DataFrame({"destination_data": []}, schema={"destination_data": pl.String})) == []


def test_the_test_destinations_come_from_the_setting() -> None:
    """Read a JSON list as one JSON text for each destination, and as recipients."""
    texts = read_test_texts('[{"celular_disparo": "5521999", "externalId": "111", "vars": {"NOME": "Ana"}}]')

    recipients = parse_recipients(texts, require_id=True)

    assert [(recipient.to, recipient.external_id) for recipient in recipients] == [("5521999", "111")]


@pytest.mark.parametrize("setting", [None, "", "   "])
def test_a_test_without_destinations_is_refused(setting: str | None) -> None:
    """Raise, so a test never falls back to the people of the query."""
    with pytest.raises(ValueError, match="DESTINATIONS"):
        read_test_texts(setting)


@pytest.mark.parametrize(
    ("test_mode", "name"),
    [
        pytest.param(True, "teste-smas-cadunico-prod", id="a_test_has_a_prefix"),
        pytest.param(False, "smas-cadunico-prod", id="a_real_run_has_the_name"),
    ],
)
def test_the_campaign_name_of_a_test_has_a_prefix(test_mode: bool, name: str) -> None:
    """Send a test as another campaign, so its results do not mix with the real ones."""
    assert Campaign(test_mode=test_mode).as_hsm() == Hsm(101, name, 71, 1000)
