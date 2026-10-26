from datetime import datetime, timedelta

import pytest
from pydantic import JsonValue

from pipelines.rj_iplanrio__betterstack_api.constants import SP_TZ
from pipelines.rj_iplanrio__betterstack_api.helpers import build_rows, read_date_range, read_next_page


def test_the_dates_given_are_kept_and_without_them_the_range_is_yesterday() -> None:
    """Keep a backfill range, and otherwise fetch yesterday in São Paulo."""
    yesterday = (datetime.now(SP_TZ) - timedelta(days=1)).date().isoformat()

    assert read_date_range("2026-01-01", "2026-01-03") == ("2026-01-01", "2026-01-03")
    assert read_date_range(None, None) == (yesterday, yesterday)
    assert read_date_range("2026-01-01", None) == (yesterday, yesterday)


@pytest.mark.parametrize(
    ("page", "expected"),
    [
        pytest.param({"pagination": {"next": "https://api/next"}}, "https://api/next", id="a_next_page"),
        pytest.param({"pagination": {"next": None}}, None, id="the_last_page"),
        pytest.param({"data": []}, None, id="no_pagination"),
        pytest.param([], None, id="a_list"),
    ],
)
def test_the_next_page_is_the_url_of_the_pagination_if_there_is_one(page: JsonValue, expected: str | None) -> None:
    """Read the next page of a response."""
    assert read_next_page(page) == expected


def test_a_row_has_text_columns_and_the_day_it_started() -> None:
    """Turn nested values into JSON text, and take the day from `started_at`, else `created_at`."""
    rows = build_rows(
        [
            {"id": "1", "attributes": {"started_at": "2026-01-02T10:00:00Z", "name": "a"}, "empty": None},
            {"id": "2", "attributes": {"created_at": "2026-01-05T08:00:00Z"}},
        ]
    )

    assert rows[0] == {
        "id": "1",
        "attributes": '{"started_at": "2026-01-02T10:00:00Z", "name": "a"}',
        "empty": None,
        "ano_particao": "2026",
        "mes_particao": "01",
        "data_particao": "2026-01-02",
    }
    assert rows[1]["data_particao"] == "2026-01-05"


def test_a_row_without_a_date_gets_today() -> None:
    """Fall back to today when the incident has no start date."""
    today = datetime.now(SP_TZ).date().isoformat()

    assert build_rows([{"id": "3"}])[0]["data_particao"] == today
