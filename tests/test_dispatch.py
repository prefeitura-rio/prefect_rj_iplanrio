from datetime import date

import pytest

from utils import dispatch
from utils.dispatch import (
    decide_switch,
    format_query,
    is_active,
    no_destinations_notice,
    post_notice,
    read_expiration,
    skip_weekends,
)

MONDAY = date(2026, 7, 13)
WEDNESDAY = date(2026, 7, 15)
THURSDAY = date(2026, 7, 16)
FRIDAY = date(2026, 7, 17)
SATURDAY = date(2026, 7, 18)
SUNDAY = date(2026, 7, 19)
QUERY = "SELECT {days_ahead_placeholder} AS days, {other} AS other"


@pytest.mark.parametrize(
    ("today", "days", "expected"),
    [
        pytest.param(MONDAY, 2, "SELECT 2 AS days, x AS other", id="a_weekday_lands_on_a_weekday"),
        pytest.param(WEDNESDAY, 2, "SELECT 2 AS days, x AS other", id="wednesday_plus_two_is_friday"),
        pytest.param(THURSDAY, 2, "SELECT 4 AS days, x AS other", id="thursday_plus_two_is_saturday"),
        pytest.param(FRIDAY, 1, "SELECT 3 AS days, x AS other", id="friday_plus_one_is_saturday"),
        pytest.param(FRIDAY, 2, "SELECT 4 AS days, x AS other", id="friday_plus_two_is_sunday"),
        pytest.param(SATURDAY, 2, None, id="saturday_skips_the_run"),
        pytest.param(SUNDAY, 2, None, id="sunday_skips_the_run"),
    ],
)
def test_the_query_of_the_day_skips_the_weekend(today: date, days: int, expected: str | None) -> None:
    """Skip the weekend as a day to run, and as a day to look at."""
    assert skip_weekends(QUERY, {"days_ahead_placeholder": days, "other": "x"}, today) == expected


@pytest.mark.parametrize(
    ("query", "replacements", "message"),
    [
        pytest.param("SELECT 1", {"days_ahead_placeholder": 1}, "placeholder", id="the_query_has_no_placeholder"),
        pytest.param(QUERY, {"other": "x"}, "must be in the replacements", id="no_value_for_it"),
    ],
)
def test_a_query_without_the_days_ahead_is_refused(
    query: str, replacements: dict[str, str | int], message: str
) -> None:
    """Raise `ValueError`."""
    with pytest.raises(ValueError, match=message):
        skip_weekends(query, replacements, MONDAY)


def test_a_replacement_is_not_changed_by_the_weekend_rule() -> None:
    """Keep the dictionary of the caller as it is."""
    replacements: dict[str, str | int] = {"days_ahead_placeholder": 2, "other": "x"}

    skip_weekends(QUERY, replacements, THURSDAY)

    assert replacements["days_ahead_placeholder"] == 2


def test_a_query_is_filled_with_a_processor_or_without_one() -> None:
    """Use the processor if named, ignore an unknown one, and fill the placeholders."""
    replacements: dict[str, str | int] = {"days_ahead_placeholder": 1, "other": "x"}

    assert format_query(QUERY, replacements, "skip_weekends", MONDAY) == "SELECT 1 AS days, x AS other"
    assert format_query(QUERY, replacements, "skip_weekends", SATURDAY) is None
    assert format_query(QUERY, replacements, "unknown", MONDAY) == "SELECT 1 AS days, x AS other"
    assert format_query("SELECT {id}", {"id": 7}, None, MONDAY) == "SELECT 7"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("1", True, id="one"),
        pytest.param(" TRUE ", True, id="true_in_capitals"),
        pytest.param("Sim", True, id="sim"),
        pytest.param(True, True, id="a_bool"),
        pytest.param("0", False, id="zero"),
        pytest.param("não", False, id="no"),
        pytest.param(None, False, id="nothing"),
    ],
)
def test_the_switch_value_is_read_as_on_or_off(raw: object, expected: bool) -> None:
    """Accept the values that the control sheet uses for on."""
    assert is_active(raw) is expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("2026-07-20", date(2026, 7, 20), id="a_date"),
        pytest.param("2026-07-20T10:00:00", date(2026, 7, 20), id="a_datetime_text"),
        pytest.param(None, None, id="nothing"),
        pytest.param("soon", None, id="not_a_date"),
    ],
)
def test_the_limit_date_is_read_from_its_text(raw: object, expected: date | None) -> None:
    """Read the first ten characters, and give nothing for what is not a date."""
    assert read_expiration(raw) == expected


@pytest.mark.parametrize(
    ("row", "environment", "active", "notice"),
    [
        pytest.param(
            {"ativo": "1", "data_limite_disparo": "2026-07-20"}, "production", True, None, id="on_and_in_time"
        ),
        pytest.param({"ativo": "1", "data_limite_disparo": None}, "staging", True, None, id="on_with_no_limit"),
        pytest.param(
            {"ativo": "1", "data_limite_disparo": "2026-07-15"}, "production", True, None, id="the_last_day_counts"
        ),
        pytest.param(None, "production", False, None, id="no_row"),
        pytest.param({"ativo": "1"}, "dev", False, None, id="an_unknown_environment"),
        pytest.param({"ativo": "0"}, "production", False, "desativado", id="off"),
        pytest.param(
            {"ativo": "1", "data_limite_disparo": "2026-07-14"}, "production", False, "data limite", id="expired"
        ),
    ],
)
def test_the_switch_of_a_campaign(
    row: dict[str, object] | None, environment: str, active: bool, notice: str | None
) -> None:
    """Run only when the campaign is on and its limit date has not passed.

    Say why when it stops.
    """
    switch = decide_switch(row, "campanha", environment, WEDNESDAY)

    assert switch.active is active
    assert (notice in (switch.notice or "")) if notice else switch.notice is None


def test_a_campaign_with_no_name_does_not_run() -> None:
    """Stop for an empty name."""
    assert decide_switch({"ativo": "1"}, "", "production", WEDNESDAY).active is False


def test_the_notices_name_the_campaign() -> None:
    """Write the campaign, the day and the reason."""
    assert "[TESTE]" in no_destinations_notice("c", test_mode=True, today=WEDNESDAY)
    assert "[TESTE]" not in no_destinations_notice("c", test_mode=False, today=WEDNESDAY)
    assert "2026-07-15" in no_destinations_notice("c", test_mode=False, today=WEDNESDAY)


def test_the_notice_of_a_stopped_campaign_has_its_readable_name_and_its_key() -> None:
    """Call the campaign by the name of the sheet, with the key of the HSM."""
    switch = decide_switch({"ativo": "0", "nome_campanha": "Puérperas"}, "hsm_x", "production", WEDNESDAY)

    assert "Puérperas (`hsm_x`)" in (switch.notice or "")


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        pytest.param("https://prefect/runs/1", "\n🔗 **Link:** https://prefect/runs/1", id="a_run_with_a_link"),
        pytest.param(None, "\n🔗 **Link:** URL indisponível", id="a_run_without_a_link"),
    ],
)
async def test_a_notice_ends_with_the_link_of_the_run(
    monkeypatch: pytest.MonkeyPatch, url: str | None, expected: str
) -> None:
    """Add the link of the flow run to each notice."""
    posted: list[str] = []

    async def post_discord(_webhook: str, message: str) -> None:
        posted.append(message)

    monkeypatch.setenv("DISCORD_WEBHOOK_URL", "https://discord/hook")
    monkeypatch.setattr(dispatch, "post_discord", post_discord)
    monkeypatch.setattr(dispatch, "current_run_url", lambda: url)

    await post_notice("hello")

    assert posted == ["hello" + expected]
