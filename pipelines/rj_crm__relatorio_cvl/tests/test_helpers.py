from datetime import date, datetime, timedelta

import polars as pl

from pipelines.rj_crm__relatorio_cvl.helpers import (
    build_query,
    build_sessions,
    fix_phone,
    flag_new_sessions,
    read_report_dates,
)


def at(day: int, hour: int) -> datetime:
    """Return a time of May 2026."""
    return datetime(2026, 5, day, hour)


def test_the_dates_given_are_kept_and_without_them_the_report_is_the_previous_month() -> None:
    """Read the dates of the report."""
    assert read_report_dates("2026-04-01", "2026-04-30") == (date(2026, 4, 1), date(2026, 4, 30))

    start, end = read_report_dates(None, None)
    assert start.day == 1
    assert (end + timedelta(days=1)).day == 1
    assert end.month == start.month


def test_the_query_carries_the_dates() -> None:
    """Render the dates in the query."""
    query = build_query(date(2026, 4, 1), date(2026, 4, 30))

    assert "DATE('2026-04-01')" in query
    assert "DATE('2026-04-30')" in query
    assert "{{" not in query


def test_a_message_starts_a_session_after_24_hours_from_the_start_of_the_current_one() -> None:
    """Flag the first message, and the first more than 24 hours after the start.

    It checks against the start of the current session.
    """
    assert flag_new_sessions([at(1, 8), at(1, 20), at(2, 8), at(2, 9), at(3, 10)]) == [True, False, False, True, True]
    assert flag_new_sessions([]) == []


def test_a_phone_of_12_digits_gets_the_nine_after_the_area_code() -> None:
    """Fix only the phones of 12 digits."""
    assert fix_phone("552198765432") == "5521998765432"
    assert fix_phone("5521998765432") == "5521998765432"
    assert fix_phone(None) is None


def test_the_sessions_number_the_messages_of_each_phone_and_keep_the_month_of_the_report() -> None:
    """Build the sessions in order, per phone, for the month of the report only."""
    messages = pl.DataFrame(
        {
            "contato_telefone": ["552198765432", "5521998765432", "5521911111111", "5521911111111", None],
            "mensagem_datahora": [
                "2026-05-02 08:00:00",
                "2026-05-03 07:00:00",
                "2026-05-31 23:00:00",
                "2026-06-01 22:00:00",
                "2026-05-04 10:00:00",
            ],
        }
    )

    sessions = build_sessions(messages, "2026-05")

    assert sessions.select("contato_telefone", "nova_sessao", "id_sessao_24h", "mes_ano").rows() == [
        ("5521911111111", 1, "5521911111111_1", "2026-05"),
        ("5521998765432", 1, "5521998765432_1", "2026-05"),
        ("5521998765432", 0, "5521998765432_1", "2026-05"),
        (None, 0, None, "2026-05"),
    ]
