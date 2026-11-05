"""Tests for helpers/janela.py — window calculation helpers."""

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.janela import (
    SP,
    build_day_window,
    build_filter_clause,
    build_hour_window,
    build_reconciliation_window,
    build_run_windows,
    convert_to_utc,
    format_window,
)

REF = datetime(2026, 11, 4, 14, 0, 0, tzinfo=SP)


class TestBuildHourWindow:
    def test_returns_one_hour_window(self) -> None:
        start, end = build_hour_window(REF)
        assert start == "2026-11-04T13:00:00Z"
        assert end == "2026-11-04T14:00:00Z"

    def test_clamps_to_day_start_at_midnight(self) -> None:
        midnight = datetime(2026, 11, 4, 0, 30, 0, tzinfo=SP)
        start, end = build_hour_window(midnight)
        assert start == "2026-11-04T00:00:00Z"

    def test_clamps_exactly_at_midnight(self) -> None:
        midnight = datetime(2026, 11, 4, 0, 0, 0, tzinfo=SP)
        start, end = build_hour_window(midnight)
        assert start == "2026-11-04T00:00:00Z"
        assert end == "2026-11-04T00:00:00Z"


class TestBuildDayWindow:
    def test_full_day(self) -> None:
        start, end = build_day_window(date(2026, 11, 3))
        assert start == "2026-11-03T00:00:00Z"
        assert end == "2026-11-04T00:00:00Z"

    def test_caps_at_reference_when_today(self) -> None:
        today = REF.date()
        start, end = build_day_window(today, REF)
        assert end == "2026-11-04T14:00:00Z"


class TestBuildReconciliationWindow:
    def test_default_7_days(self) -> None:
        start, end = build_reconciliation_window(days=7, reference=REF)
        expected_start = (REF - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        assert start == expected_start.strftime("%Y-%m-%dT%H:%M:%SZ")

    def test_raises_when_days_zero(self) -> None:
        with pytest.raises(ValueError, match="days"):
            build_reconciliation_window(days=0)


class TestConvertToUtc:
    def test_converts_sp_to_utc(self) -> None:
        # SP in November is UTC-3.
        utc_start, utc_end = convert_to_utc("2026-11-04T00:00:00Z", "2026-11-04T01:00:00Z")
        assert utc_start == "2026-11-04T03:00:00Z"
        assert utc_end == "2026-11-04T04:00:00Z"


class TestBuildFilterClause:
    def test_empty_on_missing_start(self) -> None:
        assert build_filter_clause("col", "", "2026-11-04T01:00:00Z") == ""

    def test_empty_on_missing_end(self) -> None:
        assert build_filter_clause("col", "2026-11-04T00:00:00Z", "") == ""

    def test_builds_where_clause_in_utc(self) -> None:
        clause = build_filter_clause("SystemModstamp__c", "2026-11-04T00:00:00Z", "2026-11-04T01:00:00Z")
        assert "WHERE SystemModstamp__c >=" in clause
        assert "2026-11-04T03:00:00Z" in clause
        assert "2026-11-04T04:00:00Z" in clause


class TestFormatWindow:
    def test_format_output(self) -> None:
        label = format_window("2026-11-04T13:00:00Z", "2026-11-04T14:00:00Z")
        assert "2026-11-04 13:00" in label
        assert "14:00" in label
        assert "(SP)" in label


class TestBuildRunWindows:
    def test_carga_inicial_returns_open_window(self) -> None:
        windows = build_run_windows("carga_inicial", None, False, None, None)
        assert windows == [(None, None)]

    def test_hora_returns_one_window(self) -> None:
        windows = build_run_windows("hora", None, False, None, None)
        assert len(windows) == 1

    def test_dia_uses_yesterday_by_default(self) -> None:
        windows = build_run_windows("dia", None, False, None, None)
        assert len(windows) == 1

    def test_reprocess_generates_day_per_day(self) -> None:
        from_date = date(2026, 10, 1)
        to_date = date(2026, 10, 3)
        windows = build_run_windows("dia", None, True, from_date, to_date)
        assert len(windows) == 3

    def test_reprocess_raises_without_from(self) -> None:
        with pytest.raises(ValueError, match="reprocessar_de"):
            build_run_windows("dia", None, True, None, None)

    def test_reprocess_raises_when_to_before_from(self) -> None:
        with pytest.raises(ValueError, match="before"):
            build_run_windows(
                "dia", None, True, date(2026, 10, 5), date(2026, 10, 3)
            )

    def test_reprocess_raises_on_future_date(self) -> None:
        from datetime import date as _date

        future = _date(2099, 1, 1)
        with pytest.raises(ValueError, match="future"):
            build_run_windows("dia", None, True, future, future)
