"""Tests for helpers/discord.py — message builders."""

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.discord import (
    LIMIT,
    build_summary_message,
    build_table_failure_message,
    truncate,
)


class TestTruncate:
    def test_short_text_unchanged(self) -> None:
        assert truncate("hello") == "hello"

    def test_long_text_truncated(self) -> None:
        long = "x" * (LIMIT + 100)
        result = truncate(long)
        assert len(result) <= LIMIT
        assert result.endswith("...")


class TestBuildTableFailureMessage:
    def test_contains_table_name(self) -> None:
        msg = build_table_failure_message("account", "Cadastro", "2026-11-04", "Some error")
        assert "account" in msg
        assert "Cadastro" in msg

    def test_uses_last_900_chars_of_error(self) -> None:
        long_error = "A" * 100 + "B" * 900
        msg = build_table_failure_message("t", "g", "w", long_error)
        # The last 900 chars (all Bs) should be present, not the first 900.
        assert "BBB" in msg
        assert "AAA" not in msg


class TestBuildSummaryMessage:
    def test_contains_label(self) -> None:
        msg = build_summary_message("modo=dia", {"Cadastro": {"account": 100}}, [], 2.5)
        assert "modo=dia" in msg
        assert "2.5 min" in msg

    def test_includes_failure_warning(self) -> None:
        msg = build_summary_message("test", {}, ["account"], 1.0)
        assert "account" in msg

    def test_no_failure_section_when_empty(self) -> None:
        msg = build_summary_message("test", {"G": {"t": 5}}, [], 1.0)
        assert "failed" not in msg.lower()
