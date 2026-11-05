"""Tests for helpers/queries.py — SQL template loader."""

import pytest

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.helpers.queries import load_query


class TestLoadQuery:
    def test_extract_renders_dlo_and_filtro(self) -> None:
        sql = load_query("extract", dlo="Account_Home__dll", filtro="WHERE col >= '2026-01-01'")
        assert "Account_Home__dll" in sql
        assert "WHERE col >= '2026-01-01'" in sql

    def test_extract_with_empty_filtro(self) -> None:
        sql = load_query("extract", dlo="WorkOrder_Home__dll", filtro="")
        assert "WorkOrder_Home__dll" in sql

    def test_missing_file_raises(self) -> None:
        with pytest.raises(FileNotFoundError):
            load_query("does_not_exist", foo="bar")

    def test_missing_placeholder_raises(self) -> None:
        with pytest.raises(KeyError):
            load_query("extract")  # missing dlo and filtro

    def test_merge_staging_renders(self) -> None:
        sql = load_query(
            "merge_staging",
            target="p.d.target",
            staging="p.d.staging",
            primary_key="id",
            set_clause="t.name = s.name",
            insert_cols="id, name",
            insert_vals="s.id, s.name",
        )
        assert "MERGE" in sql
        assert "p.d.target" in sql
        assert "p.d.staging" in sql
