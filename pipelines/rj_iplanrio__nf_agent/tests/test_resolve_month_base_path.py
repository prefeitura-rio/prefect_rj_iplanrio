"""Tests for ``utils.pipeline.resolve_month_base_path``."""

import pytest

from pipelines.rj_iplanrio__nf_agent.utils.pipeline import resolve_month_base_path


def test_none_returns_base_unchanged():
    assert resolve_month_base_path("staging/brutos_osinfo_mongo/files_pdfs", None) == (
        "staging/brutos_osinfo_mongo/files_pdfs"
    )


def test_valid_date_scopes_to_month_subfolder():
    assert resolve_month_base_path("staging/brutos_osinfo_mongo/files_pdfs", "2021-11-01") == (
        "staging/brutos_osinfo_mongo/files_pdfs/mes_envio=2021-11-01"
    )


def test_trailing_slash_on_base_does_not_double_slash():
    assert resolve_month_base_path("pdfs/", "2024-05-01") == "pdfs/mes_envio=2024-05-01"


@pytest.mark.parametrize("bad", ["2024-05", "05/2024", "2024-13-45", "mes_envio=2024-05-01", "", "20240501"])
def test_invalid_format_raises(bad):
    with pytest.raises(ValueError, match="mes_envio"):
        resolve_month_base_path("pdfs", bad)
