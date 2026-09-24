"""Tests for merging NFs that are the same fiscal note."""

from pipelines.rj_iplanrio__nf_agent.utils.nf_merge import coalesce_nfs_by_numero, same_nf_key


def nf(numero, cnpj, data, pagina, **extra):
    return {"numero_nf": numero, "cnpj_emitente": cnpj, "data_emissao": data, "pagina": pagina, **extra}


def test_same_nf_key_normalizes_cnpj_and_requires_all_fields():
    assert same_nf_key(nf("10", "12.345.678/0001-90", "01/02/2025", 1)) == ("10", "12345678000190", "01/02/2025")
    assert same_nf_key(nf("10", None, "01/02/2025", 1)) is None
    assert same_nf_key(nf(None, "123", "01/02/2025", 1)) is None


def test_merges_only_the_same_note():
    merged = coalesce_nfs_by_numero([
        nf("10", "12.345.678/0001-90", "01/02/2025", 2, valor_total=None),
        nf("10", "12345678000190", "01/02/2025", 1, valor_total=50.0),
    ])
    assert len(merged) == 1
    assert merged[0]["pagina"] == 1
    assert merged[0]["valor_total"] == 50.0  # noqa: PLR2004 -- the fixture's valor_total, clearest as a literal here


def test_same_number_from_different_issuers_is_not_merged():
    merged = coalesce_nfs_by_numero([
        nf("10", "11111111000111", "01/02/2025", 1),
        nf("10", "22222222000122", "01/02/2025", 2),
    ])
    assert len(merged) == 2  # noqa: PLR2004 -- 2 inputs stay unmerged, clearest as a literal here


def test_same_number_on_different_dates_is_not_merged():
    merged = coalesce_nfs_by_numero([nf("10", "111", "01/02/2025", 1), nf("10", "111", "01/03/2025", 2)])
    assert len(merged) == 2  # noqa: PLR2004 -- 2 inputs stay unmerged, clearest as a literal here


def test_incomplete_nfs_are_never_merged():
    merged = coalesce_nfs_by_numero([nf("10", None, "01/02/2025", 1), nf("10", None, "01/02/2025", 2)])
    assert len(merged) == 2  # noqa: PLR2004 -- 2 inputs stay unmerged, clearest as a literal here
