"""Relatório de paridade F1: contagens por regra e por classe, e o veredito com tolerância zero."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from pipelines.rj_smfp__nota_carioca_paridade_f1.utils.paridade import (
    EXPECTED_RULES,
    ResumoInvalidoError,
    build_report,
    parse_resumo,
)

GERADO_EM = datetime(2026, 10, 23, 12, 0, tzinfo=UTC)
CORTE_NSU = 1_500_000


def row(regra: str, classe: str, quantidade: int, corte_nsu: int = CORTE_NSU, gerado_em: datetime = GERADO_EM):
    return {"regra": regra, "classe": classe, "quantidade": quantidade, "corte_nsu": corte_nsu, "gerado_em": gerado_em}


def all_equal_rows() -> list[dict[str, object]]:
    return [row(regra, "igual", 100 + index) for index, regra in enumerate(EXPECTED_RULES)]


def report_for(rows: list[dict[str, object]], **kwargs):
    return build_report(parse_resumo(rows), **kwargs)


def rule(report, regra: str):
    return next(item for item in report.regras if item.regra == regra)


def test_expected_rules_are_r01_to_r11() -> None:
    # Given/When: as regras `dbt` da matriz de paridade §10 que o resumo cobre
    # Then: R-01 a R-11, em ordem
    assert EXPECTED_RULES == tuple(f"R-{number:02d}" for number in range(1, 12))


def test_all_equal_summary_is_approved_with_comparisons_in_every_rule() -> None:
    # Given: um resumo em que toda regra só tem linhas iguais
    # When: o relatório é montado
    report = report_for(all_equal_rows())

    # Then: aprovado, sem divergentes e com comparados > 0 em cada uma das 11 regras
    assert report.resultado == "aprovado"
    assert report.motivos == ()
    assert [item.regra for item in report.regras] == list(EXPECTED_RULES)
    assert all(item.divergentes == 0 and item.comparados > 0 for item in report.regras)
    assert report.corte_nsu == CORTE_NSU
    assert report.gerado_em == GERADO_EM


def test_one_divergent_row_rejects_and_names_the_rule() -> None:
    # Given: um centavo de diferença em R-11
    rows = [*all_equal_rows(), row("R-11", "divergente", 1)]

    # When: o relatório é montado
    report = report_for(rows)

    # Then: reprovado, R-11 com divergentes=1, e o motivo cita só R-11
    assert report.resultado == "reprovado"
    assert rule(report, "R-11").divergentes == 1
    assert len(report.motivos) == 1
    assert "R-11" in report.motivos[0]
    assert "divergente" in report.motivos[0]


def test_rule_with_nothing_compared_rejects() -> None:
    # Given: R-04 só tem uma linha com quantidade zero
    rows = [item for item in all_equal_rows() if item["regra"] != "R-04"] + [row("R-04", "igual", 0)]

    # When: o relatório é montado
    report = report_for(rows)

    # Then: reprovado porque nada foi comparado em R-04
    assert report.resultado == "reprovado"
    assert rule(report, "R-04").comparados == 0
    assert any("R-04" in motivo and "comparados=0" in motivo for motivo in report.motivos)


def test_expected_rule_missing_from_summary_rejects() -> None:
    # Given: o resumo não tem nenhuma linha de R-07
    rows = [item for item in all_equal_rows() if item["regra"] != "R-07"]

    # When: o relatório é montado
    report = report_for(rows)

    # Then: reprovado, R-07 aparece no relatório com comparados=0 e o motivo diz que ela faltou
    assert report.resultado == "reprovado"
    assert rule(report, "R-07").comparados == 0
    assert any("R-07" in motivo and "ausente" in motivo for motivo in report.motivos)


def test_cataloged_corrections_are_aggregated_by_correction_id_and_count_as_compared() -> None:
    # Given: R-06 com iguais e com correções B-04 (em duas linhas) e B-06
    rows = [item for item in all_equal_rows() if item["regra"] != "R-06"] + [
        row("R-06", "igual", 10),
        row("R-06", "correcao_catalogada:B-04", 3),
        row("R-06", "correcao_catalogada:B-04", 2),
        row("R-06", "correcao_catalogada:B-06", 1),
    ]

    # When: o relatório é montado
    report = report_for(rows)

    # Then: correções agregadas por id; comparados = iguais + correções; correção catalogada não reprova
    r06 = rule(report, "R-06")
    assert r06.correcoes_catalogadas == {"B-04": 5, "B-06": 1}
    assert r06.iguais == 10
    assert r06.comparados == 16
    assert r06.divergentes == 0
    assert report.resultado == "aprovado"


def test_report_json_is_deterministic_and_ordered_by_rule() -> None:
    # Given: as mesmas linhas em duas ordens diferentes
    rows = [
        *all_equal_rows(),
        row("R-06", "correcao_catalogada:B-06", 1),
        row("R-06", "correcao_catalogada:B-04", 3),
    ]

    # When: dois relatórios são serializados
    first = report_for(rows).to_json()
    second = report_for(list(reversed(rows))).to_json()

    # Then: o texto é idêntico, com as chaves do contrato e regras ordenadas
    assert first == second
    payload = json.loads(first)
    assert list(payload) == ["corte_nsu", "gerado_em", "regras", "resultado", "motivos"]
    assert [item["regra"] for item in payload["regras"]] == list(EXPECTED_RULES)
    assert list(payload["regras"][0]) == ["regra", "comparados", "iguais", "correcoes_catalogadas", "divergentes"]
    assert payload["regras"][5]["correcoes_catalogadas"] == {"B-04": 3, "B-06": 1}
    assert payload["corte_nsu"] == CORTE_NSU
    assert payload["gerado_em"] == "2026-10-23T12:00:00+00:00"
    assert payload["resultado"] == "aprovado"


def test_unknown_class_in_summary_is_a_contract_violation() -> None:
    # Given: uma classe fora do contrato
    rows = [*all_equal_rows(), row("R-01", "quase_igual", 1)]

    # When / Then: a leitura do resumo recusa, em vez de ignorar a linha
    with pytest.raises(ResumoInvalidoError, match="quase_igual"):
        parse_resumo(rows)


def test_correction_without_id_is_a_contract_violation() -> None:
    # Given / When / Then: correcao_catalogada sem o id da correção
    with pytest.raises(ResumoInvalidoError, match="correcao_catalogada"):
        parse_resumo([row("R-01", "correcao_catalogada:", 1)])


def test_negative_quantity_is_a_contract_violation() -> None:
    with pytest.raises(ResumoInvalidoError, match="quantidade"):
        parse_resumo([row("R-01", "igual", -1)])


def test_summary_from_another_nsu_cut_than_requested_rejects() -> None:
    # Given: o resumo foi gerado com o corte 1.500.000, mas o flow pediu 2.000.000
    # When: o relatório é montado com o corte pedido
    report = report_for(all_equal_rows(), requested_corte_nsu=2_000_000)

    # Then: reprovado, citando os dois cortes
    assert report.resultado == "reprovado"
    assert any("1500000" in motivo and "2000000" in motivo for motivo in report.motivos)


def test_summary_mixing_nsu_cuts_rejects() -> None:
    # Given: duas regras com cortes diferentes
    rows = [*all_equal_rows(), row("R-02", "igual", 5, corte_nsu=CORTE_NSU + 1)]

    # When / Then: reprovado, e o relatório usa o maior corte
    report = report_for(rows)
    assert report.resultado == "reprovado"
    assert report.corte_nsu == CORTE_NSU + 1
    assert any("corte" in motivo for motivo in report.motivos)


def test_summary_generated_before_the_dbt_run_started_rejects() -> None:
    # Given: o dbt começou depois da geração do resumo (resumo de uma execução anterior)
    not_before = GERADO_EM + timedelta(minutes=1)

    # When: o relatório é montado
    report = report_for(all_equal_rows(), not_before=not_before)

    # Then: reprovado por resumo antigo
    assert report.resultado == "reprovado"
    assert any("anterior" in motivo for motivo in report.motivos)


def test_empty_summary_rejects_every_expected_rule() -> None:
    # Given / When: nenhuma linha
    report = report_for([])

    # Then: reprovado, uma razão por regra esperada, e sem corte nem hora
    assert report.resultado == "reprovado"
    assert report.corte_nsu is None
    assert report.gerado_em is None
    assert len([motivo for motivo in report.motivos if "ausente" in motivo]) == len(EXPECTED_RULES)
