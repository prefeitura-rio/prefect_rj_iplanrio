# -*- coding: utf-8 -*-
"""Os logs de calculate_24h_sessions não trazem o telefone do cidadão nem linhas do DataFrame."""

import logging

import pandas as pd
import pytest

from pipelines.rj_crm__relatorio_cvl.tasks import calculate_24h_sessions

# Telefones sintéticos: o de 12 dígitos é normalizado com o '9' depois do DDD.
TELEFONE_A = "552190000001"
TELEFONE_A_NORMALIZADO = "5521990000001"
TELEFONE_B = "5521980000002"
TELEFONES = (TELEFONE_A, TELEFONE_A_NORMALIZADO, TELEFONE_B)
LINHAS_EM_OUTUBRO = 4


def _interacoes() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "contato_telefone": [TELEFONE_A, TELEFONE_A, TELEFONE_A, TELEFONE_B, TELEFONE_B],
            "mensagem_datahora": [
                "2026-10-01 10:00:00",
                "2026-10-01 12:00:00.500000",
                "2026-10-03 09:00:00",
                "2026-10-05 08:00:00",
                "2026-09-30 23:00:00",
            ],
        }
    )


def _saida_completa(capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture) -> str:
    capturado = capsys.readouterr()
    return "\n".join([capturado.out, capturado.err, caplog.text])


def test_nao_loga_telefone_nem_linhas(capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG)

    calculate_24h_sessions.fn(_interacoes(), "2026-10")

    saida = _saida_completa(capsys, caplog)
    for telefone in TELEFONES:
        assert telefone not in saida, "log com o telefone do cidadão"
    assert "contato_telefone" not in saida, "log com linha ou colunas do DataFrame"
    assert "id_sessao_24h" not in saida, "log com linha ou colunas do DataFrame"


def test_loga_so_contagens(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)

    calculate_24h_sessions.fn(_interacoes(), "2026-10")

    assert "5 linhas, 4 sessões" in caplog.text
    assert "Linhas no mês do relatório 2026-10: 4" in caplog.text


def test_resultado_das_sessoes_inalterado() -> None:
    estatisticas, sessoes = calculate_24h_sessions.fn(_interacoes(), "2026-10")

    assert len(sessoes) == LINHAS_EM_OUTUBRO
    assert set(sessoes["contato_telefone"]) == {TELEFONE_A_NORMALIZADO, TELEFONE_B}
    assert sorted(sessoes["id_sessao_24h"]) == sorted(
        [
            f"{TELEFONE_A_NORMALIZADO}_1",
            f"{TELEFONE_A_NORMALIZADO}_1",
            f"{TELEFONE_A_NORMALIZADO}_2",
            f"{TELEFONE_B}_2",
        ]
    )
    assert estatisticas["numero_sessoes"].tolist() == [3]
    assert estatisticas["clientes_unicos"].tolist() == [2]


def test_mes_sem_linhas_nao_quebra(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)

    estatisticas, sessoes = calculate_24h_sessions.fn(_interacoes(), "2026-11")

    assert sessoes.empty
    assert estatisticas.empty
    assert "Linhas no mês do relatório 2026-11: 0" in caplog.text
