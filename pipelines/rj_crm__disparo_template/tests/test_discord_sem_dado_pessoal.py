# -*- coding: utf-8 -*-
"""A notificação de sucesso do disparo no Discord não traz dado pessoal do cidadão.

Todos os dados abaixo são sintéticos.
"""

from types import SimpleNamespace

import pandas as pd
import pytest

from pipelines.rj_crm__disparo_template.utils import discord

WEBHOOK = "https://discord.invalid/api/webhooks/sintetico"
FLOW_RUN_URL = "https://prefect.invalid/runs/flow-run/sintetico"
CAMPANHA = "campanha_sintetica"
CPF = "00011122233"
TELEFONE = "5521900000001"
TELEFONE_RETENTATIVA = "5521900000003"
NOME = "Fulana Sintetica"
ENDERECO = "Rua Ficticia 123"
DIVIDA = "9876.54"

DADOS_PESSOAIS = (
    CPF,
    TELEFONE,
    TELEFONE_RETENTATIVA,
    # Prefixos que uma máscara parcial deixaria passar.
    CPF[:3],
    TELEFONE[:8],
    NOME,
    "Fulana",
    ENDERECO,
    DIVIDA,
)


@pytest.fixture
def enviadas(monkeypatch) -> list[tuple[str, str]]:
    """Captura o que iria ao webhook, sem rede."""
    mensagens: list[tuple[str, str]] = []

    async def _webhook(webhook_url: str, message: str) -> None:
        mensagens.append((webhook_url, message))

    monkeypatch.setattr(discord, "_send_discord_webhook", _webhook)
    monkeypatch.setattr(discord, "log", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(discord, "prefect_flow_run", SimpleNamespace(ui_url=FLOW_RUN_URL))
    monkeypatch.setenv("DISCORD_WEBHOOK_URL_DISPAROS", WEBHOOK)
    return mensagens


def _destino() -> dict:
    """Primeira linha do lote, como o flow passa (`current_df.iloc[0].to_dict()`)."""
    df = pd.DataFrame(
        {
            "cpf": [CPF],
            "telefone": [TELEFONE],
            "nome": [NOME],
            "endereco": [ENDERECO],
            "valor_divida": [float(DIVIDA)],
            "others": [[TELEFONE_RETENTATIVA]],
        }
    )
    return df.iloc[0].to_dict()


def _notificar(sample_destination: dict | None) -> None:
    discord.send_dispatch_success_notification(
        total_dispatches=42,
        dispatch_date="2026-01-02 03:04:05",
        campaign_name=CAMPANHA,
        total_batches=3,
        sample_destination=sample_destination,
        test_mode=False,
        attempt_number=2,
        total_attempt_number=4,
    )


def test_notificacao_de_sucesso_nao_leva_dado_pessoal(enviadas):
    _notificar(_destino())

    assert len(enviadas) == 1
    webhook_url, mensagem = enviadas[0]
    assert webhook_url == WEBHOOK
    assert [dado for dado in DADOS_PESSOAIS if dado in mensagem] == []


def test_notificacao_de_sucesso_mantem_contagens_campanha_lote_tentativa_e_link(enviadas):
    _notificar(_destino())

    mensagem = enviadas[0][1]
    assert "✅ **Disparo Realizado com Sucesso**" in mensagem
    assert "42 disparos" in mensagem
    assert "3 lotes" in mensagem
    assert "2026-01-02 03:04:05" in mensagem
    assert CAMPANHA in mensagem
    assert "2 de 4" in mensagem
    assert FLOW_RUN_URL in mensagem


def test_notificacao_de_sucesso_lista_so_os_nomes_dos_campos(enviadas):
    _notificar(_destino())

    mensagem = enviadas[0][1]
    assert "**Campos do destinatário:** `cpf`, `telefone`, `nome`, `endereco`, `valor_divida`, `others`" in mensagem


def test_notificacao_de_sucesso_sem_destinatario_de_exemplo(enviadas):
    _notificar(None)

    mensagem = enviadas[0][1]
    assert "Campos do destinatário" not in mensagem
    assert "42 disparos" in mensagem


def test_formatacao_do_destinatario_nao_devolve_valores():
    formatado = discord._format_sample_destination(_destino())

    assert formatado == "`cpf`, `telefone`, `nome`, `endereco`, `valor_divida`, `others`"
    assert [dado for dado in DADOS_PESSOAIS if dado in formatado] == []
