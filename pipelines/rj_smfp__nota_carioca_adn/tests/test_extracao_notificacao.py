"""Notificação do Discord por ambiente: webhook do ambiente, marcador de staging e nunca o canal de prod."""

import json

import httpx
import pytest

from pipelines.rj_smfp__nota_carioca_adn.utils.discord import Environment
from pipelines.rj_smfp__nota_carioca_adn.utils.modelos import ResultadoExtracao
from pipelines.rj_smfp__nota_carioca_adn.utils.notify import ExtracaoNotificador

HOOK_STAGING = "https://discord.test/api/webhooks/1/staging"
HOOK_PROD = "https://discord.test/api/webhooks/2/prod"


def _transporte(requisicoes: list[httpx.Request]) -> httpx.MockTransport:
    def responder(requisicao: httpx.Request) -> httpx.Response:
        requisicoes.append(requisicao)
        return httpx.Response(200, json={"id": "mensagem-1"})

    return httpx.MockTransport(responder)


def _resultado() -> ResultadoExtracao:
    return ResultadoExtracao(
        cursor_inicial=0,
        cursor_final=1234,
        lotes=25,
        documentos=1234,
        ilegiveis=2,
        lacunas_preenchidas=3,
        lacunas_inexistentes=1,
        motivo_fim="sem_documentos",
    )


def _textos(requisicoes: list[httpx.Request]) -> str:
    return " ".join(json.dumps(json.loads(requisicao.content), ensure_ascii=False) for requisicao in requisicoes)


def test_extracao_staging_posta_so_no_webhook_de_staging_e_marca_a_mensagem(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: os dois webhooks no ambiente
    monkeypatch.setenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA_STAGING", HOOK_STAGING)
    monkeypatch.setenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", HOOK_PROD)
    requisicoes: list[httpx.Request] = []
    notificador = ExtracaoNotificador.criar(Environment.STAGING, transporte=_transporte(requisicoes))

    # When: um run de staging do início ao fim
    notificador.iniciar(0)
    notificador.concluir(_resultado())

    # Then: todas as chamadas foram ao webhook de staging, com o marcador e os números do run
    assert requisicoes
    assert all(str(requisicao.url).startswith(HOOK_STAGING) for requisicao in requisicoes)
    texto = _textos(requisicoes)
    assert "STAGING" in texto
    assert "1234" in texto


def test_extracao_staging_sem_webhook_proprio_fica_calado_em_vez_de_usar_o_de_prod(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Given: só o webhook de prod configurado
    monkeypatch.setenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", HOOK_PROD)
    requisicoes: list[httpx.Request] = []
    notificador = ExtracaoNotificador.criar(Environment.STAGING, transporte=_transporte(requisicoes))

    # When
    notificador.iniciar(0)
    notificador.concluir(_resultado())

    # Then: nenhuma mensagem de staging vai para o canal de prod
    assert requisicoes == []


def test_extracao_prod_usa_o_webhook_de_prod_sem_marcador(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given
    monkeypatch.setenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", HOOK_PROD)
    requisicoes: list[httpx.Request] = []
    notificador = ExtracaoNotificador.criar(Environment.PROD, transporte=_transporte(requisicoes))

    # When
    notificador.iniciar(0)
    notificador.falhar(RuntimeError("falha de teste"))

    # Then: vai ao webhook de prod, sem o marcador de staging, e a falha aparece
    assert all(str(requisicao.url).startswith(HOOK_PROD) for requisicao in requisicoes)
    texto = _textos(requisicoes)
    assert "STAGING" not in texto
    assert "RuntimeError" in texto


def test_extracao_notificacao_desligada_nao_faz_requisicao(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: webhook configurado, mas notificações desligadas por parâmetro
    monkeypatch.setenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", HOOK_PROD)
    requisicoes: list[httpx.Request] = []
    notificador = ExtracaoNotificador.criar(Environment.PROD, habilitado=False, transporte=_transporte(requisicoes))

    # When
    notificador.iniciar(0)
    notificador.concluir(_resultado())

    # Then
    assert requisicoes == []


def test_extracao_falha_do_discord_nunca_derruba_o_run(monkeypatch: pytest.MonkeyPatch) -> None:
    # Given: webhook que responde 500 e depois derruba a conexão
    monkeypatch.setenv("DISCORD_WEBHOOK_URL_NOTA_CARIOCA", HOOK_PROD)

    def quebrar(_requisicao: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("sem rede")

    notificador = ExtracaoNotificador.criar(Environment.PROD, transporte=httpx.MockTransport(quebrar))

    # When / Then: nenhuma exceção sobe
    notificador.iniciar(0)
    notificador.progresso(_resultado())
    notificador.concluir(_resultado())
    notificador.falhar(RuntimeError("x"))
