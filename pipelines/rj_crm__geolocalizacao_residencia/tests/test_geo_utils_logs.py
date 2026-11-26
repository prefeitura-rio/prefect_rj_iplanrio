# -*- coding: utf-8 -*-
"""Os logs de erro do LocationIQ e do OpenCage não trazem a chave da API nem o endereço do cidadão."""

from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests

from pipelines.rj_crm__geolocalizacao_residencia.utils import geo_utils

CHAVE = "chave-sintetica-123"
ENDERECO = "Rua Sintetica 123, Bairro Ficticio, Rio de Janeiro"
URL_COM_CHAVE = f"https://us1.locationiq.com/v1/search.php?key={CHAVE}&q={ENDERECO}&format=json"
URL_OPENCAGE_COM_CHAVE = f"https://api.opencagedata.com/geocode/v1/json?q={ENDERECO}&key={CHAVE}"


def _resposta(payload, status_code=200):
    resposta = MagicMock()
    resposta.status_code = status_code
    resposta.json.return_value = payload
    return resposta


def _enderecos():
    return pd.DataFrame({"endereco": [ENDERECO]})


def _falha_de_conexao(url):
    """Erro como o do requests numa falha de rede: a mensagem traz a URL inteira."""
    return requests.exceptions.ConnectionError(
        f"HTTPSConnectionPool(host='exemplo'): Max retries exceeded with url: {url} (Caused by NewConnectionError)"
    )


@pytest.fixture(autouse=True)
def _sem_espera(monkeypatch):
    monkeypatch.setattr(geo_utils, "sleep", lambda *_: None)
    monkeypatch.setattr(geo_utils, "exponential_backoff_sleep", lambda *_args, **_kwargs: None)


@pytest.fixture
def mensagens(monkeypatch):
    registradas = []
    monkeypatch.setattr(geo_utils, "log", lambda mensagem, *_args, **_kwargs: registradas.append(str(mensagem)))
    return registradas


def _sem_dado_sensivel(texto):
    return CHAVE not in texto and "Rua Sintetica" not in texto and "Bairro Ficticio" not in texto


def _sessao_falsa(monkeypatch, **kwargs):
    sessao = MagicMock()
    sessao.get = MagicMock(**kwargs)
    monkeypatch.setattr(geo_utils.requests, "Session", lambda: sessao)
    return sessao


def _saldo(monkeypatch, **kwargs):
    saldo = MagicMock(**kwargs)
    monkeypatch.setattr(geo_utils.requests, "get", saldo)
    return saldo


def test_locationiq_falha_de_rede_na_busca_nao_loga_chave_nem_endereco(monkeypatch, mensagens, capsys):
    _saldo(monkeypatch, return_value=_resposta({"balance": {"day": 5}}))
    _sessao_falsa(monkeypatch, side_effect=_falha_de_conexao(URL_COM_CHAVE))

    resultado = geo_utils.geocode_locationiq(_enderecos(), "endereco", api_key=CHAVE)

    assert mensagens == ["LocationIQ: erro ao processar o endereço da linha 0: ConnectionError"]
    assert _sem_dado_sensivel(capsys.readouterr().out)
    assert resultado["latitude"].isna().all()
    assert (resultado["geocode"] == "locationiq").all()


def test_locationiq_falha_de_rede_no_saldo_nao_loga_chave(monkeypatch, mensagens, capsys):
    _saldo(monkeypatch, side_effect=_falha_de_conexao(f"/v1/balance?key={CHAVE}&format=json"))
    sessao = _sessao_falsa(monkeypatch, return_value=_resposta([]))

    resultado = geo_utils.geocode_locationiq(_enderecos(), "endereco", api_key=CHAVE)

    assert mensagens == ["Erro ao verificar o saldo de requisições do LocationIQ: ConnectionError"]
    assert _sem_dado_sensivel(capsys.readouterr().out)
    assert sessao.get.call_count == 0
    assert len(resultado) == 1


def test_locationiq_endereco_nao_encontrado_loga_so_a_linha(monkeypatch, mensagens, capsys):
    _saldo(monkeypatch, return_value=_resposta({"balance": {"day": 5}}))
    _sessao_falsa(monkeypatch, return_value=_resposta([]))

    geo_utils.geocode_locationiq(_enderecos(), "endereco", api_key=CHAVE)

    assert mensagens == ["LocationIQ: endereço da linha 0 não encontrado."]
    assert _sem_dado_sensivel(capsys.readouterr().out)


def test_locationiq_quota_excedida_loga_o_status_sem_dado_sensivel(monkeypatch, mensagens):
    _saldo(monkeypatch, return_value=_resposta({"balance": {"day": 5}}))
    _sessao_falsa(monkeypatch, return_value=_resposta({}, status_code=402))

    geo_utils.geocode_locationiq(_enderecos(), "endereco", api_key=CHAVE)

    assert mensagens == ["LocationIQ: erro ao processar o endereço da linha 0: HTTPError (HTTP 402)"]


@pytest.mark.usefixtures("mensagens")
def test_locationiq_envia_chave_e_endereco_em_params_e_nao_na_url(monkeypatch):
    saldo = _saldo(monkeypatch, return_value=_resposta({"balance": {"day": 5}}))
    sessao = _sessao_falsa(monkeypatch, return_value=_resposta([]))

    geo_utils.geocode_locationiq(_enderecos(), "endereco", api_key=CHAVE)

    assert saldo.call_args.args[0] == "https://us1.locationiq.com/v1/balance"
    assert saldo.call_args.kwargs["params"] == {"key": CHAVE, "format": "json"}
    assert sessao.get.call_args.args[0] == "https://us1.locationiq.com/v1/search.php"
    assert sessao.get.call_args.kwargs["params"] == {"key": CHAVE, "q": ENDERECO, "format": "json"}


def test_locationiq_resultado_valido_continua_preenchendo_os_campos(monkeypatch, mensagens):
    _saldo(monkeypatch, return_value=_resposta({"balance": {"day": 5}}))
    resultado_api = {
        "lat": "-22.9",
        "lon": "-43.2",
        "importance": 0.7,
        "display_name": "Rua Sintetica, Bairro Ficticio, Rio de Janeiro, Regiao Metropolitana, Rio de Janeiro, "
        "Regiao Sudeste, 20000-000, Brasil",
    }
    _sessao_falsa(monkeypatch, return_value=_resposta([resultado_api]))

    resultado = geo_utils.geocode_locationiq(_enderecos(), "endereco", api_key=CHAVE)

    linha = resultado.iloc[0]
    assert mensagens == []
    assert linha["latitude"] == pytest.approx(-22.9)
    assert linha["longitude"] == pytest.approx(-43.2)
    assert linha["confianca"] == pytest.approx(0.7)
    assert linha["logradouro_geocode"] == "rua sintetica"
    assert linha["bairro_geocode"] == "bairro ficticio"
    assert linha["cidade_geocode"] == "rio de janeiro"
    assert linha["cep_geocode"] == "20000-000"


def test_opencage_falha_de_rede_nao_loga_chave_nem_endereco(monkeypatch, mensagens):
    monkeypatch.setattr(geo_utils.requests, "get", MagicMock(side_effect=_falha_de_conexao(URL_OPENCAGE_COM_CHAVE)))

    resultado = geo_utils.geocode_opencage(_enderecos(), "endereco", api_key=CHAVE)

    assert mensagens == ["OpenCage: erro ao processar o endereço da linha 0: ConnectionError"]
    assert resultado["latitude"].isna().all()
    assert (resultado["geocode"] == "opencage").all()


def test_opencage_endereco_nao_encontrado_loga_so_a_linha(monkeypatch, mensagens):
    monkeypatch.setattr(geo_utils.requests, "get", MagicMock(return_value=_resposta({"results": []})))

    geo_utils.geocode_opencage(_enderecos(), "endereco", api_key=CHAVE)

    assert mensagens == ["OpenCage: endereço da linha 0 não encontrado."]


def test_opencage_quota_excedida_loga_o_status_sem_dado_sensivel(monkeypatch, mensagens):
    monkeypatch.setattr(geo_utils.requests, "get", MagicMock(return_value=_resposta({}, status_code=402)))

    geo_utils.geocode_opencage(_enderecos(), "endereco", api_key=CHAVE)

    assert mensagens == ["OpenCage: erro ao processar o endereço da linha 0: HTTPError (HTTP 402)"]


def test_opencage_resultado_valido_continua_preenchendo_os_campos(monkeypatch, mensagens):
    resultado_api = {
        "results": [
            {
                "geometry": {"lat": -22.9, "lng": -43.2},
                "components": {
                    "road": "Rua Sintetica",
                    "suburb": "Bairro Ficticio",
                    "city": "Rio de Janeiro",
                    "state": "Rio de Janeiro",
                    "postcode": "20000-000",
                },
                "confidence": 9,
            }
        ]
    }
    get = MagicMock(return_value=_resposta(resultado_api))
    monkeypatch.setattr(geo_utils.requests, "get", get)

    resultado = geo_utils.geocode_opencage(_enderecos(), "endereco", api_key=CHAVE)

    linha = resultado.iloc[0]
    assert mensagens == []
    assert get.call_args.kwargs["params"] == {"q": ENDERECO, "key": CHAVE, "language": "pt", "limit": 1}
    assert linha["latitude"] == pytest.approx(-22.9)
    assert linha["longitude"] == pytest.approx(-43.2)
    assert linha["confianca"] == pytest.approx(0.9)
    assert linha["logradouro_geocode"] == "rua sintetica"
    assert linha["cep_geocode"] == "20000-000"


@pytest.mark.parametrize(
    ("erro", "esperado"),
    [
        (_falha_de_conexao(URL_COM_CHAVE), "ConnectionError"),
        (requests.exceptions.Timeout(URL_COM_CHAVE), "Timeout"),
        (requests.exceptions.HTTPError(URL_COM_CHAVE, response=_resposta({}, status_code=500)), "HTTPError (HTTP 500)"),
    ],
)
def test_describe_request_error_descarta_a_mensagem(erro, esperado):
    assert geo_utils._describe_request_error(erro) == esperado
