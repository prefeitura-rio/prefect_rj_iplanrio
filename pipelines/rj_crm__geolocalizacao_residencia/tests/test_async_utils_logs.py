# -*- coding: utf-8 -*-
"""Os logs da geocodificação (async e síncrona) não trazem a chave da API nem o endereço do cidadão."""

import aiohttp
import pandas as pd
import pytest
import requests
import yarl
from geopy.exc import GeocoderServiceError
from multidict import CIMultiDict, CIMultiDictProxy

from pipelines.rj_crm__geolocalizacao_residencia.utils import async_utils, geo_utils

CHAVE = "chave-sintetica-123"
ENDERECO = "Rua Sintetica 123, Bairro Ficticio, Rio de Janeiro"
URL_COM_CHAVE = f"https://api.exemplo/v1/search?key={CHAVE}&q={ENDERECO}"
JOB_URL = "https://api.geoapify.com/v1/batch/geocode/search?id=job-sintetico"
LATITUDE = -22.9


class _Resposta:
    def __init__(self, status=200, payload=None, erro=None):
        self.status = status
        self._payload = payload
        self._erro = erro

    async def __aenter__(self):
        if self._erro is not None:
            raise self._erro
        return self

    async def __aexit__(self, *_args):
        return False

    async def json(self):
        return self._payload


class _Sessao:
    """Sessão do aiohttp falsa: responde em ordem e guarda cada chamada."""

    def __init__(self, respostas):
        self._respostas = list(respostas)
        self.chamadas = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def _responder(self, metodo, url, kwargs):
        self.chamadas.append((metodo, url, kwargs))
        return self._respostas.pop(0) if len(self._respostas) > 1 else self._respostas[0]

    def get(self, url, **kwargs):
        return self._responder("GET", url, kwargs)

    def post(self, url, **kwargs):
        return self._responder("POST", url, kwargs)


def _erro_http(status=401, url=URL_COM_CHAVE):
    """Erro como o do aiohttp numa resposta 4xx/5xx: a mensagem traz a URL inteira."""
    info = aiohttp.RequestInfo(yarl.URL(url), "GET", CIMultiDictProxy(CIMultiDict()), yarl.URL(url))
    return aiohttp.ClientResponseError(info, (), status=status, message="Unauthorized")


def _erro_de_conexao(url=URL_COM_CHAVE):
    return aiohttp.ClientConnectionError(f"Cannot connect to host api.exemplo:443 ssl:default [{url}]")


def _falha_de_conexao_requests(url=URL_COM_CHAVE):
    return requests.exceptions.ConnectionError(
        f"HTTPSConnectionPool(host='exemplo'): Max retries exceeded with url: {url} (Caused by NewConnectionError)"
    )


def _enderecos(n=1):
    return pd.DataFrame({"endereco": [ENDERECO] * n})


def _sem_dado_sensivel(texto):
    return CHAVE not in texto and "Rua Sintetica" not in texto and "Bairro Ficticio" not in texto


@pytest.fixture
def mensagens(monkeypatch):
    registradas = []

    def _log(mensagem, *_args, **_kwargs):
        registradas.append(str(mensagem))

    monkeypatch.setattr(async_utils, "log", _log)
    monkeypatch.setattr(geo_utils, "log", _log)
    return registradas


@pytest.fixture(autouse=True)
def _sem_espera(monkeypatch):
    monkeypatch.setattr(geo_utils, "sleep", lambda *_: None)
    monkeypatch.setattr(geo_utils, "exponential_backoff_sleep", lambda *_args, **_kwargs: None)


def _sessao_falsa(monkeypatch, *respostas):
    sessao = _Sessao(respostas)
    monkeypatch.setattr(async_utils.aiohttp, "ClientSession", lambda *_args, **_kwargs: sessao)
    return sessao


def _assert_logs_limpos(mensagens, capsys):
    assert all(_sem_dado_sensivel(m) for m in mensagens), mensagens
    assert _sem_dado_sensivel(capsys.readouterr().out)


def test_describe_async_error_descarta_a_mensagem():
    erro_http = _erro_http(401)
    erro_conexao = _erro_de_conexao()
    assert CHAVE in str(erro_http)
    assert CHAVE in str(erro_conexao)

    assert async_utils._describe_async_error(erro_http) == "ClientResponseError (HTTP 401)"
    assert async_utils._describe_async_error(erro_conexao) == "ClientConnectionError"
    assert async_utils._describe_async_error(_falha_de_conexao_requests()) == "ConnectionError"


# --- Nominatim (async) -------------------------------------------------------------------------------------


def test_nominatim_async_status_de_erro_loga_a_linha_e_o_status_sem_endereco(monkeypatch, mensagens, capsys):
    _sessao_falsa(monkeypatch, _Resposta(status=503))

    resultado = async_utils.run_geocode_nominatim_async(
        _enderecos(2), "endereco", "ua-sintetico", "nominatim.exemplo", sleep_time=0, use_exponential_backoff=False
    )

    falhas = sorted(m for m in mensagens if m.startswith("Nominatim:"))
    assert falhas == [
        "Nominatim: requisição do endereço da linha 0 falhou com HTTP 503",
        "Nominatim: requisição do endereço da linha 1 falhou com HTTP 503",
    ]
    _assert_logs_limpos(mensagens, capsys)
    assert resultado["latitude"].isna().all()


@pytest.mark.usefixtures("mensagens")
def test_nominatim_async_resultado_valido_continua_preenchendo_os_campos(monkeypatch):
    payload = [{"lat": "-22.9", "lon": "-43.2", "address": {"road": "Rua Sintetica", "city": "Rio de Janeiro"}}]
    sessao = _sessao_falsa(monkeypatch, _Resposta(payload=payload))

    resultado = async_utils.run_geocode_nominatim_async(
        _enderecos(), "endereco", "ua-sintetico", "nominatim.exemplo", sleep_time=0, use_exponential_backoff=False
    )

    assert resultado.loc[0, "latitude"] == "-22.9"
    assert resultado.loc[0, "logradouro_geocode"] == "rua sintetica"
    assert sessao.chamadas[0][1] == "https://nominatim.exemplo/search"


# --- LocationIQ (async) ------------------------------------------------------------------------------------


def test_locationiq_async_falha_no_saldo_nao_loga_chave(monkeypatch, mensagens, capsys):
    sessao = _sessao_falsa(monkeypatch, _Resposta(erro=_erro_http(401)))

    resultado = async_utils.run_geocode_locationiq_async(
        _enderecos(), "endereco", api_key=CHAVE, sleep_time=0, use_exponential_backoff=False
    )

    assert "Erro ao verificar o saldo de requisições do LocationIQ: ClientResponseError (HTTP 401)" in mensagens
    _assert_logs_limpos(mensagens, capsys)
    assert len(sessao.chamadas) == 1
    assert len(resultado) == 1


def test_locationiq_async_envia_chave_e_endereco_em_params_e_geocodifica(monkeypatch, mensagens, capsys):
    resultado_api = [
        {
            "display_name": (
                "Rua Sintetica, Bairro Ficticio, Rio de Janeiro, Regiao, Rio de Janeiro, Sudeste, 20000-000, Brasil"
            ),
            "lat": "-22.9",
            "lon": "-43.2",
            "importance": 0.5,
        }
    ]
    sessao = _sessao_falsa(monkeypatch, _Resposta(payload={"balance": {"day": 5}}), _Resposta(payload=resultado_api))

    resultado = async_utils.run_geocode_locationiq_async(
        _enderecos(), "endereco", api_key=CHAVE, sleep_time=0, use_exponential_backoff=False
    )

    (_, url_saldo, kwargs_saldo), (_, url_busca, kwargs_busca) = sessao.chamadas
    assert url_saldo == "https://us1.locationiq.com/v1/balance"
    assert kwargs_saldo["params"] == {"key": CHAVE, "format": "json"}
    assert url_busca == "https://us1.locationiq.com/v1/search.php"
    assert kwargs_busca["params"] == {"key": CHAVE, "q": ENDERECO, "format": "json"}
    assert resultado.loc[0, "latitude"] == LATITUDE
    assert resultado.loc[0, "bairro_geocode"] == "bairro ficticio"
    assert resultado.loc[0, "geocode"] == "locationiq"
    _assert_logs_limpos(mensagens, capsys)


# --- Geocode.xyz (async) -----------------------------------------------------------------------------------


@pytest.mark.usefixtures("mensagens")
def test_geocodexyz_async_envia_a_chave_em_params(monkeypatch):
    payload = {"latt": "-22.9", "longt": "-43.2", "standard": {"addresst": "Rua Sintetica", "city": "Rio"}}
    sessao = _sessao_falsa(monkeypatch, _Resposta(payload=payload))

    resultado = async_utils.run_geocode_geocodexyz_async(
        _enderecos(), "endereco", api_key=CHAVE, sleep_time=0, use_exponential_backoff=False
    )

    (_, url, kwargs) = sessao.chamadas[0]
    assert url == f"https://geocode.xyz/{ENDERECO}?json=1"
    assert kwargs["params"] == {"auth": CHAVE}
    assert resultado.loc[0, "latitude"] == "-22.9"


# --- Geoapify (async) --------------------------------------------------------------------------------------


def _resultado_geoapify():
    return {"status": "completed", "results": [{"lat": -22.9, "lon": -43.2, "street": "Rua Sintetica"}]}


def test_geoapify_envia_a_chave_em_params_e_nao_loga_a_url_do_lote(monkeypatch, mensagens, capsys):
    sessao = _sessao_falsa(monkeypatch, _Resposta(payload={"url": JOB_URL}), _Resposta(payload=_resultado_geoapify()))

    resultado = async_utils.run_geoapify_batch_geocoding_async(
        _enderecos(), "endereco", api_key=CHAVE, max_wait_time=1, poll_interval=0.01
    )

    (metodo_envio, url_envio, kwargs_envio), (metodo_consulta, url_consulta, kwargs_consulta) = sessao.chamadas
    assert (metodo_envio, url_envio) == ("POST", "https://api.geoapify.com/v1/batch/geocode/search")
    assert kwargs_envio["params"] == {"lang": "pt", "filter": "countrycode:br", "apiKey": CHAVE}
    assert kwargs_envio["json"] == [ENDERECO]
    assert (metodo_consulta, url_consulta) == ("GET", JOB_URL)
    assert kwargs_consulta["params"] == {"apiKey": CHAVE}
    assert "Geoapify: lote enviado com sucesso" in mensagens
    assert not any("job-sintetico" in m for m in mensagens)
    _assert_logs_limpos(mensagens, capsys)
    assert resultado.loc[0, "latitude"] == LATITUDE
    assert resultado.loc[0, "logradouro_geocode"] == "rua sintetica"


@pytest.mark.usefixtures("mensagens")
def test_geoapify_nao_repete_a_chave_quando_a_url_do_lote_ja_a_traz(monkeypatch):
    job_url_com_chave = f"{JOB_URL}&apiKey={CHAVE}"
    sessao = _sessao_falsa(
        monkeypatch, _Resposta(payload={"url": job_url_com_chave}), _Resposta(payload=_resultado_geoapify())
    )

    async_utils.run_geoapify_batch_geocoding_async(
        _enderecos(), "endereco", api_key=CHAVE, max_wait_time=1, poll_interval=0.01
    )

    (_, url_consulta, kwargs_consulta) = sessao.chamadas[1]
    assert url_consulta == job_url_com_chave
    assert kwargs_consulta["params"] == {}


def test_geoapify_falha_no_envio_nao_loga_chave_nem_endereco(monkeypatch, mensagens, capsys):
    _sessao_falsa(monkeypatch, _Resposta(erro=_erro_de_conexao()))

    resultado = async_utils.run_geoapify_batch_geocoding_async(_enderecos(), "endereco", api_key=CHAVE)

    assert "Error submitting Geoapify batch job: ClientConnectionError" in mensagens
    _assert_logs_limpos(mensagens, capsys)
    assert resultado["latitude"].isna().all()


def test_geoapify_falha_na_consulta_nao_loga_chave(monkeypatch, mensagens, capsys):
    _sessao_falsa(monkeypatch, _Resposta(payload={"url": JOB_URL}), _Resposta(erro=_erro_http(500)))

    async_utils.run_geoapify_batch_geocoding_async(
        _enderecos(), "endereco", api_key=CHAVE, max_wait_time=0.02, poll_interval=0.01
    )

    assert "Error checking Geoapify batch status: ClientResponseError (HTTP 500)" in mensagens
    _assert_logs_limpos(mensagens, capsys)


# --- Wrappers run_*_async ----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "caso",
    [
        (
            "run_geocode_nominatim_async",
            "geocode_nominatim_async",
            {"user_agent": "ua", "domain": "d"},
            "Nominatim async geocoding failed: ClientResponseError (HTTP 401)",
        ),
        (
            "run_geocode_waze_async",
            "geocode_waze_async",
            {},
            "Waze async geocoding failed: ClientResponseError (HTTP 401)",
        ),
        (
            "run_geocode_maptiler_async",
            "geocode_maptiler_async",
            {"api_key": CHAVE},
            "MapTiler async geocoding failed: ClientResponseError (HTTP 401)",
        ),
        (
            "run_geoapify_batch_geocoding_async",
            "geocode_geoapify_batch_async",
            {"api_key": CHAVE},
            "Geoapify batch geocoding failed: ClientResponseError (HTTP 401)",
        ),
    ],
)
def test_wrappers_logam_so_o_tipo_e_o_status(monkeypatch, mensagens, capsys, caso):
    wrapper, funcao, argumentos, mensagem = caso

    async def _falha(*_args, **_kwargs):
        raise _erro_http(401)

    monkeypatch.setattr(async_utils, funcao, _falha)

    resultado = getattr(async_utils, wrapper)(_enderecos(), "endereco", **argumentos)

    assert mensagens == [mensagem]
    _assert_logs_limpos(mensagens, capsys)
    assert resultado["latitude"].isna().all()


# --- geo_utils (síncrono) ----------------------------------------------------------------------------------


def test_nominatim_sincrono_falha_nao_loga_endereco(monkeypatch, mensagens, capsys):
    class _NominatimFalso:
        def __init__(self, **_kwargs):
            pass

        def geocode(self, *_args, **_kwargs):
            raise GeocoderServiceError(f"Non-successful status code 403 for {URL_COM_CHAVE}")

    monkeypatch.setattr(geo_utils, "Nominatim", _NominatimFalso)
    monkeypatch.setattr(geo_utils, "getenv_or_action", lambda *_args, **_kwargs: "valor-sintetico")

    resultado = geo_utils.geocode_nominatim(_enderecos(), "endereco")

    assert "Nominatim: erro ao georreferenciar o endereço da linha 0: GeocoderServiceError" in mensagens
    _assert_logs_limpos(mensagens, capsys)
    assert resultado["latitude"].isna().all()


def test_geocodexyz_sincrono_falha_nao_loga_chave_nem_endereco_e_termina(monkeypatch, mensagens, capsys):
    chamadas = []

    def _get(url, **kwargs):
        chamadas.append((url, kwargs))
        raise _falha_de_conexao_requests(f"{url}&auth={CHAVE}")

    monkeypatch.setattr(geo_utils.requests, "get", _get)

    resultado = geo_utils.geocode_geocodexyz(_enderecos(), "endereco", api_key=CHAVE)

    assert mensagens[0] == "Geocode.xyz: erro ao processar o endereço da linha 0: ConnectionError"
    _assert_logs_limpos(mensagens, capsys)
    assert chamadas == [(f"https://geocode.xyz/{ENDERECO}?json=1", {"params": {"auth": CHAVE}, "timeout": 10})]
    assert (resultado["geocode"] == "geocode_xyz").all()


def test_geocodexyz_sincrono_quota_loga_o_status(monkeypatch, mensagens):
    resposta = requests.Response()
    resposta.status_code = 402
    monkeypatch.setattr(geo_utils.requests, "get", lambda *_args, **_kwargs: resposta)

    geo_utils.geocode_geocodexyz(_enderecos(), "endereco", api_key=CHAVE)

    assert "Geocode.xyz: erro ao processar o endereço da linha 0: HTTPError (HTTP 402)" in mensagens


@pytest.mark.usefixtures("mensagens")
def test_geocodexyz_sincrono_resultado_valido_continua_preenchendo_os_campos(monkeypatch):
    resposta = requests.Response()
    resposta.status_code = 200
    resposta._content = b'{"latt": "-22.9", "longt": "-43.2", "standard": {"addresst": "Rua Sintetica"}}'
    monkeypatch.setattr(geo_utils.requests, "get", lambda *_args, **_kwargs: resposta)

    resultado = geo_utils.geocode_geocodexyz(_enderecos(), "endereco", api_key=CHAVE)

    assert resultado.loc[0, "latitude"] == "-22.9"
    assert resultado.loc[0, "logradouro_geocode"] == "rua sintetica"


def test_maptiler_sincrono_falha_nao_loga_chave_nem_endereco(monkeypatch, mensagens, capsys):
    def _get(*_args, **_kwargs):
        raise _falha_de_conexao_requests()

    monkeypatch.setattr(geo_utils.requests, "get", _get)

    resultado = geo_utils.geocode_maptiler(_enderecos(), "endereco", api_key=CHAVE)

    assert "MapTiler: erro ao processar o endereço da linha 0: ConnectionError" in mensagens
    _assert_logs_limpos(mensagens, capsys)
    assert resultado["latitude"].isna().all()


def test_maptiler_sincrono_rate_limit_loga_o_status(monkeypatch, mensagens):
    resposta = requests.Response()
    resposta.status_code = 429
    monkeypatch.setattr(geo_utils.requests, "get", lambda *_args, **_kwargs: resposta)

    geo_utils.geocode_maptiler(_enderecos(), "endereco", api_key=CHAVE)

    assert "MapTiler: erro ao processar o endereço da linha 0: HTTPError (HTTP 429)" in mensagens
