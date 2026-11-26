# -*- coding: utf-8 -*-
"""As chamadas HTTP síncronas ao LocationIQ e ao OpenCage passam timeout e sobrevivem a ele."""

from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests

from pipelines.rj_crm__geolocalizacao_residencia.utils import geo_utils


def _resposta(payload, status_code=200):
    resposta = MagicMock()
    resposta.status_code = status_code
    resposta.json.return_value = payload
    return resposta


def _enderecos():
    return pd.DataFrame({"endereco": ["Rua Exemplo 1, Centro, Rio de Janeiro", "Rua Exemplo 2, Lapa, Rio de Janeiro"]})


@pytest.fixture(autouse=True)
def _sem_espera(monkeypatch):
    monkeypatch.setattr(geo_utils, "sleep", lambda *_: None)
    monkeypatch.setattr(geo_utils, "exponential_backoff_sleep", lambda *_args, **_kwargs: None)


def _sessao_falsa(monkeypatch, get):
    sessao = MagicMock()
    sessao.get.side_effect = get
    monkeypatch.setattr(geo_utils.requests, "Session", lambda: sessao)
    return sessao


def test_locationiq_passa_timeout_no_saldo_e_na_busca(monkeypatch):
    saldo = MagicMock(return_value=_resposta({"balance": {"day": 5}}))
    monkeypatch.setattr(geo_utils.requests, "get", saldo)
    sessao = _sessao_falsa(monkeypatch, lambda *_args, **_kwargs: _resposta([]))

    geo_utils.geocode_locationiq(_enderecos(), "endereco", api_key="chave-sintetica")

    assert saldo.call_count == 1
    assert saldo.call_args.kwargs["timeout"] == geo_utils.DEFAULT_TIMEOUT
    assert sessao.get.call_count == 2
    assert all(c.kwargs["timeout"] == geo_utils.DEFAULT_TIMEOUT for c in sessao.get.call_args_list)


def test_locationiq_timeout_no_saldo_devolve_dataframe_sem_geocodificar(monkeypatch):
    monkeypatch.setattr(geo_utils.requests, "get", MagicMock(side_effect=requests.exceptions.Timeout("lento")))
    sessao = _sessao_falsa(monkeypatch, lambda *_args, **_kwargs: _resposta([]))

    resultado = geo_utils.geocode_locationiq(_enderecos(), "endereco", api_key="chave-sintetica")

    assert sessao.get.call_count == 0
    assert len(resultado) == 2
    assert resultado["latitude"].isna().all()


def test_locationiq_timeout_numa_busca_segue_para_o_proximo_endereco(monkeypatch):
    monkeypatch.setattr(geo_utils.requests, "get", MagicMock(return_value=_resposta({"balance": {"day": 5}})))
    respostas = iter([requests.exceptions.Timeout("lento"), _resposta([])])

    def _get(*_args, **_kwargs):
        resposta = next(respostas)
        if isinstance(resposta, Exception):
            raise resposta
        return resposta

    sessao = _sessao_falsa(monkeypatch, _get)

    resultado = geo_utils.geocode_locationiq(_enderecos(), "endereco", api_key="chave-sintetica")

    assert sessao.get.call_count == 2
    assert (resultado["geocode"] == "locationiq").all()


def test_opencage_passa_timeout_em_cada_busca(monkeypatch):
    get = MagicMock(return_value=_resposta({"results": []}))
    monkeypatch.setattr(geo_utils.requests, "get", get)
    monkeypatch.setattr(geo_utils, "log", lambda *_args, **_kwargs: None)

    geo_utils.geocode_opencage(_enderecos(), "endereco", api_key="chave-sintetica")

    assert get.call_count == 2
    assert all(c.kwargs["timeout"] == geo_utils.DEFAULT_TIMEOUT for c in get.call_args_list)


def test_opencage_timeout_numa_busca_segue_para_o_proximo_endereco(monkeypatch):
    get = MagicMock(side_effect=[requests.exceptions.Timeout("lento"), _resposta({"results": []})])
    monkeypatch.setattr(geo_utils.requests, "get", get)
    monkeypatch.setattr(geo_utils, "log", lambda *_args, **_kwargs: None)

    resultado = geo_utils.geocode_opencage(_enderecos(), "endereco", api_key="chave-sintetica")

    assert get.call_count == 2
    assert len(resultado) == 2
    assert resultado["latitude"].isna().all()
    assert (resultado["geocode"] == "opencage").all()
