# -*- coding: utf-8 -*-
"""Os logs das tasks da geocodificação não trazem a chave da API nem valores das linhas do cidadão."""

import pandas as pd
import pytest

from pipelines.rj_crm__geolocalizacao_residencia import tasks

CHAVE = "zq9Xk-chave-sintetica-123"
PREFIXO_CHAVE = CHAVE[:5]
ENDERECO = "Rua Sintetica 123, Bairro Ficticio, Rio de Janeiro"
LOGRADOURO = "Rua Sintetica"
LATITUDE = "-22.9123456"
LONGITUDE = "-43.1987654"
URL_COM_CHAVE = f"https://api.exemplo/v1/search?key={CHAVE}&q={ENDERECO}"
VALORES_SENSIVEIS = (CHAVE, PREFIXO_CHAVE, ENDERECO, LOGRADOURO, LATITUDE, LONGITUDE)


@pytest.fixture
def logs(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Guarda tudo o que as tasks mandam ao log e devolve a chave sintética no lugar do segredo."""
    mensagens: list[str] = []
    monkeypatch.setattr(tasks, "log", lambda msg, *_args, **_kwargs: mensagens.append(str(msg)))
    monkeypatch.setattr(tasks, "getenv_or_action", lambda *_args, **_kwargs: CHAVE)
    return mensagens


def _enderecos(coluna: str = "address") -> pd.DataFrame:
    return pd.DataFrame({coluna: [ENDERECO], "logradouro_tratado": [LOGRADOURO]})


def _geocodificado(dataframe: pd.DataFrame) -> pd.DataFrame:
    resultado = dataframe.copy()
    resultado["latitude"] = LATITUDE
    resultado["longitude"] = LONGITUDE
    resultado["logradouro_geocode"] = LOGRADOURO
    return resultado


def _sem_resultado(dataframe: pd.DataFrame) -> pd.DataFrame:
    resultado = dataframe.copy()
    resultado["latitude"] = None
    resultado["longitude"] = None
    return resultado


def _assert_sem_dados_sensiveis(mensagens: list[str]) -> None:
    assert mensagens, "a task deveria ter logado algo"
    for mensagem in mensagens:
        for valor in VALORES_SENSIVEIS:
            assert valor not in mensagem, f"log com dado sensível: {mensagem!r}"


def test_fallback_nao_loga_chave_nem_linha_nem_mensagem_do_erro(
    logs: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    chaves_recebidas: list[str] = []

    def waze(dataframe, **_kwargs):
        return _sem_resultado(dataframe)

    def maptiler(api_key, **_kwargs):
        chaves_recebidas.append(api_key)
        raise RuntimeError(f"402 quota exceeded for {URL_COM_CHAVE}")

    def geocodexyz(dataframe, api_key, **_kwargs):
        chaves_recebidas.append(api_key)
        return _geocodificado(dataframe)

    def nao_chamado(*_args, **_kwargs):
        raise AssertionError("sem endereços pendentes, o geocodificador não deveria ser chamado")

    monkeypatch.setattr(tasks, "run_geocode_waze_async", waze)
    monkeypatch.setattr(tasks, "run_geocode_maptiler_async", maptiler)
    monkeypatch.setattr(tasks, "run_geocode_geocodexyz_async", geocodexyz)
    monkeypatch.setattr(tasks, "run_geocode_locationiq_async", nao_chamado)
    monkeypatch.setattr(tasks, "run_geocode_opencage_async", nao_chamado)

    resultado = tasks.async_geocoding_dataframe_with_fallback.fn(_enderecos())

    assert len(resultado) == 1
    assert chaves_recebidas == [CHAVE, CHAVE], "a chave continua chegando aos geocodificadores"
    _assert_sem_dados_sensiveis(logs)
    assert any("MapTiler" in m and "RuntimeError" in m for m in logs), "o erro deveria citar provedor e tipo"


def test_fallback_mantem_deteccao_de_quota_sem_logar_a_mensagem(
    logs: list[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    chamadas: list[str] = []

    def sem_resultado(dataframe, **_kwargs):
        return _sem_resultado(dataframe)

    def quota(*_args, **_kwargs):
        chamadas.append("maptiler")
        raise RuntimeError(f"402 quota exceeded for {URL_COM_CHAVE}")

    def sucesso(dataframe, **_kwargs):
        chamadas.append("geocodexyz")
        return _geocodificado(dataframe)

    monkeypatch.setattr(tasks, "run_geocode_waze_async", sem_resultado)
    monkeypatch.setattr(tasks, "run_geocode_maptiler_async", quota)
    monkeypatch.setattr(tasks, "run_geocode_geocodexyz_async", sucesso)

    tasks.async_geocoding_dataframe_with_fallback.fn(_enderecos())

    assert chamadas == ["maptiler", "geocodexyz"]
    _assert_sem_dados_sensiveis(logs)


def test_geoapify_nao_loga_prefixo_do_token_nem_linha(logs: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    chaves_recebidas: list[str] = []

    def geoapify(dataframe, api_key, **_kwargs):
        chaves_recebidas.append(api_key)
        return _geocodificado(dataframe)

    monkeypatch.setattr(tasks, "run_geoapify_batch_geocoding_async", geoapify)

    resultado = tasks.geoapify_batch_geocoding_task.fn(_enderecos("endereco_completo"))

    assert len(resultado) == 1
    assert chaves_recebidas == [CHAVE]
    _assert_sem_dados_sensiveis(logs)


def test_geoapify_sem_token_continua_avisando(logs: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "getenv_or_action", lambda *_args, **_kwargs: "")

    resultado = tasks.geoapify_batch_geocoding_task.fn(_enderecos("endereco_completo"))

    assert resultado.empty
    assert any("No Geoapify API token found" in m for m in logs)
    _assert_sem_dados_sensiveis(logs)


def test_nominatim_nao_loga_linha(logs: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tasks, "run_geocode_nominatim_async", lambda dataframe, **_kwargs: _geocodificado(dataframe))

    resultado = tasks.async_geocoding_dataframe.fn(_enderecos())

    assert len(resultado) == 1
    _assert_sem_dados_sensiveis(logs)


def test_merge_nao_loga_linha(logs: list[str]) -> None:
    combinado = tasks.merge_and_deduplicate_geocoding_results.fn(
        [_geocodificado(_enderecos()).assign(numero_porta="1", bairro="Ficticio"), pd.DataFrame()]
    )

    assert len(combinado) == 1
    _assert_sem_dados_sensiveis(logs)


def test_dataframe_to_file_nao_loga_o_dataframe(logs: list[str], tmp_path) -> None:
    pasta = tasks.dataframe_to_file.fn(_geocodificado(_enderecos()), file_folder=str(tmp_path), file_format="csv")

    assert pasta == tmp_path
    assert len(list(tmp_path.glob("*.csv"))) == 1
    _assert_sem_dados_sensiveis(logs)
