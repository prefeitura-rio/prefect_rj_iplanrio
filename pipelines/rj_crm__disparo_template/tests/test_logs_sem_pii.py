# -*- coding: utf-8 -*-
"""Os logs do disparo SF não trazem CPF, telefone, nome nem os demais dados do cidadão.

Todos os dados abaixo são sintéticos.
"""

import json
import logging

import pandas as pd
import pytest
import requests

from pipelines.rj_crm__disparo_template import flow
from pipelines.rj_crm__disparo_template.utils import dispatch, validators, whitelist
from pipelines.rj_crm__disparo_template.utils.enrichers import consulta_debitos

CAMPANHA = "campanha_sintetica"
CPF_1 = "00011122233"
CPF_2 = "00044455566"
CPF_JA_DISPARADO = "00077788899"
TELEFONE_1 = "5521900000001"
TELEFONE_2 = "5521900000002"
TELEFONE_RETENTATIVA = "5521900000003"
TELEFONE_JA_DISPARADO = "5521900000009"
NOME = "Fulana Sintetica"
ENDERECO = "Rua Ficticia 123"
DIVIDA = "R$ 9.876,54"

DADOS_PESSOAIS = (
    CPF_1,
    CPF_2,
    CPF_JA_DISPARADO,
    TELEFONE_1,
    TELEFONE_2,
    TELEFONE_RETENTATIVA,
    TELEFONE_JA_DISPARADO,
    # Prefixos que uma máscara parcial deixaria passar.
    CPF_1[:4],
    TELEFONE_1[:8],
    NOME,
    "Fulana",
    ENDERECO,
    DIVIDA,
)


def _sem_dado_pessoal(texto: str) -> list[str]:
    return [dado for dado in DADOS_PESSOAIS if dado in texto]


@pytest.fixture
def registros(monkeypatch, caplog):
    """Junta o que vai para o log: stdout (log_prints), logging e o `log` do iplanrio."""
    mensagens_log: list[str] = []

    def _log(msg, *_args, **_kwargs):
        mensagens_log.append(str(msg))

    for modulo in (dispatch, validators, consulta_debitos):
        monkeypatch.setattr(modulo, "log", _log)
    caplog.set_level(logging.DEBUG)

    def _texto(capsys) -> str:
        saida = capsys.readouterr()
        logs = "\n".join(record.getMessage() for record in caplog.records)
        return "\n".join([saida.out, saida.err, logs, *mensagens_log])

    return _texto


def _destinos() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "cpf": [CPF_1, CPF_2],
            "telefone": [TELEFONE_1, TELEFONE_2],
            "nome": [NOME, NOME],
            "endereco": [ENDERECO, ENDERECO],
            "valor_divida": [DIVIDA, DIVIDA],
            "others": [[TELEFONE_RETENTATIVA], []],
        }
    )


def _ja_disparados() -> pd.DataFrame:
    hoje = pd.Timestamp.now(tz="America/Sao_Paulo").date()
    return pd.DataFrame(
        {
            "cpf": [CPF_JA_DISPARADO],
            "telefone": [TELEFONE_JA_DISPARADO],
            "status": ["enviado"],
            "nome_campanha": [CAMPANHA],
            "data_particao": [str(hoje)],
        }
    )


@pytest.fixture
def flow_sem_rede(monkeypatch, tmp_path):
    """Troca BigQuery, SFTP, Prefect e esperas por substitutos; a lógica de filtro é a real."""
    envios: list[pd.DataFrame] = []

    def _nao_deve_encerrar(*_args, **_kwargs):
        raise AssertionError("o flow não deveria encerrar sem destinatários")

    def _salva_csv(df, **_kwargs):
        envios.append(df.copy())
        return str(tmp_path / "disparo.csv"), "2026-11-27 10:00:00"

    def _log_df(df, dispatch_date, campaign_name):
        return pd.DataFrame({"dispatch_date": [dispatch_date] * len(df), "nome_campanha": campaign_name})

    substituicoes = {
        "rename_current_flow_run_task": lambda **_kwargs: None,
        "inject_bd_credentials_task": lambda **_kwargs: None,
        "check_flow_status": lambda **_kwargs: "ativo",
        "validate_campaign_name": lambda **_kwargs: CAMPANHA,
        "task_download_data_from_bigquery": lambda **_kwargs: _destinos(),
        "send_dispatch_no_destinations_found": _nao_deve_encerrar,
        "filter_duplicated": dispatch.filter_duplicated.fn,
        "filter_already_dispatched_phones_or_cpfs": dispatch.filter_already_dispatched_phones_or_cpfs.fn,
        "get_failed_phones": lambda **_kwargs: {TELEFONE_2},
        "get_failed_cpfs": lambda **_kwargs: {CPF_1},
        "get_already_dispatched_data": lambda **_kwargs: _ja_disparados(),
        "save_csv_for_sftp": _salva_csv,
        "send_to_sftp": lambda **_kwargs: None,
        "create_log_df": _log_df,
        "create_date_partitions": lambda **_kwargs: str(tmp_path),
        "create_table_and_upload_to_gcs_task": lambda **_kwargs: None,
    }
    for nome, substituto in substituicoes.items():
        monkeypatch.setattr(flow, nome, substituto)
    monkeypatch.setattr(flow.time, "sleep", lambda *_args: None)
    return envios


def _executa_flow(**params) -> None:
    flow.rj_crm__disparo_template_sf.fn(
        campaign_name=CAMPANHA,
        query="SELECT 1",
        filter_failed_phones=True,
        max_dispatch_retries=1,
        sleep_minutes=0,
        monitor_after_sftp=False,
        materialize_after_sftp=False,
        **params,
    )


def test_flow_com_debug_padrao_nao_loga_dado_pessoal(flow_sem_rede, registros, capsys):
    _executa_flow()

    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    # O fluxo foi até o fim: disparo inicial e retentativa com o telefone de 'others'.
    assert [df["telefone"].tolist() for df in flow_sem_rede] == [[TELEFONE_1], [TELEFONE_RETENTATIVA]]
    # Com debug=True (padrão) o log continua dizendo o tamanho e as colunas de cada etapa.
    assert "Após filtrar CPFs duplicados: 2 linha(s), colunas=['cpf', 'telefone', 'nome'" in texto
    assert "Já disparados da mesma campanha: 1 linha(s)" in texto
    assert "Destinatários disparados: 1 linha(s)" in texto


def test_flow_sem_debug_nao_loga_dado_pessoal(flow_sem_rede, registros, capsys):
    _executa_flow(debug=False)

    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    assert "Após filtrar CPFs duplicados" not in texto
    assert len(flow_sem_rede) == 2


def test_describe_dataframe_mostra_so_tamanho_e_colunas():
    resumo = dispatch.describe_dataframe(_destinos())

    assert resumo == "2 linha(s), colunas=['cpf', 'telefone', 'nome', 'endereco', 'valor_divida', 'others']"
    assert _sem_dado_pessoal(resumo) == []
    assert dispatch.describe_dataframe(None) == "0 linha(s), colunas=[]"


def test_get_failed_cpfs_nao_loga_os_cpfs(monkeypatch, registros, capsys):
    monkeypatch.setattr(
        dispatch, "download_data_from_bigquery", lambda **_kwargs: pd.DataFrame({"cpf": [CPF_1, CPF_2]})
    )

    resultado = dispatch.get_failed_cpfs.fn(billing_project_id="projeto", campaign_name=CAMPANHA)

    assert resultado == {CPF_1, CPF_2}
    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    assert "CPFs com falha detectados para retentativa: 2" in texto


def test_get_failed_phones_nao_loga_os_telefones(monkeypatch, registros, capsys):
    monkeypatch.setattr(
        dispatch, "download_data_from_bigquery", lambda **_kwargs: pd.DataFrame({"telefone": [TELEFONE_1]})
    )

    resultado = dispatch.get_failed_phones.fn(billing_project_id="projeto")

    assert resultado == {TELEFONE_1}
    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    assert "Telefones com falha detectados: 1" in texto


@pytest.mark.parametrize("tarefa", ["add", "remove"])
def test_whitelist_com_destino_invalido_nao_loga_o_destino(tarefa, registros, capsys):
    destinos = [f"{NOME};{TELEFONE_1};{CPF_1}"]  # não é dict: .get falha

    if tarefa == "add":
        dispatch.add_contacts_to_whitelist.fn(
            destinations=destinos, percentage_to_insert=50, group_name="grupo", environment="staging"
        )
    else:
        dispatch.remove_contacts_from_whitelist.fn(destinations=destinos, environment="staging")

    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    assert "AttributeError" in texto


class _WhitelistFalsa:
    """API da whitelist sem rede: aceita a autenticação, o grupo e as operações."""

    def __init__(self, *_args, **_kwargs):
        self.adicionados: list[str] = []
        self.removidos: list[str] = []

    def authenticate(self) -> bool:
        return True

    def find_group_by_name(self, group_name):
        return {"id": 7, "name": group_name}

    def get_existing_numbers_set(self, force_add_on_whitelist_group):
        return set()

    def add_numbers_to_group(self, group_id, phone_numbers):
        self.adicionados.extend(phone_numbers)
        return True

    def remove_numbers_bulk(self, phone_numbers):
        self.removidos.extend(phone_numbers)
        return True


SEGREDO = "segredo-sintetico-da-whitelist"


@pytest.fixture
def whitelist_falsa(monkeypatch):
    instancias: list[_WhitelistFalsa] = []

    def _fabrica(*args, **kwargs):
        instancia = _WhitelistFalsa(*args, **kwargs)
        instancias.append(instancia)
        return instancia

    config = {"issuer": "https://exemplo.invalid", "client_id": "cliente", "client_secret": SEGREDO, "api_base_url": "https://exemplo.invalid/api"}
    monkeypatch.setattr(dispatch, "get_environment_config", lambda _env: config)
    monkeypatch.setattr(dispatch, "validate_environment_config", lambda _config: None)
    monkeypatch.setattr(dispatch, "BetaGroupManager", _fabrica)
    return instancias


def test_whitelist_add_com_telefones_validos_nao_loga_telefone_nem_segredo(whitelist_falsa, registros, capsys):
    destinos = [{"telefone": TELEFONE_1}, {"telefone": TELEFONE_2}]

    dispatch.add_contacts_to_whitelist.fn(
        destinations=destinos, percentage_to_insert=100, group_name="grupo", environment="staging"
    )

    assert whitelist_falsa[0].adicionados  # chegou à API
    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    assert SEGREDO not in texto
    assert "Telefones a adicionar na whitelist: 2" in texto


def test_whitelist_remove_com_telefones_validos_nao_loga_telefone_nem_segredo(whitelist_falsa, registros, capsys):
    dispatch.remove_contacts_from_whitelist.fn(
        destinations=[{"telefone": TELEFONE_1}, {"telefone": TELEFONE_2}], environment="staging"
    )

    assert whitelist_falsa[0].removidos  # chegou à API
    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    assert SEGREDO not in texto
    assert "Telefones a remover da whitelist: 2" in texto


class _Resposta:
    def __init__(self, status_code: int = 200, texto: str = ""):
        self.status_code = status_code
        self.text = texto

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} para {TELEFONE_1}", response=self)

    def json(self) -> dict:
        return {"access_token": "token-sintetico"}


def test_beta_group_manager_nao_loga_segredo_nem_telefone(monkeypatch, registros, capsys):
    gerente = whitelist.BetaGroupManager("https://exemplo.invalid", "cliente", SEGREDO, "https://exemplo.invalid/api")
    monkeypatch.setattr(whitelist.requests, "post", lambda *_args, **_kwargs: _Resposta())
    assert gerente.authenticate() is True

    monkeypatch.setattr(whitelist.requests, "delete", lambda *_args, **_kwargs: _Resposta())
    assert gerente.remove_number_from_whitelist(TELEFONE_1) is True
    monkeypatch.setattr(whitelist.requests, "delete", lambda *_args, **_kwargs: _Resposta(500))
    assert gerente.remove_number_from_whitelist(TELEFONE_1) is False

    eco = _Resposta(400, texto=f'{{"phone_numbers": ["{TELEFONE_1}"]}}')
    monkeypatch.setattr(whitelist.requests, "post", lambda *_args, **_kwargs: eco)
    assert gerente.add_numbers_to_group(7, [TELEFONE_1]) is False
    assert gerente.remove_numbers_bulk([TELEFONE_1]) is False

    texto = registros(capsys)
    assert SEGREDO not in texto
    assert _sem_dado_pessoal(texto) == []
    assert "Resposta da API da whitelist: HTTP 400" in texto


def test_get_destinations_loga_so_as_chaves(registros, capsys):
    destinos = [{"celular_disparo": TELEFONE_1, "cpf": CPF_1, "vars": {"nome": NOME, "divida": DIVIDA}}]

    resultado = dispatch.get_destinations.fn(destinations=json.dumps(destinos), query=None)

    assert resultado[0]["telefone"] == TELEFONE_1
    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    assert "['celular_disparo', 'cpf', 'vars']" in texto


def test_destino_invalido_nao_loga_cpf_nem_telefone_parciais(registros, capsys):
    destinos = [{"telefone": TELEFONE_1, "cpf": " "}, {"telefone": TELEFONE_2, "cpf": CPF_2}]

    resultado = dispatch.get_destinations.fn(destinations=json.dumps(destinos), query=None)

    assert [destino["cpf"] for destino in resultado] == [CPF_2]
    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    assert "Destinatário inválido - Registro 1: cpf:" in texto


@pytest.mark.parametrize("tarefa", ["remove_duplicate_phones", "remove_duplicate_cpfs"])
def test_remocao_de_duplicados_nao_loga_cpf_nem_telefone(tarefa, registros, capsys):
    destinos = [{"telefone": TELEFONE_1, "cpf": CPF_1}, {"telefone": TELEFONE_1, "cpf": CPF_1}]

    resultado = getattr(dispatch, tarefa).fn(destinos)

    assert len(resultado) == 1
    assert _sem_dado_pessoal(registros(capsys)) == []


def test_consulta_debitos_falha_nao_loga_o_cpf(monkeypatch, registros, capsys):
    def _falha(*_args, **_kwargs):
        raise requests.exceptions.ConnectionError(f"falha sintetica ao consultar {CPF_1}")

    monkeypatch.setattr(consulta_debitos.requests, "post", _falha)
    monkeypatch.setattr(consulta_debitos.time, "sleep", lambda *_args: None)

    resultado = consulta_debitos.consultar_debitos(CPF_1, api_url="https://exemplo.invalid/api", max_retries=2)

    assert resultado is None
    texto = registros(capsys)
    assert _sem_dado_pessoal(texto) == []
    assert "consulta_debitos falhou (tentativa 1/2): ConnectionError" in texto
