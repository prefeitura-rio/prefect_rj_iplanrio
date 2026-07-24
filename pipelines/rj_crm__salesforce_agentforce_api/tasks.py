# -*- coding: utf-8 -*-
"""
Tasks para ingestão de dados via Salesforce CRM Bulk API 2.0.

Diferente da pipeline rj_crm__salesforce_api (que acessa a SFMC REST API),
esta pipeline acessa a org do Salesforce CRM (Sales/Service Cloud) onde
ficam os dados do Agentforce: sessões de agente, mensagens, conversas, etc.

Autenticação via OAuth2 Username-Password Flow.
Credenciais necessárias no Infisical (path configurável no prefect.yaml):
  - SF_CRM_CLIENT_ID      : Client ID da Connected App no Salesforce CRM
  - SF_CRM_CLIENT_SECRET  : Client Secret da Connected App no Salesforce CRM
  - SF_CRM_USERNAME       : Username do usuário de integração
  - SF_CRM_PASSWORD       : Senha + Security Token concatenados (ex: SenhaXXXTOKENYYY)
  - SF_CRM_LOGIN_URL      : URL de login (ex: https://login.salesforce.com)
                            Para sandbox: https://test.salesforce.com

Fluxo da Bulk API 2.0:
  1. POST /jobs/query  → cria job assíncrono com SOQL
  2. GET  /jobs/query/:id → polling até status = "JobComplete"
  3. GET  /jobs/query/:id/results → baixa CSV com todos os registros
"""

import csv
import io
import json
import time
from datetime import date
from typing import Any

import pandas as pd
import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task


# ---------------------------------------------------------------------------
# Credenciais e autenticação
# ---------------------------------------------------------------------------

def _get_crm_credentials() -> dict[str, str]:
    """
    Lê as credenciais do Salesforce CRM a partir das variáveis de ambiente
    injetadas pelo Infisical.

    Returns:
        dict com as chaves: client_id, client_secret, username, password, login_url
    """
    return {
        "client_id": getenv_or_action("SF_CRM_CLIENT_ID"),
        "client_secret": getenv_or_action("SF_CRM_CLIENT_SECRET"),
        "username": getenv_or_action("SF_CRM_USERNAME"),
        "password": getenv_or_action("SF_CRM_PASSWORD"),
        "login_url": getenv_or_action("SF_CRM_LOGIN_URL").rstrip("/"),
    }


def _get_access_token(creds: dict[str, str]) -> tuple[str, str]:
    """
    Obtém um access token OAuth2 via Username-Password Flow.

    Args:
        creds: Dicionário com credenciais do Salesforce CRM.

    Returns:
        Tupla (access_token, instance_url).
        instance_url é a URL base da org (ex: https://suaorg.my.salesforce.com).
    """
    token_url = f"{creds['login_url']}/services/oauth2/token"
    payload = {
        "grant_type": "password",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "username": creds["username"],
        "password": creds["password"],
    }
    response = requests.post(token_url, data=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    access_token = data["access_token"]
    instance_url = data["instance_url"].rstrip("/")
    print(f"[SF_CRM] Autenticado. Instance URL: {instance_url}")
    return access_token, instance_url


# ---------------------------------------------------------------------------
# Bulk API 2.0 — Query assíncrona
# ---------------------------------------------------------------------------

def _create_bulk_query_job(
    instance_url: str,
    access_token: str,
    soql: str,
    api_version: str = "v59.0",
) -> str:
    """
    Cria um job de query assíncrono na Bulk API 2.0.

    Args:
        instance_url: URL base da org Salesforce.
        access_token: Token de acesso OAuth2.
        soql: Query SOQL a executar.
        api_version: Versão da API Salesforce.

    Returns:
        ID do job criado.
    """
    url = f"{instance_url}/services/data/{api_version}/jobs/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "operation": "query",
        "query": soql,
    }
    print(f"[SF_CRM][BULK] Criando job de query...")
    print(f"[SF_CRM][BULK] SOQL: {soql}")
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()
    job_id = data["id"]
    print(f"[SF_CRM][BULK] Job criado: {job_id} | Status inicial: {data.get('state')}")
    return job_id


def _wait_for_job(
    instance_url: str,
    access_token: str,
    job_id: str,
    api_version: str = "v59.0",
    poll_interval_seconds: int = 5,
    max_wait_seconds: int = 600,
) -> dict:
    """
    Faz polling do status do job até que esteja completo ou com falha.

    Args:
        instance_url: URL base da org Salesforce.
        access_token: Token de acesso OAuth2.
        job_id: ID do job de query.
        api_version: Versão da API Salesforce.
        poll_interval_seconds: Intervalo entre verificações de status.
        max_wait_seconds: Tempo máximo de espera antes de lançar erro.

    Returns:
        Dicionário com o status final do job.

    Raises:
        RuntimeError: Se o job falhar ou exceder o tempo máximo.
    """
    url = f"{instance_url}/services/data/{api_version}/jobs/query/{job_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    elapsed = 0

    if poll_interval_seconds <= 0:
        raise ValueError(
            f"poll_interval_seconds deve ser positivo, recebido: {poll_interval_seconds}"
        )

    while elapsed < max_wait_seconds:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        data = response.json()
        state = data.get("state")
        records_processed = data.get("numberRecordsProcessed", "?")
        print(f"[SF_CRM][BULK] Job {job_id} | Estado: {state} | Registros processados: {records_processed}")

        if state == "JobComplete":
            print(f"[SF_CRM][BULK] Job concluído com {records_processed} registros.")
            return data
        elif state in ("Failed", "Aborted"):
            error_msg = data.get("errorMessage", "sem detalhe")
            raise RuntimeError(f"[SF_CRM][BULK] Job {job_id} falhou com estado '{state}': {error_msg}")

        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

    raise RuntimeError(
        f"[SF_CRM][BULK] Job {job_id} não concluiu em {max_wait_seconds}s. "
        "Aumente max_wait_seconds ou verifique o volume de dados."
    )


def _download_job_results(
    instance_url: str,
    access_token: str,
    job_id: str,
    api_version: str = "v59.0",
) -> list[dict]:
    """
    Baixa os resultados do job como CSV e converte para lista de dicionários.
    Lida automaticamente com paginação via header Sforce-Locator.

    Args:
        instance_url: URL base da org Salesforce.
        access_token: Token de acesso OAuth2.
        job_id: ID do job de query concluído.
        api_version: Versão da API Salesforce.

    Returns:
        Lista de dicionários com todos os registros retornados pelo job.
    """
    url = f"{instance_url}/services/data/{api_version}/jobs/query/{job_id}/results"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "text/csv",
    }
    all_records = []
    locator = None
    page = 1

    while True:
        params = {}
        if locator:
            params["locator"] = locator

        print(f"[SF_CRM][BULK] Baixando resultados — página {page}...")
        response = requests.get(url, headers=headers, params=params, timeout=120)
        response.raise_for_status()

        # Parsear CSV
        content = response.text
        reader = csv.DictReader(io.StringIO(content))
        records = list(reader)
        all_records.extend(records)
        print(f"[SF_CRM][BULK] Página {page}: {len(records)} registros. Total acumulado: {len(all_records)}")
        if records:
            print(f"[SF_CRM][BULK] Campos presentes: {list(records[0].keys())}")

        # Verificar se há próxima página
        locator = response.headers.get("Sforce-Locator")
        if not locator or locator == "null":
            break
        page += 1

    print(f"[SF_CRM][BULK] Download concluído. Total: {len(all_records)} registros.")
    return all_records


# ---------------------------------------------------------------------------
# Task principal
# ---------------------------------------------------------------------------

@task(log_prints=True)
def fetch_crm_bulk_query(
    soql: str,
    api_version: str = "v59.0",
    poll_interval_seconds: int = 5,
    max_wait_seconds: int = 600,
) -> list[dict]:
    """
    Executa uma query SOQL via Salesforce CRM Bulk API 2.0 e retorna todos
    os registros como lista de dicionários.

    Fluxo:
      1. Autentica via OAuth2 Username-Password
      2. Cria job de query assíncrono
      3. Aguarda conclusão (polling)
      4. Baixa resultados em CSV (com paginação automática)

    Args:
        soql: Query SOQL a executar.
              Ex: "SELECT Id, StartTime, EndTime FROM BotSession WHERE StartTime = YESTERDAY"
        api_version: Versão da Salesforce API. Padrão: "v59.0".
        poll_interval_seconds: Intervalo de polling do status do job. Padrão: 5s.
        max_wait_seconds: Timeout máximo para conclusão do job. Padrão: 600s.

    Returns:
        Lista de dicionários com todos os registros.
    """
    creds = _get_crm_credentials()
    access_token, instance_url = _get_access_token(creds)

    job_id = _create_bulk_query_job(
        instance_url=instance_url,
        access_token=access_token,
        soql=soql,
        api_version=api_version,
    )

    _wait_for_job(
        instance_url=instance_url,
        access_token=access_token,
        job_id=job_id,
        api_version=api_version,
        poll_interval_seconds=poll_interval_seconds,
        max_wait_seconds=max_wait_seconds,
    )

    records = _download_job_results(
        instance_url=instance_url,
        access_token=access_token,
        job_id=job_id,
        api_version=api_version,
    )

    print(f"[SF_CRM] Total de registros coletados: {len(records)}")
    return records


@task(log_prints=True)
def list_available_objects(api_version: str = "v59.0") -> list[str]:
    """
    Lista todos os objetos disponíveis na org Salesforce CRM.
    Filtra por objetos relacionados a Agentforce/Bot/Conversation.

    Útil para descobrir quais objetos estão disponíveis antes de construir
    as queries SOQL.

    Args:
        api_version: Versão da Salesforce API. Padrão: "v59.0".

    Returns:
        Lista de nomes de objetos encontrados na org.
    """
    creds = _get_crm_credentials()
    access_token, instance_url = _get_access_token(creds)

    url = f"{instance_url}/services/data/{api_version}/sobjects/"
    headers = {"Authorization": f"Bearer {access_token}"}

    print(f"[SF_CRM] Listando objetos disponíveis na org...")
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()

    all_objects = [o["name"] for o in data.get("sobjects", [])]

    # Filtrar objetos relevantes para Agentforce
    keywords = ["Bot", "Agent", "Conversation", "Messaging", "Chat", "Einstein"]
    relevant = [name for name in all_objects if any(kw.lower() in name.lower() for kw in keywords)]

    print(f"[SF_CRM] Total de objetos na org: {len(all_objects)}")
    print(f"[SF_CRM] Objetos relevantes (Bot/Agent/Conversation/Messaging/Chat/Einstein): {relevant}")
    return relevant


# ---------------------------------------------------------------------------
# Task de construção do DataFrame
# ---------------------------------------------------------------------------

@task(log_prints=True)
def build_dataframe(
    records: list[dict],
    partition_date: date | None = None,
) -> pd.DataFrame:
    """
    Constrói um DataFrame a partir dos registros retornados pela Bulk API.

    Cada linha contém:
      - data          : JSON string com o conteúdo completo do registro
      - data_particao : data de particionamento (hoje se não informada)

    Args:
        records: Lista de dicionários retornados por fetch_crm_bulk_query.
        partition_date: Data de particionamento. Padrão: data de hoje.

    Returns:
        DataFrame com colunas 'data' e 'data_particao'.
    """
    if partition_date is None:
        partition_date = date.today()

    rows = [
        {
            "data": json.dumps(record, ensure_ascii=False),
            "data_particao": str(partition_date),
        }
        for record in records
    ]

    df = pd.DataFrame(rows, columns=["data", "data_particao"])
    print(f"[SF_CRM] DataFrame criado com {len(df)} linhas e colunas: {list(df.columns)}")
    return df
