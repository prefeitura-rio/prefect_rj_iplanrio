# -*- coding: utf-8 -*-
"""
Extração de objetos padrão do Salesforce CRM via Bulk API 2.0 assíncrona.

Usado por:
  - Fase 2a: MessagingSession (objeto padrão CRM)
  - Qualquer objeto CRM padrão (BotSession, BotMessage, etc.)

Fluxo:
  1. POST /jobs/query  → cria job assíncrono com SOQL
  2. GET  /jobs/query/:id → polling até 'JobComplete'
  3. GET  /jobs/query/:id/results → download CSV paginado
"""

from __future__ import annotations

import csv
import io
import time

import pandas as pd
import requests
from prefect import task


# ---------------------------------------------------------------------------
# Funções internas (não são tasks — reutilizadas pelo template)
# ---------------------------------------------------------------------------


def _create_bulk_job(
    instance_url: str,
    access_token: str,
    soql: str,
    api_version: str = "v59.0",
) -> str:
    url = f"{instance_url}/services/data/{api_version}/jobs/query"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {"operation": "query", "query": soql}
    print(f"[BULK] Criando job | SOQL: {soql[:120]}...")
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    job_id = data["id"]
    print(f"[BULK] Job criado: {job_id} | estado inicial: {data.get('state')}")
    return job_id


def _wait_for_bulk_job(
    instance_url: str,
    access_token: str,
    job_id: str,
    api_version: str = "v59.0",
    poll_interval_seconds: int = 10,
    max_wait_seconds: int = 900,
) -> dict:
    url = f"{instance_url}/services/data/{api_version}/jobs/query/{job_id}"
    headers = {"Authorization": f"Bearer {access_token}"}
    elapsed = 0

    while elapsed < max_wait_seconds:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        state = data.get("state")
        processed = data.get("numberRecordsProcessed", "?")
        print(f"[BULK] Job {job_id} | estado: {state} | processados: {processed}")

        if state == "JobComplete":
            print(f"[BULK] Concluido com {processed} registros.")
            return data
        if state in ("Failed", "Aborted"):
            raise RuntimeError(
                f"[BULK] Job {job_id} falhou: estado='{state}', "
                f"erro='{data.get('errorMessage', 'sem detalhe')}'"
            )

        time.sleep(poll_interval_seconds)
        elapsed += poll_interval_seconds

    raise RuntimeError(
        f"[BULK] Job {job_id} nao concluiu em {max_wait_seconds}s. "
        "Aumente max_wait_seconds ou verifique o volume de dados."
    )


def _download_bulk_results(
    instance_url: str,
    access_token: str,
    job_id: str,
    api_version: str = "v59.0",
) -> pd.DataFrame:
    """Baixa todos os resultados CSV do job (com paginacao) e retorna DataFrame."""
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

        print(f"[BULK] Baixando pagina {page}...")
        resp = requests.get(url, headers=headers, params=params, timeout=120)
        resp.raise_for_status()

        reader = csv.DictReader(io.StringIO(resp.text))
        records = list(reader)
        all_records.extend(records)
        print(f"[BULK] Pagina {page}: {len(records)} registros | acumulado: {len(all_records)}")

        locator = resp.headers.get("Sforce-Locator")
        if not locator or locator == "null":
            break
        page += 1

    if not all_records:
        print("[BULK] Nenhum registro retornado.")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    print(f"[BULK] Download concluido: {len(df)} linhas, {len(df.columns)} colunas.")
    return df


# ---------------------------------------------------------------------------
# Task pública
# ---------------------------------------------------------------------------


@task(
    log_prints=True,
    retries=3,
    retry_delay_seconds=[30, 60, 120],
)
def extract_via_bulk_api(
    bulk_session: dict,
    soql: str,
    api_version: str = "v59.0",
    poll_interval_seconds: int = 10,
    max_wait_seconds: int = 900,
) -> pd.DataFrame:
    """
    Extrai dados de um objeto CRM via Bulk API 2.0.

    Args:
        bulk_session: dict com 'access_token' e 'instance_url'.
        soql: Query SOQL. Use colunas explícitas (nunca SELECT *).
              Filtro de watermark deve estar incluso na query.
        api_version: Versão da API. Padrão: 'v59.0'.
        poll_interval_seconds: Intervalo de polling. Padrão: 10s.
        max_wait_seconds: Timeout máximo. Padrão: 900s.

    Returns:
        pd.DataFrame com os registros retornados, ou DataFrame vazio se não houver dados.
    """
    access_token = bulk_session["access_token"]
    instance_url = bulk_session["instance_url"]

    job_id = _create_bulk_job(
        instance_url=instance_url,
        access_token=access_token,
        soql=soql,
        api_version=api_version,
    )

    _wait_for_bulk_job(
        instance_url=instance_url,
        access_token=access_token,
        job_id=job_id,
        api_version=api_version,
        poll_interval_seconds=poll_interval_seconds,
        max_wait_seconds=max_wait_seconds,
    )

    df = _download_bulk_results(
        instance_url=instance_url,
        access_token=access_token,
        job_id=job_id,
        api_version=api_version,
    )

    return df
