# -*- coding: utf-8 -*-
"""
Tasks para ingestão de dados via Salesforce Marketing Cloud REST API.

Autenticação via OAuth2 Client Credentials (grant_type=client_credentials).
Credenciais necessárias no Infisical:
  - sfmc_client_id      : Client ID da Connected App no SFMC
  - sfmc_client_secret  : Client Secret da Connected App no SFMC
  - sfmc_auth_url       : URL de autenticação (ex: https://<tenant>.auth.marketingcloudapis.com)
  - sfmc_rest_base_url  : URL base da REST API  (ex: https://<tenant>.rest.marketingcloudapis.com)

Métodos suportados (parâmetro http_method no flow):
  - "get"  : GET com paginação automática via $page / $pageSize
  - "post" : POST com body JSON e paginação automática via page.page / page.pageSize
"""

import json
from datetime import date
from typing import Any, Literal

import pandas as pd
import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task


# ---------------------------------------------------------------------------
# Credenciais e autenticação
# ---------------------------------------------------------------------------

def _get_sfmc_credentials() -> dict[str, str]:
    """
    Lê as credenciais da SFMC a partir das variáveis de ambiente
    injetadas pelo Infisical (via secretName no job do Kubernetes).

    As variáveis são lidas com getenv_or_action, que lança erro claro
    caso a variável não esteja definida no ambiente.

    Returns:
        dict com as chaves: client_id, client_secret, auth_url, rest_base_url
    """
    return {
        "client_id": getenv_or_action("sfmc_client_id"),
        "client_secret": getenv_or_action("sfmc_client_secret"),
        "auth_url": getenv_or_action("sfmc_auth_url").rstrip("/"),
        "rest_base_url": getenv_or_action("sfmc_rest_base_url").rstrip("/"),
    }


def _get_access_token(client_id: str, client_secret: str, auth_url: str) -> str:
    """
    Obtém um access token OAuth2 da Salesforce Marketing Cloud.

    Args:
        client_id: Client ID da Connected App.
        client_secret: Client Secret da Connected App.
        auth_url: URL base de autenticação do tenant SFMC.

    Returns:
        Access token como string.
    """
    token_url = f"{auth_url}/v2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    response = requests.post(token_url, json=payload, timeout=30)
    response.raise_for_status()
    token_data = response.json()
    print(f"[SFMC] Token obtido com sucesso. Expira em {token_data.get('expires_in', 'N/A')}s.")
    return token_data["access_token"]


# ---------------------------------------------------------------------------
# GET com paginação automática
# ---------------------------------------------------------------------------

def _fetch_all_pages_get(
    url: str,
    headers: dict,
    query_params: dict[str, Any],
    page_size: int,
) -> list[dict]:
    """
    Itera todas as páginas de uma rota GET do SFMC usando $page e $pageSize.

    A resposta deve conter os campos `count` (total de registros) e `items`
    (lista de registros da página atual).

    Args:
        url: URL completa da rota.
        headers: Headers HTTP (Authorization, Content-Type).
        query_params: Query params base (sem $page/$pageSize — adicionados aqui).
        page_size: Quantidade de registros por página.

    Returns:
        Lista com todos os registros de todas as páginas.
    """
    all_records = []
    page = 1

    while True:
        params = {**query_params, "$page": page, "$pageSize": page_size}
        print(f"[SFMC][GET] Buscando página {page} (pageSize={page_size})...")

        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        print(f"[SFMC][GET] Resposta da página {page} (primeiros 2000 chars):")
        print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])

        items = data.get("items", [])
        all_records.extend(items)

        total = data.get("count", len(items))
        print(f"[SFMC][GET] Página {page}: {len(items)} registros. Total acumulado: {len(all_records)} / {total}.")

        if len(all_records) >= total or not items:
            break

        page += 1

    return all_records


# ---------------------------------------------------------------------------
# POST /query com paginação automática
# ---------------------------------------------------------------------------

def _fetch_all_pages_post(
    url: str,
    headers: dict,
    body: dict[str, Any],
    page_size: int,
) -> list[dict]:
    """
    Itera todas as páginas de uma rota POST /query do SFMC.

    O body deve conter uma chave `page` com `page` e `pageSize`.
    A resposta deve conter `count` (total) e `items` (página atual).

    Args:
        url: URL completa da rota (ex: .../asset/v1/content/assets/query).
        headers: Headers HTTP (Authorization, Content-Type).
        body: Body JSON base. A chave `page` é sobrescrita a cada iteração.
        page_size: Quantidade de registros por página.

    Returns:
        Lista com todos os registros de todas as páginas.
    """
    all_records = []
    page = 1

    while True:
        page_body = {
            **body,
            "page": {"page": page, "pageSize": page_size},
        }
        print(f"[SFMC][POST] Buscando página {page} (pageSize={page_size})...")

        response = requests.post(url, headers=headers, json=page_body, timeout=60)
        response.raise_for_status()
        data = response.json()

        print(f"[SFMC][POST] Resposta da página {page} (primeiros 2000 chars):")
        print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])

        items = data.get("items", [])
        all_records.extend(items)

        total = data.get("count", len(items))
        print(f"[SFMC][POST] Página {page}: {len(items)} registros. Total acumulado: {len(all_records)} / {total}.")

        if len(all_records) >= total or not items:
            break

        page += 1

    return all_records


# ---------------------------------------------------------------------------
# Task principal
# ---------------------------------------------------------------------------

@task(log_prints=True)
def fetch_sfmc_route(
    route: str,
    http_method: Literal["get", "post"] = "get",
    route_params: dict[str, Any] | None = None,
    query_params: dict[str, Any] | None = None,
    body_json: dict[str, Any] | None = None,
    page_size: int = 200,
) -> list[dict]:
    """
    Chama uma rota da Salesforce Marketing Cloud REST API e retorna todos os registros,
    iterando automaticamente todas as páginas.

    Args:
        route: Rota relativa da API. Pode conter placeholders {chave}.
               Ex: "/asset/v1/content/assets" ou "/asset/v1/content/assets/query"
        http_method: Método HTTP a usar. "get" ou "post". Padrão: "get".
        route_params: Substituição de placeholders na rota.
                      Ex: {"assetId": "12345"} para "/asset/v1/content/assets/{assetId}".
        query_params: Query string parameters adicionais para GET.
                      Ex: {"scope": "Ours,Shared"}. $page e $pageSize são gerenciados
                      automaticamente — não é necessário informar.
        body_json: Body JSON para POST. A chave "page" é gerenciada automaticamente
                   pela paginação — não é necessário informar page/pageSize aqui.
        page_size: Quantidade de registros por página. Padrão: 200.

    Returns:
        Lista com todos os registros retornados pela API (todas as páginas).
    """
    creds = _get_sfmc_credentials()
    access_token = _get_access_token(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        auth_url=creds["auth_url"],
    )

    # Substituir placeholders na rota
    resolved_route = route.format(**(route_params or {}))
    url = f"{creds['rest_base_url']}{resolved_route}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    print(f"[SFMC] Método: {http_method.upper()} | URL: {url}")

    if http_method == "get":
        records = _fetch_all_pages_get(
            url=url,
            headers=headers,
            query_params=query_params or {},
            page_size=page_size,
        )
    elif http_method == "post":
        records = _fetch_all_pages_post(
            url=url,
            headers=headers,
            body=body_json or {},
            page_size=page_size,
        )
    else:
        raise ValueError(f"http_method inválido: '{http_method}'. Use 'get' ou 'post'.")

    print(f"[SFMC] Total de registros coletados (todas as páginas): {len(records)}")
    return records


# ---------------------------------------------------------------------------
# Task de construção do DataFrame
# ---------------------------------------------------------------------------

@task(log_prints=True)
def build_dataframe(
    records: list[dict],
    partition_date: date | None = None,
) -> pd.DataFrame:
    """
    Constrói um DataFrame a partir dos registros retornados pela API.

    Cada linha contém:
      - data: JSON string com o conteúdo completo do registro
      - data_particao: data de particionamento (hoje se não informada)

    Args:
        records: Lista de dicionários retornados por fetch_sfmc_route.
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
    print(f"[SFMC] DataFrame criado com {len(df)} linhas e colunas: {list(df.columns)}")
    return df
