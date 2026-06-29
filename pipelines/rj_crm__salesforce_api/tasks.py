# -*- coding: utf-8 -*-
"""
Tasks para ingestão de dados via Salesforce Marketing Cloud REST API.

Autenticação via OAuth2 Client Credentials (grant_type=client_credentials).
Credenciais necessárias no Infisical:
  - sfmc_client_id      : Client ID da Connected App no SFMC
  - sfmc_client_secret  : Client Secret da Connected App no SFMC
  - sfmc_auth_url       : URL de autenticação (ex: https://<tenant>.auth.marketingcloudapis.com)
  - sfmc_rest_base_url  : URL base da REST API  (ex: https://<tenant>.rest.marketingcloudapis.com)
"""

import json
import os
from datetime import date
from typing import Any

import pandas as pd
import requests
from prefect import task


def _get_sfmc_credentials() -> dict[str, str]:
    """
    Lê as credenciais da SFMC a partir das variáveis de ambiente
    (injetadas pelo Infisical via inject_bd_credentials_task ou secretName).

    Returns:
        dict com as chaves: client_id, client_secret, auth_url, rest_base_url
    """
    client_id = os.environ["sfmc_client_id"]
    client_secret = os.environ["sfmc_client_secret"]
    auth_url = os.environ["sfmc_auth_url"].rstrip("/")
    rest_base_url = os.environ["sfmc_rest_base_url"].rstrip("/")
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "auth_url": auth_url,
        "rest_base_url": rest_base_url,
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
    access_token = token_data["access_token"]
    print(f"[SFMC] Token obtido com sucesso. Expira em {token_data.get('expires_in', 'N/A')}s.")
    return access_token


@task(log_prints=True)
def fetch_sfmc_route(
    route: str,
    route_params: dict[str, Any] | None = None,
    query_params: dict[str, Any] | None = None,
) -> list[dict]:
    """
    Chama uma rota GET da Salesforce Marketing Cloud REST API e retorna os dados.

    A rota pode conter placeholders no estilo {chave} que serão substituídos
    pelos valores de `route_params`. Ex: "/asset/v1/content/assets/{assetId}"
    com route_params={"assetId": "12345"} resulta em "/asset/v1/content/assets/12345".

    Se a resposta for uma lista (campo "items"), retorna todos os itens.
    Se for um objeto único, retorna-o encapsulado em lista.

    Args:
        route: Rota relativa da API. Ex: "/asset/v1/content/assets/{assetId}".
        route_params: Dicionário de parâmetros para substituição na rota.
        query_params: Query string parameters adicionais (ex: {"$page": 1, "$pageSize": 50}).

    Returns:
        Lista de dicionários com os dados retornados pela API.
    """
    creds = _get_sfmc_credentials()
    access_token = _get_access_token(
        client_id=creds["client_id"],
        client_secret=creds["client_secret"],
        auth_url=creds["auth_url"],
    )

    # Substituir placeholders na rota
    resolved_route = route
    if route_params:
        resolved_route = route.format(**route_params)

    url = f"{creds['rest_base_url']}{resolved_route}"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    print(f"[SFMC] Chamando GET {url}")
    if query_params:
        print(f"[SFMC] Query params: {query_params}")

    response = requests.get(url, headers=headers, params=query_params, timeout=60)
    response.raise_for_status()

    data = response.json()

    # Print dos dados recebidos para inspeção
    print(f"[SFMC] Resposta recebida (primeiros 2000 chars):")
    print(json.dumps(data, indent=2, ensure_ascii=False)[:2000])

    # Normaliza para lista
    if isinstance(data, list):
        records = data
    elif "items" in data:
        records = data["items"]
        print(f"[SFMC] Total de itens retornados (campo 'items'): {len(records)}")
    else:
        # Objeto único — encapsula em lista
        records = [data]
        print(f"[SFMC] Objeto único retornado, encapsulado em lista.")

    print(f"[SFMC] Total de registros: {len(records)}")
    return records


@task(log_prints=True)
def build_dataframe(
    records: list[dict],
    partition_date: date | None = None,
) -> pd.DataFrame:
    """
    Constrói um DataFrame a partir dos registros retornados pela API.

    Cada linha do DataFrame contém:
      - data: JSON string com o conteúdo do registro (todos os campos originais)
      - data_particao: data de particionamento (hoje se não informada)

    Args:
        records: Lista de dicionários retornados por fetch_sfmc_route.
        partition_date: Data de particionamento. Padrão: data de hoje.

    Returns:
        DataFrame com colunas 'data' e 'data_particao'.
    """
    if partition_date is None:
        partition_date = date.today()

    rows = []
    for record in records:
        rows.append(
            {
                "data": json.dumps(record, ensure_ascii=False),
                "data_particao": str(partition_date),
            }
        )

    df = pd.DataFrame(rows, columns=["data", "data_particao"])
    print(f"[SFMC] DataFrame criado com {len(df)} linhas e colunas: {list(df.columns)}")
    return df
