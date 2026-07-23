# -*- coding: utf-8 -*-
"""
Autenticação para Salesforce CRM (Bulk API 2.0 e Data Cloud Query API).

Dois conectores distintos:
  - Bulk API 2.0        : OAuth2 Username-Password (objetos padrão CRM)
  - Data Cloud REST API : OAuth2 Client Credentials (DMOs do Agentforce)

Credenciais no Infisical (path: /salesforce_crm):

  Bulk API (Username-Password):
    SF_CRM_CLIENT_ID      Client ID da Connected App CRM
    SF_CRM_CLIENT_SECRET  Client Secret da Connected App CRM
    SF_CRM_USERNAME       Username do usuário de integração
    SF_CRM_PASSWORD       Senha + Security Token concatenados
    SF_CRM_LOGIN_URL      https://login.salesforce.com (ou test.salesforce.com)

  Data Cloud (Client Credentials):
    SF_DC_CLIENT_ID       Consumer Key da Connected App DataCloud_Integration
    SF_DC_CLIENT_SECRET   Consumer Secret da Connected App DataCloud_Integration
    SF_DC_INSTANCE_URL    https://<org>.my.salesforce.com
    SF_DC_DATASPACE       default (ou outro dataspace configurado)
"""

from __future__ import annotations

import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task


# ---------------------------------------------------------------------------
# Leitura de credenciais
# ---------------------------------------------------------------------------


def _get_crm_credentials() -> dict[str, str]:
    """Lê credenciais Salesforce CRM (Bulk API) das env vars."""
    return {
        "client_id": getenv_or_action("SF_CRM_CLIENT_ID"),
        "client_secret": getenv_or_action("SF_CRM_CLIENT_SECRET"),
        "username": getenv_or_action("SF_CRM_USERNAME"),
        "password": getenv_or_action("SF_CRM_PASSWORD"),
        "login_url": getenv_or_action("SF_CRM_LOGIN_URL").rstrip("/"),
    }


def _get_dc_credentials() -> dict[str, str]:
    """Lê credenciais Salesforce Data Cloud (Client Credentials) das env vars."""
    return {
        "client_id": getenv_or_action("SF_DC_CLIENT_ID"),
        "client_secret": getenv_or_action("SF_DC_CLIENT_SECRET"),
        "instance_url": getenv_or_action("SF_DC_INSTANCE_URL").rstrip("/"),
        "dataspace": getenv_or_action("SF_DC_DATASPACE", default="default"),
    }


# ---------------------------------------------------------------------------
# Bulk API 2.0 — autenticação OAuth2 Username-Password
# ---------------------------------------------------------------------------


@task(log_prints=True, retries=3, retry_delay_seconds=30)
def get_bulk_api_session() -> dict[str, str]:
    """
    Autentica na Salesforce Bulk API 2.0 via OAuth2 Username-Password Flow.

    Returns:
        dict com 'access_token' e 'instance_url'.

    Raises:
        requests.HTTPError: Se a autenticação falhar.
    """
    creds = _get_crm_credentials()
    token_url = f"{creds['login_url']}/services/oauth2/token"
    payload = {
        "grant_type": "password",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
        "username": creds["username"],
        "password": creds["password"],
    }

    print("[AUTH][BULK] Autenticando na Bulk API 2.0...")
    response = requests.post(token_url, data=payload, timeout=30)
    response.raise_for_status()

    data = response.json()
    instance_url = data["instance_url"].rstrip("/")
    print(f"[AUTH][BULK] OK — instance_url: {instance_url}")

    return {
        "access_token": data["access_token"],
        "instance_url": instance_url,
    }


# ---------------------------------------------------------------------------
# Data Cloud Query API — OAuth2 Client Credentials + REST
# ---------------------------------------------------------------------------


@task(log_prints=True, retries=3, retry_delay_seconds=30)
def get_data_cloud_session() -> dict[str, str]:
    """
    Autentica no Salesforce Data Cloud via OAuth2 Client Credentials Flow.

    Usa a Connected App 'DataCloud_Integration' (scope: cdp_api).
    Não requer username/password — apenas client_id e client_secret.

    Returns:
        dict com 'access_token', 'instance_url' e 'dataspace'.

    Raises:
        requests.HTTPError: Se a autenticação falhar.
    """
    creds = _get_dc_credentials()
    token_url = f"{creds['instance_url']}/services/oauth2/token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": creds["client_id"],
        "client_secret": creds["client_secret"],
    }

    print("[AUTH][DC] Autenticando no Data Cloud (client_credentials)...")
    response = requests.post(token_url, data=payload, timeout=30)
    response.raise_for_status()

    data = response.json()
    instance_url = data.get("instance_url", creds["instance_url"]).rstrip("/")
    print(f"[AUTH][DC] OK — instance_url: {instance_url}, scope: {data.get('scope')}")

    return {
        "access_token": data["access_token"],
        "instance_url": instance_url,
        "dataspace": creds["dataspace"],
    }


# ---------------------------------------------------------------------------
# Retrocompatibilidade: alias para código legado que usa get_data_cloud_connection
# ---------------------------------------------------------------------------


@task(log_prints=True, retries=3, retry_delay_seconds=30)
def get_data_cloud_connection():
    """
    DEPRECATED: use get_data_cloud_session() no lugar.

    Mantido por retrocompatibilidade. Retorna um objeto simples com método
    de execução de queries compatível com a interface anterior.
    """
    print("[AUTH][DC] WARN: get_data_cloud_connection() está depreciado. "
          "Use get_data_cloud_session().")
    return get_data_cloud_session()
