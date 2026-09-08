# -*- coding: utf-8 -*-
"""
Autenticação Salesforce Data Cloud (Query API) — OAuth2 Client Credentials.

Credenciais no Infisical (path: /salesforce_crm):
    SF_DC_CLIENT_ID       Consumer Key da Connected App DataCloud_Integration
    SF_DC_CLIENT_SECRET   Consumer Secret da Connected App DataCloud_Integration
    SF_DC_INSTANCE_URL    https://<org>.my.salesforce.com
    SF_DC_DATASPACE       default (ou outro dataspace configurado)

A CRM REST API (usada por extract_crm.py, F2a — messaging_session/
messaging_end_user) reaproveita esta mesma sessão (client_credentials serve
pros dois); não tem autenticação própria.
"""

from __future__ import annotations

import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task


def _get_dc_credentials() -> dict[str, str]:
    """Lê credenciais Salesforce Data Cloud (Client Credentials) das env vars."""
    return {
        "client_id": getenv_or_action("SF_DC_CLIENT_ID"),
        "client_secret": getenv_or_action("SF_DC_CLIENT_SECRET"),
        "instance_url": getenv_or_action("SF_DC_INSTANCE_URL").rstrip("/"),
        "dataspace": getenv_or_action("SF_DC_DATASPACE") or "default",
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
