"""
Autenticação Salesforce Data Cloud (Query API) — OAuth2 Client Credentials.

Credenciais no Infisical (path: /salesforce_crm):
    SF_DC_CLIENT_ID       Consumer Key da Connected App DataCloud_Integration
    SF_DC_CLIENT_SECRET   Consumer Secret da Connected App DataCloud_Integration
    SF_DC_INSTANCE_URL    https://<org>.my.salesforce.com
    SF_DC_DATASPACE       default (ou outro dataspace configurado)

A CRM REST API (usada por extract_from_crm_rest — messaging_session/
messaging_end_user) reaproveita esta mesma sessão (client_credentials serve
pros dois); não tem autenticação própria.
"""

import requests
from prefect import task

from pipelines.rj_crm__salesforce_datacloud.helpers.retry import so_erro_temporario
from pipelines.rj_crm__salesforce_datacloud.settings import Settings


def _get_dc_credentials() -> dict[str, str]:
    """Lê credenciais Salesforce Data Cloud (Client Credentials) das env vars."""
    settings = Settings()

    return {
        "client_id": settings.SF_DC_CLIENT_ID,
        "client_secret": settings.SF_DC_CLIENT_SECRET.get_secret_value(),
        "instance_url": settings.SF_DC_INSTANCE_URL.rstrip("/"),
        "dataspace": settings.SF_DC_DATASPACE or "default",
    }


# ---------------------------------------------------------------------------
# Data Cloud Query API — OAuth2 Client Credentials + REST
# ---------------------------------------------------------------------------


@task(log_prints=True, retries=3, retry_delay_seconds=30, retry_condition_fn=so_erro_temporario)
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

    response = requests.post(token_url, data=payload, timeout=30)
    response.raise_for_status()

    data = response.json()
    instance_url = data.get("instance_url", creds["instance_url"]).rstrip("/")

    return {
        "access_token": data["access_token"],
        "instance_url": instance_url,
        "dataspace": creds["dataspace"],
    }
