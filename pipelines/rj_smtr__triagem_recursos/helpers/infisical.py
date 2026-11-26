"""Carregamento dinâmico de segredos via API REST do Infisical (secrets.dados.rio)."""

import os
from pathlib import Path

import httpx

from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

INFISICAL_K8S_LOGIN_PATH = "/api/v1/auth/kubernetes-auth/login"
INFISICAL_UNIVERSAL_LOGIN_PATH = "/api/v1/auth/universal-auth/login"
INFISICAL_SECRETS_PATH = "/api/v3/secrets/raw"
SERVICE_ACCOUNT_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")


def autenticar_infisical(client: httpx.Client) -> str:
    """Autentica no Infisical via Universal Auth ou token de ServiceAccount Kubernetes."""
    client_id = os.environ.get("INFISICAL_CLIENT_ID", "").strip()
    client_secret = os.environ.get("INFISICAL_CLIENT_SECRET", "").strip()
    if client_id and client_secret:
        resp = client.post(
            INFISICAL_UNIVERSAL_LOGIN_PATH,
            json={"clientId": client_id, "clientSecret": client_secret},
        )
        resp.raise_for_status()
        return str(resp.json()["accessToken"])

    identity_id = os.environ.get("INFISICAL_IDENTITY_ID", "").strip()
    if identity_id and SERVICE_ACCOUNT_TOKEN_PATH.exists():
        jwt = SERVICE_ACCOUNT_TOKEN_PATH.read_text(encoding="utf-8").strip()
        resp = client.post(
            INFISICAL_K8S_LOGIN_PATH,
            json={"identityId": identity_id, "jwt": jwt},
        )
        resp.raise_for_status()
        return str(resp.json()["accessToken"])

    raise RuntimeError(
        "Credenciais de acesso ao Infisical ausentes (INFISICAL_CLIENT_ID/SECRET ou INFISICAL_IDENTITY_ID)."
    )


def montar_paths_infisical(secret_path_override: str | None = None) -> list[str]:
    """Monta lista ordenada de pastas do Infisical, priorizando override quando informado."""
    paths_raw = os.environ.get("INFISICAL_PATHS", "").strip()
    candidatos: list[str] = []
    if secret_path_override and secret_path_override.strip():
        override = "/" + secret_path_override.strip().lstrip("/")
        if override != "/":
            candidatos.append(override)
    for item in paths_raw.split(","):
        limpo = item.strip()
        if limpo and limpo not in candidatos:
            candidatos.append(limpo)
    return candidatos


def carregar_secrets_infisical_se_necessario(
    secret_path_override: str | None = None,
    transport: httpx.BaseTransport | None = None,
) -> list[str]:
    """Busca segredos no Infisical e injeta em os.environ quando credenciais estiverem no pod.

    Retorna a lista de chaves carregadas (nunca valores). Em execução local sem
    credenciais de máquina do Infisical configuradas, retorna lista vazia sem falhar.
    """
    infisical_url = os.environ.get("INFISICAL_URL", "").strip()
    project_id = os.environ.get("INFISICAL_PROJECT_ID", "").strip()
    client_id = os.environ.get("INFISICAL_CLIENT_ID", "").strip()
    client_secret = os.environ.get("INFISICAL_CLIENT_SECRET", "").strip()
    identity_id = os.environ.get("INFISICAL_IDENTITY_ID", "").strip()

    possui_credencial = bool((client_id and client_secret) or (identity_id and SERVICE_ACCOUNT_TOKEN_PATH.exists()))
    if not (infisical_url and project_id and possui_credencial):
        return []

    environment = os.environ.get("INFISICAL_ENVIRONMENT", "stg").strip() or "stg"
    paths = montar_paths_infisical(secret_path_override)
    if not paths:
        paths = ["/rj_smtr__triagem_recursos", "/shared"]

    encontrados: dict[str, str] = {}
    client_kwargs: dict[str, object] = {
        "base_url": infisical_url.rstrip("/"),
        "timeout": 30.0,
    }
    if transport is not None:
        client_kwargs["transport"] = transport

    with httpx.Client(**client_kwargs) as client:  # type: ignore[arg-type]
        token = autenticar_infisical(client)
        headers = {"Authorization": f"Bearer {token}"}

        for idx, path in enumerate(paths):
            params = {
                "workspaceId": project_id,
                "environment": environment,
                "secretPath": path,
                "include_imports": "true",
            }
            resp = client.get(INFISICAL_SECRETS_PATH, params=params, headers=headers)
            if resp.status_code == 404 and idx > 0:
                logger.warning("Pasta auxiliar '%s' ausente no Infisical (env=%s); ignorando.", path, environment)
                continue
            resp.raise_for_status()
            items = resp.json().get("secrets", [])
            if not isinstance(items, list):
                raise RuntimeError(f"Resposta inesperada do Infisical para pasta {path}: 'secrets' não é lista.")
            for item in items:
                if isinstance(item, dict) and "secretKey" in item and "secretValue" in item:
                    encontrados.setdefault(str(item["secretKey"]), str(item["secretValue"]))

    injetadas: list[str] = []
    for chave, valor in encontrados.items():
        if not os.environ.get(chave):
            os.environ[chave] = valor
            injetadas.append(chave)

    logger.info(
        "Infisical sincronizado (%s | env=%s | paths=%s): %d chaves carregadas (%s).",
        infisical_url,
        environment,
        paths,
        len(injetadas),
        sorted(encontrados.keys()),
    )
    return sorted(encontrados.keys())
