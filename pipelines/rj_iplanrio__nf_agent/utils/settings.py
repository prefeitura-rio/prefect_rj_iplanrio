"""Configuração de runtime lida das variáveis de ambiente injetadas pelo Infisical."""

import base64
import os
from dataclasses import dataclass
from pathlib import Path

CREDENTIALS_ENV = "RJ_NF_AGENT_CREDENTIALS"
CREDENTIALS_PATH = Path("/tmp/rj_nf_agent_credentials.json")

REQUIRED_ENV = {
    "bifrost_bucket": "BIFROST_GCS_BUCKET",
    "output_bucket": "GCS_BUCKET",
    "output_base_path": "GCS_OUTPUT_BASE_PATH",
    "extracao_pagina_table": "BQ_EXTRACAO_PAGINA_TABLE",
    "nf_batch_jobs_table": "NF_BATCH_JOBS_TABLE",
}


@dataclass(frozen=True)
class Settings:
    """Recursos GCP usados pela pipeline."""

    bifrost_bucket: str
    output_bucket: str
    output_base_path: str
    extracao_pagina_table: str
    nf_batch_jobs_table: str


def load_settings() -> Settings:
    """Lê as configurações obrigatórias do ambiente.

    :returns: Configurações preenchidas.
    :raises RuntimeError: Se alguma variável obrigatória estiver ausente ou vazia.
    """
    missing = sorted(env for env in REQUIRED_ENV.values() if not os.environ.get(env))
    if missing:
        raise RuntimeError(f"Variáveis de ambiente ausentes: {', '.join(missing)}")
    return Settings(**{field: os.environ[env] for field, env in REQUIRED_ENV.items()})


def inject_gcp_credentials() -> None:
    """Grava a service account (base64 em ``RJ_NF_AGENT_CREDENTIALS``) e aponta o ADC para ela.

    Sem a variável, mantém o ADC já configurado (uso local com ``gcloud auth``).

    :raises ValueError: Se a variável não contiver base64 válido.
    """
    encoded = os.environ.get(CREDENTIALS_ENV)
    if not encoded:
        return
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except ValueError as exc:  # binascii.Error e texto não ASCII
        raise ValueError(f"{CREDENTIALS_ENV} não contém base64 válido.") from exc
    CREDENTIALS_PATH.write_bytes(decoded)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(CREDENTIALS_PATH)
