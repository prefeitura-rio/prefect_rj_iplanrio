"""Autenticação no Salesforce Data Cloud via OAuth2 Client Credentials.

Um jogo de credenciais por org de origem, lido das variáveis de ambiente
injetadas pelo Infisical:

    SF_DC_QA_CLIENT_ID        SF_DC_STAGING_CLIENT_ID        SF_DC_PROD_CLIENT_ID
    SF_DC_QA_CLIENT_SECRET    ...                            ...
    SF_DC_QA_INSTANCE_URL     ...                            ...
    SF_DC_QA_DATASPACE        ...                            ...

O sync do Infisical monta o nome como ``<PASTA>__<CHAVE>``
(ex.: ``CZRM_SALESFORCE__SF_DC_PROD_CLIENT_ID``). O código aceita ambas as formas.
"""

from __future__ import annotations

import requests
from iplanrio.pipelines_utils.env import getenv_or_action
from prefect import task

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import DATASPACE, ENV_PREFIX, TIMEOUT
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.retry import condicao_retry

CHAVES_CREDENCIAL = ("CLIENT_ID", "CLIENT_SECRET", "INSTANCE_URL", "DATASPACE")


def ler_variavel(origem: str, chave: str, default: str | None = None) -> str | None:
    """Lê ``SF_DC_<ORIGEM>_<CHAVE>`` com ou sem o prefixo de pasta do Infisical.

    :param origem: ``qa``, ``staging`` ou ``prod``.
    :param chave: sufixo da variável (ex.: ``CLIENT_ID``).
    :param default: valor retornado quando a variável não existe.
    :returns: valor da variável ou ``default``.
    """
    sufixo = f"SF_DC_{origem.upper()}_{chave}"
    for nome in (f"{ENV_PREFIX}__{sufixo}", sufixo):
        valor = getenv_or_action(nome, action="ignore")
        if valor:
            return valor
    return default


def ler_credenciais(origem: str) -> dict[str, str]:
    """Lê e valida as credenciais da org informada.

    :param origem: ``qa``, ``staging`` ou ``prod``.
    :returns: dicionário com ``client_id``, ``client_secret``, ``instance_url`` e ``dataspace``.
    :raises ValueError: se alguma variável obrigatória estiver ausente.
    """
    lidos = {chave: ler_variavel(origem, chave) for chave in CHAVES_CREDENCIAL}
    faltando = sorted(chave for chave, valor in lidos.items() if not valor)
    if faltando:
        esperados = [f"{ENV_PREFIX}__SF_DC_{origem.upper()}_{c}" for c in faltando]
        raise ValueError(
            f"Credenciais ausentes para origem '{origem}': {faltando}. "
            f"Esperado no secret do work pool: {esperados} (ou sem o prefixo {ENV_PREFIX}__)."
        )
    return {
        "client_id": lidos["CLIENT_ID"],
        "client_secret": lidos["CLIENT_SECRET"],
        "instance_url": lidos["INSTANCE_URL"].rstrip("/"),
        "dataspace": lidos["DATASPACE"] or DATASPACE,
    }


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120], retry_condition_fn=condicao_retry)
def get_session(origem: str) -> dict[str, str]:
    """Autentica na org informada e retorna a sessão para a Query API.

    :param origem: ``qa``, ``staging`` ou ``prod``.
    :returns: dicionário com ``access_token``, ``instance_url`` e ``dataspace``.
    :raises requests.HTTPError: se a autenticação falhar.
    """
    cred = ler_credenciais(origem)
    resposta = requests.post(
        f"{cred['instance_url']}/services/oauth2/token",
        data={
            "grant_type": "client_credentials",
            "client_id": cred["client_id"],
            "client_secret": cred["client_secret"],
        },
        timeout=TIMEOUT,
    )
    resposta.raise_for_status()
    corpo = resposta.json()
    return {
        "access_token": corpo["access_token"],
        "instance_url": corpo.get("instance_url", cred["instance_url"]).rstrip("/"),
        "dataspace": cred["dataspace"],
    }
