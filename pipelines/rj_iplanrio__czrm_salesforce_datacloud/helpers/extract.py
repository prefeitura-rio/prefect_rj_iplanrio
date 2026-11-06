"""Extração do Salesforce Data Cloud via Query API.

A API não tem paginação por cursor: a única forma é reenviar a SQL com
``ORDER BY <chave> LIMIT 1000 OFFSET n``. O teto real de uma resposta varia
entre ~1400 e ~1700 linhas (limite de payload), por isso a página de 1000.
"""

from __future__ import annotations

import pandas as pd
import requests
from prefect import task

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import API_VERSION, MAX_ROWS, PAGE_SIZE, TIMEOUT
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.log import logger_da_pipeline
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.queries import ler_query
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.retry import condicao_retry

logger = logger_da_pipeline(__name__)


def executar_consulta(session: dict, sql: str) -> dict:
    """Envia uma SQL ao Data Cloud e retorna o corpo JSON da resposta.

    :param session: sessão de :func:`helpers.auth.get_session`.
    :param sql: SQL completa a executar.
    :returns: corpo da resposta com chaves ``data``, ``metadata`` e ``status``.
    :raises requests.HTTPError: se a API responder com erro HTTP.
    """
    url = (
        f"{session['instance_url']}/services/data/{API_VERSION}/ssot/query-sql"
        f"?dataspace={session['dataspace']}&workloadName=BatchQuery"
    )
    resposta = requests.post(
        url,
        headers={"Authorization": f"Bearer {session['access_token']}", "Content-Type": "application/json"},
        json={"sql": sql},
        timeout=TIMEOUT,
    )
    resposta.raise_for_status()
    return resposta.json()


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120], retry_condition_fn=condicao_retry)
def descobrir_colunas(session: dict, dlo: str, tabela: str = "") -> list[dict]:
    """Descobre colunas e tipos da DLO via ``SELECT * LIMIT 0``.

    :param session: sessão autenticada.
    :param dlo: nome da DLO (ex.: ``WorkOrder_Home__dll``).
    :param tabela: nome lógico usado no log.
    :returns: lista de ``{"name": ..., "type": ...}``.
    :raises RuntimeError: se a resposta vier sem metadados.
    """
    sql = ler_query("extract", dlo=dlo, filtro="").rstrip() + "\nLIMIT 0"
    corpo = executar_consulta(session, sql)
    metadata = corpo.get("metadata") or []
    if not metadata:
        raise RuntimeError(f"'{tabela or dlo}' não devolveu metadados — a DLO existe?")
    logger.info("[DC] %s: %d colunas descobertas", tabela or dlo, len(metadata))
    return [{"name": c["name"], "type": c["type"]} for c in metadata]


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120], retry_condition_fn=condicao_retry)
def extrair(session: dict, dlo: str, filtro: str, order_by: str, tabela: str = "") -> pd.DataFrame:
    """Extrai a DLO completa paginando via ``ORDER BY`` + ``LIMIT/OFFSET``.

    :param session: sessão autenticada.
    :param dlo: nome da DLO.
    :param filtro: cláusula WHERE já renderizada; vazia na carga inicial.
    :param order_by: coluna estável de paginação (ex.: ``Id__c``).
    :param tabela: nome lógico usado no log.
    :returns: DataFrame com todas as linhas da janela.
    :raises RuntimeError: se o volume ultrapassar ``MAX_ROWS``.
    """
    base = ler_query("extract", dlo=dlo, filtro=filtro).rstrip().rstrip(";")
    linhas: list[list] = []
    colunas: list[str] = []
    offset = 0
    pagina = 0

    while True:
        pagina += 1
        corpo = executar_consulta(session, f"{base} ORDER BY {order_by} LIMIT {PAGE_SIZE} OFFSET {offset}")
        if not colunas:
            colunas = [c["name"] for c in corpo.get("metadata") or []]
        novas = corpo.get("data") or []
        if not novas:
            # Página vazia é o único sinal confiável de fim: o servidor pode devolver
            # menos que PAGE_SIZE quando o payload estoura — parar por tamanho
            # da página truncava a extração em silêncio.
            break
        linhas.extend(novas)
        offset += len(novas)
        if offset >= MAX_ROWS:
            raise RuntimeError(
                f"{tabela or dlo}: limite de segurança de {MAX_ROWS} linhas atingido. "
                "Use modo='reprocessar' por dia para esse volume."
            )

    logger.info("[DC] %s: %d linhas em %d página(s)", tabela or dlo, len(linhas), pagina)
    return pd.DataFrame(linhas, columns=colunas) if linhas else pd.DataFrame(columns=colunas)
