"""Extração do Data Cloud (Query API).

A API não tem paginação por cursor: a única forma é reenviar a SQL com
``ORDER BY <chave> LIMIT 1000 OFFSET n``. O teto real de uma resposta varia
entre ~1400 e ~1700 linhas (limite de payload, não de contagem), por isso a
página de 1000.

Usado tanto para extrair (`extrair`) quanto para descobrir o schema
(`descobrir_colunas`, com ``LIMIT 0``).
"""

from __future__ import annotations

import pandas as pd
import requests
from prefect import task

from pipelines.rj_iplanrio__czrm_salesforce_datacloud.constants import (
    API_VERSION,
    MAX_ROWS,
    PAGE_SIZE,
    TIMEOUT,
)
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.queries import ler_query
from pipelines.rj_iplanrio__czrm_salesforce_datacloud.utils.retry import so_erro_temporario
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)


def _consulta(session: dict, sql: str) -> dict:
    """Executa uma SQL no Data Cloud e devolve o JSON da resposta.

    :param session: sessão de :func:`tasks.auth.get_session`.
    :param sql: SQL completa (sem LIMIT/OFFSET, quando paginada pelo chamador).
    :returns: corpo da resposta (``data``, ``metadata``, ``status``).
    :raises RuntimeError: Se a API responder erro.
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
    if not resposta.ok:
        logger.error("Data Cloud respondeu %s. SQL:\n%s\nCorpo: %s", resposta.status_code, sql, resposta.text[:600])
    resposta.raise_for_status()
    return resposta.json()


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120], retry_condition_fn=so_erro_temporario)
def descobrir_colunas(session: dict, dlo: str, tabela: str = "") -> list[dict]:
    """Descobre as colunas e tipos da DLO com ``SELECT * LIMIT 0``.

    :param session: sessão autenticada.
    :param dlo: nome da DLO (ex.: ``WorkOrder_Home__dll``).
    :param tabela: nome lógico, só para log.
    :returns: lista ``[{"name": ..., "type": ...}]``.
    :raises RuntimeError: Se a resposta vier sem metadados.
    """
    sql = ler_query("extract", dlo=dlo, filtro="").rstrip() + "\nLIMIT 0"
    corpo = _consulta(session, sql)
    metadata = corpo.get("metadata") or []
    if not metadata:
        raise RuntimeError(f"[DC] '{tabela or dlo}' não devolveu metadados de coluna — a DLO existe?")
    logger.info("[DC] %s: %d colunas descobertas", tabela or dlo, len(metadata))
    return [{"name": c["name"], "type": c["type"]} for c in metadata]


@task(log_prints=True, retries=3, retry_delay_seconds=[30, 60, 120], retry_condition_fn=so_erro_temporario)
def extrair(session: dict, dlo: str, filtro: str, order_by: str, tabela: str = "") -> pd.DataFrame:
    """Extrai a DLO paginando por ``ORDER BY`` + ``LIMIT/OFFSET``.

    :param session: sessão autenticada.
    :param dlo: nome da DLO.
    :param filtro: cláusula WHERE já renderizada (vazia na carga inicial).
    :param order_by: coluna estável para a paginação (ex.: ``Id__c``).
    :param tabela: nome lógico, só para log.
    :returns: DataFrame com todas as linhas da janela.
    """
    base = ler_query("extract", dlo=dlo, filtro=filtro).rstrip().rstrip(";")
    linhas: list[list] = []
    colunas: list[str] = []
    offset = 0
    pagina = 0
    while True:
        pagina += 1
        corpo = _consulta(session, f"{base} ORDER BY {order_by} LIMIT {PAGE_SIZE} OFFSET {offset}")
        if not colunas:
            colunas = [c["name"] for c in corpo.get("metadata") or []]
        novas = corpo.get("data") or []
        linhas.extend(novas)
        if len(novas) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        if offset >= MAX_ROWS:
            logger.warning("[DC] %s: limite de segurança (%s linhas) atingido", tabela or dlo, MAX_ROWS)
            break

    logger.info("[DC] %s: %d linhas em %d página(s)", tabela or dlo, len(linhas), pagina)
    return pd.DataFrame(linhas, columns=colunas) if linhas else pd.DataFrame(columns=colunas)
