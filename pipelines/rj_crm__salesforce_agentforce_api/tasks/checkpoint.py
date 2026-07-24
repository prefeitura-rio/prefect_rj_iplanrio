# -*- coding: utf-8 -*-
"""
Watermark/checkpoint para ingestão incremental.

Persiste o watermark (data/hora do último registro processado com sucesso)
em uma tabela de controle no BigQuery:
  dataset: agentforce_control (ou configurável)
  tabela : pipeline_checkpoints
  schema : table_name STRING, watermark TIMESTAMP, updated_at TIMESTAMP

A leitura do watermark no início do flow determina o filtro da query SOQL/DC.
A escrita ocorre somente após validação bem-sucedida (para garantir consistência).

Estratégia de overlap:
  watermark_with_buffer = watermark - 30 minutos
  (evita perder registros no limite do dia por atrasos de replicação)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from google.cloud import bigquery
from prefect import task

_CONTROL_TABLE = "pipeline_checkpoints"
_DEFAULT_LOOKBACK_DAYS = 1  # se não houver watermark, busca último N dias


# ---------------------------------------------------------------------------
# DDL da tabela de controle (executado na primeira vez que não existe)
# ---------------------------------------------------------------------------

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `{full_table_id}` (
    table_name   STRING NOT NULL,
    watermark    TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL
)
OPTIONS (
    description = 'Watermarks de ingestao incremental da pipeline Agentforce'
)
"""


def _ensure_control_table(client: bigquery.Client, project_id: str, control_dataset: str) -> str:
    """Cria tabela de controle se não existir. Retorna o full table ID."""
    full_id = f"{project_id}.{control_dataset}.{_CONTROL_TABLE}"
    sql = _CREATE_TABLE_SQL.format(full_table_id=full_id)
    client.query(sql).result()
    return full_id


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------


@task(log_prints=True)
def read_watermark(
    table_name: str,
    project_id: str,
    control_dataset: str,
    lookback_days: int = _DEFAULT_LOOKBACK_DAYS,
    overlap_minutes: int = 30,
) -> str:
    """
    Lê o watermark da última execução bem-sucedida para uma tabela.

    Args:
        table_name      : Nome da tabela (ex: 'ai_agent_session').
        project_id      : ID do projeto GCP.
        control_dataset : Dataset de controle (ex: 'agentforce_control').
        lookback_days   : Se não houver watermark, usa NOW() - N dias. Padrão: 1.
        overlap_minutes : Buffer de overlap para evitar registros na borda. Padrão: 30.

    Returns:
        Watermark como string ISO 8601 (ex: '2024-01-14T23:30:00Z').
    """
    client = bigquery.Client(project=project_id)
    full_id = _ensure_control_table(client, project_id, control_dataset)

    query = f"""
        SELECT watermark
        FROM `{full_id}`
        WHERE table_name = '{table_name}'
        ORDER BY updated_at DESC
        LIMIT 1
    """

    result = list(client.query(query).result())

    if result:
        watermark: datetime = result[0].watermark
        # Aplicar overlap buffer
        watermark_with_buffer = watermark - timedelta(minutes=overlap_minutes)
        watermark_str = watermark_with_buffer.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(
            f"[CHECKPOINT] '{table_name}': watermark={watermark.isoformat()}, "
            f"com buffer={watermark_str}"
        )
    else:
        # Primeira execução — buscar últimos N dias
        fallback = datetime.now(tz=timezone.utc) - timedelta(days=lookback_days)
        watermark_str = fallback.strftime("%Y-%m-%dT%H:%M:%SZ")
        print(
            f"[CHECKPOINT] '{table_name}': sem watermark anterior — "
            f"usando fallback {lookback_days}d: {watermark_str}"
        )

    return watermark_str


@task(log_prints=True)
def write_watermark(
    table_name: str,
    watermark: str,
    project_id: str,
    control_dataset: str,
) -> None:
    """
    Persiste o watermark após carga e validação bem-sucedidas.

    Args:
        table_name      : Nome da tabela.
        watermark       : Novo watermark ISO 8601 (ex: '2024-01-15T00:00:00Z').
        project_id      : ID do projeto GCP.
        control_dataset : Dataset de controle.
    """
    client = bigquery.Client(project=project_id)
    full_id = _ensure_control_table(client, project_id, control_dataset)
    now = datetime.now(tz=timezone.utc).isoformat()

    # MERGE para upsert por table_name
    merge_sql = f"""
        MERGE `{full_id}` AS t
        USING (SELECT '{table_name}' AS table_name) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN
            UPDATE SET watermark = '{watermark}', updated_at = '{now}'
        WHEN NOT MATCHED THEN
            INSERT (table_name, watermark, updated_at)
            VALUES ('{table_name}', '{watermark}', '{now}')
    """

    client.query(merge_sql).result()
    print(f"[CHECKPOINT] '{table_name}': watermark atualizado para {watermark}.")
