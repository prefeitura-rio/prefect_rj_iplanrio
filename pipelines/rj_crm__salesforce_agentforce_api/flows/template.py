# -*- coding: utf-8 -*-
"""
Template genérico sf_to_bq — "receita de bolo" da pipeline Agentforce.

Encapsula o ciclo completo:
  1. Ler watermark
  2. Extrair (Bulk API ou Data Cloud)
  3. Transformar
  4. Carregar no BigQuery
  5. Validar contagem
  6. Escrever watermark

Qualquer nova fonte é adicionada apenas em settings.yaml + um flow de 5 linhas
que chama sf_to_bq com os parâmetros da tabela desejada.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from pipelines.rj_crm__salesforce_agentforce_api.tasks.checkpoint import (
    read_watermark,
    write_watermark,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.extract_bulk_api import (
    extract_via_bulk_api,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.extract_chunked import (
    extract_chunked_from_data_cloud,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.extract_data_cloud import (
    extract_from_data_cloud,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.load_bigquery import (
    load_chunk_to_staging,
    load_to_bigquery,
    merge_staging_to_target,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.transform import transform_dataframe
from pipelines.rj_crm__salesforce_agentforce_api.tasks.validate import validate_row_count


def sf_to_bq(
    source: str,
    query_template: str,
    target_table: str,
    project_id: str,
    dataset_id: str,
    control_dataset: str,
    bulk_session: dict | None = None,
    dc_session: dict | None = None,
    dc_conn=None,  # DEPRECATED: use dc_session
    write_mode: str = "append",
    primary_key: str = "id",
    partition_date: date | None = None,
    is_data_cloud: bool = False,
    date_columns: list[str] | None = None,
    duration_ns_columns: list[str] | None = None,
    staging_table: str | None = None,
    chunk_size: int = 50_000,
    api_version: str = "v59.0",
    skip_checkpoint: bool = False,
) -> int:
    """
    Executa o ciclo completo extract → transform → load para uma tabela.

    Args:
        source          : 'bulk_api', 'data_cloud' ou 'data_cloud_chunked'.
        query_template  : Query com placeholder {watermark} para substituição.
        target_table    : Tabela destino no BigQuery.
        project_id      : ID do projeto GCP.
        dataset_id      : Dataset de destino no BQ.
        control_dataset : Dataset de controle (watermarks).
        bulk_session    : dict com 'access_token' e 'instance_url' (para bulk_api).
        dc_session      : dict com 'access_token', 'instance_url', 'dataspace'
                          (retornado por get_data_cloud_session — para data_cloud).
        dc_conn         : DEPRECATED. Ignorado. Use dc_session.
        write_mode      : 'append', 'replace' ou 'merge'.
        primary_key     : Campo de deduplicação (para merge).
        partition_date  : Data de partição. Padrão: hoje.
        is_data_cloud   : Se True, remove prefixo ssot__ dos campos.
        date_columns    : Colunas para converter para datetime UTC (pós-normalização).
        duration_ns_columns: Colunas em ns para converter para ms.
        staging_table   : Tabela staging (necessária para data_cloud_chunked + merge).
        chunk_size      : Tamanho do chunk para extração paginada.
        api_version     : Versão da Salesforce API.
        skip_checkpoint : Se True, não lê/escreve watermark (útil para backfill).

    Returns:
        Total de linhas carregadas.
    """
    # Retrocompatibilidade: dc_conn era a interface antiga (DB-API cursor)
    if dc_conn is not None and dc_session is None:
        raise ValueError(
            "[TEMPLATE] dc_conn está depreciado. Use dc_session (dict retornado por "
            "get_data_cloud_session) no lugar."
        )
    if partition_date is None:
        partition_date = date.today()

    partition_str = str(partition_date)

    # --- 1. Watermark ---
    if skip_checkpoint:
        watermark = f"{partition_date}T00:00:00Z"
        print(f"[TEMPLATE] '{target_table}': skip_checkpoint=True, watermark={watermark}")
    else:
        watermark = read_watermark(
            table_name=target_table,
            project_id=project_id,
            control_dataset=control_dataset,
        )

    # --- 2. Substituir watermark na query ---
    query = query_template.format(watermark=watermark)

    # --- 3. Extração ---
    total_rows = 0

    if source == "bulk_api":
        assert bulk_session, "bulk_session é obrigatório para source='bulk_api'"
        df = extract_via_bulk_api(
            bulk_session=bulk_session,
            soql=query,
            api_version=api_version,
        )
        if df.empty:
            print(f"[TEMPLATE] '{target_table}': sem dados — pulando carga.")
            return 0

        df = transform_dataframe(
            df=df,
            table_name=target_table,
            is_data_cloud=False,
            date_columns=date_columns,
            duration_ns_columns=duration_ns_columns,
            partition_date=partition_date,
        )

        total_rows = load_to_bigquery(
            df=df,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=target_table,
            write_mode=write_mode,
            partition_field="data_particao",
        )

    elif source == "data_cloud":
        assert dc_session, "dc_session é obrigatório para source='data_cloud'"
        df = extract_from_data_cloud(
            dc_session=dc_session,
            query=query,
            table_name=target_table,
        )
        if df.empty:
            print(f"[TEMPLATE] '{target_table}': sem dados — pulando carga.")
            return 0

        df = transform_dataframe(
            df=df,
            table_name=target_table,
            is_data_cloud=True,
            date_columns=date_columns,
            duration_ns_columns=duration_ns_columns,
            partition_date=partition_date,
        )

        total_rows = load_to_bigquery(
            df=df,
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=target_table,
            write_mode=write_mode,
            partition_field="data_particao",
        )

    elif source == "data_cloud_chunked":
        assert dc_session, "dc_session é obrigatório para source='data_cloud_chunked'"
        assert staging_table, "staging_table é obrigatório para source='data_cloud_chunked'"

        chunks = extract_chunked_from_data_cloud(
            dc_session=dc_session,
            query=query,
            table_name=target_table,
            chunk_size=chunk_size,
        )

        if not chunks:
            print(f"[TEMPLATE] '{target_table}': sem dados chunked — pulando carga.")
            return 0

        for i, df_chunk in enumerate(chunks, start=1):
            df_chunk = transform_dataframe(
                df=df_chunk,
                table_name=f"{target_table}_chunk_{i}",
                is_data_cloud=True,
                date_columns=date_columns,
                duration_ns_columns=duration_ns_columns,
                partition_date=partition_date,
            )
            rows = load_chunk_to_staging(
                df_chunk=df_chunk,
                project_id=project_id,
                dataset_id=dataset_id,
                staging_table_id=staging_table,
                chunk_num=i,
            )
            total_rows += rows

        # MERGE staging → target
        merge_staging_to_target(
            project_id=project_id,
            dataset_id=dataset_id,
            staging_table_id=staging_table,
            target_table_id=target_table,
            primary_key=primary_key,
            partition_field="data_particao",
        )

    else:
        raise ValueError(f"[TEMPLATE] source inválido: '{source}'. Use 'bulk_api', 'data_cloud' ou 'data_cloud_chunked'.")

    # --- 4. Validar ---
    validate_row_count(
        source_count=total_rows,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=target_table,
        partition_date=partition_str,
    )

    # --- 5. Escrever watermark ---
    if not skip_checkpoint and total_rows > 0:
        new_watermark = f"{partition_date}T23:59:59Z"
        write_watermark(
            table_name=target_table,
            watermark=new_watermark,
            project_id=project_id,
            control_dataset=control_dataset,
        )

    return total_rows
