# -*- coding: utf-8 -*-
"""
Template genérico sf_to_bq — "receita de bolo" da pipeline Agentforce.

Extrai por janela de tempo com sobreposição intencional (ver tasks/janela.py:
janela_hora()/janela_dia()), não por watermark forward-only — um watermark
que só avança não se recupera sozinho de um tick perdido (schedule pausado,
erro transitório, pico de volume); foi a causa raiz de um buraco de quase
100% num dia inteiro, achado nesta investigação. A tabela de controle
(watermarks/checkpoints) foi removida do pipeline em 2026-09-08 — não existe
mais estado a ler no início nem a escrever no fim.

Ciclo: extrair (Data Cloud, Data Cloud chunked ou CRM REST) → transformar →
carregar no BigQuery (staging + MERGE, sempre) → validar contagem.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from pipelines.rj_crm__salesforce_agentforce_api.tasks.extract_chunked import (
    extract_chunked_from_data_cloud,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.extract_crm import (
    extract_from_crm_rest,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.extract_data_cloud import (
    extract_from_data_cloud,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.load_bigquery import (
    load_chunk_to_staging,
    merge_staging_to_target,
)
from pipelines.rj_crm__salesforce_agentforce_api.tasks.transform import transform_dataframe
from pipelines.rj_crm__salesforce_agentforce_api.tasks.validate import validate_row_count


def sf_to_bq(
    source: str,
    query_template: str,
    target_table: str,
    staging_table: str,
    project_id: str,
    dataset_id: str,
    janela: tuple[str, str],
    partition_date: date,
    dc_session: dict | None = None,
    crm_session: dict | None = None,
    primary_key: str = "id",
    is_data_cloud: bool = False,
    date_columns: list[str] | None = None,
    duration_ns_columns: list[str] | None = None,
    chunk_size: int = 1_000,  # o servidor corta payload em ~1400-1700 linhas
    # independente do LIMIT pedido (confirmado 04/09/2026) — 50_000 estourava
    # esse teto e fazia extract_chunked_from_data_cloud parar cedo achando que
    # tinha chegado na última página. Ver extract_data_cloud.py e
    # extract_chunked.py para o histórico completo.
    clustering_fields: list[str] | None = None,
    output_value_text_action_step_only: bool = False,
) -> int:
    """
    Executa o ciclo completo extract → transform → load para uma tabela.

    Args:
        source            : 'data_cloud', 'data_cloud_chunked' ou 'crm_rest'.
        query_template    : Query com {data_inicio}/{data_fim} — ambos os limites,
                            vindos de `janela`.
        target_table      : Tabela destino no BigQuery.
        staging_table     : Tabela staging (schema pré-cadastrado em ensure_tables.py) —
                            sempre obrigatória, todo carregamento passa por staging+MERGE.
        project_id        : ID do projeto GCP.
        dataset_id        : Dataset de destino no BQ.
        janela            : (data_inicio, data_fim) já formatados — vem de
                            janela_hora()/janela_dia() (tasks/janela.py).
        partition_date    : Data de partição — normalmente o 3º elemento do mesmo
                            retorno de janela_hora()/janela_dia().
        dc_session        : dict com 'access_token', 'instance_url', 'dataspace'
                            (retornado por get_data_cloud_session — para data_cloud
                            e data_cloud_chunked).
        crm_session       : dict com 'access_token' e 'instance_url' (para crm_rest —
                            mesma credencial de dc_session, nome próprio por clareza).
        primary_key       : Campo de deduplicação do MERGE.
        is_data_cloud     : Se True, remove prefixo ssot__ dos campos.
        date_columns      : Colunas para converter para datetime UTC (pós-normalização).
        duration_ns_columns: Colunas em ns para converter para ms.
        chunk_size        : Tamanho do chunk para extração paginada (data_cloud_chunked).
        clustering_fields : Campos de clustering da tabela destino no BQ (ex: ['id']).
                            Deve corresponder ao clustering definido na tabela — omitir
                            em tabelas sem clustering causaria erro 400 do BigQuery.
        output_value_text_action_step_only: Zera output_value_text fora de ACTION_STEP
                            (só ai_agent_interaction_step usa isso).

    Returns:
        Total de linhas afetadas pelo MERGE.
    """
    data_inicio, data_fim = janela
    partition_str = str(partition_date)
    query = query_template.format(data_inicio=data_inicio, data_fim=data_fim)
    print(f"[TEMPLATE] '{target_table}': janela=[{data_inicio}, {data_fim})")

    # --- Extração ---
    if source == "data_cloud":
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
            output_value_text_action_step_only=output_value_text_action_step_only,
        )
        staged_rows = load_chunk_to_staging(
            df_chunk=df,
            project_id=project_id,
            dataset_id=dataset_id,
            staging_table_id=staging_table,
            chunk_num=1,
        )
        total_rows = 0 if staged_rows == 0 else merge_staging_to_target(
            project_id=project_id,
            dataset_id=dataset_id,
            staging_table_id=staging_table,
            target_table_id=target_table,
            primary_key=primary_key,
            partition_field="data_particao",
        )

    elif source == "data_cloud_chunked":
        assert dc_session, "dc_session é obrigatório para source='data_cloud_chunked'"

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
            load_chunk_to_staging(
                df_chunk=df_chunk,
                project_id=project_id,
                dataset_id=dataset_id,
                staging_table_id=staging_table,
                chunk_num=i,
            )

        # linhas afetadas pelo MERGE (deduplicadas), não o total bruto da
        # staging (que pode ter duplicata entre chunks)
        total_rows = merge_staging_to_target(
            project_id=project_id,
            dataset_id=dataset_id,
            staging_table_id=staging_table,
            target_table_id=target_table,
            primary_key=primary_key,
            partition_field="data_particao",
        )

    elif source == "crm_rest":
        assert crm_session, "crm_session é obrigatório para source='crm_rest'"
        df = extract_from_crm_rest(
            crm_session=crm_session,
            soql=query,
            table_name=target_table,
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
        staged_rows = load_chunk_to_staging(
            df_chunk=df,
            project_id=project_id,
            dataset_id=dataset_id,
            staging_table_id=staging_table,
            chunk_num=1,
        )
        total_rows = 0 if staged_rows == 0 else merge_staging_to_target(
            project_id=project_id,
            dataset_id=dataset_id,
            staging_table_id=staging_table,
            target_table_id=target_table,
            primary_key=primary_key,
            partition_field="data_particao",
        )

    else:
        raise ValueError(f"[TEMPLATE] source inválido: '{source}'. Use 'data_cloud', 'data_cloud_chunked' ou 'crm_rest'.")

    # --- Validar ---
    validate_row_count(
        source_count=total_rows,
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=target_table,
        partition_date=partition_str,
        write_mode="merge",
    )

    return total_rows
