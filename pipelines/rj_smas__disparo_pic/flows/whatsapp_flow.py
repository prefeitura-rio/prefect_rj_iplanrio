# -*- coding: utf-8 -*-
"""
WhatsApp dispatch flow for PIC SMAS - Complete pipeline with batching and BigQuery storage
"""

import os

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.logging import log
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_smas__disparo_pic.constants import PicConstants
from pipelines.rj_smas__disparo_pic.tasks.dispatch import send_to_api
from pipelines.rj_smas__disparo_pic.tasks.extract import extract_from_bigquery
from pipelines.rj_smas__disparo_pic.tasks.prepare import prepare_destinations
from pipelines.rj_smas__disparo_pic.tasks.record import create_dispatch_record
from pipelines.rj_smas__disparo_pic.tasks.transform import transform_data
from pipelines.rj_smas__disparo_pic.tasks.validate import validate_data
from pipelines.rj_smas__disparo_pic.utils.tasks import create_date_partitions


@flow(name="whatsapp_dispatch_pipeline", log_prints=True)
def whatsapp_flow(
    sql: str | None = None,
    config_path: str | None = None,
    id_hsm: int | None = None,
    campaign_name: str | None = None,
    cost_center_id: int | None = None,
    chunk_size: int | None = None,
    dataset_id: str | None = None,
    table_id: str | None = None,
    dump_mode: str | None = None,
    infisical_secret_path: str = "/wetalkie",
    dispatch_route: str = "dispatch",
    login_route: str = "users/login",
    params: dict | None = None,
):
    """
    Pipeline completa de disparo WhatsApp (BigQuery → Transform → API → BigQuery).

    Executa fluxo completo:
    1. Extrai dados do BigQuery usando query (com suporte a processadores)
    2. Valida e transforma os dados
    3. Prepara destinatários para API
    4. Envia em lotes via API WeTalkie
    5. Cria registros de disparo
    6. Particiona dados por data
    7. Salva resultados no BigQuery

    Credenciais da API WeTalkie são obtidas automaticamente via Infisical.
    Todos os parâmetros têm valores padrão obtidos das constantes.

    Args:
        sql: Query SQL para extrair dados (default: usar constante PIC_QUERY)
        config_path: Caminho para YAML de transformações (default: usar padrão)
        id_hsm: ID do template HSM (default: usar constante PIC_ID_HSM)
        campaign_name: Nome da campanha (default: usar constante PIC_CAMPAIGN_NAME)
        cost_center_id: ID do centro de custo (default: usar constante PIC_COST_CENTER_ID)
        chunk_size: Tamanho dos lotes (default: usar constante PIC_CHUNK_SIZE)
        dataset_id: Dataset do BigQuery (default: usar constante PIC_DATASET_ID)
        table_id: Tabela do BigQuery (default: usar constante PIC_TABLE_ID)
        dump_mode: Modo de dump (default: usar constante PIC_DUMP_MODE)
        infisical_secret_path: Path das credenciais no Infisical
        dispatch_route: Rota da API para dispatch (padrão para PIC: "dispatch")
        login_route: Rota da API para login
        params: Parâmetros opcionais para substituição na query SQL
    """
    # Load defaults from constants
    sql = sql or PicConstants.PIC_QUERY.value
    config_path = config_path or "config/example_transform.yaml"
    id_hsm = id_hsm or PicConstants.PIC_ID_HSM.value
    campaign_name = campaign_name or PicConstants.PIC_CAMPAIGN_NAME.value
    cost_center_id = cost_center_id or PicConstants.PIC_COST_CENTER_ID.value
    chunk_size = chunk_size or PicConstants.PIC_CHUNK_SIZE.value
    dataset_id = dataset_id or PicConstants.PIC_DATASET_ID.value
    table_id = table_id or PicConstants.PIC_TABLE_ID.value
    dump_mode = dump_mode or PicConstants.PIC_DUMP_MODE.value

    # Rename flow run for easier tracking in Prefect UI
    rename_flow_run = rename_current_flow_run_task(new_name=f"{table_id}_{dataset_id}")

    # Inject BigQuery credentials
    crd = inject_bd_credentials_task(environment="prod")

    # Extract, validate, transform
    log(f"Extracting data from BigQuery with query")
    df = extract_from_bigquery(sql, params)
    log(f"Extracted {len(df)} rows from BigQuery")

    df_validated = validate_data(df)
    log(f"Data validation completed")

    df_transformed = transform_data(df_validated, config_path)
    log(f"Data transformation completed")

    destinations = prepare_destinations(df_transformed)
    log(f"Prepared {len(destinations)} destinations for API dispatch")

    # Dispatch to API in batches
    dispatch_date = send_to_api(
        destinations=destinations,
        id_hsm=id_hsm,
        campaign_name=campaign_name,
        cost_center_id=cost_center_id,
        chunk_size=chunk_size,
        infisical_secret_path=infisical_secret_path,
        dispatch_route=dispatch_route,
        login_route=login_route,
    )

    log(f"Dispatch completed at {dispatch_date}")

    # Create dispatch record DataFrame
    dfr = create_dispatch_record(
        destinations=destinations,
        id_hsm=id_hsm,
        campaign_name=campaign_name,
        cost_center_id=cost_center_id,
        dispatch_date=dispatch_date,
    )

    log(f"Created dispatch record with {len(dfr)} rows")

    # Create date partitions
    partitions_path = create_date_partitions(
        dataframe=dfr,
        partition_column="dispatch_date",
        file_format="csv",
        root_folder="./data_dispatch/",
    )

    if not partitions_path:
        raise ValueError("partitions_path is None - partition creation failed")

    if not os.path.exists(partitions_path):
        raise ValueError(f"partitions_path does not exist: {partitions_path}")

    log(f"Generated partitions_path: {partitions_path}")

    # Debug: list files in partitions path
    if os.path.exists(partitions_path):
        files_in_path = []
        for root, dirs, files in os.walk(partitions_path):
            files_in_path.extend([os.path.join(root, f) for f in files])
        log(f"Files in partitions path: {files_in_path}")

    # Upload to GCS and BigQuery
    create_table = create_table_and_upload_to_gcs_task(
        data_path=partitions_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=False,
    )

    log(f"Data uploaded to {dataset_id}.{table_id}")
