# -*- coding: utf-8 -*-
"""
Flow unificado para dump de collections do MongoDB do TaxiRio - Prefect 3.0.

Este flow processa qualquer tabela do TaxiRio baseado no parâmetro table_id,
consolidando a lógica que antes estava distribuída em múltiplos flows.
"""

import importlib
from typing import Optional

from iplanrio.pipelines_templates.dump_db.tasks import get_database_username_and_password_from_secret_task
from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_iplanrio__taxirio.constants import TABLE_CONFIGS, Constants
from pipelines.rj_iplanrio__taxirio.tasks import (
    dump_collection_from_mongodb,
    dump_collection_from_mongodb_per_period,
    get_dates_for_dump_mode,
    get_mongodb_client,
    get_mongodb_collection,
    get_mongodb_connection_string,
)


@flow(log_prints=True)
def rj_iplanrio__taxirio(
    table_id: str,
    path: str = "output",
    dataset_id: str = Constants.DATASET_ID.value,
    secret_name: str = Constants.MONGODB_CONNECTION_STRING.value,
    dump_mode: Optional[str] = None,
    frequency: Optional[str] = None,
):
    """
    Flow unificado para dump de collections do MongoDB do TaxiRio para BigQuery.

    Este flow processa qualquer tabela do TaxiRio, automaticamente determinando
    o modo de processamento (dump completo ou por período) baseado na configuração
    da tabela.

    Args:
        table_id: Nome da tabela/collection a ser processada (ex: 'cities', 'races', 'drivers')
        path: Caminho temporário para armazenar os dados extraídos
        dataset_id: ID do dataset no BigQuery
        secret_name: Nome do secret no Infisical contendo a connection string do MongoDB
        dump_mode: Modo de dump ('overwrite' ou 'append'). Se None, usa a configuração padrão da tabela
        frequency: Frequência para processamento por período ('D' para diário). Se None, usa a configuração padrão da tabela

    Raises:
        ValueError: Se table_id não estiver configurado em TABLE_CONFIGS

    Examples:
        Para processar a tabela cities:
        >>> rj_iplanrio__taxirio(table_id="cities")

        Para processar a tabela races com modo específico:
        >>> rj_iplanrio__taxirio(table_id="races", dump_mode="append")
    """
    # Renomear o flow run para facilitar identificação
    rename_current_flow_run_task(new_name=f"{table_id}")

    # Injetar credenciais do BigQuery
    inject_bd_credentials_task(environment="prod")

    # Validar e obter configuração da tabela
    if table_id not in TABLE_CONFIGS:
        raise ValueError(
            f"Tabela '{table_id}' não configurada. "
            f"Tabelas disponíveis: {list(TABLE_CONFIGS.keys())}"
        )

    config = TABLE_CONFIGS[table_id]

    # Usar configurações padrão se não fornecidas
    dump_mode = dump_mode or config.dump_mode
    frequency = frequency or config.frequency

    # Importar dinamicamente o módulo mongodb da tabela
    mongodb_module = importlib.import_module(f"pipelines.rj_iplanrio__taxirio.{table_id}.mongodb")
    schema = mongodb_module.schema

    # Obter conexão com MongoDB
    connection_string = get_mongodb_connection_string(
        secret_name=secret_name,
        secret_path=get_database_username_and_password_from_secret_task(
            infisical_secret_path=Constants.INFISICAL_SECRET_PATH.value
        ),
    )

    client = get_mongodb_client(connection=connection_string)

    collection = get_mongodb_collection(
        client=client,
        database=Constants.MONGODB_DATABASE_NAME.value,
        collection=table_id,
    )

    # Processar baseado no tipo de tabela
    if config.use_period:
        # Tabelas que precisam ser processadas por período (races, passengers)
        generate_pipeline = mongodb_module.generate_pipeline

        start_date, end_date = get_dates_for_dump_mode(
            dump_mode=dump_mode,
            collection=collection,
        )

        data_path = dump_collection_from_mongodb_per_period(
            collection=collection,
            path=path,
            generate_pipeline=generate_pipeline,
            schema=schema,
            freq=frequency,
            start_date=start_date,
            end_date=end_date,
            partition_cols=config.partition_cols,
        )
    else:
        # Tabelas com dump completo (cities, drivers, users, etc.)
        pipeline = mongodb_module.pipeline

        data_path = dump_collection_from_mongodb(
            collection=collection,
            path=path,
            schema=schema,
            pipeline=pipeline,
            partition_cols=config.partition_cols,
        )

    # Upload para BigQuery
    upload_table = create_table_and_upload_to_gcs_task(
        data_path=data_path,
        dataset_id=dataset_id,
        dump_mode=dump_mode,
        source_format="parquet",
        table_id=table_id,
    )

    return upload_table
