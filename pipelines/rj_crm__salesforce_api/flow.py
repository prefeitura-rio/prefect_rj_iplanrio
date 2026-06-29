# -*- coding: utf-8 -*-
"""
Flow genérico para ingestão de dados via rotas GET da Salesforce Marketing Cloud REST API.

Cada scheduler no prefect.yaml define uma rota e um table_id distintos.
Os dados são salvos no BigQuery no formato:
  - dataset_id (padrão: brutos_salesforce)
  - table_id   (definido por parâmetro)
  - Colunas: data (JSON string), data_particao (YYYY-MM-DD)

"""

import os

from iplanrio.pipelines_utils.bd import create_table_and_upload_to_gcs_task
from iplanrio.pipelines_utils.env import inject_bd_credentials_task
from iplanrio.pipelines_utils.prefect import rename_current_flow_run_task
from prefect import flow

from pipelines.rj_crm__salesforce_api.constants import APIConstants
from pipelines.rj_crm__salesforce_api.tasks import build_dataframe, fetch_sfmc_route
from pipelines.rj_crm__disparo_template.utils.tasks import create_date_partitions


@flow(log_prints=True)
def rj_crm__salesforce_api(
    route: str,
    table_id: str,
    dataset_id: str | None = None,
    dump_mode: str | None = None,
    route_params: dict | None = None,
    query_params: dict | None = None,
    infisical_secret_path: str | None = None,
):
    """
    Flow genérico para ingestão de qualquer rota GET da Salesforce Marketing Cloud REST API.

    Os dados retornados pela API são armazenados no BigQuery como uma tabela com
    duas colunas: `data` (JSON string com o conteúdo do registro) e
    `data_particao` (data de execução, usada para particionamento).

    Args:
        route: Rota relativa da API SFMC. Pode conter placeholders {chave}.
               Ex: "/asset/v1/content/assets/{assetId}"
        table_id: Nome da tabela de destino no BigQuery.
        dataset_id: Dataset de destino. Padrão: "brutos_salesforce".
        dump_mode: Modo de ingestão BigQuery ("append" ou "overwrite"). Padrão: "append".
        route_params: Dicionário para substituição de placeholders na rota.
                      Ex: {"assetId": "12345"}
        query_params: Query string parameters adicionais.
                      Ex: {"$page": 1, "$pageSize": 50}
        infisical_secret_path: Path no Infisical para as credenciais da SFMC.
                               Padrão: "/salesforce_marketing_cloud".
    """
    dataset_id = dataset_id or APIConstants.DATASET_ID.value
    dump_mode = dump_mode or APIConstants.DUMP_MODE.value
    infisical_secret_path = infisical_secret_path or APIConstants.INFISICAL_SECRET_PATH.value
    root_folder = APIConstants.ROOT_FOLDER.value
    file_format = APIConstants.FILE_FORMAT.value
    partition_column = APIConstants.PARTITION_COLUMN.value

    rename_current_flow_run_task(new_name=f"sfmc_api__{table_id}__{dataset_id}")
    inject_bd_credentials_task(environment="prod")

    # 1. Chamar a rota GET da SFMC REST API
    records = fetch_sfmc_route(
        route=route,
        route_params=route_params,
        query_params=query_params,
    )

    # 2. Montar DataFrame com colunas 'data' e 'data_particao'
    df = build_dataframe(records=records)

    # 3. Criar partições por data
    partitions_path = create_date_partitions(
        dataframe=df,
        partition_column=partition_column,
        file_format=file_format,
        root_folder=f"{root_folder}/{table_id}",
    )

    print(f"[SFMC] Partitions path gerado: {partitions_path}")
    if os.path.exists(partitions_path):
        files_in_path = []
        for root, dirs, files in os.walk(partitions_path):
            files_in_path.extend([os.path.join(root, f) for f in files])
        print(f"[SFMC] Arquivos gerados: {files_in_path}")

    # 4. Upload para GCS / BigQuery
    create_table_and_upload_to_gcs_task(
        data_path=partitions_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=False,
    )
