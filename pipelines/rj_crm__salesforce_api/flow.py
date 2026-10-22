"""
Flow genérico para ingestão de dados via Salesforce Marketing Cloud REST API.

Suporta rotas GET (com paginação automática) e POST /query (com paginação automática).
O método HTTP é controlado pelo parâmetro `http_method` ("get" ou "post").

Cada scheduler no prefect.yaml define uma rota, um método e um table_id distintos.
Os dados são salvos no BigQuery com duas colunas:
  - data          : JSON string com o conteúdo completo do registro
  - data_particao : data de execução (usada para particionamento)

Credenciais necessárias no Infisical (path: /salesforce_marketing_cloud):
  - sfmc_client_id
  - sfmc_client_secret
  - sfmc_auth_url
  - sfmc_rest_base_url
"""

import os
from pathlib import Path
from typing import Literal

from prefect import flow

from pipelines.rj_crm__salesforce_api.constants import APIConstants
from pipelines.rj_crm__salesforce_api.helpers import create_date_partitions
from pipelines.rj_crm__salesforce_api.tasks import build_dataframe, fetch_sfmc_route
from utils.tasks import publish_table_in_bq, upload_files_to_gcs
from utils.types import Cloud, Table
from utils.utils import capture_options


@flow(log_prints=True, flow_run_name="sfmc_api__{http_method}__{table_id}")
async def rj_crm__salesforce_api(
    *,
    cloud: Cloud,
    route: str,
    table_id: str,
    http_method: Literal["get", "post"] = "get",
    dataset_id: str | None = None,
    dump_mode: str | None = None,
    route_params: dict | None = None,
    query_params: dict | None = None,
    body_json: dict | None = None,
    page_size: int = 200,
):
    """
    Flow genérico para ingestão de qualquer rota da Salesforce Marketing Cloud REST API.

    Os dados retornados pela API são armazenados no BigQuery como uma tabela com
    duas colunas: `data` (JSON string com o conteúdo do registro) e
    `data_particao` (data de execução, usada para particionamento).

    Args:
        route: Rota relativa da API SFMC. Pode conter placeholders {chave}.
               Exemplos:
                 GET  → "/asset/v1/content/assets"
                 POST → "/asset/v1/content/assets/query"
        table_id: Nome da tabela de destino no BigQuery.
        http_method: Método HTTP da rota. "get" ou "post". Padrão: "get".
                     - "get"  : paginação automática via $page/$pageSize
                     - "post" : paginação automática via page.page/page.pageSize no body
        dataset_id: Dataset de destino no BigQuery. Padrão: "brutos_salesforce".
        dump_mode: Modo de ingestão BigQuery ("append" ou "overwrite"). Padrão: "append".
        route_params: Substituição de placeholders na rota.
                      Ex: {"assetId": "12345"} para "/asset/v1/content/assets/{assetId}".
        query_params: Query string parameters adicionais (apenas para GET).
                      Ex: {"scope": "Ours,Shared"}.
                      Não é necessário informar $page/$pageSize — gerenciados automaticamente.
        body_json: Body JSON para POST. Não é necessário informar page/pageSize —
                   gerenciados automaticamente pela paginação.
        page_size: Registros por página. Padrão: 200.
    """
    dataset_id = dataset_id or APIConstants.DATASET_ID.value
    dump_mode = dump_mode or APIConstants.DUMP_MODE.value
    root_folder = APIConstants.ROOT_FOLDER.value
    file_format = APIConstants.FILE_FORMAT.value
    partition_column = APIConstants.PARTITION_COLUMN.value


    # 1. Chamar a rota da SFMC REST API (GET ou POST, com paginação automática)
    records = fetch_sfmc_route(
        route=route,
        http_method=http_method,
        route_params=route_params,
        query_params=query_params,
        body_json=body_json,
        page_size=page_size,
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
    table = Table(dataset_id=dataset_id, table_id=table_id)
    await upload_files_to_gcs(
        Path(partitions_path),
        table,
        bucket=cloud["bucket"],
        dump_mode=dump_mode,
        source_format="csv",
        options=capture_options(dump_mode),
    )
    await publish_table_in_bq(table, cloud)
