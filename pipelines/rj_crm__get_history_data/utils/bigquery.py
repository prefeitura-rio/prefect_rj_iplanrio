# -*- coding: utf-8 -*-
"""
Funções auxiliares (não-task) para leitura/escrita das tabelas históricas
no BigQuery.
"""

from typing import List

import pandas as pd
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env
from iplanrio.pipelines_utils.logging import log


def historico_table_schema() -> List[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("de_nome", "STRING"),
        bigquery.SchemaField("telefone", "STRING"),
        bigquery.SchemaField("id_de", "STRING"),
        bigquery.SchemaField("entrada_data", "STRING"),
        bigquery.SchemaField("dados_json", "STRING"),
        bigquery.SchemaField("data_particao", "DATE"),
    ]


def get_bq_client(project_id: str) -> bigquery.Client:
    """Cria um cliente BQ autenticado com as credenciais de produção."""
    credentials = get_bd_credentials_from_env(mode="prod")
    return bigquery.Client(credentials=credentials, project=project_id)


def ensure_historico_tables(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    tmp_table_id: str,
    final_table_id: str,
) -> None:
    """
    Cria (se não existirem) a tabela temporária e a tabela final, ambas
    nativas do BigQuery, particionadas por mês (data_particao) e
    clusterizadas por de_nome.

    Precisam ser tabelas nativas (não BigLake/externas) porque a tabela final
    recebe MERGE, e o BigQuery não permite DML em tabelas externas/BigLake.
    """
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)

    for table_id in (tmp_table_id, final_table_id):
        table = bigquery.Table(dataset_ref.table(table_id), schema=historico_table_schema())
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH, field="data_particao"
        )
        table.clustering_fields = ["de_nome"]
        client.create_table(table, exists_ok=True)
        log(f"Tabela garantida: {project_id}.{dataset_id}.{table_id}")


def truncate_and_load_tmp_table(
    client: bigquery.Client,
    df: pd.DataFrame,
    project_id: str,
    dataset_id: str,
    tmp_table_id: str,
) -> None:
    """Trunca a tabela temporária e carrega nela os dados recentes já filtrados."""
    table_ref = bigquery.DatasetReference(project_id, dataset_id).table(tmp_table_id)
    job_config = bigquery.LoadJobConfig(
        schema=historico_table_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    log(f"{job.output_rows} linha(s) carregada(s) em {project_id}.{dataset_id}.{tmp_table_id} (truncate+load).")
