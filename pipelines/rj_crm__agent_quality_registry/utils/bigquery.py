"""Execute operações de persistência no BigQuery."""

import uuid
from string import Template
from typing import Any

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from iplanrio.pipelines_utils.env import get_bd_credentials_from_env

from pipelines.rj_crm__agent_quality_registry.constants import SchemaFields
from pipelines.rj_crm__agent_quality_registry.utils.schemas import IngestionConfig, TableSpec


def get_client(config: IngestionConfig) -> bigquery.Client:
    """Crie um cliente BigQuery com as credenciais injetadas no ambiente.

    :param config: Configuração de destino da ingestão.
    :returns: Cliente BigQuery autenticado.
    """
    credentials = get_bd_credentials_from_env(mode=config.environment)
    return bigquery.Client(credentials=credentials, project=config.project_id)


def build_schema(fields: SchemaFields) -> list[bigquery.SchemaField]:
    """Converta a definição simplificada para o schema BigQuery.

    :param fields: Nome, tipo e modo de cada campo.
    :returns: Campos compatíveis com a API BigQuery.
    """
    return [bigquery.SchemaField(name, field_type, mode=mode) for name, field_type, mode in fields]


def ensure_table(
    client: bigquery.Client,
    config: IngestionConfig,
    table: TableSpec,
) -> None:
    """Crie uma tabela particionada quando ela ainda não existir.

    :param client: Cliente BigQuery autenticado.
    :param config: Configuração de destino da ingestão.
    :param table: Definição da tabela a garantir.
    """
    full_id = f"{config.project_id}.{config.dataset_id}.{table.name}"
    try:
        client.get_table(full_id)
        return
    except NotFound:
        bigquery_table = bigquery.Table(full_id, schema=build_schema(table.fields))
        bigquery_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=table.partition_field,
        )
        client.create_table(bigquery_table, exists_ok=True)


def upsert_rows(
    client: bigquery.Client,
    config: IngestionConfig,
    table: TableSpec,
    rows: list[dict[str, Any]],
    merge_template: str,
) -> int:
    """Faça upsert de linhas por meio de uma tabela temporária BigQuery.

    :param client: Cliente BigQuery autenticado.
    :param config: Configuração de destino da ingestão.
    :param table: Definição da tabela de destino.
    :param rows: Linhas serializáveis a persistir.
    :param merge_template: Template SQL carregado do diretório ``queries/``.
    :returns: Quantidade de linhas submetidas ao BigQuery.
    """
    if not rows:
        return 0
    staging = f"{config.project_id}.{config.dataset_id}.agent_quality_stg_{uuid.uuid4().hex}"
    job_config = bigquery.LoadJobConfig(schema=build_schema(table.fields), write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_json(rows, staging, job_config=job_config)
    job.result()
    columns = [name for name, _, _ in table.fields]
    query = Template(merge_template).substitute(
        target=f"`{config.project_id}.{config.dataset_id}.{table.name}`",
        staging=staging,
        key=table.key,
        updates=", ".join(f"T.{column} = S.{column}" for column in columns if column != table.key),
        inserts=", ".join(columns),
        values=", ".join(f"S.{column}" for column in columns),
    )
    try:
        client.query(query).result()
    finally:
        client.delete_table(staging, not_found_ok=True)
    return len(rows)
