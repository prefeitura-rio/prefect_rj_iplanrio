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


def ensure_dataset(client: bigquery.Client, config: IngestionConfig) -> None:
    """Valide que o dataset de destino existe e está acessível."""
    dataset_id = f"{config.project_id}.{config.dataset_id}"
    try:
        client.get_dataset(dataset_id)
    except NotFound as error:
        raise ValueError(f"Dataset BigQuery não encontrado: {dataset_id}") from error


def _validate_and_evolve_table(
    client: bigquery.Client,
    existing: bigquery.Table,
    table: TableSpec,
) -> None:
    """Valide campos existentes e acrescente campos anuláveis ausentes."""
    expected = {field.name: field for field in build_schema(table.fields)}
    actual = {field.name: field for field in existing.schema}
    incompatible = [
        name
        for name, field in expected.items()
        if name in actual and (actual[name].field_type != field.field_type or actual[name].mode != field.mode)
    ]
    if incompatible:
        raise ValueError(f"Schema incompatível em {existing.full_table_id}: {', '.join(sorted(incompatible))}")

    missing = [field for name, field in expected.items() if name not in actual]
    non_nullable = [field.name for field in missing if field.mode == "REQUIRED"]
    if non_nullable:
        raise ValueError(f"Campos obrigatórios ausentes em {existing.full_table_id}: {', '.join(non_nullable)}")
    changed_fields: list[str] = []
    if missing:
        existing.schema = [*existing.schema, *missing]
        changed_fields.append("schema")

    partition_field = existing.time_partitioning.field if existing.time_partitioning else None
    if partition_field != table.partition_field:
        raise ValueError(
            f"Particionamento incompatível em {existing.full_table_id}: "
            f"esperado={table.partition_field}, atual={partition_field}"
        )

    expected_clustering = list(table.clustering_fields) or None
    if existing.clustering_fields != expected_clustering:
        existing.clustering_fields = expected_clustering
        changed_fields.append("clustering_fields")
    if changed_fields:
        client.update_table(existing, changed_fields)


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
        existing = client.get_table(full_id)
        _validate_and_evolve_table(client, existing, table)
        return
    except NotFound:
        bigquery_table = bigquery.Table(full_id, schema=build_schema(table.fields))
        bigquery_table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=table.partition_field,
        )
        bigquery_table.clustering_fields = list(table.clustering_fields) or None
        client.create_table(bigquery_table, exists_ok=True)


def get_processed_package_file_ids(
    client: bigquery.Client,
    config: IngestionConfig,
    aggregate_tables: tuple[TableSpec, TableSpec],
) -> set[int]:
    """Leia os IDs de arquivos que já foram persistidos nas tabelas agregadas."""
    selects = [
        f"SELECT package_file_id FROM `{config.project_id}.{config.dataset_id}.{table.name}` "
        "WHERE package_file_id IS NOT NULL"
        for table in aggregate_tables
    ]
    rows = client.query(" UNION DISTINCT ".join(selects)).result()
    return {int(row.package_file_id) for row in rows}


def _render_merge(
    config: IngestionConfig,
    table: TableSpec,
    staging: str,
    merge_template: str,
) -> str:
    columns = [name for name, _, _ in table.fields]
    return Template(merge_template).substitute(
        target=f"`{config.project_id}.{config.dataset_id}.{table.name}`",
        staging=staging,
        key=table.key,
        updates=", ".join(f"T.{column} = S.{column}" for column in columns if column != table.key),
        inserts=", ".join(columns),
        values=", ".join(f"S.{column}" for column in columns),
    )


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
    try:
        client.load_table_from_json(rows, staging, job_config=job_config).result()
        query = _render_merge(config, table, staging, merge_template)
        client.query(query).result()
    finally:
        client.delete_table(staging, not_found_ok=True)
    return len(rows)


def upsert_artifact(  # noqa: PLR0913 - operação transacional exige as duas tabelas e seus dados
    client: bigquery.Client,
    config: IngestionConfig,
    aggregate_table: TableSpec,
    detail_table: TableSpec,
    aggregate: dict[str, Any],
    details: list[dict[str, Any]],
    merge_template: str,
) -> tuple[int, int]:
    """Grave agregado e detalhes do artefato em uma única transação BigQuery."""
    suffix = uuid.uuid4().hex
    aggregate_staging = f"{config.project_id}.{config.dataset_id}.agent_quality_agg_stg_{suffix}"
    detail_staging = f"{config.project_id}.{config.dataset_id}.agent_quality_detail_stg_{suffix}"
    staging_tables = [aggregate_staging]
    try:
        aggregate_config = bigquery.LoadJobConfig(
            schema=build_schema(aggregate_table.fields),
            write_disposition="WRITE_TRUNCATE",
        )
        client.load_table_from_json([aggregate], aggregate_staging, job_config=aggregate_config).result()
        aggregate_merge = _render_merge(config, aggregate_table, aggregate_staging, merge_template)

        statements = [
            "BEGIN TRANSACTION",
            aggregate_merge,
            (
                f"DELETE FROM `{config.project_id}.{config.dataset_id}.{detail_table.name}` "
                "WHERE release_key = @release_key"
            ),
        ]
        if details:
            staging_tables.append(detail_staging)
            detail_config = bigquery.LoadJobConfig(
                schema=build_schema(detail_table.fields),
                write_disposition="WRITE_TRUNCATE",
            )
            client.load_table_from_json(details, detail_staging, job_config=detail_config).result()
            statements.append(_render_merge(config, detail_table, detail_staging, merge_template))
        statements.append("COMMIT TRANSACTION")
        query = ";\n".join(statements) + ";"
        query_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("release_key", "STRING", aggregate["release_key"]),
            ]
        )
        client.query(query, job_config=query_config).result()
    finally:
        for staging in staging_tables:
            client.delete_table(staging, not_found_ok=True)
    return 1, len(details)
