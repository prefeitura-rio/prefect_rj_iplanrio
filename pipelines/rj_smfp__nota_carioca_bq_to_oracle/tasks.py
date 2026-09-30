"""Tasks da carga BigQuery → GCS → SQL*Loader → Oracle."""

from prefect import task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run

from iplanrio.pipelines_utils.logging import log
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils import bigquery, oracle, sqlldr
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import LoadPlan, build_load_plan


@task
def list_tables_task(project: str, dataset_id: str, table_ids: list[str] | None) -> list[str]:
    """Retorna as tabelas pedidas ou, se nenhuma for informada, todas as do dataset."""
    return table_ids or bigquery.list_tables(project=project, dataset_id=dataset_id)


@task
def get_table_schema_task(project: str, dataset_id: str, table_id: str) -> dict[str, object]:
    """Lê o schema e a contagem de linhas da tabela no BigQuery."""
    return bigquery.get_table_schema(project=project, dataset_id=dataset_id, table_id=table_id)


@task(cache_policy=NO_CACHE)
def plan_load_task(
    infisical_secret_path: str, template_schema: str | None, table_id: str, table_schema: dict[str, object]
) -> LoadPlan:
    """Monta o plano de carga a partir da tabela original no Oracle e do schema do BigQuery."""
    config = oracle.read_oracle_config(infisical_secret_path)
    template = oracle.fetch_template_columns(
        config=config,
        template_schema=oracle.validate_identifier(template_schema or config.schema),
        table=oracle.validate_identifier(table_id),
    )
    return build_load_plan(table_schema["fields"], template)


@task(cache_policy=NO_CACHE)
def ensure_oracle_table_task(
    infisical_secret_path: str, project: str, dataset_id: str, table_id: str, plan: LoadPlan
) -> str:
    """Cria ou confere a tabela de destino e retorna o nome dela no Oracle."""
    config = oracle.read_oracle_config(infisical_secret_path)
    table = oracle.oracle_table_name(table_id)
    action = oracle.ensure_table(
        config=config, table=table, columns=plan.columns, source=f"{project}.{dataset_id}.{table_id}"
    )
    log(action)
    return table


@task(cache_policy=NO_CACHE)
def extract_table_to_gcs_task(project: str, dataset_id: str, table_id: str, bucket: str) -> list[str]:
    """Exporta a tabela para um prefixo exclusivo deste flow run no GCS."""
    prefix = f"{dataset_id}/{table_id}/{flow_run.id}"
    return bigquery.extract_table_to_gcs(
        project=project, dataset_id=dataset_id, table_id=table_id, bucket=bucket, prefix=prefix
    )


@task(cache_policy=NO_CACHE)
def truncate_oracle_table_task(infisical_secret_path: str, table: str) -> str:
    """Esvazia a tabela de destino e retorna o nome dela."""
    oracle.truncate_table(config=oracle.read_oracle_config(infisical_secret_path), table=table)
    return table


@task(cache_policy=NO_CACHE)
def load_into_oracle_task(  # noqa: PLR0913
    infisical_secret_path: str,
    table: str,
    plan: LoadPlan,
    bucket: str,
    blob_names: list[str],
    sessions: int,
) -> int:
    """Carrega os arquivos exportados com SQL*Loader direct path e retorna as linhas carregadas."""
    job = sqlldr.LoadJob(
        table=table,
        fields=plan.fields,
        bucket=bucket,
        blob_names=blob_names,
        sessions=sessions,
    )
    return sqlldr.load_from_gcs(config=oracle.read_oracle_config(infisical_secret_path), job=job)


@task(cache_policy=NO_CACHE)
def validate_row_count_task(
    infisical_secret_path: str, table: str, table_schema: dict[str, object], loaded_rows: int
) -> int:
    """Confere se o BigQuery, o SQL*Loader e o Oracle têm o mesmo número de linhas.

    :raises ValueError: Se as contagens divergirem.
    """
    expected = int(table_schema["num_rows"])
    oracle_rows = oracle.count_rows(config=oracle.read_oracle_config(infisical_secret_path), table=table)
    if not expected == loaded_rows == oracle_rows:
        raise ValueError(f"{table}: BigQuery={expected}, SQL*Loader={loaded_rows}, Oracle={oracle_rows}")
    return oracle_rows


@task(cache_policy=NO_CACHE)
def delete_gcs_files_task(project: str, bucket: str, blob_names: list[str]) -> None:
    """Remove do GCS os arquivos exportados."""
    bigquery.delete_blobs(project=project, bucket=bucket, blob_names=blob_names)
