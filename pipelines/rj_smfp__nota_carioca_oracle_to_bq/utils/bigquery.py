"""Operações no BigQuery: tabela temporária, load, troca por copy job e limpeza."""

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import AIRBYTE_EXTRACTED_AT, AIRBYTE_META, QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import TableProof
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import BqField
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)


def get_existing_fields(project: str, dataset_id: str, table_id: str) -> tuple[BqField, ...] | None:
    """Lê o schema da tabela de destino atual, se ela existir.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :returns: Campos da tabela, ou ``None`` se ela não existir.
    """
    try:
        table = bigquery.Client(project=project).get_table(f"{project}.{dataset_id}.{table_id}")
    except NotFound:
        return None
    return tuple(BqField(field.name, field.field_type, field.mode) for field in table.schema)


def recreate_temp_table(
    project: str, dataset_id: str, temp_id: str, fields: tuple[BqField, ...], cluster_fields: tuple[str, ...]
) -> None:
    """Cria a tabela temporária vazia, descartando restos de uma execução anterior.

    A tabela nasce particionada por dia em ``_airbyte_extracted_at`` e com o
    mesmo cluster da final, para que o copy job a troque sem reescrever dados.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param temp_id: Nome da tabela temporária (nunca o da final).
    :param fields: Schema completo.
    :param cluster_fields: Colunas de cluster.
    """
    client = bigquery.Client(project=project)
    table = bigquery.Table(
        f"{project}.{dataset_id}.{temp_id}",
        schema=[
            bigquery.SchemaField(field.name, field.field_type, mode=field.mode, default_value_expression=field.default)
            for field in fields
        ],
    )
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY, field=AIRBYTE_EXTRACTED_AT
    )
    table.clustering_fields = list(cluster_fields)
    client.delete_table(table, not_found_ok=True)
    client.create_table(table)


def load_parquet(project: str, dataset_id: str, temp_id: str, uri: str) -> int:
    """Carrega os Parquet do GCS na tabela temporária, sem custo de query.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param temp_id: Tabela temporária, já criada com o schema final.
    :param uri: URI com curinga dos arquivos.
    :returns: Linhas carregadas.
    """
    client = bigquery.Client(project=project)
    config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET, write_disposition=bigquery.WriteDisposition.WRITE_EMPTY
    )
    job = client.load_table_from_uri(uri, f"{project}.{dataset_id}.{temp_id}", job_config=config)
    job.result()
    return int(job.output_rows or 0)


def drop_meta_default(project: str, dataset_id: str, table_id: str) -> None:
    """Remove o ``DEFAULT`` de ``_airbyte_meta`` depois do load, para o schema final ficar igual ao do Airbyte.

    Os valores já foram gravados pelo load; o DDL só altera metadados.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Tabela temporária.
    """
    sql = load_query(
        QUERIES_ANCHOR,
        "drop_column_default",
        project=project,
        dataset_id=dataset_id,
        table_id=table_id,
        column=AIRBYTE_META,
    )
    bigquery.Client(project=project).query(sql).result()


def count_rows(project: str, dataset_id: str, table_id: str) -> int:
    """Lê a contagem de linhas da tabela pelos metadados (sem query).

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :returns: Número de linhas.
    """
    return int(bigquery.Client(project=project).get_table(f"{project}.{dataset_id}.{table_id}").num_rows or 0)


def stamp_table(project: str, dataset_id: str, temp_id: str, labels: dict[str, str], description: str) -> None:
    """Grava labels e descrição na tabela temporária, sem tocar nos dados.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param temp_id: Tabela temporária.
    :param labels: Labels a gravar (valores já no charset do BigQuery).
    :param description: Descrição legível da validação.
    """
    client = bigquery.Client(project=project)
    table = client.get_table(f"{project}.{dataset_id}.{temp_id}")
    table.labels = {**(table.labels or {}), **labels}
    table.description = description
    client.update_table(table, ["labels", "description"])


def read_proof(project: str, dataset_id: str, table_id: str) -> TableProof | None:
    """Lê os labels e a contagem de linhas da tabela, se ela existir.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :returns: Labels e linhas, ou ``None`` se a tabela não existir.
    """
    try:
        table = bigquery.Client(project=project).get_table(f"{project}.{dataset_id}.{table_id}")
    except NotFound:
        return None
    return TableProof(labels=dict(table.labels or {}), num_rows=int(table.num_rows or 0))


def publish_table(project: str, dataset_id: str, temp_id: str, final_id: str) -> None:
    """Troca a tabela final pela temporária com um copy job ``WRITE_TRUNCATE``.

    :param project: Projeto das tabelas.
    :param dataset_id: Dataset das tabelas.
    :param temp_id: Tabela temporária validada.
    :param final_id: Tabela final, substituída por inteiro (dados e schema).
    """
    client = bigquery.Client(project=project)
    config = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    client.copy_table(
        f"{project}.{dataset_id}.{temp_id}", f"{project}.{dataset_id}.{final_id}", job_config=config
    ).result()


def delete_temp_table(project: str, dataset_id: str, temp_id: str, suffix: str) -> None:
    """Apaga uma tabela temporária; recusa qualquer nome que não termine com o sufixo da pipeline.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param temp_id: Nome da tabela temporária.
    :param suffix: Sufixo que marca as tabelas temporárias.
    :raises PermissionError: Se o nome não terminar com ``suffix``.
    """
    if not temp_id.endswith(suffix):
        raise PermissionError(f"{temp_id} não termina com {suffix}; a pipeline só apaga tabelas temporárias.")
    bigquery.Client(project=project).delete_table(f"{project}.{dataset_id}.{temp_id}", not_found_ok=True)
