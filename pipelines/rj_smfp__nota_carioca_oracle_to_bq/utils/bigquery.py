"""Operações no BigQuery: layout das tabelas, tabela temporária, load, copy jobs e limpeza."""

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import AIRBYTE_META, QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.checksum import (
    ColumnChecksum,
    parse_checksum_row,
    render_checksum_select,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import BqField, LayoutError, TableLayout, TableState
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)


def read_layout(table: bigquery.Table) -> TableLayout:
    """Extrai particionamento por tempo e cluster dos metadados de uma tabela.

    :param table: Tabela lida da API.
    :returns: Layout da tabela.
    :raises LayoutError: Se a tabela for particionada por faixa de inteiros, o que a pipeline não reproduz.
    """
    if table.range_partitioning is not None:
        raise LayoutError(f"{table.table_id}: partição por faixa de inteiros não é suportada.")
    partitioning = table.time_partitioning
    return TableLayout(
        partition_type=None if partitioning is None else partitioning.type_,
        partition_field=None if partitioning is None else partitioning.field,
        clustering=tuple(table.clustering_fields or ()),
    )


def read_table(project: str, dataset_id: str, table_id: str) -> TableState | None:
    """Lê da API o schema, o layout, os labels e a contagem de linhas da tabela, se ela existir.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Nome da tabela.
    :returns: Estado da tabela, ou ``None`` se ela não existir.
    :raises LayoutError: Se o particionamento da tabela não for suportado.
    """
    try:
        table = bigquery.Client(project=project).get_table(f"{project}.{dataset_id}.{table_id}")
    except NotFound:
        return None
    return TableState(
        fields=tuple(BqField(field.name, field.field_type, field.mode) for field in table.schema),
        layout=read_layout(table),
        labels=dict(table.labels or {}),
        num_rows=int(table.num_rows or 0),
    )


def recreate_temp_table(
    project: str, dataset_id: str, temp_id: str, fields: tuple[BqField, ...], layout: TableLayout
) -> None:
    """Cria a tabela temporária vazia, descartando restos de uma execução anterior.

    A tabela nasce com o layout recebido, que espelha o da final, para que o
    copy job a troque: ele exige partição e cluster compatíveis.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param temp_id: Nome da tabela temporária (nunca o da final).
    :param fields: Schema completo.
    :param layout: Particionamento e cluster da tabela.
    """
    client = bigquery.Client(project=project)
    table = bigquery.Table(
        f"{project}.{dataset_id}.{temp_id}",
        schema=[
            bigquery.SchemaField(field.name, field.field_type, mode=field.mode, default_value_expression=field.default)
            for field in fields
        ],
    )
    if layout.partition_type is not None:
        table.time_partitioning = bigquery.TimePartitioning(type_=layout.partition_type, field=layout.partition_field)
    table.clustering_fields = list(layout.clustering) or None
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


def copy_over(project: str, dataset_id: str, source_id: str, final_id: str) -> None:
    """Substitui a tabela ``final_id`` por ``source_id`` com um copy job ``WRITE_TRUNCATE``.

    ``source_id`` pode levar um decorator de time travel (``TABELA@<ms>``): a referência é montada sem passar pelo
    parser de strings do client, que o recusaria. O copy a partir do snapshot mantém partição e cluster.

    :param project: Projeto das tabelas.
    :param dataset_id: Dataset das tabelas.
    :param source_id: Tabela de origem, com ou sem decorator.
    :param final_id: Tabela final, substituída por inteiro (dados e schema).
    """
    client = bigquery.Client(project=project)
    config = bigquery.CopyJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    source = bigquery.TableReference(bigquery.DatasetReference(project, dataset_id), source_id)
    client.copy_table(source, f"{project}.{dataset_id}.{final_id}", job_config=config).result()


def drop_table(project: str, dataset_id: str, table_id: str) -> None:
    """Apaga uma tabela final que a própria execução criou, ao desfazer uma publicação parcial.

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Tabela final criada nesta execução.
    """
    bigquery.Client(project=project).delete_table(f"{project}.{dataset_id}.{table_id}", not_found_ok=True)


def read_checksums(project: str, dataset_id: str, table_id: str, names: tuple[str, ...]) -> dict[str, ColumnChecksum]:
    """Lê contagem de não nulos e soma exata das colunas de checksum da tabela, numa só query.

    Só as colunas de checksum são lidas (custo de poucos GB).

    :param project: Projeto da tabela.
    :param dataset_id: Dataset da tabela.
    :param table_id: Tabela temporária.
    :param names: Colunas de checksum.
    :returns: Checksum de cada coluna.
    """
    sql = render_checksum_select(project, dataset_id, table_id, names)
    (row,) = bigquery.Client(project=project).query(sql).result()
    return parse_checksum_row(dict(row.items()), names)


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
