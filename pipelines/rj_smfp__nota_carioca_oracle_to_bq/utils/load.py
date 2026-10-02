"""Etapas depois da extração: load, validação, troca das tabelas finais e limpeza."""

from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import TEMP_TABLE_SUFFIX
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import bigquery, gcs
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractResult
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, Snapshot, count_as_of_scn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.plan import TablePlan, assert_counts_match


@dataclass(frozen=True)
class Destination:
    """Onde as tabelas são carregadas.

    :param project: Projeto do BigQuery e do GCS.
    :param dataset_id: Dataset das tabelas finais e temporárias.
    :param bucket: Bucket dos arquivos Parquet.
    """

    project: str
    dataset_id: str
    bucket: str


def load_table(destination: Destination, plan: TablePlan, extracted: ExtractResult) -> int:
    """Cria a tabela temporária e carrega nela os Parquet da extração.

    :param destination: Projeto, dataset e bucket.
    :param plan: Plano da tabela.
    :param extracted: Resultado da extração.
    :returns: Linhas carregadas.
    """
    bigquery.recreate_temp_table(
        destination.project, destination.dataset_id, plan.temp_id, plan.fields, plan.cluster_fields
    )
    if extracted.files == 0:
        return 0
    uri = f"gs://{destination.bucket}/{extracted.prefix}/*.parquet"
    rows = bigquery.load_parquet(destination.project, destination.dataset_id, plan.temp_id, uri)
    bigquery.drop_meta_default(destination.project, destination.dataset_id, plan.temp_id)
    return rows


def validate_table(
    config: OracleConfig, destination: Destination, plan: TablePlan, extracted: ExtractResult, snapshot: Snapshot
) -> int:
    """Confere a contagem da tabela temporária contra o Oracle no SCN da foto.

    :param config: Conexão com o Oracle.
    :param destination: Projeto, dataset e bucket.
    :param plan: Plano da tabela.
    :param extracted: Resultado da extração.
    :param snapshot: Foto da carga.
    :returns: Linhas validadas.
    :raises CountMismatchError: Se as contagens divergirem.
    """
    oracle_rows = count_as_of_scn(config, plan.schema, plan.table_id, snapshot)
    bigquery_rows = bigquery.count_rows(destination.project, destination.dataset_id, plan.temp_id)
    assert_counts_match(plan.table_id, oracle_rows, bigquery_rows, extracted.rows)
    return bigquery_rows


def publish_tables(destination: Destination, plans: list[TablePlan]) -> None:
    """Troca cada tabela final pela temporária, só depois de todas validadas.

    :param destination: Projeto, dataset e bucket.
    :param plans: Planos das tabelas, todas já validadas.
    """
    for plan in plans:
        bigquery.publish_table(destination.project, destination.dataset_id, plan.temp_id, plan.table_id)


def cleanup(destination: Destination, plans: list[TablePlan], prefixes: list[str]) -> None:
    """Apaga as tabelas temporárias e os arquivos desta execução; nunca toca nas tabelas finais.

    :param destination: Projeto, dataset e bucket.
    :param plans: Planos das tabelas.
    :param prefixes: Prefixos dos arquivos desta execução no bucket.
    """
    for plan in plans:
        bigquery.delete_temp_table(destination.project, destination.dataset_id, plan.temp_id, TEMP_TABLE_SUFFIX)
    for prefix in prefixes:
        gcs.delete_prefix(destination.project, destination.bucket, prefix)
