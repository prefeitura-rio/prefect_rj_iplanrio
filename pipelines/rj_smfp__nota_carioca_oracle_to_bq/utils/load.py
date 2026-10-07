"""Etapas depois da extração: load, validação, marca de validação e limpeza."""

from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import TEMP_TABLE_SUFFIX
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import bigquery, gcs
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.checksum import assert_checksums_match
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractResult
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, Snapshot, count_as_of_scn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.parallel import encode_proof
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
    bigquery.recreate_temp_table(destination.project, destination.dataset_id, plan.temp_id, plan.fields, plan.layout)
    if extracted.files == 0:
        return 0
    uri = f"gs://{destination.bucket}/{extracted.prefix}/*.parquet"
    rows = bigquery.load_parquet(destination.project, destination.dataset_id, plan.temp_id, uri)
    bigquery.drop_meta_default(destination.project, destination.dataset_id, plan.temp_id)
    return rows


def validate_table(
    config: OracleConfig, destination: Destination, plan: TablePlan, extracted: ExtractResult, snapshot: Snapshot
) -> int:
    """Confere a contagem e o checksum de conteúdo da tabela temporária contra o Oracle e a extração.

    :param config: Conexão com o Oracle.
    :param destination: Projeto, dataset e bucket.
    :param plan: Plano da tabela.
    :param extracted: Resultado da extração.
    :param snapshot: Foto da carga.
    :returns: Linhas validadas.
    :raises CountMismatchError: Se as contagens divergirem.
    :raises ChecksumMismatchError: Se a contagem de não nulos ou a soma de uma coluna de checksum divergir.
    """
    oracle_rows = count_as_of_scn(config, plan.schema, plan.table_id, snapshot)
    bigquery_rows = bigquery.count_rows(destination.project, destination.dataset_id, plan.temp_id)
    assert_counts_match(plan.table_id, oracle_rows, bigquery_rows, extracted.rows)
    loaded = bigquery.read_checksums(destination.project, destination.dataset_id, plan.temp_id, plan.checksum_columns)
    assert_checksums_match(plan.table_id, extracted.checksums, loaded)
    return bigquery_rows


def stamp_validated(destination: Destination, plan: TablePlan, run_id: str, snapshot: Snapshot, rows: int) -> None:
    """Marca a tabela temporária validada com o run id do pai, o SCN e a contagem, para o pai conferir.

    :param destination: Projeto, dataset e bucket.
    :param plan: Plano da tabela.
    :param run_id: Flow run do pai.
    :param snapshot: Foto da carga.
    :param rows: Linhas validadas.
    """
    description = f"Validada para a execução {run_id} no SCN {snapshot.scn}: {rows} linhas."
    bigquery.stamp_table(
        destination.project, destination.dataset_id, plan.temp_id, encode_proof(run_id, snapshot.scn, rows), description
    )


def cleanup(destination: Destination, plans: list[TablePlan], prefixes: list[str], drop_temp: bool = True) -> None:
    """Apaga os arquivos desta execução e, se pedido, as tabelas temporárias; nunca toca nas tabelas finais.

    :param destination: Projeto, dataset e bucket.
    :param plans: Planos das tabelas.
    :param prefixes: Prefixos dos arquivos desta execução no bucket.
    :param drop_temp: Se ``False``, mantém as temporárias (um filho bem-sucedido as deixa para o pai publicar).
    """
    if drop_temp:
        for plan in plans:
            bigquery.delete_temp_table(destination.project, destination.dataset_id, plan.temp_id, TEMP_TABLE_SUFFIX)
    for prefix in prefixes:
        gcs.delete_prefix(destination.project, destination.bucket, prefix)
