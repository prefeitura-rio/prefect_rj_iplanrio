"""Plano de carga de uma tabela: colunas, schema do BigQuery e conferências."""

from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import AIRBYTE_EXTRACTED_AT, CLUSTER_KEYS, TEMP_TABLE_SUFFIX
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import bigquery
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, Snapshot, read_columns
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import (
    BqField,
    SchemaChanges,
    assert_compatible,
    build_fields,
    diff_schemas,
)


@dataclass(frozen=True)
class TablePlan:
    """Tudo que se decide antes de tocar no BigQuery.

    :param table_id: Nome da tabela, igual no Oracle e no BigQuery.
    :param schema: Dono da tabela no Oracle.
    :param columns: Colunas lidas do dicionário do Oracle.
    :param fields: Schema completo da tabela no BigQuery.
    :param cluster_fields: Colunas de cluster.
    :param changes: Diferença em relação à tabela final atual; ``None`` se ela não existe.
    """

    table_id: str
    schema: str
    columns: tuple[OracleColumn, ...]
    fields: tuple[BqField, ...]
    cluster_fields: tuple[str, ...]
    changes: SchemaChanges | None

    @property
    def temp_id(self) -> str:
        """Retorna o nome da tabela temporária, sempre com o sufixo de proteção."""
        return f"{self.table_id}{TEMP_TABLE_SUFFIX}"


class CountMismatchError(RuntimeError):
    """A contagem do BigQuery difere da do Oracle na foto; a tabela final não é trocada."""


def cluster_fields_for(table_id: str, columns: tuple[OracleColumn, ...]) -> tuple[str, ...]:
    """Define o cluster da tabela: a coluna-chave e ``_airbyte_extracted_at``.

    :param table_id: Nome da tabela.
    :param columns: Colunas da tabela.
    :returns: Colunas de cluster, na ordem do destino atual.
    :raises ValueError: Se a tabela não tiver cluster definido ou a coluna-chave não existir.
    """
    key = CLUSTER_KEYS.get(table_id)
    if key is None:
        raise ValueError(f"Sem cluster definido para {table_id}; tabelas conhecidas: {sorted(CLUSTER_KEYS)}")
    if key not in {column.name for column in columns}:
        raise ValueError(f"Coluna de cluster {key} não existe em {table_id}.")
    return (key, AIRBYTE_EXTRACTED_AT)


def plan_table(config: OracleConfig, source_schema: str, table_id: str, snapshot: Snapshot) -> TablePlan:
    """Lê as colunas no Oracle e monta o plano da tabela, sem tocar no BigQuery.

    :param config: Conexão com o Oracle.
    :param source_schema: Dono da tabela no Oracle.
    :param table_id: Nome da tabela.
    :param snapshot: Foto da carga; seu SCN vira o ``sync_id``.
    :returns: Plano sem a conferência com o destino (``changes`` nulo).
    :raises NotImplementedError: Se alguma coluna tiver tipo não suportado.
    """
    columns = read_columns(config, source_schema, table_id)
    return TablePlan(
        table_id=table_id,
        schema=source_schema,
        columns=columns,
        fields=build_fields(columns, snapshot.sync_id),
        cluster_fields=cluster_fields_for(table_id, columns),
        changes=None,
    )


def check_destination(project: str, dataset_id: str, plan: TablePlan) -> TablePlan:
    """Compara o schema novo com a tabela final atual, antes de alterar qualquer coisa.

    :param project: Projeto do BigQuery.
    :param dataset_id: Dataset de destino.
    :param plan: Plano da tabela.
    :returns: O plano com ``changes`` preenchido, ou nulo se a final não existe.
    :raises SchemaChangeError: Se houver coluna removida ou tipo alterado.
    """
    existing = bigquery.get_existing_fields(project, dataset_id, plan.table_id)
    if existing is None:
        return plan
    changes = diff_schemas(existing, plan.fields)
    assert_compatible(plan.table_id, changes)
    return TablePlan(plan.table_id, plan.schema, plan.columns, plan.fields, plan.cluster_fields, changes)


def assert_counts_match(table_id: str, oracle_rows: int, bigquery_rows: int, extracted_rows: int) -> None:
    """Confere que Oracle (``AS OF SCN``), arquivos extraídos e BigQuery têm a mesma contagem.

    :param table_id: Nome da tabela.
    :param oracle_rows: ``COUNT(*)`` no Oracle no SCN da foto.
    :param bigquery_rows: Linhas da tabela temporária.
    :param extracted_rows: Linhas gravadas nos Parquet.
    :raises CountMismatchError: Se qualquer contagem divergir.
    """
    if not oracle_rows == bigquery_rows == extracted_rows:
        raise CountMismatchError(
            f"{table_id}: Oracle {oracle_rows:,} linhas, extraídas {extracted_rows:,}, "
            f"BigQuery {bigquery_rows:,}; a tabela final não foi alterada."
        )
