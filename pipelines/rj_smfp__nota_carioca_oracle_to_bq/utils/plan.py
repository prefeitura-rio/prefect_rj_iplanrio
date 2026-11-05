"""Plano de carga de uma tabela: colunas, schema do BigQuery e conferências."""

from dataclasses import dataclass, replace

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import (
    AIRBYTE_EXTRACTED_AT,
    CHECKSUM_COLUMNS,
    CLUSTER_KEYS,
    TEMP_TABLE_SUFFIX,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils import bigquery
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.checksum import validate_checksum_columns
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, Snapshot, read_columns
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import (
    BqField,
    SchemaChanges,
    TableLayout,
    TableState,
    assert_compatible,
    assert_layout_fields,
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
    :param layout: Particionamento e cluster da tabela temporária: o da final se ela existe, senão o padrão.
    :param changes: Diferença em relação à tabela final atual; ``None`` se ela não existe.
    :param checksum_columns: Colunas ``NUMBER`` do checksum de conteúdo.
    """

    table_id: str
    schema: str
    columns: tuple[OracleColumn, ...]
    fields: tuple[BqField, ...]
    layout: TableLayout
    changes: SchemaChanges | None
    checksum_columns: tuple[str, ...] = ()

    @property
    def temp_id(self) -> str:
        """Retorna o nome da tabela temporária, sempre com o sufixo de proteção."""
        return f"{self.table_id}{TEMP_TABLE_SUFFIX}"


class CountMismatchError(RuntimeError):
    """A contagem do BigQuery difere da do Oracle na foto; a tabela final não é trocada."""


def cluster_fields_for(table_id: str) -> tuple[str, ...]:
    """Define o cluster padrão da tabela: a coluna-chave e ``_airbyte_extracted_at``.

    Só vale para tabela sem final; a existência da coluna é conferida por ``assert_layout_fields``.

    :param table_id: Nome da tabela.
    :returns: Colunas de cluster, na ordem do destino do Airbyte.
    :raises ValueError: Se a tabela não tiver cluster definido.
    """
    key = CLUSTER_KEYS.get(table_id)
    if key is None:
        raise ValueError(f"Sem cluster definido para {table_id}; tabelas conhecidas: {sorted(CLUSTER_KEYS)}")
    return (key, AIRBYTE_EXTRACTED_AT)


def default_layout(table_id: str) -> TableLayout:
    """Layout de uma tabela cuja final ainda não existe: partição por dia e cluster padrão.

    :param table_id: Nome da tabela.
    :returns: Partição ``DAY`` em ``_airbyte_extracted_at`` e cluster de :func:`cluster_fields_for`.
    """
    return TableLayout("DAY", AIRBYTE_EXTRACTED_AT, cluster_fields_for(table_id))


def plan_table(config: OracleConfig, source_schema: str, table_id: str, snapshot: Snapshot) -> TablePlan:
    """Lê as colunas no Oracle e monta o plano da tabela, sem tocar no BigQuery.

    :param config: Conexão com o Oracle.
    :param source_schema: Dono da tabela no Oracle.
    :param table_id: Nome da tabela.
    :param snapshot: Foto da carga; seu SCN vira o ``sync_id``.
    :returns: Plano sem a conferência com o destino (``changes`` nulo).
    :raises NotImplementedError: Se alguma coluna tiver tipo não suportado.
    :raises ChecksumColumnError: Se uma coluna de checksum não existir no Oracle ou não for ``NUMBER``.
    """
    columns = read_columns(config, source_schema, table_id)
    checksum_columns = CHECKSUM_COLUMNS.get(table_id)
    if checksum_columns is None:
        raise ValueError(f"Sem colunas de checksum definidas para {table_id}; conhecidas: {sorted(CHECKSUM_COLUMNS)}")
    validate_checksum_columns(table_id, columns, checksum_columns)
    return TablePlan(
        table_id=table_id,
        schema=source_schema,
        columns=columns,
        fields=build_fields(columns, snapshot.sync_id),
        layout=default_layout(table_id),
        changes=None,
        checksum_columns=checksum_columns,
    )


def resolve_destination(plan: TablePlan, existing: TableState | None) -> TablePlan:
    """Aplica ao plano a tabela final atual: diferença de schema e layout espelhado.

    Com a final existente, a temporária recebe exatamente o particionamento e o cluster dela; sem final, vale o
    layout padrão.

    :param plan: Plano da tabela, com o layout padrão.
    :param existing: Estado da tabela final, ou ``None`` se não existe.
    :returns: O plano com ``changes`` e ``layout`` definitivos; sem alteração se a final não existe.
    :raises SchemaChangeError: Se houver coluna removida ou tipo alterado.
    :raises LayoutError: Se a coluna de partição ou de cluster não existir no schema novo.
    """
    if existing is None:
        assert_layout_fields(plan.table_id, plan.layout, plan.fields)
        return plan
    changes = diff_schemas(existing.fields, plan.fields)
    assert_compatible(plan.table_id, changes)
    assert_layout_fields(plan.table_id, existing.layout, plan.fields)
    return replace(plan, layout=existing.layout, changes=changes)


def check_destination(project: str, dataset_id: str, plan: TablePlan) -> TablePlan:
    """Compara o schema novo com a tabela final atual e espelha o layout dela, antes de alterar qualquer coisa.

    :param project: Projeto do BigQuery.
    :param dataset_id: Dataset de destino.
    :param plan: Plano da tabela.
    :returns: O plano resolvido por :func:`resolve_destination`.
    :raises SchemaChangeError: Se houver coluna removida ou tipo alterado.
    :raises LayoutError: Se o layout da final não puder ser reproduzido.
    """
    return resolve_destination(plan, bigquery.read_table(project, dataset_id, plan.table_id))


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
