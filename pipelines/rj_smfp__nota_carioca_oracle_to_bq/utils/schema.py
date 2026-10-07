"""Schema do destino no BigQuery e conferência contra a tabela já existente."""

from dataclasses import dataclass

import pyarrow as pa

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import (
    AIRBYTE_EXTRACTED_AT,
    AIRBYTE_GENERATION_ID,
    AIRBYTE_META,
    RETIRED_COLUMNS,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn, bq_type, output_type

# O load de Parquet não aceita JSON: a coluna fica fora dos arquivos e o BigQuery a preenche com este DEFAULT.
META_DEFAULT_TEMPLATE = 'JSON \'{"changes":[],"sync_id":%d}\''


@dataclass(frozen=True)
class BqField:
    """Campo do schema da tabela de destino.

    :param name: Nome da coluna.
    :param field_type: Tipo do BigQuery (``STRING``, ``NUMERIC``, ``JSON``...).
    :param mode: ``REQUIRED`` ou ``NULLABLE``.
    :param default: ``DEFAULT`` da coluna, quando houver.
    """

    name: str
    field_type: str
    mode: str = "NULLABLE"
    default: str | None = None


@dataclass(frozen=True)
class SchemaChanges:
    """Diferença entre a tabela de destino atual e o schema novo.

    :param added: Colunas novas, aceitas.
    :param removed: Colunas que sumiram da origem, recusadas.
    :param changed: Colunas cujo tipo mudou, no formato ``COL: ANTES → DEPOIS``.
    """

    added: tuple[str, ...]
    removed: tuple[str, ...]
    changed: tuple[str, ...]


class SchemaChangeError(ValueError):
    """O schema novo remove coluna ou muda tipo em relação ao destino atual."""


class LayoutError(ValueError):
    """O particionamento ou o cluster da tabela final não pode ser reproduzido ou não bate com o da temporária."""


@dataclass(frozen=True)
class TableLayout:
    """Particionamento por tempo e cluster de uma tabela do BigQuery.

    O copy job ``WRITE_TRUNCATE`` só troca a tabela final por uma temporária de layout igual.

    :param partition_type: Granularidade da partição por tempo (``DAY``...); ``None`` se a tabela não é particionada.
    :param partition_field: Coluna da partição; ``None`` na partição por data de ingestão ou sem partição.
    :param clustering: Colunas de cluster, em ordem; vazio se não há cluster.
    """

    partition_type: str | None
    partition_field: str | None
    clustering: tuple[str, ...]

    def describe(self) -> str:
        """Descreve o layout para o log e as mensagens de erro."""
        partition = (
            "sem partição" if self.partition_type is None else f"{self.partition_type} em {self.partition_field}"
        )
        return f"partição {partition}; cluster {list(self.clustering)}"


@dataclass(frozen=True)
class TableState:
    """Estado de uma tabela do BigQuery lido da API.

    :param fields: Schema da tabela.
    :param layout: Particionamento e cluster.
    :param labels: Labels da tabela.
    :param num_rows: Linhas segundo os metadados.
    """

    fields: tuple[BqField, ...]
    layout: TableLayout
    labels: dict[str, str]
    num_rows: int


def meta_default(sync_id: int) -> str:
    """Monta o ``DEFAULT`` de ``_airbyte_meta``.

    :param sync_id: Identificador da carga, gravado em ``sync_id``.
    :returns: Expressão JSON do BigQuery.
    """
    return META_DEFAULT_TEMPLATE % sync_id


def build_fields(columns: tuple[OracleColumn, ...], sync_id: int) -> tuple[BqField, ...]:
    """Monta o schema completo da tabela: colunas ``_airbyte_*`` e as do Oracle.

    :param columns: Colunas da tabela de origem.
    :param sync_id: Identificador da carga, gravado em ``_airbyte_meta``.
    :returns: Campos na ordem de gravação.
    :raises NotImplementedError: Se alguma coluna tiver tipo não suportado.
    """
    airbyte = (
        BqField(AIRBYTE_EXTRACTED_AT, "TIMESTAMP", "REQUIRED"),
        BqField(AIRBYTE_META, "JSON", "REQUIRED", meta_default(sync_id)),
        BqField(AIRBYTE_GENERATION_ID, "INTEGER"),
    )
    return airbyte + tuple(BqField(column.name, bq_type(column)) for column in columns)


def parquet_schema(columns: tuple[OracleColumn, ...]) -> pa.Schema:
    """Monta o schema do Parquet: tudo de ``build_fields`` exceto ``_airbyte_meta``.

    As colunas ``REQUIRED`` são não nulas no Parquet, senão o load na tabela
    existente falha por mudança de modo.

    :param columns: Colunas da tabela de origem.
    :returns: Schema Arrow.
    """
    return pa.schema(
        [
            pa.field(AIRBYTE_EXTRACTED_AT, pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field(AIRBYTE_GENERATION_ID, pa.int64()),
            *[pa.field(column.name, output_type(column)) for column in columns],
        ]
    )


def diff_schemas(existing: tuple[BqField, ...], expected: tuple[BqField, ...]) -> SchemaChanges:
    """Compara, por nome, o schema da tabela atual com o novo.

    Colunas de ``RETIRED_COLUMNS`` que só existem no destino não contam como removidas: a pipeline deixou de
    gravá-las de propósito, e a troca por copy ``WRITE_TRUNCATE`` as tira da tabela final.

    :param existing: Campos da tabela de destino atual.
    :param expected: Campos do schema novo.
    :returns: Colunas acrescentadas, removidas e com tipo alterado.
    """
    current = {field.name: field.field_type for field in existing}
    wanted = {field.name: field.field_type for field in expected}
    return SchemaChanges(
        added=tuple(name for name in wanted if name not in current),
        removed=tuple(name for name in current if name not in wanted and name not in RETIRED_COLUMNS),
        changed=tuple(
            f"{name}: {current[name]} → {wanted[name]}"
            for name in wanted
            if name in current and current[name] != wanted[name]
        ),
    )


def assert_compatible(table_id: str, changes: SchemaChanges) -> None:
    """Recusa o schema novo se ele remover coluna ou mudar tipo.

    :param table_id: Nome da tabela, usado na mensagem.
    :param changes: Resultado de :func:`diff_schemas`.
    :raises SchemaChangeError: Se houver coluna removida ou tipo alterado.
    """
    if changes.removed or changes.changed:
        raise SchemaChangeError(
            f"{table_id}: o schema novo é incompatível com o destino atual; nada foi alterado no BigQuery. "
            f"Removidas: {list(changes.removed)}; tipo alterado: {list(changes.changed)}."
        )


def assert_layout_fields(table_id: str, layout: TableLayout, fields: tuple[BqField, ...]) -> None:
    """Confere que a coluna de partição e as de cluster existem no schema novo.

    :param table_id: Nome da tabela, usado na mensagem.
    :param layout: Layout que a tabela temporária vai ter.
    :param fields: Schema novo.
    :raises LayoutError: Se a coluna de partição ou alguma coluna de cluster não existir no schema novo.
    """
    names = {field.name for field in fields}
    needed = (() if layout.partition_field is None else (layout.partition_field,)) + layout.clustering
    missing = [name for name in needed if name not in names]
    if missing:
        raise LayoutError(
            f"{table_id}: a tabela final usa {layout.describe()}, mas {missing} não existe no schema novo; "
            "nada foi alterado no BigQuery."
        )


def assert_layouts_match(table_id: str, temp: TableLayout, final: TableLayout) -> None:
    """Confere que a temporária tem o mesmo particionamento e cluster da final, requisito do copy job.

    :param table_id: Nome da tabela, usado na mensagem.
    :param temp: Layout da tabela temporária.
    :param final: Layout da tabela final.
    :raises LayoutError: Se os layouts divergirem.
    """
    if temp != final:
        raise LayoutError(
            f"{table_id}: layout incompatível, temporária com {temp.describe()} e final com {final.describe()}; "
            "o copy job falharia e nenhuma tabela foi publicada."
        )
