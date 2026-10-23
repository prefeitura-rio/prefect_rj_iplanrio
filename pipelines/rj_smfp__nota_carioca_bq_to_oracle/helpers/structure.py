"""Estrutura física da tabela original (tablespace, partições e índices) e o DDL equivalente na tabela carregada."""

from dataclasses import replace

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_ddl import describe_partitioning
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import (
    FUNCTION_BASED_INDEX_TYPE,
    INDEX_NAME_PATTERN,
    INMEMORY_COMPRESSIONS,
    INMEMORY_DISTRIBUTES,
    INMEMORY_DUPLICATES,
    INMEMORY_PRIORITIES,
    LOADED_TABLE_INMEMORY,
    SNAPSHOT_INDEX_PREFIX,
    SUPPORTED_INDEX_TYPES,
    SUPPORTED_PARTITIONING,
    IndexDefinition,
    IndexExpression,
    Partitioning,
    StructurePlan,
    TableLayout,
    TablePartition,
)


def partitioning_from_dictionary(
    table_row: dict[str, object], key_columns: list[str], partition_rows: list[dict[str, object]]
) -> Partitioning:
    """Monta o particionamento a partir das linhas do dicionário do Oracle.

    :param table_row: Linha de ``all_part_tables`` (``partitioning_type``,
        ``subpartitioning_type`` e ``interval``).
    :param key_columns: Colunas de ``all_part_key_columns``, na ordem.
    :param partition_rows: Linhas de ``all_tab_partitions`` (``partition_name``,
        ``high_value``, ``tablespace_name`` e ``interval``), na ordem.
    :returns: Particionamento, sem as partições criadas pelo ``INTERVAL``.
    """
    interval = str(table_row["interval"]).strip() if table_row["interval"] else None
    partitions = tuple(
        TablePartition(
            name=str(row["partition_name"]),
            high_value=str(row["high_value"]).strip(),
            tablespace=None if row["tablespace_name"] is None else str(row["tablespace_name"]),
        )
        for row in partition_rows
        if row["interval"] != "YES"
    )
    subpartitioning = table_row["subpartitioning_type"]
    kind = str(table_row["partitioning_type"])
    if subpartitioning not in (None, "NONE"):
        kind = f"{kind}-{subpartitioning}"
    return Partitioning(kind=kind, key_columns=tuple(key_columns), interval=interval, partitions=partitions)


def inmemory_option(row: dict[str, object], key: str, allowed: tuple[str, ...]) -> str | None:
    """Lê uma opção do In-Memory e confere se ela pode entrar no DDL.

    :param row: Linha do dicionário do Oracle.
    :param key: Nome da coluna.
    :param allowed: Valores aceitos.
    :returns: O valor, ou ``None`` se a coluna for nula.
    :raises NotImplementedError: Se o valor não for aceito.
    """
    value = row[key]
    if value is None:
        return None
    if value not in allowed:
        raise NotImplementedError(f"INMEMORY: {key} = {value!r} sem suporte na carga.")
    return str(value)


def inmemory_from_dictionary(row: dict[str, object]) -> str | None:
    """Monta a cláusula ``INMEMORY`` a partir do dicionário do Oracle.

    Em tabela particionada, a linha deve trazer o padrão das partições
    (``all_part_tables.def_inmemory*``), que as partições declaradas e as do
    ``INTERVAL`` herdam.

    :param row: Linha com ``inmemory``, ``inmemory_priority``,
        ``inmemory_compression``, ``inmemory_distribute`` e ``inmemory_duplicate``.
    :returns: Por exemplo ``INMEMORY PRIORITY HIGH MEMCOMPRESS FOR QUERY HIGH
        DISTRIBUTE AUTO NO DUPLICATE``, ou ``None`` se o In-Memory não estiver
        ativo.
    :raises NotImplementedError: Se alguma opção tiver valor desconhecido.
    """
    if row["inmemory"] != "ENABLED":
        return None
    priority = inmemory_option(row, "inmemory_priority", INMEMORY_PRIORITIES)
    compression = inmemory_option(row, "inmemory_compression", INMEMORY_COMPRESSIONS)
    distribute = inmemory_option(row, "inmemory_distribute", INMEMORY_DISTRIBUTES)
    duplicate = inmemory_option(row, "inmemory_duplicate", INMEMORY_DUPLICATES)
    parts = ["INMEMORY"]
    if priority:
        parts.append(f"PRIORITY {priority}")
    if compression:
        parts.append(compression if compression == "NO MEMCOMPRESS" else f"MEMCOMPRESS {compression}")
    if distribute:
        parts.append(f"DISTRIBUTE {distribute}")
    if duplicate:
        parts.append(duplicate)
    return " ".join(parts)


def index_key(row: dict[str, object]) -> str:
    """Lê a chave de uma posição do índice a partir do dicionário do Oracle.

    Nas posições de um índice baseado em função, ``all_ind_columns`` traz só um
    nome oculto (``SYS_NC...$``); a expressão vem de ``all_ind_expressions``. Uma
    coluna descendente também é guardada como expressão, e o ``DESC`` entra no texto.

    :param row: Linha com ``column_name``, ``descend`` e ``column_expression``
        (nulo se a posição for uma coluna comum).
    :returns: Nome da coluna, ou :class:`IndexExpression` com a expressão.
    """
    expression = row["column_expression"]
    if expression is None:
        return str(row["column_name"])
    text = str(expression).strip()
    return IndexExpression(f"{text} DESC" if row["descend"] == "DESC" else text)


def index_from_dictionary(row: dict[str, object], columns: list[str]) -> IndexDefinition:
    """Monta a definição de um índice a partir das linhas do dicionário do Oracle.

    :param row: Linha com ``owner``, ``index_name``, ``index_type``,
        ``uniqueness``, ``locality``, ``tablespace_name``, ``degree`` e ``status``.
    :param columns: Chaves do índice, na ordem: nomes de coluna ou
        :class:`IndexExpression` nas posições com expressão.
    :returns: Definição do índice.
    """
    return IndexDefinition(
        name=str(row["index_name"]),
        index_type=str(row["index_type"]),
        unique=row["uniqueness"] == "UNIQUE",
        columns=tuple(columns),
        locality=None if row["locality"] is None else str(row["locality"]),
        tablespace=None if row["tablespace_name"] is None else str(row["tablespace_name"]),
        degree=str(row["degree"] or "").strip(),
        status=str(row["status"] or ""),
        owner=str(row["owner"] or ""),
    )


def plan_structure(template: TableLayout, loaded_columns: list[str], index_prefix: str) -> StructurePlan:
    """Define a estrutura da tabela carregada a partir da tabela original.

    Tablespace e particionamento são copiados, e a tabela vai sempre para o
    In-Memory (``LOADED_TABLE_INMEMORY``). Os índices são copiados com o
    nome ``<prefixo><nome original>``, exceto os ``I_SNAP$`` de views
    materializadas.

    :param template: Estrutura da tabela original.
    :param loaded_columns: Colunas criadas na tabela carregada.
    :param index_prefix: Prefixo dos índices da tabela carregada.
    :returns: Estrutura da tabela carregada e índices da original não replicados.
    :raises NotImplementedError: Se o particionamento ou algum índice não tiver
        suporte na carga.
    :raises ValueError: Se a chave de partição ou um índice usar coluna que não é
        carregada, ou se o nome de um índice ficar inválido com o prefixo.
    """
    available = set(loaded_columns)
    partitioning = template.partitioning
    if partitioning is not None:
        if partitioning.kind not in SUPPORTED_PARTITIONING:
            raise NotImplementedError(f"Particionamento {partitioning.kind} sem suporte na carga.")
        missing_keys = [name for name in partitioning.key_columns if name not in available]
        if missing_keys:
            raise ValueError(f"A chave de partição da original usa colunas que não são carregadas: {missing_keys}")

    indexes, skipped = [], []
    for index in template.indexes:
        if index.name.startswith(SNAPSHOT_INDEX_PREFIX):
            skipped.append(index.name)
            continue
        if index.index_type not in SUPPORTED_INDEX_TYPES or index.locality == "GLOBAL":
            raise NotImplementedError(
                f"Índice {index.name}: {index.locality or 'não particionado'} {index.index_type} sem suporte na carga."
            )
        used = {
            column for key in index.columns for column in (key.columns if isinstance(key, IndexExpression) else {key})
        }
        if index.index_type == FUNCTION_BASED_INDEX_TYPE and not any(
            isinstance(key, IndexExpression) for key in index.columns
        ):
            raise NotImplementedError(f"Índice {index.name}: expressão do índice baseado em função não foi lida.")
        missing = sorted(used - available)
        if missing:
            raise ValueError(f"Índice {index.name} usa colunas que não são carregadas: {missing}")
        name = f"{index_prefix}{index.name}"
        if not INDEX_NAME_PATTERN.match(name):
            raise ValueError(f"Nome de índice inválido para o Oracle: {name!r}")
        indexes.append(replace(index, name=name))
    layout = TableLayout(
        tablespace=template.tablespace,
        partitioning=partitioning,
        indexes=tuple(indexes),
        inmemory=LOADED_TABLE_INMEMORY,
    )
    return StructurePlan(layout=layout, skipped_indexes=tuple(skipped))


def slot_indexes(indexes: tuple[IndexDefinition, ...], slot: str) -> tuple[IndexDefinition, ...]:
    """Renomeia os índices para uma das tabelas físicas (``A`` ou ``B``).

    Nomes de índice são únicos no schema, então cada tabela física tem os seus,
    com o sufixo dela.

    :param indexes: Índices com o nome base (``BQLOAD_<índice original>``).
    :param slot: Sufixo da tabela física.
    :returns: Índices com o nome ``<nome base>_<slot>``.
    :raises ValueError: Se algum nome ficar inválido para o Oracle.
    """
    renamed = []
    for index in indexes:
        name = f"{index.name}_{slot}"
        if not INDEX_NAME_PATTERN.match(name):
            raise ValueError(f"Nome de índice inválido para o Oracle: {name!r}")
        renamed.append(replace(index, name=name))
    return tuple(renamed)


def layout_differences(existing: TableLayout, expected: TableLayout) -> list[str]:
    """Lista as diferenças de tablespace, In-Memory e particionamento entre duas tabelas.

    Índices não entram: a carga os apaga e recria a cada execução.

    :param existing: Estrutura da tabela existente.
    :param expected: Estrutura esperada.
    :returns: Diferenças no formato ``atual → esperado``; vazia se forem iguais.
    """
    differences = []
    if existing.tablespace != expected.tablespace:
        differences.append(f"tablespace: {existing.tablespace or '(padrão)'} → {expected.tablespace or '(padrão)'}")
    if existing.inmemory != expected.inmemory:
        differences.append(f"inmemory: {existing.inmemory or 'NO INMEMORY'} → {expected.inmemory or 'NO INMEMORY'}")
    found, wanted = existing.partitioning, expected.partitioning
    if found is None or wanted is None:
        if found != wanted:
            differences.append(f"particionamento: {describe_partitioning(found)} → {describe_partitioning(wanted)}")
        return differences
    if found.summary != wanted.summary:
        differences.append(f"particionamento: {found.summary} → {wanted.summary}")
    found_partitions = {partition.description for partition in found.partitions}
    wanted_partitions = {partition.description for partition in wanted.partitions}
    differences += [f"partição a mais: {text}" for text in sorted(found_partitions - wanted_partitions)]
    differences += [f"partição ausente: {text}" for text in sorted(wanted_partitions - found_partitions)]
    return differences
