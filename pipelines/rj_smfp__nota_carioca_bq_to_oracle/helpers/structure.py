"""The physical structure of the original table (tablespace, partitions, indexes).

It gives the equivalent DDL for the loaded table.
"""

from dataclasses import replace

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_ddl import describe_partitioning
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import (
    FUNCTION_BASED_INDEX_TYPE,
    INDEX_NAME_PATTERN,
    INMEMORY_COMPRESSIONS,
    INMEMORY_DISTRIBUTES,
    INMEMORY_DUPLICATES,
    INMEMORY_PRIORITIES,
    SNAPSHOT_INDEX_PREFIX,
    SUPPORTED_INDEX_TYPES,
    SUPPORTED_PARTITIONING,
    IndexDefinition,
    IndexExpression,
    Partitioning,
    PartitionStorage,
    StructurePlan,
    TableLayout,
    TablePartition,
)


def partitioning_from_dictionary(
    table_row: dict[str, object], key_columns: list[str], partition_rows: list[dict[str, object]]
) -> Partitioning:
    """Build the partitioning from the rows of the Oracle dictionary.

    `table_row` is the row of `all_part_tables` (`partitioning_type`,
    `subpartitioning_type` and `interval`). `key_columns` are the columns of
    `all_part_key_columns`, in order. `partition_rows` are the rows of
    `all_tab_partitions` (`partition_name`, `high_value`, `tablespace_name`, `interval`
    and, if read, `segment_created` and the `STORAGE` parameters), in order. Return the
    partitioning, without the partitions that `INTERVAL` created.
    """
    interval = str(table_row["interval"]).strip() if table_row["interval"] else None
    partitions = tuple(
        TablePartition(
            name=str(row["partition_name"]),
            high_value=str(row["high_value"]).strip(),
            tablespace=None if row["tablespace_name"] is None else str(row["tablespace_name"]),
            storage=partition_storage_from_dictionary(row),
        )
        for row in partition_rows
        if row["interval"] != "YES"
    )
    subpartitioning = table_row["subpartitioning_type"]
    kind = str(table_row["partitioning_type"])
    if subpartitioning not in (None, "NONE"):
        kind = f"{kind}-{subpartitioning}"
    return Partitioning(kind=kind, key_columns=tuple(key_columns), interval=interval, partitions=partitions)


def partition_storage_from_dictionary(row: dict[str, object]) -> PartitionStorage | None:
    """Read the segment creation and the `STORAGE` of a row of `all_tab_partitions`.

    The row has `segment_created`, `initial_extent`, `next_extent`, `min_extent`,
    `max_extent` and `pct_increase`. Return `None` if it has no `segment_created`.
    """
    segment_created = row.get("segment_created")
    if segment_created is None:
        return None

    def number(key: str) -> int | None:
        value = row.get(key)
        return None if value is None else int(str(value))

    return PartitionStorage(
        segment_created=segment_created == "YES",
        initial=number("initial_extent"),
        next=number("next_extent"),
        min_extents=number("min_extent"),
        max_extents=number("max_extent"),
        pct_increase=number("pct_increase"),
    )


def inmemory_option(row: dict[str, object], key: str, allowed: tuple[str, ...]) -> str | None:
    """Read an In-Memory option and check that it can go in the DDL.

    Return the value, or `None` if the column is null. Raise `NotImplementedError` if
    the value is not accepted.
    """
    value = row[key]
    if value is None:
        return None
    if value not in allowed:
        raise NotImplementedError(f"INMEMORY: {key} = {value!r} sem suporte na carga.")
    return str(value)


def inmemory_from_dictionary(row: dict[str, object]) -> str | None:
    """Build the `INMEMORY` clause from the Oracle dictionary.

    In a partitioned table, the row must bring the default of the partitions
    (`all_part_tables.def_inmemory*`), which the declared partitions and those of
    `INTERVAL` inherit. The row has `inmemory`, `inmemory_priority`,
    `inmemory_compression`, `inmemory_distribute` and `inmemory_duplicate`.

    ```text
    INMEMORY PRIORITY HIGH MEMCOMPRESS FOR QUERY HIGH DISTRIBUTE AUTO NO DUPLICATE
    ```

    Return `None` if the In-Memory is not on. Raise `NotImplementedError` if an option
    has an unknown value.
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
    """Read the key of one position of the index from the Oracle dictionary.

    In the positions of a function-based index, `all_ind_columns` gives only a hidden
    name (`SYS_NC...$`), and the expression comes from `all_ind_expressions`. A
    descending column is also kept as an expression, and the `DESC` goes in the text.
    The row has `column_name`, `descend` and `column_expression` (null if the position
    is a plain column). Return the column name, or an `IndexExpression` with the
    expression.
    """
    expression = row["column_expression"]
    if expression is None:
        return str(row["column_name"])
    text = str(expression).strip()
    return IndexExpression(f"{text} DESC" if row["descend"] == "DESC" else text)


def index_from_dictionary(row: dict[str, object], columns: list[str]) -> IndexDefinition:
    """Build the definition of an index from the rows of the Oracle dictionary.

    The row has `owner`, `index_name`, `index_type`, `uniqueness`, `locality`,
    `tablespace_name`, `degree` and `status`. `columns` are the keys of the index, in
    order: the column names, or an `IndexExpression` in the positions with an
    expression.
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
    """Define the structure of the loaded table from the original table.

    The tablespace and the partitioning are copied. The table is born `NO INMEMORY`: the
    partitions of the window enter the In-Memory after the load
    (`LOADED_TABLE_INMEMORY`, `helpers/inmemory.py`). The indexes are copied with the
    name
    `<prefix><original name>`, except the `I_SNAP$` of materialized views. Return the
    structure of the loaded table and the indexes of the original that were not copied.
    Raise `NotImplementedError` if the load does not support the partitioning or an
    index. Raise `ValueError` if the partition key or an index uses a column that is not
    loaded, or if the name of an index becomes invalid with the prefix.
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
        inmemory=None,
    )
    return StructurePlan(layout=layout, skipped_indexes=tuple(skipped))


def slot_indexes(indexes: tuple[IndexDefinition, ...], slot: str) -> tuple[IndexDefinition, ...]:
    """Rename the indexes for one of the physical tables (`A` or `B`).

    Index names are unique in the schema, so each physical table has its own, with its
    suffix. The indexes come with the base name (`BQLOAD_<original index>`), and they go
    out as `<base name>_<slot>`. Raise `ValueError` if a name is not valid for Oracle.
    """
    renamed = []
    for index in indexes:
        name = f"{index.name}_{slot}"
        if not INDEX_NAME_PATTERN.match(name):
            raise ValueError(f"Nome de índice inválido para o Oracle: {name!r}")
        renamed.append(replace(index, name=name))
    return tuple(renamed)


def layout_differences(existing: TableLayout, expected: TableLayout) -> list[str]:
    """List the differences of tablespace, In-Memory and partitioning of two tables.

    The indexes do not count: the load drops and creates them again on every run. Return
    the differences as `current → expected`, empty if they are equal.
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
    found_storage = {partition.name: partition.storage for partition in found.partitions}
    for partition in wanted.partitions:
        storage = found_storage.get(partition.name)
        if partition.name in found_storage and not storage_matches(storage, partition.storage):
            current = storage.clause if storage else "(não lido)"
            wanted_clause = partition.storage.clause if partition.storage else "(não lido)"
            differences.append(f"storage da partição {partition.name}: {current} → {wanted_clause}")
    return differences


def storage_matches(existing: PartitionStorage | None, expected: PartitionStorage | None) -> bool:
    """Tell whether the partition has the segment and `STORAGE` of the original.

    Only what the original declares counts: for a deferred original with no `STORAGE`,
    the segment of the existing one depends only on whether it has rows, and that is not
    a difference.
    """
    if expected is None or not expected.declared:
        return True
    if existing is None or existing.parameters != expected.parameters:
        return False
    return existing.segment_created or not expected.segment_created
