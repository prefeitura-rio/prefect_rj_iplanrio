"""Compare columns, partitions, indexes, synonyms and grants of BigQuery and Oracle."""

from re import compile as compile_pattern

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure import storage_matches
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import (
    USABLE_INDEX_STATUSES,
    IndexDefinition,
    Partitioning,
    TablePartition,
)

STATUS_OK = "OK"


STATUS_DIVERGE = "DIVERGE"


DATE_BOUND_PATTERN = compile_pattern(r"^TO_DATE\('\s*([0-9-]+ [0-9:]+)'")


def describe_column(column: OracleColumn | None) -> str:
    """Describe the type and the nullability of a column as in `CREATE TABLE`.

    ```text
    VARCHAR2(14 BYTE) NOT NULL
    ```

    Return `(ausente)` if the column is `None`.
    """
    if column is None:
        return "(ausente)"
    try:
        ddl_type = column.ddl_type
    except NotImplementedError:
        ddl_type = column.data_type
    return ddl_type + ("" if column.nullable else " NOT NULL")


def compare_columns(
    template: list[OracleColumn], actual: list[OracleColumn], bq_types: dict[str, str]
) -> list[list[str]]:
    """Compare the original table with the loaded one, position by position.

    The name, order, type, size and `NOT NULL` must be equal. `bq_types` is the BigQuery
    type of each column, by its name in upper case. Return the rows `[#, column,
    BigQuery type, original, loaded, status]`.
    """
    rows = []
    for index in range(max(len(template), len(actual))):
        original = template[index] if index < len(template) else None
        loaded = actual[index] if index < len(actual) else None
        name = original.name if original else "(ausente)"
        same_name = original is not None and loaded is not None and original.name == loaded.name
        same_definition = same_name and describe_column(original) == describe_column(loaded)
        rows.append(
            [
                str(index + 1),
                name if same_name or loaded is None else f"{name} ≠ {loaded.name}",
                bq_types.get(name, "-"),
                describe_column(original),
                describe_column(loaded),
                STATUS_OK if same_definition else STATUS_DIVERGE,
            ]
        )
    return rows


def short_bound(high_value: str) -> str:
    """Shorten the limit of a date partition for display.

    Return `< YYYY-MM-DD HH:MI:SS` for a `TO_DATE` limit, and the original text in the
    other cases.
    """
    match = DATE_BOUND_PATTERN.match(high_value)
    return f"< {match.group(1)}" if match else high_value


def describe_partition(partition: TablePartition | None, with_storage: bool = False) -> str:
    """Describe the limit and the tablespace of a partition.

    With `with_storage`, add the segment creation and the `STORAGE`, if they were read.

    ```text
    < 2026-02-01 00:00:00 em DFEN_BIG_DATA
    ```

    Return `(ausente)` if the partition is `None`.
    """
    if partition is None:
        return "(ausente)"
    text = short_bound(partition.high_value) + (f" em {partition.tablespace}" if partition.tablespace else "")

    return text + (f" · {partition.storage.clause}" if with_storage and partition.storage else "")


def same_partition(expected: TablePartition, loaded: TablePartition | None) -> tuple[bool, bool]:
    """Compare a partition of the original with the loaded one, as the load does.

    The load uses the same rule to create the table again. Return whether the two
    match, and whether the difference, if any, is only in the segment or the `STORAGE`.
    """
    if loaded is None:
        return False, False
    if (loaded.high_value, loaded.tablespace) != (expected.high_value, expected.tablespace):
        return False, False
    matches = storage_matches(loaded.storage, expected.storage)

    return matches, not matches


def compare_partitions(expected: Partitioning | None, actual: Partitioning | None) -> list[list[str]]:
    """Compare the partitioning of the original table with the loaded one.

    The first row compares the kind, the key and the interval, and the next rows compare
    each declared partition by name, in the order of the original. The partitions that
    `INTERVAL` created do not count.
    """
    expected_summary = expected.summary if expected else "sem partição"
    actual_summary = actual.summary if actual else "sem partição"
    status = STATUS_OK if expected_summary == actual_summary else STATUS_DIVERGE
    rows = [["-", "(particionamento)", expected_summary, actual_summary, status]]
    wanted = list(expected.partitions) if expected else []
    found = {partition.name: partition for partition in actual.partitions} if actual else {}
    for position, partition in enumerate(wanted, start=1):
        loaded = found.pop(partition.name, None)
        matches, storage_only = same_partition(partition, loaded)
        rows.append(
            [
                str(position),
                partition.name,
                describe_partition(partition, storage_only),
                describe_partition(loaded, storage_only),
                STATUS_OK if matches else STATUS_DIVERGE,
            ]
        )
    rows += [
        ["+", name, "(ausente)", describe_partition(partition), STATUS_DIVERGE] for name, partition in found.items()
    ]
    return rows


def compare_indexes(expected: list[IndexDefinition], actual: list[IndexDefinition]) -> list[list[str]]:
    """Compare the expected indexes (the original, renamed) with the loaded ones.

    Besides the definition, the index must be usable and have no stored degree of
    parallelism. `expected` already has the name of the loaded table. Return the rows
    `[index, expected, loaded, parallelism, index status, result]`.
    """
    found = {index.name: index for index in actual}
    rows = []
    for index in expected:
        loaded = found.pop(index.name, None)
        healthy = loaded is not None and loaded.degree == "1" and loaded.status in USABLE_INDEX_STATUSES
        rows.append(
            [
                index.name,
                index.description,
                loaded.description if loaded else "(ausente)",
                loaded.degree if loaded else "-",
                loaded.status if loaded else "-",
                STATUS_OK if healthy and loaded == index else STATUS_DIVERGE,
            ]
        )
    rows += [
        [name, "(não esperado)", index.description, index.degree, index.status, STATUS_DIVERGE]
        for name, index in found.items()
    ]
    return rows


def compare_synonyms(owners: tuple[str, ...], targets: dict[str, str], active: str | None) -> list[list[str]]:
    """Check that all the synonyms of the table point to the physical table in use.

    `owners` are the expected owners of the synonyms, and `targets` is the object that
    the synonym of each owner points to (`owner.table`). `active` is the table in use
    (`owner.table`), or `None` if there is none. Return the rows `[synonym, points to,
    status]`.
    """
    rows = []
    for owner in owners:
        target = targets.get(owner)
        status = STATUS_OK if target is not None and target == active else STATUS_DIVERGE
        rows.append([owner, target or "(ausente)", status])
    return rows


def compare_grants(expected: tuple[tuple[str, str], ...], found: list[tuple[str, str]]) -> list[list[str]]:
    """Check the privileges granted to the consumers on the physical table.

    `expected` are the pairs `(grantee, privileges separated by commas)`, and `found`
    are the pairs `(grantee, privilege)` granted on the table. Return the rows
    `[grantee, expected, granted, status]`. Extra privileges do not count as a
    divergence.
    """
    granted: dict[str, set[str]] = {}
    for grantee, privilege in found:
        granted.setdefault(grantee, set()).add(privilege)
    rows = []
    for grantee, privileges in expected:
        wanted = {privilege.strip() for privilege in privileges.split(",")}
        has = granted.get(grantee, set())
        rows.append(
            [
                grantee,
                ", ".join(sorted(wanted)),
                ", ".join(sorted(has)) or "(nenhum)",
                STATUS_OK if wanted <= has else STATUS_DIVERGE,
            ]
        )
    return rows
