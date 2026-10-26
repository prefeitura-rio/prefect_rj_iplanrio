"""The DDL of the physical structure: partitions, storage and index commands."""

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import (
    IndexDefinition,
    IndexExpression,
    Partitioning,
    TableLayout,
)


def quote(name: str) -> str:
    """Wrap an identifier that was already validated in double quotes."""
    return f'"{name}"'


def partitioning_clause(partitioning: Partitioning) -> str:
    """Build the `PARTITION BY` clause of `CREATE TABLE`.

    It is a fragment with one partition on each line.
    """
    keys = ", ".join(quote(name) for name in partitioning.key_columns)
    header = f"PARTITION BY {partitioning.kind} ({keys})"
    if partitioning.interval:
        header += f" INTERVAL ({partitioning.interval})"
    bound = "VALUES LESS THAN" if partitioning.kind == "RANGE" else "VALUES"
    partitions = ",\n".join(
        f"  PARTITION {quote(partition.name)} {bound} ({partition.high_value})"
        + (f" TABLESPACE {quote(partition.tablespace)}" if partition.tablespace else "")
        for partition in partitioning.partitions
    )
    return f"{header} (\n{partitions}\n)"


def storage_clause(layout: TableLayout) -> str:
    """Build the storage clauses of `CREATE TABLE`.

    Without the `INMEMORY` clause in the layout, the table is created with `NO
    INMEMORY`, even if the tablespace has `INMEMORY` as its default. Return the SQL
    fragment that follows the list of columns.
    """
    clauses = [f"TABLESPACE {quote(layout.tablespace)}"] if layout.tablespace else []
    clauses.append(layout.inmemory or "NO INMEMORY")
    if layout.partitioning is not None:
        clauses.append(partitioning_clause(layout.partitioning))
    return "\n".join(clauses)


def index_statement_parts(index: IndexDefinition, parallel_degree: int) -> dict[str, str]:
    """Build the variable parts of `CREATE INDEX`.

    Return `kind` (`UNIQUE`, `BITMAP` or empty), `columns` (the columns in quotes and
    the expressions as they are) and `options` (the tablespace, `LOCAL` and `PARALLEL`).
    """
    kind = "UNIQUE" if index.unique else ("BITMAP" if index.index_type == "BITMAP" else "")
    options = [f"TABLESPACE {quote(index.tablespace)}"] if index.tablespace else []
    if index.locality == "LOCAL":
        options.append("LOCAL")
    options.append(f"PARALLEL {parallel_degree}")
    return {
        "kind": kind,
        "columns": ", ".join(key if isinstance(key, IndexExpression) else quote(key) for key in index.columns),
        "options": " ".join(options),
    }


def describe_partitioning(partitioning: Partitioning | None) -> str:
    """Describe the partitioning with the number of declared partitions.

    ```text
    RANGE (DATA) com 61 partições
    ```

    Return `sem partição` for `None`.
    """
    if partitioning is None:
        return "sem partição"
    return f"{partitioning.summary} com {len(partitioning.partitions)} partição(ões) declarada(s)"
