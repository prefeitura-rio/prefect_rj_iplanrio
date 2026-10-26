"""Modelos da estrutura física de uma tabela: partições, índices e plano."""

import re
from dataclasses import dataclass, field

INDEX_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")


# O Oracle cria I_SNAP$_<mview> sozinho para o fast refresh de views materializadas;
# numa tabela comum não tem uso.
SNAPSHOT_INDEX_PREFIX = "I_SNAP$"


SUPPORTED_PARTITIONING = ("RANGE", "LIST")


SUPPORTED_INDEX_TYPES = ("NORMAL", "BITMAP", "FUNCTION-BASED NORMAL")


FUNCTION_BASED_INDEX_TYPE = "FUNCTION-BASED NORMAL"


# Identificadores entre aspas duplas, como o dicionário do Oracle grava as colunas
# dentro de uma expressão de índice.
QUOTED_IDENTIFIER_PATTERN = re.compile(r'"([^"]+)"')


STRING_LITERAL_PATTERN = re.compile(r"'(?:[^']|'')*'")


USABLE_INDEX_STATUSES = ("VALID", "N/A")


# Valores de all_tables e all_part_tables aceitos no INMEMORY; o texto entra no DDL como
# está.
INMEMORY_PRIORITIES = ("NONE", "LOW", "MEDIUM", "HIGH", "CRITICAL")


INMEMORY_COMPRESSIONS = (
    "NO MEMCOMPRESS",
    "FOR DML",
    "FOR QUERY LOW",
    "FOR QUERY HIGH",
    "FOR CAPACITY LOW",
    "FOR CAPACITY HIGH",
    "AUTO",
)


INMEMORY_DISTRIBUTES = ("AUTO", "BY ROWID RANGE", "BY PARTITION", "BY SUBPARTITION")


INMEMORY_DUPLICATES = ("NO DUPLICATE", "DUPLICATE", "DUPLICATE ALL")


# Toda tabela carregada vai para o In-Memory, com as opções das MVT_ originais. A
# cláusula tem todas as opções, como o dicionário as devolve; sem DISTRIBUTE e
# DUPLICATE, a tabela criada seria vista como divergente e recriada.
LOADED_TABLE_INMEMORY = "INMEMORY PRIORITY HIGH MEMCOMPRESS FOR QUERY HIGH DISTRIBUTE AUTO NO DUPLICATE"


@dataclass(frozen=True)
class TablePartition:
    """A partition declared on the table, as in `all_tab_partitions`.

    `high_value` is the limit of the partition in the text of the dictionary, such as
    `TO_DATE(' 2026-01-01 00:00:00', ...)`.
    """

    name: str
    high_value: str
    tablespace: str | None

    @property
    def description(self) -> str:
        """Describe the partition in one line, for logs and differences."""
        return f"{self.name} ({self.high_value})" + (f" em {self.tablespace}" if self.tablespace else "")


@dataclass(frozen=True)
class Partitioning:
    """The partitioning of the table.

    `kind` is `RANGE` or `LIST`, and `key_columns` are the columns of the partition key,
    in order. `interval` is the expression of `INTERVAL`, or `None` if there is none.
    `partitions` are the declared partitions. Those that `INTERVAL` created on its own
    stay out, because Oracle creates them again in the load.
    """

    kind: str
    key_columns: tuple[str, ...]
    interval: str | None
    partitions: tuple[TablePartition, ...]

    @property
    def summary(self) -> str:
        """Descreve tipo, chave e intervalo em uma linha."""
        text = f"{self.kind} ({', '.join(self.key_columns)})"
        return text + (f" INTERVAL {self.interval}" if self.interval else "")


class IndexExpression(str):
    """An index key that is a SQL expression, and not a column name.

    It is a function-based index.

    The text comes from `all_ind_expressions.column_expression`, and it goes in
    `CREATE INDEX` as it is. It compares and hashes as a `str`, and the type only tells
    the expression from a column name, which gets quotes.
    """

    @property
    def columns(self) -> set[str]:
        """List the columns that the expression names, outside text literals.

        They are in double quotes.
        """
        return set(QUOTED_IDENTIFIER_PATTERN.findall(STRING_LITERAL_PATTERN.sub("''", self)))


@dataclass(frozen=True)
class IndexDefinition:
    """An index of a table, as in `all_indexes` and `all_ind_columns`.

    The degree of parallelism, the status and the owner do not count when two indexes
    are compared. `index_type` is the type in the dictionary (`NORMAL`, `BITMAP`,
    `FUNCTION-BASED NORMAL`...). `columns` are the keys of the index, in order: column
    names, or an `IndexExpression` in the positions of a function-based index.
    `locality` is `LOCAL` or `GLOBAL` if the index is partitioned, and `None` if not.
    `tablespace` is the tablespace of the index (the default of the partitions, if it is
    partitioned). `degree` is the degree of parallelism that is stored in the index, and
    `status` is its status (`VALID`, `UNUSABLE`, `N/A`...).
    """

    name: str
    index_type: str
    unique: bool
    columns: tuple[str, ...]
    locality: str | None
    tablespace: str | None
    degree: str = field(default="1", compare=False)
    status: str = field(default="VALID", compare=False)
    owner: str = field(default="", compare=False)

    @property
    def description(self) -> str:
        """Describe the index in one line, without its name."""
        kind = " ".join(part for part in (self.locality, "UNIQUE" if self.unique else None, self.index_type) if part)
        return f"{kind} ({', '.join(self.columns)})" + (f" em {self.tablespace}" if self.tablespace else "")


@dataclass(frozen=True)
class TableLayout:
    """The tablespace, partitioning, indexes and In-Memory of a table.

    `tablespace` is the tablespace of the table (the default of the partitions, if it is
    partitioned). `partitioning` is `None` if the table is not partitioned. `inmemory`
    is the `INMEMORY` clause of the table (the default of the partitions, if it is
    partitioned), or `None` if it is not in the In-Memory.
    """

    tablespace: str | None
    partitioning: Partitioning | None
    indexes: tuple[IndexDefinition, ...] = ()
    inmemory: str | None = None


@dataclass(frozen=True)
class StructurePlan:
    """The structure that the loaded table must have, derived from the original.

    `layout` is the tablespace and the partitioning of the original, with the indexes
    already renamed for the loaded table. `skipped_indexes` are the indexes of the
    original that are not copied.
    """

    layout: TableLayout
    skipped_indexes: tuple[str, ...]
