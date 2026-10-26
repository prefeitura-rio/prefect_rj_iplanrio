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
    """Partição declarada na tabela, como em ``all_tab_partitions``.

    :param name: Nome da partição.
    :param high_value: Limite da partição, no texto do dicionário
        (por exemplo ``TO_DATE(' 2026-01-01 00:00:00', ...)``).
    :param tablespace: Tablespace da partição.
    """

    name: str
    high_value: str
    tablespace: str | None

    @property
    def description(self) -> str:
        """Descreve a partição em uma linha, para logs e diferenças."""
        return f"{self.name} ({self.high_value})" + (f" em {self.tablespace}" if self.tablespace else "")


@dataclass(frozen=True)
class Partitioning:
    """Particionamento da tabela.

    :param kind: Tipo (``RANGE`` ou ``LIST``).
    :param key_columns: Colunas da chave de partição, na ordem.
    :param interval: Expressão do ``INTERVAL``, ou ``None`` se não houver.
    :param partitions: Partições declaradas. As criadas automaticamente pelo
        ``INTERVAL`` ficam de fora: o próprio Oracle as recria na carga.
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
    """Chave de índice que é uma expressão SQL (índice baseado em função), e não o nome de uma coluna.

    O texto vem de ``all_ind_expressions.column_expression`` e entra no
    ``CREATE INDEX`` como está. Compara e calcula o hash como uma ``str``; o tipo
    só distingue a expressão de um nome de coluna, que recebe aspas.
    """

    @property
    def columns(self) -> set[str]:
        """Colunas citadas na expressão, entre aspas duplas, fora de literais de texto."""
        return set(QUOTED_IDENTIFIER_PATTERN.findall(STRING_LITERAL_PATTERN.sub("''", self)))


@dataclass(frozen=True)
class IndexDefinition:
    """Índice de uma tabela, como em ``all_indexes`` e ``all_ind_columns``.

    Grau de paralelismo, status e dono não entram na comparação entre índices.

    :param name: Nome do índice.
    :param index_type: Tipo no dicionário (``NORMAL``, ``BITMAP``,
        ``FUNCTION-BASED NORMAL``...).
    :param unique: Se o índice é único.
    :param columns: Chaves do índice, na ordem: nomes de coluna ou, nas posições de
        um índice baseado em função, :class:`IndexExpression`.
    :param locality: ``LOCAL`` ou ``GLOBAL`` se o índice for particionado;
        ``None`` se não for.
    :param tablespace: Tablespace do índice (o padrão das partições, se for
        particionado).
    :param degree: Grau de paralelismo gravado no índice.
    :param status: Status do índice (``VALID``, ``UNUSABLE``, ``N/A``...).
    :param owner: Dono do índice.
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
        """Descreve o índice em uma linha, sem o nome."""
        kind = " ".join(part for part in (self.locality, "UNIQUE" if self.unique else None, self.index_type) if part)
        return f"{kind} ({', '.join(self.columns)})" + (f" em {self.tablespace}" if self.tablespace else "")


@dataclass(frozen=True)
class TableLayout:
    """Tablespace, particionamento, índices e In-Memory de uma tabela.

    :param tablespace: Tablespace da tabela (o padrão das partições, se for
        particionada).
    :param partitioning: Particionamento, ou ``None`` se a tabela não for
        particionada.
    :param indexes: Índices da tabela.
    :param inmemory: Cláusula ``INMEMORY`` da tabela (o padrão das partições, se
        for particionada), ou ``None`` se ela não estiver no In-Memory.
    """

    tablespace: str | None
    partitioning: Partitioning | None
    indexes: tuple[IndexDefinition, ...] = ()
    inmemory: str | None = None


@dataclass(frozen=True)
class StructurePlan:
    """Estrutura que a tabela carregada deve ter, derivada da original.

    :param layout: Tablespace e particionamento da original, com os índices já
        renomeados para a tabela carregada.
    :param skipped_indexes: Índices da original que não são replicados.
    """

    layout: TableLayout
    skipped_indexes: tuple[str, ...]
