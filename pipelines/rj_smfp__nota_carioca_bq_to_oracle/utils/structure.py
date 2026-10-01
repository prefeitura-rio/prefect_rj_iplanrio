"""Estrutura física da tabela original (tablespace, partições e índices) e o DDL equivalente na tabela carregada."""

import re
from dataclasses import dataclass, field, replace

INDEX_NAME_PATTERN = re.compile(r"^[A-Z][A-Z0-9_$#]{0,127}$")
# O Oracle cria I_SNAP$_<mview> sozinho para o fast refresh de views materializadas; numa tabela comum não tem uso.
SNAPSHOT_INDEX_PREFIX = "I_SNAP$"
SUPPORTED_PARTITIONING = ("RANGE", "LIST")
SUPPORTED_INDEX_TYPES = ("NORMAL", "BITMAP")
USABLE_INDEX_STATUSES = ("VALID", "N/A")
# Valores de all_tables e all_part_tables aceitos no INMEMORY; o texto entra no DDL como está.
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
# Toda tabela carregada vai para o In-Memory, com as opções das MVT_ originais. A cláusula tem todas as opções,
# como o dicionário as devolve; sem DISTRIBUTE e DUPLICATE, a tabela criada seria vista como divergente e recriada.
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


@dataclass(frozen=True)
class IndexDefinition:
    """Índice de uma tabela, como em ``all_indexes`` e ``all_ind_columns``.

    Grau de paralelismo, status e dono não entram na comparação entre índices.

    :param name: Nome do índice.
    :param index_type: Tipo no dicionário (``NORMAL``, ``BITMAP``,
        ``FUNCTION-BASED NORMAL``...).
    :param unique: Se o índice é único.
    :param columns: Colunas do índice, na ordem.
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


def index_from_dictionary(row: dict[str, object], columns: list[str]) -> IndexDefinition:
    """Monta a definição de um índice a partir das linhas do dicionário do Oracle.

    :param row: Linha com ``owner``, ``index_name``, ``index_type``,
        ``uniqueness``, ``locality``, ``tablespace_name``, ``degree`` e ``status``.
    :param columns: Colunas do índice, na ordem.
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
        missing = [name for name in index.columns if name not in available]
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


def quote(name: str) -> str:
    """Delimita um identificador com aspas duplas.

    :param name: Identificador já validado.
    :returns: Identificador entre aspas.
    """
    return f'"{name}"'


def partitioning_clause(partitioning: Partitioning) -> str:
    """Monta a cláusula ``PARTITION BY`` do ``CREATE TABLE``.

    :param partitioning: Particionamento da tabela.
    :returns: Fragmento SQL com uma partição por linha.
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
    """Monta as cláusulas de armazenamento do ``CREATE TABLE``.

    Sem a cláusula ``INMEMORY`` no layout, a tabela é criada com ``NO INMEMORY``,
    mesmo que o tablespace tenha ``INMEMORY`` como padrão.

    :param layout: Tablespace, particionamento e In-Memory da tabela.
    :returns: Fragmento SQL que segue a lista de colunas.
    """
    clauses = [f"TABLESPACE {quote(layout.tablespace)}"] if layout.tablespace else []
    clauses.append(layout.inmemory or "NO INMEMORY")
    if layout.partitioning is not None:
        clauses.append(partitioning_clause(layout.partitioning))
    return "\n".join(clauses)


def index_statement_parts(index: IndexDefinition, parallel_degree: int) -> dict[str, str]:
    """Monta os trechos variáveis do ``CREATE INDEX``.

    :param index: Índice a criar.
    :param parallel_degree: Grau de paralelismo da criação.
    :returns: ``kind`` (``UNIQUE``, ``BITMAP`` ou vazio), ``columns`` e
        ``options`` (tablespace, ``LOCAL`` e ``PARALLEL``).
    """
    kind = "UNIQUE" if index.unique else ("BITMAP" if index.index_type == "BITMAP" else "")
    options = [f"TABLESPACE {quote(index.tablespace)}"] if index.tablespace else []
    if index.locality == "LOCAL":
        options.append("LOCAL")
    options.append(f"PARALLEL {parallel_degree}")
    return {"kind": kind, "columns": ", ".join(quote(name) for name in index.columns), "options": " ".join(options)}


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


def describe_partitioning(partitioning: Partitioning | None) -> str:
    """Descreve o particionamento com o número de partições declaradas.

    :param partitioning: Particionamento, ou ``None``.
    :returns: Por exemplo ``RANGE (DATA) com 61 partições`` ou ``sem partição``.
    """
    if partitioning is None:
        return "sem partição"
    return f"{partitioning.summary} com {len(partitioning.partitions)} partição(ões) declarada(s)"
