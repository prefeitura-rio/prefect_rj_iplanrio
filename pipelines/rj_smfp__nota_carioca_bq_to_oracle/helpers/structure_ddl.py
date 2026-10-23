"""DDL da estrutura física: cláusulas de partição e armazenamento e comandos de índice."""

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.structure_models import (
    IndexDefinition,
    IndexExpression,
    Partitioning,
    TableLayout,
)


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
    :returns: ``kind`` (``UNIQUE``, ``BITMAP`` ou vazio), ``columns`` (colunas
        entre aspas e expressões como estão) e ``options`` (tablespace, ``LOCAL``
        e ``PARALLEL``).
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
    """Descreve o particionamento com o número de partições declaradas.

    :param partitioning: Particionamento, ou ``None``.
    :returns: Por exemplo ``RANGE (DATA) com 61 partições`` ou ``sem partição``.
    """
    if partitioning is None:
        return "sem partição"
    return f"{partitioning.summary} com {len(partitioning.partitions)} partição(ões) declarada(s)"
