"""Divisão de uma tabela em faixas de ROWID com ``DBMS_PARALLEL_EXECUTE``."""

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Protocol

import oracledb

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import (
    OracleConfig,
    connect,
    fetch_rows,
    to_int,
    validate_identifier,
)
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)

TASK_NAME_UNSAFE = re.compile(r"[^A-Z0-9_]")
# Prefixo de todas as tarefas de chunking desta pipeline (ver ``chunk_task_name``).
TASK_PREFIX = "O2BQ_"
# Caractere de escape do ``LIKE`` de ``queries/list_chunk_tasks.sql``.
LIKE_ESCAPE = "\\"


@dataclass(frozen=True)
class Chunk:
    """Faixa contígua de ROWIDs de uma tabela.

    :param chunk_id: Identificador da faixa na tarefa de chunking.
    :param start_rowid: Primeiro ROWID da faixa, em texto.
    :param end_rowid: Último ROWID da faixa, em texto.
    """

    chunk_id: int
    start_rowid: str
    end_rowid: str


@dataclass(frozen=True)
class ChunkRequest:
    """Pedido de divisão de uma tabela em faixas.

    :param config: Conexão com o Oracle.
    :param schema: Dono da tabela.
    :param table: Nome da tabela.
    :param task_name: Nome único da tarefa de chunking desta execução.
    :param chunk_size_blocks: Tamanho aproximado de cada faixa, em blocos.
    """

    config: OracleConfig
    schema: str
    table: str
    task_name: str
    chunk_size_blocks: int


def chunk_task_name(table: str, run_id: str) -> str:
    """Monta o nome único da tarefa de chunking: um por tabela e execução.

    :param table: Nome da tabela.
    :param run_id: Identificador do flow run.
    :returns: Nome em maiúsculas, só com letras, dígitos e ``_``.
    """
    return TASK_NAME_UNSAFE.sub("_", f"{TASK_PREFIX}{table}_{run_id}".upper())[:128]


def prefix_like_pattern(prefix: str = TASK_PREFIX) -> str:
    """Monta o padrão de ``LIKE`` que casa nomes começando por ``prefix``.

    ``_`` e ``%`` do prefixo são escapados, para que ``_`` não case qualquer caractere.

    :param prefix: Prefixo literal dos nomes de tarefa.
    :returns: Padrão para ``LIKE :pattern ESCAPE '\\'``.
    """
    escaped = (
        prefix.replace(LIKE_ESCAPE, LIKE_ESCAPE * 2).replace("_", f"{LIKE_ESCAPE}_").replace("%", f"{LIKE_ESCAPE}%")
    )
    return f"{escaped}%"


class ChunkTaskStore(Protocol):
    """Acesso às tarefas de chunking existentes no Oracle."""

    def exists(self, task_name: str) -> bool:
        """Informa se há uma tarefa com exatamente este nome."""
        ...

    def list_by_prefix(self, prefix: str) -> list[str]:
        """Lista os nomes das tarefas que começam por ``prefix``."""
        ...

    def drop(self, task_name: str) -> None:
        """Apaga a tarefa e suas faixas."""
        ...


class OracleChunkTaskStore:
    """:class:`ChunkTaskStore` sobre um cursor aberto; só enxerga as tarefas do usuário conectado."""

    def __init__(self, cursor: oracledb.Cursor) -> None:
        """Guarda o cursor de uma conexão aberta."""
        self.cursor = cursor

    def exists(self, task_name: str) -> bool:
        """Consulta ``user_parallel_execute_tasks`` pelo nome exato."""
        return bool(fetch_rows(self.cursor, "get_chunk_task", {"task_name": task_name}))

    def list_by_prefix(self, prefix: str) -> list[str]:
        """Consulta ``user_parallel_execute_tasks`` com ``LIKE`` e ``ESCAPE``."""
        rows = fetch_rows(self.cursor, "list_chunk_tasks", {"pattern": prefix_like_pattern(prefix)})
        return [str(row["task_name"]) for row in rows]

    def drop(self, task_name: str) -> None:
        """Executa ``queries/drop_chunk_task.sql``."""
        self.cursor.execute(load_query(QUERIES_ANCHOR, "drop_chunk_task"), {"task_name": task_name})


def drop_stale_task(store: ChunkTaskStore, task_name: str) -> bool:
    """Apaga uma tarefa de mesmo nome deixada por uma tentativa interrompida, se existir.

    :param store: Tarefas existentes no Oracle.
    :param task_name: Nome da tarefa que vai ser criada.
    :returns: ``True`` se havia uma tarefa e ela foi apagada.
    """
    if not store.exists(task_name):
        return False
    store.drop(task_name)
    logger.warning("Tarefa de chunking %s de uma tentativa interrompida apagada antes de recriá-la", task_name)
    return True


def drop_prefixed_tasks(store: ChunkTaskStore, prefix: str = TASK_PREFIX) -> list[str]:
    """Apaga todas as tarefas cujo nome começa por ``prefix``.

    :param store: Tarefas existentes no Oracle.
    :param prefix: Prefixo literal dos nomes.
    :returns: Nomes apagados.
    """
    names = store.list_by_prefix(prefix)
    for name in names:
        store.drop(name)
    return names


def drop_leftover_chunk_tasks(config: OracleConfig) -> list[str]:
    """Apaga as tarefas de chunking de execuções anteriores desta pipeline.

    Só é seguro quando nenhuma outra execução está ativa.

    :param config: Conexão com o Oracle.
    :returns: Nomes das tarefas apagadas.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        return drop_prefixed_tasks(OracleChunkTaskStore(cursor))


def read_chunks(request: ChunkRequest) -> list[Chunk]:
    """Cria a tarefa e as faixas de ROWID, e as lê do dicionário.

    :param request: Tabela e tamanho das faixas.
    :returns: Faixas em ordem de ``chunk_id``.
    """
    binds = {
        "task_name": request.task_name,
        "owner": validate_identifier(request.schema),
        "table_name": validate_identifier(request.table),
        "chunk_size": request.chunk_size_blocks,
    }
    with connect(request.config) as connection, connection.cursor() as cursor:
        drop_stale_task(OracleChunkTaskStore(cursor), request.task_name)
        cursor.execute(load_query(QUERIES_ANCHOR, "create_chunk_task"), {"task_name": request.task_name})
        cursor.execute(load_query(QUERIES_ANCHOR, "create_chunks_by_rowid"), binds)
        rows = fetch_rows(cursor, "get_chunks", {"task_name": request.task_name})
    return [Chunk(to_int(row["chunk_id"]), str(row["start_rowid"]), str(row["end_rowid"])) for row in rows]


def drop_chunk_task(config: OracleConfig, task_name: str) -> None:
    """Apaga a tarefa de chunking e suas faixas.

    :param config: Conexão com o Oracle.
    :param task_name: Nome da tarefa.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(load_query(QUERIES_ANCHOR, "drop_chunk_task"), {"task_name": task_name})


@contextmanager
def rowid_chunks(request: ChunkRequest) -> Iterator[list[Chunk]]:
    """Entrega as faixas de ROWID e garante que a tarefa seja apagada ao sair.

    :param request: Tabela e tamanho das faixas.
    :yields: Faixas em ordem de ``chunk_id``.
    """
    try:
        yield read_chunks(request)
    finally:
        drop_chunk_task(request.config, request.task_name)
