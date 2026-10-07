"""Divisão de uma tabela em faixas de ROWID com ``DBMS_PARALLEL_EXECUTE``."""

import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import (
    OracleConfig,
    connect,
    fetch_rows,
    to_int,
    validate_identifier,
)
from prefect_rj_iplanrio.sql import load_query

TASK_NAME_UNSAFE = re.compile(r"[^A-Z0-9_]")


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
    return TASK_NAME_UNSAFE.sub("_", f"O2BQ_{table}_{run_id}".upper())[:128]


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
