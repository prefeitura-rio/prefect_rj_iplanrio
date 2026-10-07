"""Divisão de uma tabela em faixas de ROWID com ``DBMS_PARALLEL_EXECUTE`` e escolha das faixas amostradas.

A parte de ``DBMS_PARALLEL_EXECUTE`` espelha o módulo de mesmo nome do PR #394 (``rj_smfp__nota_carioca_oracle_to_bq``);
manter em sincronia e remover quando o PR for mesclado. ``pick_chunks`` é exclusiva da sonda.
"""

import re
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass

import oracledb

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import (
    OracleConfig,
    connect,
    to_int,
    validate_identifier,
)
from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import CHUNK_TASK_PREFIX, QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.extents import group_extents, range_rowids, read_extents
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.session import connect_read_only, query_rows
from prefect_rj_iplanrio.logging import get_logger
from prefect_rj_iplanrio.sql import load_query

logger = get_logger(__name__)

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
    :returns: Nome em maiúsculas, com o prefixo da sonda, só com letras, dígitos e ``_``.
    """
    return TASK_NAME_UNSAFE.sub("_", f"{CHUNK_TASK_PREFIX}{table}_{run_id}".upper())[:128]


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
        rows = query_rows(cursor, "get_chunks", {"task_name": request.task_name})
    return [Chunk(to_int(row["chunk_id"]), str(row["start_rowid"]), str(row["end_rowid"])) for row in rows]


def drop_chunk_task(config: OracleConfig, task_name: str) -> None:
    """Apaga a tarefa de chunking e suas faixas.

    :param config: Conexão com o Oracle.
    :param task_name: Nome da tarefa.
    """
    with connect(config) as connection, connection.cursor() as cursor:
        cursor.execute(load_query(QUERIES_ANCHOR, "drop_chunk_task"), {"task_name": task_name})


@dataclass(frozen=True)
class ChunkSet:
    """Faixas de ROWID de uma tabela e como foram calculadas.

    :param chunks: Faixas, em ordem.
    :param source: ``DBMS_PARALLEL_EXECUTE`` ou ``dba_extents`` com o motivo do fallback.
    """

    chunks: list[Chunk]
    source: str


def drop_chunk_task_if_created(config: OracleConfig, task_name: str) -> None:
    """Apaga a tarefa depois de uma falha no chunking, caso ela tenha chegado a ser criada.

    :param config: Conexão com o Oracle.
    :param task_name: Nome da tarefa.
    """
    try:
        drop_chunk_task(config, task_name)
    except oracledb.DatabaseError as error:
        logger.warning("Tarefa %s não apagada (provavelmente não foi criada): %s", task_name, first_line(error))


def first_line(error: oracledb.DatabaseError) -> str:
    """Resume o erro do Oracle em uma linha, incluindo a causa ``PLS-`` que segue o ``ORA-06550``.

    :param error: Erro do driver.
    :returns: Texto curto, ex. ``ORA-06550: line 2, column 5: PLS-00201: ...``.
    """
    lines = [line.strip() for line in str(error).splitlines() if line.strip()]
    return " ".join(lines[:2])


def extent_chunks(request: ChunkRequest) -> list[Chunk]:
    """Calcula as faixas a partir dos extents, sem ``DBMS_PARALLEL_EXECUTE`` e sem gravar nada.

    :param request: Tabela e tamanho das faixas.
    :returns: Faixas numeradas a partir de 1.
    """
    with connect_read_only(request.config) as connection, connection.cursor() as cursor:
        extents = read_extents(cursor, request.schema, request.table)
    ranges = group_extents(extents, request.chunk_size_blocks)
    return [Chunk(index, *range_rowids(block_range)) for index, block_range in enumerate(ranges, start=1)]


@contextmanager
def rowid_chunks(request: ChunkRequest) -> Iterator[ChunkSet]:
    """Entrega as faixas de ROWID; com ``DBMS_PARALLEL_EXECUTE``, apaga a tarefa ao sair.

    Se o pacote falhar (ex.: sem ``EXECUTE``), calcula as faixas pelos extents.

    :param request: Tabela e tamanho das faixas.
    :yields: Faixas e a forma como foram calculadas.
    """
    chunks: list[Chunk] | None = None
    failure = ""
    try:
        chunks = read_chunks(request)
    except oracledb.DatabaseError as error:
        failure = first_line(error)
        drop_chunk_task_if_created(request.config, request.task_name)
    if chunks is None:
        yield ChunkSet(extent_chunks(request), f"dba_extents (DBMS_PARALLEL_EXECUTE falhou: {failure})")
        return
    try:
        yield ChunkSet(chunks, "DBMS_PARALLEL_EXECUTE")
    finally:
        drop_chunk_task(request.config, request.task_name)


def pick_chunks(chunks: Sequence[Chunk], count: int, is_populated: Callable[[Chunk], bool]) -> tuple[Chunk, ...]:
    """Escolhe ``count`` faixas espalhadas por toda a lista, uma por fatia igual, preferindo as que têm linhas.

    Em cada fatia parte-se da faixa central e, se ela estiver vazia (ex.: partição futura), avança
    até a primeira com linhas dentro da fatia; se a fatia toda estiver vazia, fica a central.

    :param chunks: Todas as faixas da tabela, em ordem.
    :param count: Quantidade desejada.
    :param is_populated: Diz se a faixa tem ao menos uma linha.
    :returns: As faixas escolhidas; todas, se houver menos que ``count``.
    """
    if len(chunks) <= count:
        return tuple(chunks)
    picked: list[Chunk] = []
    for position in range(count):
        low, high = position * len(chunks) // count, (position + 1) * len(chunks) // count
        center = int((position + 0.5) * len(chunks) / count)
        order = [*range(center, high), *range(low, center)]
        picked.append(next((chunks[index] for index in order if is_populated(chunks[index])), chunks[center]))
    return tuple(picked)
