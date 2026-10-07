"""Primitivas compartilhadas da medição: SELECT da faixa, leitura em lotes, cronômetro e Parquet."""

import resource
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import validate_identifier
from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.chunks import Chunk
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.columns import OracleColumn, fetch_schema
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.schema import parquet_schema
from prefect_rj_iplanrio.sql import load_query

KIB_PER_MIB = 1024


@dataclass(frozen=True)
class StageTiming:
    """Tempo de uma etapa.

    :param wall_seconds: Tempo de relógio.
    :param cpu_seconds: Tempo de CPU do processo (``time.process_time``).
    """

    wall_seconds: float
    cpu_seconds: float


@dataclass(frozen=True)
class ChunkTask:
    """Leitura de uma faixa de ROWID.

    :param sql: SELECT renderizado da faixa.
    :param chunk: Faixa a ler.
    :param scn: SCN da foto; ``None`` para a leitura sem ``AS OF SCN``.
    :param extracted_at: Horário da foto, gravado em ``_airbyte_extracted_at``.
    :param columns: Colunas do SELECT.
    :param batch_rows: Linhas por lote lido do Oracle.
    """

    sql: str
    chunk: Chunk
    scn: int | None
    extracted_at: datetime
    columns: tuple[OracleColumn, ...]
    batch_rows: int


def render_select(columns: tuple[OracleColumn, ...], schema: str, table: str, with_scn: bool) -> str:
    """Renderiza o SELECT de uma faixa.

    :param columns: Colunas do SELECT.
    :param schema: Dono da tabela.
    :param table: Nome da tabela.
    :param with_scn: ``True`` para ``AS OF SCN``, como na pipeline de extração.
    :returns: SQL com binds ``start_rowid``, ``end_rowid`` e, com SCN, ``scn``.
    """
    return load_query(
        QUERIES_ANCHOR,
        "select_chunk" if with_scn else "select_chunk_no_scn",
        columns=", ".join(column.quoted for column in columns),
        schema=validate_identifier(schema),
        table=validate_identifier(table),
    )


def fetch_batches(connection: oracledb.Connection, task: ChunkTask) -> Iterator[oracledb.DataFrame]:
    """Lê a faixa em lotes, como a pipeline de extração.

    :param connection: Conexão aberta.
    :param task: Faixa a ler.
    :returns: Lotes entregues pelo driver.
    """
    binds: dict[str, object] = {"start_rowid": task.chunk.start_rowid, "end_rowid": task.chunk.end_rowid}
    if task.scn is not None:
        binds["scn"] = task.scn
    return connection.fetch_df_batches(
        task.sql, binds, size=task.batch_rows, fetch_decimals=True, requested_schema=fetch_schema(task.columns)
    )


def timed[T](call: Callable[[], T]) -> tuple[T, StageTiming]:
    """Executa a chamada medindo tempo de relógio e de CPU.

    :param call: Função sem argumentos.
    :returns: O resultado e o tempo gasto.
    """
    wall, cpu = time.perf_counter(), time.process_time()
    result = call()
    return result, StageTiming(time.perf_counter() - wall, time.process_time() - cpu)


def write_parquet(path: Path, tables: list[pa.Table], columns: tuple[OracleColumn, ...], compression: str) -> int:
    """Grava as tabelas num arquivo Parquet.

    :param path: Arquivo de destino.
    :param tables: Tabelas já convertidas por ``to_output_table``.
    :param columns: Colunas do SELECT.
    :param compression: Compressão do Parquet.
    :returns: Tamanho do arquivo, em bytes.
    """
    with pq.ParquetWriter(path, parquet_schema(columns), compression=compression) as writer:
        for table in tables:
            writer.write_table(table)
    return path.stat().st_size


def peak_rss_mb() -> float:
    """Retorna o pico de memória residente do processo.

    :returns: MiB (``ru_maxrss`` do Linux é em KiB).
    """
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / KIB_PER_MIB
