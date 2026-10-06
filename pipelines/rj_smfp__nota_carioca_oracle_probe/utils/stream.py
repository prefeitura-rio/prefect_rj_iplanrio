"""Processo worker: caminho completo (leitura, conversão, Parquet, envio) de uma faixa, como na pipeline de extração."""

import tempfile
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import OracleConfig
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.convert import to_output_table
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.gcs import open_bucket, upload_then_delete
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.schema import parquet_schema
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.session import connect_read_only
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.work import ChunkTask, fetch_batches


@dataclass(frozen=True)
class StreamTask:
    """Trabalho de um worker.

    :param chunk: Faixa a ler.
    :param compression: Compressão do Parquet.
    :param blob_name: Objeto de envio; ``None`` para não enviar.
    :param fetch_only: Se ``True``, só lê do Oracle e descarta os lotes.
    """

    chunk: ChunkTask
    compression: str
    blob_name: str | None
    fetch_only: bool


@dataclass(frozen=True)
class ChunkMetrics:
    """Resultado de uma faixa no worker.

    :param rows: Linhas lidas.
    :param parquet_bytes: Bytes do Parquet; zero na leitura sem gravação.
    :param started_at: Início (epoch, comparável entre processos).
    :param ended_at: Fim (epoch).
    :param cpu_seconds: CPU do processo gasta na faixa.
    :param fetch_seconds: Tempo esperando o Oracle.
    :param convert_seconds: Tempo em ``to_output_table``.
    :param write_seconds: Tempo gravando o Parquet.
    :param upload_seconds: Tempo enviando ao GCS.
    """

    rows: int
    parquet_bytes: int
    started_at: float
    ended_at: float
    cpu_seconds: float
    fetch_seconds: float
    convert_seconds: float
    write_seconds: float
    upload_seconds: float


class WorkerContext:
    """Recursos de um processo worker, criados uma vez pelo ``initializer`` do pool."""

    connection: oracledb.Connection | None = None
    bucket: storage.Bucket | None = None


WORKER = WorkerContext()


def init_worker(config: OracleConfig, project: str, bucket: str | None) -> None:
    """Abre a conexão somente leitura e o client do GCS do worker.

    :param config: Conexão com o Oracle.
    :param project: Projeto do GCS.
    :param bucket: Bucket do envio; ``None`` se não houver envio.
    """
    WORKER.connection = connect_read_only(config)
    WORKER.bucket = None if bucket is None else open_bucket(project, bucket)


def timed_batches(connection: oracledb.Connection, task: ChunkTask, spent: list[float]) -> Iterator[pa.Table]:
    """Entrega os lotes acumulando em ``spent[0]`` o tempo esperando o Oracle.

    :param connection: Conexão aberta.
    :param task: Faixa a ler.
    :param spent: Acumulador de um elemento, em segundos.
    :yields: Cada lote como tabela Arrow.
    """
    iterator = iter(fetch_batches(connection, task))
    while True:
        mark = time.perf_counter()
        frame = next(iterator, None)
        batch = None if frame is None else pa.table(frame)
        spent[0] += time.perf_counter() - mark
        if batch is None:
            return
        yield batch


def stream_chunk(connection: oracledb.Connection, bucket: storage.Bucket | None, task: StreamTask) -> ChunkMetrics:
    """Executa o caminho da faixa e mede cada etapa.

    :param connection: Conexão do worker.
    :param bucket: Bucket do envio, se houver.
    :param task: Trabalho.
    :returns: Linhas, bytes e tempos.
    """
    started_at, cpu = time.time(), time.process_time()
    fetch_spent, convert, write, upload = [0.0], 0.0, 0.0, 0.0
    rows = size = 0
    with tempfile.TemporaryDirectory(prefix="oracle_probe_") as directory:
        path = Path(directory) / "chunk.parquet"
        schema = parquet_schema(task.chunk.columns)
        writer = None if task.fetch_only else pq.ParquetWriter(path, schema, compression=task.compression)
        try:
            for batch in timed_batches(connection, task.chunk, fetch_spent):
                rows += batch.num_rows
                if writer is None or not batch.num_rows:
                    continue
                mark = time.perf_counter()
                output = to_output_table(batch, task.chunk.columns, task.chunk.extracted_at)
                convert += time.perf_counter() - mark
                mark = time.perf_counter()
                writer.write_table(output)
                write += time.perf_counter() - mark
        finally:
            if writer is not None:
                writer.close()
        if writer is not None and rows:
            size = path.stat().st_size
            if bucket is not None and task.blob_name is not None:
                upload = upload_then_delete(bucket, path, task.blob_name)
    return ChunkMetrics(
        rows=rows,
        parquet_bytes=size,
        started_at=started_at,
        ended_at=time.time(),
        cpu_seconds=time.process_time() - cpu,
        fetch_seconds=fetch_spent[0],
        convert_seconds=convert,
        write_seconds=write,
        upload_seconds=upload,
    )


def run_chunk(task: StreamTask) -> ChunkMetrics:
    """Processa uma faixa no worker.

    :param task: Trabalho.
    :returns: Métricas da faixa.
    :raises RuntimeError: Se o worker não foi inicializado.
    """
    if WORKER.connection is None:
        raise RuntimeError("Worker sem inicialização; use init_worker como initializer do pool.")
    return stream_chunk(WORKER.connection, WORKER.bucket, task)
