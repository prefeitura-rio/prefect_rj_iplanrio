"""Processo worker: lê faixas de ROWID do Oracle e envia Parquet ao GCS."""

import tempfile
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from multiprocessing.synchronize import BoundedSemaphore
from pathlib import Path

import oracledb
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.checksum import ColumnChecksum, chunk_checksums, merge_checksums
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.chunks import Chunk
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn, fetch_schema
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.convert import to_output_table
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.gcs import upload_file
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import OracleConfig, connect
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.schema import parquet_schema
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

MAX_CHUNK_ATTEMPTS = 3
PARQUET_COMPRESSION = "zstd"


@dataclass(frozen=True)
class ChunkJob:
    """Trabalho de um worker: ler uma faixa de ROWID e gravar um Parquet.

    :param sql: SELECT da faixa, com binds ``scn``, ``start_rowid`` e ``end_rowid``.
    :param chunk: Faixa a ler.
    :param scn: SCN da foto.
    :param extracted_at: Horário da foto.
    :param columns: Colunas do SELECT.
    :param blob_name: Objeto de destino no GCS.
    :param batch_rows: Linhas por lote.
    :param checksum_columns: Colunas ``NUMBER`` cuja contagem de não nulos e soma exata a faixa devolve.
    """

    sql: str
    chunk: Chunk
    scn: int
    extracted_at: datetime
    columns: tuple[OracleColumn, ...]
    blob_name: str
    batch_rows: int
    checksum_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class ChunkResult:
    """Resultado de uma faixa.

    :param rows: Linhas gravadas.
    :param bytes_written: Bytes do Parquet enviado; zero se a faixa não tinha linhas.
    :param checksums: Checksum, calculado sobre os arrays gravados no Parquet, de cada coluna de checksum.
    """

    rows: int
    bytes_written: int
    checksums: Mapping[str, ColumnChecksum] = field(default_factory=dict)


class WorkerContext:
    """Recursos de um processo worker, criados uma vez pelo ``initializer`` do pool."""

    config: OracleConfig | None = None
    connection: oracledb.Connection | None = None
    bucket: storage.Bucket | None = None
    upload_slots: BoundedSemaphore | None = None


WORKER = WorkerContext()


def init_worker(config: OracleConfig, project: str, bucket: str, upload_slots: BoundedSemaphore) -> None:
    """Abre a conexão do Oracle e o client do GCS do processo worker.

    :param config: Conexão com o Oracle.
    :param project: Projeto do GCS.
    :param bucket: Bucket de destino.
    :param upload_slots: Semáforo compartilhado entre os workers que limita os uploads simultâneos.
    """
    WORKER.upload_slots = upload_slots
    WORKER.config = config
    WORKER.connection = connect(config)
    WORKER.bucket = storage.Client(project=project).bucket(bucket)


def write_chunk(
    connection: oracledb.Connection, bucket: storage.Bucket, job: ChunkJob, upload_slots: BoundedSemaphore
) -> ChunkResult:
    """Lê uma faixa em lotes, grava um Parquet local e o envia ao GCS.

    A leitura é sempre ``AS OF SCN`` (``job.sql`` vem de ``select_chunk.sql``): sem o SCN a leitura por faixa de
    ROWID foi ~100x mais lenta em prod.

    :param connection: Conexão do worker.
    :param bucket: Bucket de destino.
    :param job: Faixa e destino.
    :param upload_slots: Semáforo que limita os uploads simultâneos do pod; só o envio o segura.
    :returns: Linhas, bytes e checksums gravados.
    """
    binds = {"scn": job.scn, "start_rowid": job.chunk.start_rowid, "end_rowid": job.chunk.end_rowid}
    batches = connection.fetch_df_batches(
        job.sql, binds, size=job.batch_rows, fetch_decimals=True, requested_schema=fetch_schema(job.columns)
    )
    rows = 0
    checksums = merge_checksums([], job.checksum_columns)
    with tempfile.TemporaryDirectory(prefix="oracle_to_bq_") as directory:
        path = Path(directory) / "chunk.parquet"
        with pq.ParquetWriter(path, parquet_schema(job.columns), compression=PARQUET_COMPRESSION) as writer:
            for frame in batches:
                batch = pa.table(frame)
                if batch.num_rows:
                    output = to_output_table(batch, job.columns, job.extracted_at)
                    writer.write_table(output)
                    checksums = merge_checksums(
                        [checksums, chunk_checksums(output, job.checksum_columns)], job.checksum_columns
                    )
                    rows += batch.num_rows
        if rows == 0:
            return ChunkResult(rows=0, bytes_written=0, checksums=checksums)
        size = path.stat().st_size
        with upload_slots:
            upload_file(bucket, path, job.blob_name)
    return ChunkResult(rows=rows, bytes_written=size, checksums=checksums)


def process_chunk(job: ChunkJob) -> ChunkResult:
    """Processa uma faixa no worker, reconectando se a conexão cair.

    Só falhas de conexão são repetidas; erro do banco (ex. ``ORA-01555``,
    SCN antigo demais) propaga, pois repetir não o resolve.

    :param job: Faixa e destino.
    :returns: Linhas e bytes gravados.
    :raises RuntimeError: Se o worker não foi inicializado.
    """
    if WORKER.config is None or WORKER.bucket is None or WORKER.upload_slots is None:
        raise RuntimeError("Worker sem inicialização; use init_worker como initializer do pool.")
    attempt = 1
    while True:
        try:
            if WORKER.connection is None:
                WORKER.connection = connect(WORKER.config)
            return write_chunk(WORKER.connection, WORKER.bucket, job, WORKER.upload_slots)
        except (oracledb.OperationalError, oracledb.InterfaceError):
            WORKER.connection = None
            if attempt >= MAX_CHUNK_ATTEMPTS:
                raise
            logger.warning("Conexão perdida na faixa %d (tentativa %d); reconectando", job.chunk.chunk_id, attempt)
            attempt += 1
