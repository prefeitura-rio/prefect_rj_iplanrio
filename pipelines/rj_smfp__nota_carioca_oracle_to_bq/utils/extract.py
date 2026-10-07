"""Extração paralela de uma tabela do Oracle para arquivos Parquet no GCS."""

import tempfile
import time
from collections.abc import Callable, Mapping
from concurrent.futures import Executor, ProcessPoolExecutor
from dataclasses import dataclass
from functools import partial
from multiprocessing import get_context
from pathlib import Path

from google.cloud import storage

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import GCS_PREFIX, QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.checksum import ColumnChecksum, merge_checksums
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.chunks import Chunk, ChunkRequest, chunk_task_name, rowid_chunks
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.gcs import blob_prefix, upload_file
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.memory import WorkerMemory, plan_worker_memory
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import (
    OracleConfig,
    Snapshot,
    count_as_of_scn,
    validate_identifier,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.overlap import BackgroundCount
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import format_progress, format_size
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.scheduler import ChunkRun, Limits, run_chunks
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.worker import ChunkJob, ChunkResult, init_worker, process_chunk
from prefect_rj_iplanrio.sql import load_query


@dataclass(frozen=True)
class ExtractOptions:
    """Parâmetros de desempenho da extração.

    :param workers: Processos de leitura, cada um com sua conexão.
    :param chunk_size_blocks: Tamanho aproximado de cada faixa de ROWID, em blocos.
    :param batch_rows: Teto de linhas por lote lido do Oracle; o lote real sai de ``worker_memory_mb``.
    :param worker_memory_mb: Orçamento de memória de cada worker, em MiB; define o lote de cada tabela.
    :param pod_memory_mb: Orçamento de memória do pod, em MiB: o REQUEST de memória do pod (2 GiB no template de job
        do K3s aplicado) menos 256 MiB de folga, e não o limite de 8 GiB. Usar mais que o request deixa o scheduler
        superalocar o nó, que ficou NotReady no incidente; a extração falha antes de começar se não couber.
    :param progress_interval_seconds: Intervalo entre linhas de progresso.
    :param upload_concurrency: Uploads ao GCS simultâneos no pod, em threads do processo principal; o link até o
        bucket é irregular (alguns fluxos TCP se arrastam a ~3 MB/s), então mais fluxos em paralelo compensam os lentos.
    :param max_pending_files: Teto de Parquet no spool local: faixas em leitura mais arquivos gravados e ainda não
        enviados. Com o teto atingido nenhuma faixa nova é iniciada; limita o disco usado.
    """

    workers: int = 2
    chunk_size_blocks: int = 32768
    batch_rows: int = 50_000
    worker_memory_mb: int = 640
    pod_memory_mb: int = 1792
    progress_interval_seconds: int = 30
    upload_concurrency: int = 4
    max_pending_files: int = 4

    def __post_init__(self) -> None:
        """Valida os parâmetros.

        :raises ValueError: Se ``upload_concurrency`` ou ``max_pending_files`` for menor que 1.
        """
        if self.upload_concurrency < 1:
            raise ValueError(f"upload_concurrency deve ser >= 1, recebido {self.upload_concurrency}.")
        if self.max_pending_files < 1:
            raise ValueError(f"max_pending_files deve ser >= 1, recebido {self.max_pending_files}.")


@dataclass(frozen=True)
class ExtractRequest:
    """Tabela a extrair e destino dos arquivos.

    :param config: Conexão com o Oracle.
    :param schema: Dono da tabela.
    :param table: Nome da tabela.
    :param columns: Colunas do SELECT, na ordem da tabela.
    :param checksum_columns: Colunas ``NUMBER`` do checksum de conteúdo.
    :param snapshot: Foto consistente de leitura.
    :param project: Projeto do GCS.
    :param bucket: Bucket de destino.
    :param run_id: Identificador do flow run, que isola os arquivos.
    :param options: Parâmetros de desempenho.
    """

    config: OracleConfig
    schema: str
    table: str
    columns: tuple[OracleColumn, ...]
    snapshot: Snapshot
    project: str
    bucket: str
    run_id: str
    options: ExtractOptions
    checksum_columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class ExtractResult:
    """Resultado da extração de uma tabela.

    :param table: Nome da tabela.
    :param rows: Linhas gravadas e enviadas em todos os arquivos.
    :param bytes_written: Bytes de Parquet gravados e enviados.
    :param chunks: Faixas lidas.
    :param files: Arquivos gerados (faixas vazias não geram arquivo).
    :param prefix: Prefixo dos arquivos no bucket.
    :param seconds: Duração da extração.
    :param checksums: Contagem de não nulos e soma exata de cada coluna de checksum, somadas sobre todas as faixas.
    :param oracle_rows: Linhas da tabela no Oracle ``AS OF SCN``, contadas em segundo plano durante a extração.
    :param max_pending_files: Pico de Parquet locais esperando envio.
    :param max_local_files: Pico de arquivos no spool (pendentes mais faixas em leitura).
    """

    table: str
    rows: int
    bytes_written: int
    chunks: int
    files: int
    prefix: str
    seconds: float
    checksums: Mapping[str, ColumnChecksum]
    oracle_rows: int
    max_pending_files: int
    max_local_files: int


def worker_memory(request: ExtractRequest) -> WorkerMemory:
    """Dimensiona o lote de leitura da tabela a partir do orçamento de memória do worker.

    :param request: Tabela e parâmetros de desempenho.
    :returns: Lote escolhido e memória estimada de um worker.
    """
    return plan_worker_memory(request.columns, request.options.worker_memory_mb, request.options.batch_rows)


def build_jobs(request: ExtractRequest, chunks: list[Chunk], batch_rows: int) -> list[ChunkJob]:
    """Cria um trabalho por faixa, com o SELECT já renderizado.

    :param request: Tabela e destino.
    :param chunks: Faixas de ROWID.
    :param batch_rows: Linhas por lote lido do Oracle.
    :returns: Trabalhos, na ordem das faixas.
    """
    sql = load_query(
        QUERIES_ANCHOR,
        "select_chunk",
        columns=", ".join(column.quoted for column in request.columns),
        schema=validate_identifier(request.schema),
        table=validate_identifier(request.table),
    )
    prefix = blob_prefix(GCS_PREFIX, request.table, request.run_id)
    return [
        ChunkJob(
            sql=sql,
            chunk=chunk,
            scn=request.snapshot.scn,
            extracted_at=request.snapshot.taken_at,
            columns=request.columns,
            blob_name=f"{prefix}/chunk-{chunk.chunk_id:06d}.parquet",
            batch_rows=batch_rows,
            checksum_columns=request.checksum_columns,
        )
        for chunk in chunks
    ]


def open_pool(request: ExtractRequest, spool: Path, jobs: int) -> Executor:
    """Abre o pool de processos de leitura; cada worker abre a sua conexão e grava no spool.

    :param request: Tabela e parâmetros de desempenho.
    :param spool: Diretório do spool local.
    :param jobs: Faixas a ler; limita o número de processos.
    :returns: Pool com ``min(workers, jobs)`` processos.
    """
    return ProcessPoolExecutor(
        max_workers=min(request.options.workers, jobs),
        mp_context=get_context("spawn"),
        initializer=init_worker,
        initargs=(request.config, spool),
    )


def open_bucket(request: ExtractRequest) -> storage.Bucket:
    """Abre o bucket de destino com o client do GCS do processo principal, usado por todas as threads de upload.

    :param request: Tabela e destino.
    :returns: Bucket.
    """
    return storage.Client(project=request.project).bucket(request.bucket)


def upload_and_remove(bucket: storage.Bucket, job: ChunkJob, result: ChunkResult) -> None:
    """Envia o Parquet de uma faixa e o apaga do spool; se o envio falhar, o arquivo fica para a limpeza do spool.

    :param bucket: Bucket de destino.
    :param job: Faixa, com o nome do objeto.
    :param result: Resultado da leitura, com o caminho do arquivo.
    :raises RuntimeError: Se o resultado não tiver arquivo.
    """
    if result.path is None:
        raise RuntimeError(f"Faixa {job.chunk.chunk_id} sem arquivo para enviar.")
    upload_file(bucket, result.path, job.blob_name)
    result.path.unlink()


def run_extraction(request: ExtractRequest, jobs: list[ChunkJob], report: Callable[[str], None]) -> ChunkRun:
    """Lê as faixas em processos e as envia ao GCS em threads, usando um spool local temporário.

    O spool é apagado ao sair, com sucesso ou falha, depois de encerrado o pool de processos.

    :param request: Tabela e destino.
    :param jobs: Faixas a extrair; vazio não abre nenhum processo.
    :param report: Função que publica uma linha de log.
    :returns: Resultados das faixas e os picos do spool.
    """
    if not jobs:
        return ChunkRun([], 0, 0)
    options = request.options
    limits = Limits(
        options.workers, options.upload_concurrency, options.max_pending_files, options.progress_interval_seconds
    )
    bucket = open_bucket(request)
    with (
        tempfile.TemporaryDirectory(prefix="oracle_to_bq_") as directory,
        open_pool(request, Path(directory), len(jobs)) as pool,
    ):
        try:
            return run_chunks(
                jobs,
                lambda job: pool.submit(process_chunk, job),
                partial(upload_and_remove, bucket),
                limits,
                lambda progress: report(format_progress(request.table, progress)),
            )
        except BaseException:
            pool.shutdown(wait=True, cancel_futures=True)
            raise


def extract_table(request: ExtractRequest, report: Callable[[str], None]) -> ExtractResult:
    """Extrai a tabela inteira, ``AS OF SCN``, para Parquet no GCS e, ao mesmo tempo, conta as linhas no Oracle.

    Os processos leem e gravam Parquet num spool local; threads do processo principal os enviam. A contagem
    ``COUNT(*) AS OF SCN`` roda numa thread, com conexão própria, desde o início das faixas até o fim da extração.

    :param request: Tabela e destino.
    :param report: Função que publica uma linha de log.
    :returns: Totais da extração, incluindo a contagem do Oracle.
    :raises Exception: A falha de um worker, de um upload ou da contagem, depois de encerrar workers e threads.
    """
    started = time.monotonic()
    chunk_request = ChunkRequest(
        config=request.config,
        schema=request.schema,
        table=request.table,
        task_name=chunk_task_name(request.table, request.run_id),
        chunk_size_blocks=request.options.chunk_size_blocks,
    )
    memory = worker_memory(request)
    with rowid_chunks(chunk_request) as chunks:
        jobs = build_jobs(request, chunks, memory.batch_rows)
        report(f"{request.table}: {len(jobs)} faixas de ROWID (~{request.options.chunk_size_blocks} blocos cada)")
        report(
            f"{request.table}: lote de {memory.batch_rows:,} linhas ({memory.row_bytes:,} bytes reservados por linha); "
            f"~{memory.worker_mb} MiB por worker, {request.options.workers} workers, "
            f"{request.options.upload_concurrency} uploads simultâneos, até {request.options.max_pending_files} "
            "arquivos locais pendentes"
        )
        count = BackgroundCount(
            request.table,
            partial(count_as_of_scn, request.config, request.schema, request.table, request.snapshot),
            report,
        )
        count.start()
        try:
            run = run_extraction(request, jobs, report)
        except BaseException:
            count.abandon()
            raise
    counted = count.result()
    results = run.results
    report(
        f"{request.table}: {format_size(sum(result.bytes_written for result in results))} enviados; pico de "
        f"{run.max_pending_files} arquivos pendentes e {run.max_local_files} no spool; contagem do Oracle levou "
        f"{counted.seconds:.0f} s, em paralelo à extração"
    )
    return ExtractResult(
        table=request.table,
        rows=sum(result.rows for result in results),
        bytes_written=sum(result.bytes_written for result in results),
        chunks=len(jobs),
        files=sum(1 for result in results if result.rows),
        prefix=blob_prefix(GCS_PREFIX, request.table, request.run_id),
        seconds=time.monotonic() - started,
        checksums=merge_checksums([result.checksums for result in results], request.checksum_columns),
        oracle_rows=counted.rows,
        max_pending_files=run.max_pending_files,
        max_local_files=run.max_local_files,
    )
