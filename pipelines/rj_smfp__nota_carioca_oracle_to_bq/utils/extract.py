"""Extração paralela de uma tabela do Oracle para arquivos Parquet no GCS."""

import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, wait
from dataclasses import dataclass
from multiprocessing import get_context

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import GCS_PREFIX, QUERIES_ANCHOR
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.chunks import Chunk, ChunkRequest, chunk_task_name, rowid_chunks
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.columns import OracleColumn
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.gcs import blob_prefix
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.memory import WorkerMemory, plan_worker_memory
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.oracle import (
    OracleConfig,
    Snapshot,
    validate_identifier,
)
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import Progress, format_progress
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.worker import ChunkJob, ChunkResult, init_worker, process_chunk
from prefect_rj_iplanrio.sql import load_query


@dataclass(frozen=True)
class ExtractOptions:
    """Parâmetros de desempenho da extração.

    :param workers: Processos de leitura, cada um com sua conexão.
    :param chunk_size_blocks: Tamanho aproximado de cada faixa de ROWID, em blocos.
    :param batch_rows: Teto de linhas por lote lido do Oracle; o lote real sai de ``worker_memory_mb``.
    :param worker_memory_mb: Orçamento de memória de cada worker, em MiB; define o lote de cada tabela.
    :param pod_memory_mb: Orçamento de memória do pod, em MiB; a extração falha antes de começar se não couber.
    :param progress_interval_seconds: Intervalo entre linhas de progresso.
    :param upload_concurrency: Uploads ao GCS simultâneos no pod; o link até o bucket
        é lento e muitos uploads em paralelo estouram o timeout de escrita.
    """

    workers: int = 2
    chunk_size_blocks: int = 32768
    batch_rows: int = 50_000
    worker_memory_mb: int = 1536
    pod_memory_mb: int = 7168
    progress_interval_seconds: int = 30
    upload_concurrency: int = 2

    def __post_init__(self) -> None:
        """Valida os parâmetros.

        :raises ValueError: Se ``upload_concurrency`` for menor que 1.
        """
        if self.upload_concurrency < 1:
            raise ValueError(f"upload_concurrency deve ser >= 1, recebido {self.upload_concurrency}.")


@dataclass(frozen=True)
class ExtractRequest:
    """Tabela a extrair e destino dos arquivos.

    :param config: Conexão com o Oracle.
    :param schema: Dono da tabela.
    :param table: Nome da tabela.
    :param columns: Colunas do SELECT, na ordem da tabela.
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


@dataclass(frozen=True)
class ExtractResult:
    """Resultado da extração de uma tabela.

    :param table: Nome da tabela.
    :param rows: Linhas gravadas em todos os arquivos.
    :param bytes_written: Bytes de Parquet enviados.
    :param chunks: Faixas lidas.
    :param files: Arquivos gerados (faixas vazias não geram arquivo).
    :param prefix: Prefixo dos arquivos no bucket.
    :param seconds: Duração da extração.
    """

    table: str
    rows: int
    bytes_written: int
    chunks: int
    files: int
    prefix: str
    seconds: float


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
        )
        for chunk in chunks
    ]


def collect_results(
    request: ExtractRequest, futures: list[Future[ChunkResult]], report: Callable[[str], None]
) -> list[ChunkResult]:
    """Espera os workers, reportando o progresso a cada intervalo.

    :param request: Tabela em extração.
    :param futures: Uma por faixa.
    :param report: Função que publica uma linha de log.
    :returns: Resultados de todas as faixas.
    :raises Exception: A primeira falha de um worker, depois de cancelar as faixas pendentes.
    """
    started = time.monotonic()
    last_report = started
    pending = set(futures)
    while pending:
        done, pending = wait(pending, timeout=request.options.progress_interval_seconds, return_when=FIRST_COMPLETED)
        for future in done:
            error = future.exception()
            if error is not None:
                for other in pending:
                    other.cancel()
                raise error
        now = time.monotonic()
        if now - last_report >= request.options.progress_interval_seconds:
            finished = [future.result() for future in futures if future.done() and not future.cancelled()]
            report(
                format_progress(
                    request.table,
                    Progress(
                        chunks_done=len(finished),
                        chunks_total=len(futures),
                        rows=sum(result.rows for result in finished),
                        bytes_written=sum(result.bytes_written for result in finished),
                        elapsed_seconds=now - started,
                    ),
                )
            )
            last_report = now
    return [future.result() for future in futures]


def extract_table(request: ExtractRequest, report: Callable[[str], None]) -> ExtractResult:
    """Extrai a tabela inteira, ``AS OF SCN``, para Parquet no GCS usando um pool de processos.

    :param request: Tabela e destino.
    :param report: Função que publica uma linha de log.
    :returns: Totais da extração.
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
            f"~{memory.worker_mb} MiB por worker, {request.options.workers} workers"
        )
        if not jobs:
            results: list[ChunkResult] = []
        else:
            context = get_context("spawn")
            with ProcessPoolExecutor(
                max_workers=min(request.options.workers, len(jobs)),
                mp_context=context,
                initializer=init_worker,
                initargs=(
                    request.config,
                    request.project,
                    request.bucket,
                    context.BoundedSemaphore(request.options.upload_concurrency),
                ),
            ) as pool:
                try:
                    results = collect_results(request, [pool.submit(process_chunk, job) for job in jobs], report)
                except BaseException:
                    pool.shutdown(wait=True, cancel_futures=True)
                    raise
    return ExtractResult(
        table=request.table,
        rows=sum(result.rows for result in results),
        bytes_written=sum(result.bytes_written for result in results),
        chunks=len(jobs),
        files=sum(1 for result in results if result.rows),
        prefix=blob_prefix(GCS_PREFIX, request.table, request.run_id),
        seconds=time.monotonic() - started,
    )
