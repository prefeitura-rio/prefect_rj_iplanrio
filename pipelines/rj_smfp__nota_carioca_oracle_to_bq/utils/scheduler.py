"""Agendamento da extração: leitura em processos, envio em threads e contrapressão pelo disco local."""

import time
from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import Progress
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.worker import ChunkJob, ChunkResult


@dataclass(frozen=True)
class Limits:
    """Limites de concorrência e de disco da extração.

    :param workers: Faixas lidas ao mesmo tempo.
    :param upload_concurrency: Uploads ao GCS simultâneos.
    :param max_pending_files: Teto de Parquet locais: faixas em leitura (cada uma vira um arquivo) mais arquivos já
        gravados e ainda não enviados. Nenhuma faixa nova é iniciada com o teto atingido, então o spool nunca passa
        de ``max_pending_files`` arquivos e os pendentes de envio também não.
    :param progress_interval_seconds: Intervalo entre linhas de progresso.
    """

    workers: int
    upload_concurrency: int
    max_pending_files: int
    progress_interval_seconds: float


@dataclass(frozen=True)
class ChunkRun:
    """Resultado do agendamento.

    :param results: Resultado de cada faixa, na ordem de ``jobs``; todos os arquivos já foram enviados e apagados.
    :param max_pending_files: Maior número de Parquet locais pendentes de envio observado.
    :param max_local_files: Maior número de faixas em leitura mais arquivos pendentes observado (teto do spool).
    """

    results: list[ChunkResult]
    max_pending_files: int
    max_local_files: int


class ChunkScheduler:
    """Estado de uma execução: faixas em leitura, uploads em andamento e contadores; só a thread principal o usa."""

    def __init__(
        self,
        submit: Callable[[ChunkJob], Future[ChunkResult]],
        upload: Callable[[ChunkJob, ChunkResult], None],
        limits: Limits,
        uploader: ThreadPoolExecutor,
    ) -> None:
        """Guarda as dependências; ``uploader`` é o pool das threads de upload, encerrado por quem o criou."""
        self.submit = submit
        self.upload = upload
        self.limits = limits
        self.uploader = uploader
        self.reading: dict[Future[ChunkResult], ChunkJob] = {}
        self.uploading: dict[Future[ChunkResult], int] = {}
        self.read: dict[int, ChunkResult] = {}
        self.uploaded = 0
        self.bytes_uploaded = 0
        self.max_pending = 0
        self.max_local = 0

    def local_files(self) -> int:
        """Conta os arquivos que existem ou vão existir no spool: faixas em leitura mais arquivos pendentes de envio."""
        return len(self.reading) + len(self.uploading)

    def start_ready(self, queue: list[ChunkJob]) -> None:
        """Inicia faixas da fila enquanto couberem nos workers e no limite de arquivos pendentes."""
        while queue and len(self.reading) < self.limits.workers and self.local_files() < self.limits.max_pending_files:
            job = queue.pop()
            self.reading[self.submit(job)] = job
            self.max_local = max(self.max_local, self.local_files())

    def handle(self, future: Future[ChunkResult]) -> None:
        """Processa um futuro concluído: um upload (conta a faixa como enviada) ou uma leitura.

        :param future: Leitura ou upload que terminou.
        :raises Exception: A falha da leitura ou do upload.
        """
        if future in self.uploading:
            future.result()
            self.uploaded += 1
            self.bytes_uploaded += self.uploading.pop(future)
        else:
            self.finish_read(future)
        self.max_local = max(self.max_local, self.local_files())

    def finish_read(self, future: Future[ChunkResult]) -> None:
        """Registra uma faixa lida e, se ela gerou arquivo, entrega-o a uma thread de upload.

        :param future: Leitura concluída.
        :raises Exception: A falha da leitura.
        """
        job = self.reading.pop(future)
        result = future.result()
        self.read[job.chunk.chunk_id] = result
        if result.path is None:
            self.uploaded += 1
            return
        self.uploading[self.uploader.submit(self.send, job, result)] = result.bytes_written
        self.max_pending = max(self.max_pending, len(self.uploading))

    def send(self, job: ChunkJob, result: ChunkResult) -> ChunkResult:
        """Envia o arquivo de uma faixa numa thread de upload.

        :param job: Faixa lida.
        :param result: Resultado da leitura, com o arquivo.
        :returns: O mesmo resultado, depois do envio.
        """
        self.upload(job, result)
        return result

    def progress(self, total: int, elapsed: float) -> Progress:
        """Monta o estado de progresso atual.

        :param total: Faixas planejadas.
        :param elapsed: Segundos desde o início.
        :returns: Estado para :func:`format_progress`.
        """
        return Progress(
            chunks_read=len(self.read),
            chunks_uploaded=self.uploaded,
            chunks_total=total,
            rows_read=sum(result.rows for result in self.read.values()),
            bytes_uploaded=self.bytes_uploaded,
            pending_files=len(self.uploading),
            elapsed_seconds=elapsed,
        )

    def abort(self) -> None:
        """Cancela as leituras não iniciadas e encerra as threads de upload, esperando as em andamento."""
        for future in self.reading:
            future.cancel()
        self.uploader.shutdown(wait=True, cancel_futures=True)


def run_chunks(
    jobs: list[ChunkJob],
    submit: Callable[[ChunkJob], Future[ChunkResult]],
    upload: Callable[[ChunkJob, ChunkResult], None],
    limits: Limits,
    report: Callable[[Progress], None],
) -> ChunkRun:
    """Lê as faixas com no máximo ``workers`` em andamento e envia cada Parquet assim que ele fica pronto.

    As faixas são entregues a ``submit`` sob demanda: nenhuma é iniciada se faixas em leitura mais arquivos
    esperando envio já somarem ``max_pending_files``, o que limita o disco. Uma faixa só conta como concluída
    depois do upload.

    :param jobs: Faixas a ler.
    :param submit: Inicia a leitura de uma faixa e devolve o futuro do resultado (um processo worker).
    :param upload: Envia o arquivo de um resultado e o apaga do disco; roda numa thread de upload.
    :param limits: Concorrência e limites de disco.
    :param report: Publica o progresso a cada intervalo.
    :returns: Resultados e os picos observados.
    :raises Exception: A primeira falha de leitura ou de upload, depois de parar de iniciar faixas, cancelar as
        pendentes e encerrar as threads de upload.
    """
    started = time.monotonic()
    last_report = started
    queue = list(reversed(jobs))
    with ThreadPoolExecutor(max_workers=limits.upload_concurrency, thread_name_prefix="gcs-upload") as uploader:
        state = ChunkScheduler(submit, upload, limits, uploader)
        try:
            while queue or state.reading or state.uploading:
                state.start_ready(queue)
                done, _ = wait(
                    {*state.reading, *state.uploading},
                    timeout=limits.progress_interval_seconds,
                    return_when=FIRST_COMPLETED,
                )
                for future in done:
                    state.handle(future)
                now = time.monotonic()
                if now - last_report >= limits.progress_interval_seconds:
                    report(state.progress(len(jobs), now - started))
                    last_report = now
        except BaseException:
            state.abort()
            raise
    return ChunkRun([state.read[job.chunk.chunk_id] for job in jobs], state.max_pending, state.max_local)
