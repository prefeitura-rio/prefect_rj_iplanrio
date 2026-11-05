"""Teste de escala: o caminho completo com N processos, cada um com sua conexão."""

import time
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from multiprocessing import get_context

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import OracleConfig
from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import GCS_PREFIX
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.measure import build_tasks
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.memory import MAIN_BASE_MB
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.options import ProbeOptions
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.profile import TableProfile
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.session import Snapshot
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.stream import StreamTask, init_worker, run_chunk

POD_HEADROOM_MB = 512


@dataclass(frozen=True)
class ScalingRun:
    """Uma execução do teste de escala.

    :param workers: Processos de leitura.
    :param mode: ``completo`` (leitura, conversão, Parquet, envio) ou ``só fetch``.
    :param rows: Linhas lidas.
    :param parquet_bytes: Bytes de Parquet gravados.
    :param work_seconds: Do início da primeira faixa ao fim da última, sem a partida dos processos.
    :param wall_seconds: Do início do pool ao fim, com a partida dos processos.
    :param cpu_seconds: CPU somada dos workers nas faixas.
    :param skipped: Motivo de a execução ter sido ignorada; ``None`` se rodou.
    """

    workers: int
    mode: str
    rows: int
    parquet_bytes: int
    work_seconds: float
    wall_seconds: float
    cpu_seconds: float
    skipped: str | None = None


def skip_reason(workers: int, worker_mb: int, pod_memory_mb: int | None) -> str | None:
    """Explica por que ``workers`` processos não cabem na memória do pod.

    :param workers: Processos pedidos.
    :param worker_mb: Memória estimada de um worker, em MiB.
    :param pod_memory_mb: Limite de memória do pod, em MiB; ``None`` sem limite.
    :returns: O motivo, ou ``None`` se cabe.
    """
    if pod_memory_mb is None:
        return None
    estimate = MAIN_BASE_MB + workers * worker_mb
    budget = pod_memory_mb - POD_HEADROOM_MB
    if estimate <= budget:
        return None
    return (
        f"estimativa {MAIN_BASE_MB} + {workers} x {worker_mb} = {estimate:,} MiB excede o limite do pod "
        f"({pod_memory_mb:,} MiB) menos {POD_HEADROOM_MB} MiB de folga = {budget:,} MiB"
    )


def run_pool(config: OracleConfig, options: ProbeOptions, tasks: list[StreamTask], workers: int) -> ScalingRun:
    """Executa as faixas com um pool de ``workers`` processos (spawn).

    :param config: Conexão com o Oracle.
    :param options: Parâmetros da sonda.
    :param tasks: Faixas a processar.
    :param workers: Processos do pool.
    :returns: Totais e tempos da execução.
    """
    bucket = options.gcs_bucket if any(task.blob_name for task in tasks) else None
    started = time.perf_counter()
    with ProcessPoolExecutor(
        max_workers=min(workers, len(tasks)),
        mp_context=get_context("spawn"),
        initializer=init_worker,
        initargs=(config, options.project, bucket),
    ) as pool:
        results = list(pool.map(run_chunk, tasks))
    wall = time.perf_counter() - started
    return ScalingRun(
        workers=workers,
        mode="só fetch" if tasks[0].fetch_only else "completo",
        rows=sum(result.rows for result in results),
        parquet_bytes=sum(result.parquet_bytes for result in results),
        work_seconds=max(result.ended_at for result in results) - min(result.started_at for result in results),
        wall_seconds=wall,
        cpu_seconds=sum(result.cpu_seconds for result in results),
    )


def stream_tasks(
    options: ProbeOptions, profile: TableProfile, snapshot: Snapshot, fetch_only: bool
) -> list[StreamTask]:
    """Cria o trabalho de cada faixa amostrada.

    :param options: Parâmetros da sonda.
    :param profile: Perfil da tabela.
    :param snapshot: Foto de leitura.
    :param fetch_only: Se ``True``, só lê do Oracle.
    :returns: Uma tarefa por faixa.
    """
    return [
        StreamTask(
            chunk=task,
            compression=options.main_compression,
            blob_name=(
                f"{GCS_PREFIX}/{options.run_id}/{profile.table}/scale-{task.chunk.chunk_id:06d}.parquet"
                if options.test_upload and not fetch_only
                else None
            ),
            fetch_only=fetch_only,
        )
        for task in build_tasks(options, profile, snapshot)
    ]


def scale_table(
    config: OracleConfig, options: ProbeOptions, profile: TableProfile, snapshot: Snapshot, pod_memory_mb: int | None
) -> list[ScalingRun]:
    """Mede a escala do caminho completo para cada N e a leitura pura no maior N.

    :param config: Conexão com o Oracle.
    :param options: Parâmetros da sonda.
    :param profile: Perfil da tabela.
    :param snapshot: Foto de leitura.
    :param pod_memory_mb: Limite de memória do pod, em MiB.
    :returns: Execuções na ordem dos testes; as ignoradas trazem o motivo.
    """
    runs: list[ScalingRun] = []
    counts = sorted(set(options.worker_counts))
    full = stream_tasks(options, profile, snapshot, fetch_only=False)
    for workers in counts:
        reason = skip_reason(workers, profile.plan.worker_mb, pod_memory_mb)
        if reason is None and full:
            runs.append(run_pool(config, options, full, workers))
        else:
            runs.append(ScalingRun(workers, "completo", 0, 0, 0.0, 0.0, 0.0, skipped=reason or "sem faixas amostradas"))
    runnable = [run.workers for run in runs if run.skipped is None]
    if runnable and max(runnable) > 1:
        only = stream_tasks(options, profile, snapshot, fetch_only=True)
        runs.append(run_pool(config, options, only, max(runnable)))
    return runs
