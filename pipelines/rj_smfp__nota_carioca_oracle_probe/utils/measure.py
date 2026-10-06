"""Medição das etapas, em um processo, sobre as faixas amostradas de uma tabela."""

import tempfile
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import oracledb
import pyarrow as pa
from google.cloud import storage

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import OracleConfig
from pipelines.rj_smfp__nota_carioca_oracle_probe.constants import GCS_PREFIX
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.convert import to_output_table
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.gcs import open_bucket, upload_then_delete
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.options import ProbeOptions
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.profile import TableProfile
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.session import Snapshot, connect_read_only
from pipelines.rj_smfp__nota_carioca_oracle_probe.utils.work import (
    ChunkTask,
    StageTiming,
    fetch_batches,
    peak_rss_mb,
    render_select,
    timed,
    write_parquet,
)


@dataclass(frozen=True)
class VariantWrite:
    """Gravação do Parquet com uma compressão.

    :param compression: Compressão usada.
    :param timing: Tempo da gravação.
    :param file_bytes: Tamanho do arquivo.
    """

    compression: str
    timing: StageTiming
    file_bytes: int


@dataclass(frozen=True)
class ChunkStages:
    """Etapas medidas separadamente para uma faixa.

    :param chunk_id: Faixa medida.
    :param rows: Linhas lidas; zero se a faixa estava vazia.
    :param fetched_bytes: Bytes Arrow entregues pelo driver.
    :param fetch: Leitura do Oracle até ``pa.table``.
    :param convert: ``to_output_table``.
    :param converted_bytes: Bytes Arrow depois da conversão.
    :param writes: Gravação do Parquet por compressão.
    :param upload_seconds: Envio da primeira variante ao GCS; ``None`` sem envio.
    :param upload_bytes: Bytes enviados.
    :param peak_rss_mb: Pico de memória do processo ao final da faixa.
    """

    chunk_id: int
    rows: int
    fetched_bytes: int
    fetch: StageTiming
    convert: StageTiming
    converted_bytes: int
    writes: tuple[VariantWrite, ...]
    upload_seconds: float | None
    upload_bytes: int
    peak_rss_mb: float


@dataclass(frozen=True)
class FetchRun:
    """Leitura sem conversão, para comparar com e sem ``AS OF SCN``.

    :param label: Descrição da leitura.
    :param rows: Linhas lidas.
    :param timing: Tempo da leitura.
    """

    label: str
    rows: int
    timing: StageTiming


@dataclass(frozen=True)
class TableBenchmark:
    """Medições de uma tabela em um processo.

    :param table: Nome da tabela.
    :param chunks: Etapas por faixa amostrada.
    :param flashback: Leituras de uma mesma faixa com e sem SCN; vazio se não pedido.
    """

    table: str
    chunks: tuple[ChunkStages, ...]
    flashback: tuple[FetchRun, ...]


@dataclass(frozen=True)
class UploadTarget:
    """Destino do envio de teste.

    :param bucket: Bucket do GCS.
    :param blob_name: Objeto sob o prefixo da sonda.
    """

    bucket: storage.Bucket
    blob_name: str


def measure_chunk(
    connection: oracledb.Connection, task: ChunkTask, variants: tuple[str, ...], target: UploadTarget | None
) -> ChunkStages:
    """Mede, uma a uma, leitura, conversão, Parquet por compressão e envio de uma faixa.

    :param connection: Conexão aberta.
    :param task: Faixa a medir.
    :param variants: Compressões; a primeira é a enviada ao GCS.
    :param target: Destino do envio, ou ``None`` para não enviar.
    :returns: Tempos e tamanhos de cada etapa.
    """
    batches, fetch = timed(lambda: [pa.table(frame) for frame in fetch_batches(connection, task)])
    batches = [batch for batch in batches if batch.num_rows]
    rows = sum(batch.num_rows for batch in batches)
    converted, convert = timed(lambda: [to_output_table(batch, task.columns, task.extracted_at) for batch in batches])
    writes: list[VariantWrite] = []
    upload_seconds: float | None = None
    upload_bytes = 0
    with tempfile.TemporaryDirectory(prefix="oracle_probe_") as directory:
        for variant in variants if rows else ():
            path = Path(directory) / f"chunk-{variant}.parquet"
            size, timing = timed(partial(write_parquet, path, converted, task.columns, variant))
            writes.append(VariantWrite(variant, timing, size))
            if target is not None and variant == variants[0]:
                upload_seconds, upload_bytes = upload_then_delete(target.bucket, path, target.blob_name), size
    return ChunkStages(
        chunk_id=task.chunk.chunk_id,
        rows=rows,
        fetched_bytes=sum(batch.nbytes for batch in batches),
        fetch=fetch,
        convert=convert,
        converted_bytes=sum(table.nbytes for table in converted),
        writes=tuple(writes),
        upload_seconds=upload_seconds,
        upload_bytes=upload_bytes,
        peak_rss_mb=peak_rss_mb(),
    )


def fetch_only(connection: oracledb.Connection, task: ChunkTask, label: str) -> FetchRun:
    """Lê a faixa inteira descartando os lotes.

    :param connection: Conexão aberta.
    :param task: Faixa a ler.
    :param label: Descrição da leitura no relatório.
    :returns: Linhas e tempo.
    """
    rows, timing = timed(lambda: sum(pa.table(frame).num_rows for frame in fetch_batches(connection, task)))
    return FetchRun(label, rows, timing)


def compare_flashback(
    connection: oracledb.Connection, with_scn: ChunkTask, without_scn: ChunkTask
) -> tuple[FetchRun, ...]:
    """Lê a mesma faixa com SCN, sem SCN e com SCN de novo, para separar o efeito do cache.

    :param connection: Conexão aberta.
    :param with_scn: Faixa com ``AS OF SCN``.
    :param without_scn: A mesma faixa sem ``AS OF SCN``.
    :returns: As três leituras, na ordem.
    """
    return (
        fetch_only(connection, with_scn, "AS OF SCN (1ª leitura)"),
        fetch_only(connection, without_scn, "sem SCN (2ª leitura)"),
        fetch_only(connection, with_scn, "AS OF SCN (3ª leitura)"),
    )


def build_tasks(options: ProbeOptions, profile: TableProfile, snapshot: Snapshot) -> list[ChunkTask]:
    """Cria a leitura ``AS OF SCN`` de cada faixa amostrada.

    :param options: Parâmetros da sonda.
    :param profile: Perfil da tabela.
    :param snapshot: Foto de leitura.
    :returns: Uma leitura por faixa.
    """
    sql = render_select(profile.columns, options.schema, profile.table, with_scn=True)
    return [
        ChunkTask(sql, chunk, snapshot.scn, snapshot.taken_at, profile.columns, profile.plan.batch_rows)
        for chunk in profile.sampled
    ]


def benchmark_table(
    config: OracleConfig, options: ProbeOptions, profile: TableProfile, snapshot: Snapshot
) -> TableBenchmark:
    """Mede as etapas de cada faixa amostrada de uma tabela, em um processo.

    :param config: Conexão com o Oracle.
    :param options: Parâmetros da sonda.
    :param profile: Perfil da tabela.
    :param snapshot: Foto de leitura.
    :returns: Medições por faixa e a comparação com/sem SCN.
    """
    tasks = build_tasks(options, profile, snapshot)
    bucket = open_bucket(options.project, options.gcs_bucket) if options.test_upload else None
    stages: list[ChunkStages] = []
    flashback: tuple[FetchRun, ...] = ()
    with connect_read_only(config) as connection:
        for task in tasks:
            blob_name = f"{GCS_PREFIX}/{options.run_id}/{profile.table}/bench-{task.chunk.chunk_id:06d}.parquet"
            target = None if bucket is None else UploadTarget(bucket, blob_name)
            stages.append(measure_chunk(connection, task, options.compression_variants, target))
        first = next((task for task, stage in zip(tasks, stages, strict=True) if stage.rows), None)
        if options.compare_no_scn and first is not None:
            plain_sql = render_select(profile.columns, options.schema, profile.table, with_scn=False)
            plain = ChunkTask(plain_sql, first.chunk, None, first.extracted_at, first.columns, first.batch_rows)
            flashback = compare_flashback(connection, first, plain)
    return TableBenchmark(profile.table, tuple(stages), flashback)
