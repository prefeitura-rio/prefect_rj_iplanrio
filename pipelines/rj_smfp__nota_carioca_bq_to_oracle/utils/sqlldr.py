"""Carga no Oracle com SQL*Loader em direct path, lendo os CSV gzip direto do GCS."""

import gzip
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO

from google.cloud import storage

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.bigquery import ExportedFile
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import LoaderField
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import OracleConfig
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

SQLLDR_BIN = "sqlldr"
COPY_BUFFER_BYTES = 1024 * 1024
LOADED_PATTERN = re.compile(r"^\s*(\d+) Rows? successfully loaded\.", re.MULTILINE)
GIGABYTE = 1024**3
POLL_SECONDS = 0.5


@dataclass(frozen=True)
class LoadJob:
    """Carga de uma tabela a partir dos arquivos exportados pelo BigQuery.

    :param table: Tabela de destino no Oracle, já vazia.
    :param fields: Campos do CSV, na ordem do schema do BigQuery.
    :param bucket: Bucket dos arquivos.
    :param files: Arquivos ``.csv.gz`` exportados, com tamanho.
    :param sessions: Número máximo de sessões simultâneas do SQL*Loader.
    """

    table: str
    fields: list[LoaderField]
    bucket: str
    files: list[ExportedFile]
    sessions: int


@dataclass(frozen=True)
class ProgressSnapshot:
    """Retrato do andamento da carga num instante.

    :param files_done: Arquivos já enviados por completo ao SQL*Loader.
    :param files_total: Total de arquivos da carga.
    :param bytes_done: Bytes comprimidos já lidos do GCS e enviados.
    :param bytes_total: Total de bytes comprimidos da carga.
    :param elapsed_seconds: Segundos desde o início da carga.
    """

    files_done: int
    files_total: int
    bytes_done: int
    bytes_total: int
    elapsed_seconds: float


@dataclass(frozen=True)
class SessionResult:
    """Resultado de uma sessão do SQL*Loader.

    :param index: Número da sessão.
    :param files: Quantidade de arquivos enviados à sessão.
    :param rows: Linhas carregadas pela sessão.
    """

    index: int
    files: int
    rows: int


@dataclass(frozen=True)
class LoadResult:
    """Resultado da carga de uma tabela.

    :param rows: Total de linhas carregadas.
    :param sessions: Resultado de cada sessão.
    :param elapsed_seconds: Duração da carga.
    """

    rows: int
    sessions: list[SessionResult]
    elapsed_seconds: float


@dataclass
class SessionProgress:
    """Contadores de uma sessão, atualizados pela thread que envia os arquivos.

    :param bytes_done: Bytes comprimidos lidos do GCS.
    :param files_done: Arquivos enviados por completo.
    :param lock: Trava que protege os contadores entre threads.
    """

    bytes_done: int = 0
    files_done: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)

    def add_bytes(self, amount: int) -> None:
        """Soma bytes lidos."""
        with self.lock:
            self.bytes_done += amount

    def add_file(self) -> None:
        """Soma um arquivo concluído."""
        with self.lock:
            self.files_done += 1


class CountingReader:
    """Leitor que repassa os bytes de outro arquivo e contabiliza o que foi lido.

    :param raw: Arquivo de origem (o objeto do GCS aberto para leitura).
    :param progress: Contadores da sessão.
    """

    def __init__(self, raw: BinaryIO, progress: SessionProgress) -> None:
        self.raw = raw
        self.progress = progress

    def read(self, size: int = -1) -> bytes:
        """Lê do arquivo de origem e soma o tamanho lido ao progresso."""
        data = self.raw.read(size)
        self.progress.add_bytes(len(data))
        return data


def format_count(value: int) -> str:
    """Formata um número inteiro com ponto como separador de milhar.

    :param value: Número a formatar.
    :returns: Por exemplo ``67.987.860``.
    """
    return f"{value:,}".replace(",", ".")


def format_size(size: int) -> str:
    """Formata um tamanho em GB com vírgula decimal.

    :param size: Tamanho em bytes.
    :returns: Por exemplo ``1,23 GB``.
    """
    return f"{size / GIGABYTE:.2f} GB".replace(".", ",")


def format_duration(seconds: float) -> str:
    """Formata uma duração em horas, minutos e segundos.

    :param seconds: Duração em segundos.
    :returns: Por exemplo ``1h02m05s`` ou ``4m10s``.
    """
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m{secs:02d}s" if hours else f"{minutes}m{secs:02d}s"


def progress_percent(snapshot: ProgressSnapshot) -> float:
    """Calcula o percentual da carga pelos bytes enviados.

    :param snapshot: Retrato do andamento.
    :returns: Percentual de 0 a 100.
    """
    if snapshot.bytes_total <= 0:
        return 100.0 if snapshot.files_done >= snapshot.files_total else 0.0
    return min(100.0, 100.0 * snapshot.bytes_done / snapshot.bytes_total)


def format_progress(table: str, snapshot: ProgressSnapshot) -> str:
    """Descreve o andamento da carga para o log.

    A estimativa de término supõe que o ritmo até agora se mantém.

    :param table: Tabela de destino.
    :param snapshot: Retrato do andamento.
    :returns: Linha de progresso.
    """
    percent = progress_percent(snapshot)
    if 0 < snapshot.bytes_done < snapshot.bytes_total:
        remaining = snapshot.elapsed_seconds * (snapshot.bytes_total - snapshot.bytes_done) / snapshot.bytes_done
        estimate = f"término estimado em ~{format_duration(remaining)}"
    elif snapshot.bytes_done >= snapshot.bytes_total:
        estimate = "arquivos enviados; aguardando o SQL*Loader concluir"
    else:
        estimate = "calculando estimativa"
    percent_text = f"{percent:.1f}".replace(".", ",")
    return (
        f"Carga de {table}: {snapshot.files_done}/{snapshot.files_total} arquivos, "
        f"{format_size(snapshot.bytes_done)} de {format_size(snapshot.bytes_total)} ({percent_text}%), "
        f"{format_duration(snapshot.elapsed_seconds)} decorridos, {estimate}"
    )


def build_control_file(schema: str, table: str, fields: list[LoaderField]) -> str:
    """Gera o control file do SQL*Loader para o CSV exportado pelo BigQuery.

    ``CSV WITH EMBEDDED`` aceita quebras de linha dentro de valores entre aspas.
    ``PRESERVE BLANKS`` mantém espaços em campos sem aspas, que o SQL*Loader
    removeria por padrão; ele precisa vir entre o método de carga e o
    ``INTO TABLE``. Campos ``FILLER`` são lidos do CSV e descartados.

    :param schema: Schema da tabela de destino.
    :param table: Nome da tabela de destino.
    :param fields: Campos na ordem do CSV.
    :returns: Conteúdo do control file.
    """
    field_lines = ",\n".join(f'  "{field.name}" {field.spec}' for field in fields)
    return (
        "LOAD DATA\n"
        "CHARACTERSET AL32UTF8\n"
        "APPEND\n"
        "PRESERVE BLANKS\n"
        f'INTO TABLE "{schema}"."{table}"\n'
        "FIELDS CSV WITH EMBEDDED TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'\n"
        "TRAILING NULLCOLS\n"
        f"(\n{field_lines}\n)\n"
    )


def split_round_robin[T](items: list[T], parts: int) -> list[list[T]]:
    """Divide uma lista em até ``parts`` grupos não vazios, alternando os itens.

    :param items: Itens a distribuir.
    :param parts: Número máximo de grupos.
    :returns: Grupos na ordem de distribuição.
    """
    groups = [items[index::parts] for index in range(parts)]
    return [group for group in groups if group]


def stream_blobs(
    bucket: storage.Bucket,
    files: list[ExportedFile],
    target: BinaryIO,
    progress: SessionProgress,
    errors: list[BaseException],
) -> None:
    """Descompacta os arquivos do GCS e escreve o CSV na entrada do SQL*Loader.

    :param bucket: Bucket dos arquivos.
    :param files: Arquivos ``.csv.gz`` a enviar, em ordem.
    :param target: Entrada padrão do processo do SQL*Loader.
    :param progress: Contadores da sessão.
    :param errors: Lista onde uma exceção da thread é registrada.
    """
    try:
        for exported in files:
            with bucket.blob(exported.name).open("rb") as raw:
                with gzip.GzipFile(fileobj=CountingReader(raw, progress)) as unzipped:
                    shutil.copyfileobj(unzipped, target, COPY_BUFFER_BYTES)
            progress.add_file()
    except BaseException as error:
        errors.append(error)
    finally:
        target.close()


def load_from_gcs(
    config: OracleConfig,
    job: LoadJob,
    on_progress: Callable[[ProgressSnapshot], None] | None = None,
    progress_interval_seconds: float = 30.0,
) -> LoadResult:
    """Carrega os arquivos exportados no Oracle com N sessões em direct path.

    Cada sessão recebe um grupo de arquivos pela entrada padrão, sem gravar nada
    em disco. Com mais de uma sessão, usa ``PARALLEL=TRUE``, que exige tabela sem
    índices. ``ERRORS=0`` faz qualquer linha rejeitada falhar a carga. Enquanto os
    arquivos são enviados, ``on_progress`` recebe o andamento a cada intervalo e
    uma última vez ao fim do envio.

    :param config: Configuração da conexão.
    :param job: Tabela, colunas, arquivos e número de sessões da carga.
    :param on_progress: Função chamada com o andamento da carga.
    :param progress_interval_seconds: Intervalo entre chamadas de ``on_progress``.
    :returns: Linhas carregadas no total e por sessão, e a duração.
    :raises RuntimeError: Se alguma sessão falhar.
    """
    groups = split_round_robin(job.files, max(job.sessions, 1))
    storage_bucket = storage.Client().bucket(job.bucket)
    env = {**os.environ, "NLS_LANG": "AMERICAN_AMERICA.AL32UTF8"}
    options = [
        "direct=true",
        f"parallel={'true' if len(groups) > 1 else 'false'}",
        "errors=0",
        "silent=header,feedback",
    ]
    bytes_total = sum(exported.size for exported in job.files)
    started = time.monotonic()

    with tempfile.TemporaryDirectory(prefix="sqlldr-") as workdir:
        work = Path(workdir)
        control = work / "load.ctl"
        control.write_text(build_control_file(config.schema, job.table, job.fields), encoding="utf-8")
        parfile = work / "userid.par"
        parfile.touch(mode=0o600)
        parfile.write_text(f'userid={config.user}/"{config.password}"@//{config.dsn}\n', encoding="utf-8")

        sessions_state = []
        for index, group in enumerate(groups):
            log_file = work / f"session-{index}.log"
            process = subprocess.Popen(
                [
                    SQLLDR_BIN,
                    f"parfile={parfile}",
                    f"control={control}",
                    "data=/dev/stdin",
                    f"log={log_file}",
                    f"bad={work / f'session-{index}.bad'}",
                    *options,
                ],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                env=env,
            )
            errors: list[BaseException] = []
            progress = SessionProgress()
            writer = threading.Thread(
                target=stream_blobs, args=(storage_bucket, group, process.stdin, progress, errors)
            )
            writer.start()
            sessions_state.append((index, group, process, writer, errors, log_file, progress))

        def snapshot() -> ProgressSnapshot:
            return ProgressSnapshot(
                files_done=sum(state[6].files_done for state in sessions_state),
                files_total=len(job.files),
                bytes_done=sum(state[6].bytes_done for state in sessions_state),
                bytes_total=bytes_total,
                elapsed_seconds=time.monotonic() - started,
            )

        next_report = started + progress_interval_seconds
        while any(state[3].is_alive() for state in sessions_state):
            time.sleep(POLL_SECONDS)
            if on_progress and time.monotonic() >= next_report:
                on_progress(snapshot())
                next_report += progress_interval_seconds
        if on_progress:
            on_progress(snapshot())

        results, failures = [], []
        for index, group, process, writer, errors, log_file, _ in sessions_state:
            writer.join()
            stderr = process.stderr.read().decode(errors="replace") if process.stderr else ""
            return_code = process.wait()
            log_text = log_file.read_text(errors="replace") if log_file.exists() else ""
            session_rows = sum(int(count) for count in LOADED_PATTERN.findall(log_text))
            results.append(SessionResult(index=index, files=len(group), rows=session_rows))
            logger.info("Sessão %d: %d arquivos, %d linhas, código %d", index, len(group), session_rows, return_code)
            if return_code != 0 or errors:
                failures.append(f"sessão {index}: código {return_code}, erro de leitura {errors}, {stderr.strip()}")
                logger.error("Log da sessão %d:\n%s", index, log_text[-4000:])

    if failures:
        raise RuntimeError(f"SQL*Loader falhou em {job.table}: {failures}")
    return LoadResult(
        rows=sum(result.rows for result in results), sessions=results, elapsed_seconds=time.monotonic() - started
    )
