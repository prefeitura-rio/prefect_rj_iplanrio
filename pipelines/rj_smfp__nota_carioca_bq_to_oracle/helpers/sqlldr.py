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
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, cast

from google.cloud import storage
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.bigquery import ExportedFile
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import LoaderField
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import OracleConfig
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.sqlldr_progress import (
    CountingReader,
    ProgressSnapshot,
    Readable,
    SessionProgress,
)

SQLLDR_BIN = "sqlldr"


COPY_BUFFER_BYTES = 1024 * 1024


LOADED_PATTERN = re.compile(r"^\s*(\d+) Rows? successfully loaded\.", re.MULTILINE)


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
        + "CHARACTERSET AL32UTF8\n"
        + "APPEND\n"
        + "PRESERVE BLANKS\n"
        + f'INTO TABLE "{schema}"."{table}"\n'
        + "FIELDS CSV WITH EMBEDDED TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'\n"
        + "TRAILING NULLCOLS\n"
        + f"(\n{field_lines}\n)\n"
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
                with gzip.GzipFile(fileobj=CountingReader(cast("Readable", raw), progress)) as unzipped:
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
            session_rows = sum(int(match.group(1)) for match in LOADED_PATTERN.finditer(log_text))
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
