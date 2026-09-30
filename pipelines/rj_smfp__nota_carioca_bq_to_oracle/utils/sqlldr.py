"""Carga no Oracle com SQL*Loader em direct path, lendo os CSV gzip direto do GCS."""

import gzip
import os
import re
import shutil
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path

from google.cloud import storage

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.columns import LoaderField
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.utils.oracle import OracleConfig
from prefect_rj_iplanrio.logging import get_logger

logger = get_logger(__name__)

SQLLDR_BIN = "sqlldr"
COPY_BUFFER_BYTES = 1024 * 1024
LOADED_PATTERN = re.compile(r"^\s*(\d+) Rows? successfully loaded\.", re.MULTILINE)


@dataclass(frozen=True)
class LoadJob:
    """Carga de uma tabela a partir dos arquivos exportados pelo BigQuery.

    :param table: Tabela de destino no Oracle, já vazia.
    :param fields: Campos do CSV, na ordem do schema do BigQuery.
    :param bucket: Bucket dos arquivos.
    :param blob_names: Arquivos ``.csv.gz`` exportados.
    :param sessions: Número máximo de sessões simultâneas do SQL*Loader.
    """

    table: str
    fields: list[LoaderField]
    bucket: str
    blob_names: list[str]
    sessions: int


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


def split_round_robin(items: list[str], parts: int) -> list[list[str]]:
    """Divide uma lista em até ``parts`` grupos não vazios, alternando os itens.

    :param items: Itens a distribuir.
    :param parts: Número máximo de grupos.
    :returns: Grupos na ordem de distribuição.
    """
    groups = [items[index::parts] for index in range(parts)]
    return [group for group in groups if group]


def stream_blobs(bucket: storage.Bucket, blob_names: list[str], target: object, errors: list[BaseException]) -> None:
    """Descompacta os arquivos do GCS e escreve o CSV na entrada do SQL*Loader.

    :param bucket: Bucket dos arquivos.
    :param blob_names: Arquivos ``.csv.gz`` a enviar, em ordem.
    :param target: Entrada padrão do processo do SQL*Loader.
    :param errors: Lista onde uma exceção da thread é registrada.
    """
    try:
        for name in blob_names:
            with bucket.blob(name).open("rb") as raw, gzip.GzipFile(fileobj=raw) as unzipped:
                shutil.copyfileobj(unzipped, target, COPY_BUFFER_BYTES)
    except BaseException as error:
        errors.append(error)
    finally:
        target.close()


def load_from_gcs(config: OracleConfig, job: LoadJob) -> int:
    """Carrega os arquivos exportados no Oracle com N sessões em direct path.

    Cada sessão recebe um grupo de arquivos pela entrada padrão, sem gravar nada
    em disco. Com mais de uma sessão, usa ``PARALLEL=TRUE``, que exige tabela sem
    índices. ``ERRORS=0`` faz qualquer linha rejeitada falhar a carga.

    :param config: Configuração da conexão.
    :param job: Tabela, colunas, arquivos e número de sessões da carga.
    :returns: Total de linhas carregadas.
    :raises RuntimeError: Se alguma sessão falhar.
    """
    groups = split_round_robin(job.blob_names, max(job.sessions, 1))
    storage_bucket = storage.Client().bucket(job.bucket)
    env = {**os.environ, "NLS_LANG": "AMERICAN_AMERICA.AL32UTF8"}
    options = [
        "direct=true",
        f"parallel={'true' if len(groups) > 1 else 'false'}",
        "errors=0",
        "silent=header,feedback",
    ]

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
            writer = threading.Thread(target=stream_blobs, args=(storage_bucket, group, process.stdin, errors))
            writer.start()
            sessions_state.append((index, group, process, writer, errors, log_file))

        loaded, failures = 0, []
        for index, group, process, writer, errors, log_file in sessions_state:
            writer.join()
            stderr = process.stderr.read().decode(errors="replace") if process.stderr else ""
            return_code = process.wait()
            log_text = log_file.read_text(errors="replace") if log_file.exists() else ""
            session_loaded = sum(int(count) for count in LOADED_PATTERN.findall(log_text))
            loaded += session_loaded
            logger.info("Sessão %d: %d arquivos, %d linhas, código %d", index, len(group), session_loaded, return_code)
            if return_code != 0 or errors:
                failures.append(f"sessão {index}: código {return_code}, erro de leitura {errors}, {stderr.strip()}")
                logger.error("Log da sessão %d:\n%s", index, log_text[-4000:])

    if failures:
        raise RuntimeError(f"SQL*Loader falhou em {job.table}: {failures}")
    return loaded
