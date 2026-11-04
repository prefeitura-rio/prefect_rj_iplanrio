"""Load into Oracle with SQL*Loader in direct path, reading gzip CSV from GCS."""

from collections.abc import Callable
from dataclasses import dataclass
from gzip import GzipFile
from os import environ
from pathlib import Path
from re import MULTILINE
from re import compile as compile_pattern
from shutil import copyfileobj
from subprocess import DEVNULL, PIPE, Popen
from tempfile import TemporaryDirectory
from threading import Thread
from time import monotonic, sleep
from typing import BinaryIO

from toolkit.gcs import ObjectReader, create_store
from toolkit.telemetry import logger

from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.bigquery import ExportedFile
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.columns import LoaderField
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.oracle import OracleConfig
from pipelines.rj_smfp__nota_carioca_bq_to_oracle.helpers.sqlldr_progress import (
    CountingReader,
    ProgressSnapshot,
    SessionProgress,
)

SQLLDR_BIN = "sqlldr"


COPY_BUFFER_BYTES = 1024 * 1024


LOADED_PATTERN = compile_pattern(r"^\s*(\d+) Rows? successfully loaded\.", MULTILINE)


POLL_SECONDS = 0.5


@dataclass(frozen=True)
class LoadJob:
    """The load of one table from the files that BigQuery exported.

    `table` is the target table in Oracle, already empty. `fields` are the fields of the
    CSV, in the order of the BigQuery schema. `files` are the exported `.csv.gz` files,
    with their sizes. `sessions` is the most SQL*Loader sessions at the same time.
    """

    table: str
    fields: list[LoaderField]
    bucket: str
    files: list[ExportedFile]
    sessions: int


@dataclass(frozen=True)
class SessionResult:
    """The result of one SQL*Loader session: its number, files sent and rows."""

    index: int
    files: int
    rows: int


@dataclass(frozen=True)
class LoadResult:
    """The result of the load of one table: rows, sessions and time."""

    rows: int
    sessions: list[SessionResult]
    elapsed_seconds: float


def build_control_file(schema: str, table: str, fields: list[LoaderField]) -> str:
    """Build the SQL*Loader control file for the CSV that BigQuery exported.

    `CSV WITH EMBEDDED` accepts line breaks inside quoted values. `PRESERVE BLANKS`
    keeps the spaces of fields with no quotes, which SQL*Loader would remove by default.
    It must come between the load method and `INTO TABLE`. `FILLER` fields are read from
    the CSV and dropped. `fields` are in the order of the CSV. Return the content of the
    file.
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
    """Split a list into up to `parts` groups that are not empty, round robin."""
    groups = [items[index::parts] for index in range(parts)]
    return [group for group in groups if group]


def stream_blobs(
    bucket: str,
    files: list[ExportedFile],
    target: BinaryIO,
    progress: SessionProgress,
    errors: list[BaseException],
) -> None:
    """Unzip the files from GCS and write the CSV to the input of SQL*Loader.

    `files` are the `.csv.gz` files to send, in order. `target` is the standard input of
    the SQL*Loader process, and `progress` has the counters of the session. `errors` is
    the list where the exception of the thread is recorded.
    """
    try:
        store = create_store(bucket)

        for exported in files:
            raw = ObjectReader(store, exported.name)
            with GzipFile(fileobj=CountingReader(raw, progress)) as unzipped:
                copyfileobj(unzipped, target, COPY_BUFFER_BYTES)
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
    """Load the exported files into Oracle with N sessions in direct path.

    Each session gets a group of files through its standard input, and nothing goes to
    disk. With more than one session it uses `PARALLEL=TRUE`, which needs a table with
    no indexes. `ERRORS=0` makes any rejected row fail the load. While the files go,
    `on_progress` gets the progress every interval, and once more at the end of the
    send. Return the rows loaded, in total and for each session, and the time. Raise
    `RuntimeError` if a session fails.
    """
    groups = split_round_robin(job.files, max(job.sessions, 1))
    env = {**environ, "NLS_LANG": "AMERICAN_AMERICA.AL32UTF8"}
    options = [
        "direct=true",
        f"parallel={'true' if len(groups) > 1 else 'false'}",
        "errors=0",
        "silent=header,feedback",
    ]
    bytes_total = sum(exported.size for exported in job.files)
    started = monotonic()

    with TemporaryDirectory(prefix="sqlldr-") as workdir:
        work = Path(workdir)
        control = work / "load.ctl"
        control.write_text(build_control_file(config.schema, job.table, job.fields), encoding="utf-8")
        parfile = work / "userid.par"
        parfile.touch(mode=0o600)
        parfile.write_text(f'userid={config.user}/"{config.password}"@//{config.dsn}\n', encoding="utf-8")

        sessions_state = []
        for index, group in enumerate(groups):
            log_file = work / f"session-{index}.log"
            process = Popen(
                [
                    SQLLDR_BIN,
                    f"parfile={parfile}",
                    f"control={control}",
                    "data=/dev/stdin",
                    f"log={log_file}",
                    f"bad={work / f'session-{index}.bad'}",
                    *options,
                ],
                stdin=PIPE,
                stdout=DEVNULL,
                stderr=PIPE,
                env=env,
            )
            errors: list[BaseException] = []
            progress = SessionProgress()
            writer = Thread(target=stream_blobs, args=(job.bucket, group, process.stdin, progress, errors))
            writer.start()
            sessions_state.append((index, group, process, writer, errors, log_file, progress))

        def snapshot() -> ProgressSnapshot:
            return ProgressSnapshot(
                files_done=sum(state[6].files_done for state in sessions_state),
                files_total=len(job.files),
                bytes_done=sum(state[6].bytes_done for state in sessions_state),
                bytes_total=bytes_total,
                elapsed_seconds=monotonic() - started,
            )

        next_report = started + progress_interval_seconds
        while any(state[3].is_alive() for state in sessions_state):
            sleep(POLL_SECONDS)
            if on_progress and monotonic() >= next_report:
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
        rows=sum(result.rows for result in results), sessions=results, elapsed_seconds=monotonic() - started
    )
