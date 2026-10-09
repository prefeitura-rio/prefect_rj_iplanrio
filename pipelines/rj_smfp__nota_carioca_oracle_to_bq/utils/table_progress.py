"""Progresso de uma tabela: o JSON que o filho grava no GCS, o repórter que o produz e a leitura pelo pai.

Os filhos rodam em pods diferentes do pai, então publicam o andamento como um JSON pequeno em
``gs://<bucket>/oracle_to_bq_progress/<run id do pai>/<TABELA>.json``. O prefixo é irmão de ``oracle_to_bq/`` e não está
dentro dele: o load lista ``oracle_to_bq/<TABELA>/<run id>/*.parquet`` e nunca vê esses JSONs. No modo sequencial o
pai usa o mesmo repórter com um callback em memória, sem passar pelo GCS. Falhas de escrita e leitura são engolidas.
"""

import json
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from enum import StrEnum

from google.api_core import exceptions as api_exceptions
from google.cloud import storage
from google.cloud.storage.retry import DEFAULT_RETRY

from pipelines.rj_smfp__nota_carioca_oracle_to_bq.constants import PROGRESS_PREFIX
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.discord import Warner
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.extract import ExtractResult
from pipelines.rj_smfp__nota_carioca_oracle_to_bq.utils.progress import Progress, estimate_remaining_seconds

REQUEST_TIMEOUT_SECONDS = 10
READ_RETRY = DEFAULT_RETRY.with_timeout(REQUEST_TIMEOUT_SECONDS)
SinkFn = Callable[["TableProgress"], None]


class TableStage(StrEnum):
    """Etapa de uma tabela; o valor é o que vai no JSON."""

    WAITING = "aguardando"
    PLANNING = "planejamento"
    EXTRACTION = "extracao"
    LOAD = "carga"
    VALIDATION = "validacao"
    VALIDATED = "validada"
    FAILED = "falhou"


@dataclass(frozen=True)
class TableProgress:
    """Retrato do andamento de uma tabela, como gravado no JSON.

    :param table: Nome da tabela.
    :param stage: Etapa atual.
    :param chunks_read: Faixas lidas do Oracle.
    :param chunks_uploaded: Faixas já enviadas ao GCS.
    :param chunks_total: Faixas planejadas (0 antes de a extração começar).
    :param rows_read: Linhas lidas do Oracle (ao fim da extração, as gravadas).
    :param oracle_rows: Linhas no Oracle ``AS OF SCN``, conhecidas só ao fim da extração.
    :param bytes_uploaded: Bytes de Parquet enviados.
    :param elapsed_seconds: Tempo desde que o repórter da tabela começou.
    :param extract_seconds: Duração da extração até agora (base da taxa de linhas/s).
    :param eta_seconds: Segundos que faltam para terminar a extração; ``None`` se não há estimativa.
    :param error: Mensagem do erro, na etapa ``FAILED``.
    :param failed_stage: Etapa em que a tabela falhou.
    :param updated_at: Instante da gravação, ISO 8601 em UTC.
    """

    table: str
    stage: TableStage
    chunks_read: int = 0
    chunks_uploaded: int = 0
    chunks_total: int = 0
    rows_read: int = 0
    oracle_rows: int | None = None
    bytes_uploaded: int = 0
    elapsed_seconds: float = 0.0
    extract_seconds: float = 0.0
    eta_seconds: float | None = None
    error: str | None = None
    failed_stage: TableStage | None = None
    updated_at: str = ""

    def to_json(self) -> str:
        """Serializa para o JSON do GCS."""
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, text: str) -> "TableProgress":
        """Lê o JSON gravado por :meth:`to_json`.

        :param text: Conteúdo do objeto.
        :returns: O progresso.
        :raises ValueError: Se o JSON for inválido ou faltarem campos obrigatórios.
        """
        try:
            data = json.loads(text)
            failed = data.get("failed_stage")
            return cls(
                **{
                    **data,
                    "stage": TableStage(data["stage"]),
                    "failed_stage": TableStage(failed) if failed else None,
                }
            )
        except (KeyError, TypeError, AttributeError) as error:
            raise ValueError(f"JSON de progresso inválido: {error!r}") from error


def progress_prefix(run_id: str) -> str:
    """Prefixo dos JSONs de progresso de uma execução, sem barra final."""
    return f"{PROGRESS_PREFIX}/{run_id}"


class ProgressStore:
    """Grava, lê e apaga os JSONs de progresso de uma execução no GCS; nenhuma falha sai daqui, só avisos.

    :param project: Projeto do GCS.
    :param bucket: Bucket dos arquivos da carga.
    :param run_id: Flow run do pai, que nomeia o prefixo.
    """

    def __init__(self, project: str, bucket: str, run_id: str) -> None:
        self.project = project
        self.bucket_name = bucket
        self.run_id = run_id
        self._bucket: storage.Bucket | None = None
        self._warn = Warner()

    def _open(self) -> storage.Bucket:
        if self._bucket is None:
            self._bucket = storage.Client(project=self.project).bucket(self.bucket_name)
        return self._bucket

    def blob_name(self, table: str) -> str:
        """Nome do objeto de uma tabela: ``oracle_to_bq_progress/<run id>/<TABELA>.json``."""
        return f"{progress_prefix(self.run_id)}/{table}.json"

    def write(self, progress: TableProgress) -> None:
        """Grava o progresso de uma tabela, sem retentativas (a próxima gravação o substitui)."""
        try:
            self._open().blob(self.blob_name(progress.table)).upload_from_string(
                progress.to_json(), content_type="application/json", timeout=REQUEST_TIMEOUT_SECONDS
            )
            self._warn.reset()
        except Exception as error:
            self._warn(f"{progress.table}: não foi possível gravar o progresso no GCS: {type(error).__name__}")

    def read_all(self, tables: Sequence[str]) -> dict[str, TableProgress]:
        """Lê o progresso das tabelas; as que ainda não gravaram (ou têm JSON ilegível) ficam de fora."""
        found: dict[str, TableProgress] = {}
        for table in tables:
            try:
                text = (
                    self._open()
                    .blob(self.blob_name(table))
                    .download_as_text(timeout=REQUEST_TIMEOUT_SECONDS, retry=READ_RETRY)
                )
                found[table] = TableProgress.from_json(text)
            except api_exceptions.NotFound:
                continue
            except Exception as error:
                self._warn(f"{table}: não foi possível ler o progresso do GCS: {type(error).__name__}")
        return found

    def delete(self) -> int:
        """Apaga ``oracle_to_bq_progress/<run id>/``; falhas viram aviso, para a limpeza da carga seguir.

        :returns: Quantidade de objetos apagados.
        """
        if not self.run_id:
            return 0
        prefix = f"{progress_prefix(self.run_id)}/"
        try:
            blobs = list(self._open().list_blobs(prefix=prefix))
            for blob in blobs:
                blob.delete()
        except Exception as error:
            self._warn(f"Não foi possível apagar {prefix} no GCS: {type(error).__name__}")
            return 0
        return len(blobs)


class TableReporter:
    """Acompanha uma tabela e entrega cada novo retrato aos ``sinks`` (gravar no GCS, atualizar o pai em memória).

    :param table: Nome da tabela.
    :param sinks: Funções que recebem cada :class:`TableProgress`; as exceções delas são engolidas.
    """

    def __init__(self, table: str, sinks: Sequence[SinkFn]) -> None:
        self._sinks = tuple(sinks)
        self._started = time.monotonic()
        self._warn = Warner()
        self.current = TableProgress(table=table, stage=TableStage.WAITING)

    def _emit(self, **changes: object) -> None:
        self.current = replace(
            self.current,
            elapsed_seconds=time.monotonic() - self._started,
            updated_at=datetime.now(UTC).isoformat(timespec="seconds"),
            **changes,
        )
        for sink in self._sinks:
            try:
                sink(self.current)
            except Exception as error:
                self._warn(f"{self.current.table}: callback de progresso falhou: {type(error).__name__}")

    def stage(self, stage: TableStage) -> None:
        """Muda a etapa da tabela."""
        self._emit(stage=stage, eta_seconds=None)

    def extraction(self, progress: Progress) -> None:
        """Registra um tick da extração (o mesmo que vira a linha de log de progresso)."""
        self._emit(
            stage=TableStage.EXTRACTION,
            chunks_read=progress.chunks_read,
            chunks_uploaded=progress.chunks_uploaded,
            chunks_total=progress.chunks_total,
            rows_read=progress.rows_read,
            bytes_uploaded=progress.bytes_uploaded,
            extract_seconds=progress.elapsed_seconds,
            eta_seconds=estimate_remaining_seconds(progress),
        )

    def extracted(self, result: ExtractResult) -> None:
        """Registra o fim da extração e passa para a carga no BigQuery."""
        self._emit(
            stage=TableStage.LOAD,
            chunks_read=result.chunks,
            chunks_uploaded=result.chunks,
            chunks_total=result.chunks,
            rows_read=result.rows,
            oracle_rows=result.oracle_rows,
            bytes_uploaded=result.bytes_written,
            extract_seconds=result.seconds,
            eta_seconds=None,
        )

    def failed(self, error: BaseException) -> None:
        """Marca a tabela como falha, guardando a etapa em que estava e a mensagem."""
        failed_at = self.current.failed_stage or self.current.stage
        self._emit(
            stage=TableStage.FAILED, failed_stage=failed_at, error=f"{type(error).__name__}: {error}", eta_seconds=None
        )

    @contextmanager
    def guard(self) -> Iterator[None]:
        """Marca a tabela como falha se o bloco levantar qualquer exceção (até ``BaseException``) e a repropaga."""
        try:
            yield
        except BaseException as error:
            self.failed(error)
            raise


_active: TableReporter | None = None


@contextmanager
def reporting(reporter: TableReporter) -> Iterator[TableReporter]:
    """Torna ``reporter`` o repórter da tabela em curso, para as tasks o encontrarem sem receberem um objeto novo.

    Há um flow run por processo, então o ponteiro de módulo basta; as tasks usam ``NO_CACHE`` e não recebem o repórter
    como parâmetro (o Prefect percorreria seus campos ao registrar entradas).
    """
    global _active  # noqa: PLW0603 - ponteiro de módulo restaurado ao sair
    previous, _active = _active, reporter
    try:
        yield reporter
    finally:
        _active = previous


def active_reporter(table: str) -> TableReporter:
    """Repórter em curso para ``table``, ou um sem destinos (que não faz nada) se não houver.

    :param table: Tabela que a task processa.
    :returns: Sempre um repórter utilizável.
    """
    if _active is not None and _active.current.table == table:
        return _active
    return TableReporter(table, ())
