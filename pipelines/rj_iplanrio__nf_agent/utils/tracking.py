"""Log de eventos das sessões de batch (tabela ``nf_batch_jobs``, só append).

Uma sessão é um lote de classificação seguido de, no máximo, um lote de extração.
O estado atual é o evento mais recente da sessão. O evento de submissão da
classificação carrega em ``contexto`` (JSON) tudo que o acompanhamento precisa.
"""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from google.cloud import bigquery

from .bq import insert_row, run_query
from .pdf import PdfPages

PHASE_CLASSIFICATION = "classification"
PHASE_EXTRACTION = "extraction"
STATE_SUBMITTED = "submitted"
STATE_DONE = "done"
STATE_FAILED = "failed"


@dataclass(frozen=True)
class SessionContext:
    """Contexto de uma sessão, gravado no submit da classificação."""

    run_id: str
    input_uri: str
    processing_version: str
    classification_prompt_version: str
    extraction_prompt_version: str
    pdfs: tuple[PdfPages, ...]

    def to_json(self) -> str:
        """Serializa para a coluna ``contexto``.

        :returns: JSON.
        """
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, text: str) -> "SessionContext":
        """Reconstrói a partir da coluna ``contexto``.

        :param text: JSON gravado por :meth:`to_json`.
        :returns: Contexto da sessão.
        """
        data = json.loads(text)
        data["pdfs"] = tuple(PdfPages(**pdf) for pdf in data["pdfs"])
        return cls(**data)


@dataclass(frozen=True)
class JobEvent:
    """Uma linha de ``nf_batch_jobs``."""

    session_id: str
    phase: str
    batch_id: str | None
    state: str
    input_file_id: str | None = None
    row_count: int | None = None
    error: str | None = None
    context: SessionContext | None = None


def event_from_row(row) -> JobEvent:
    """Converte uma linha de resultado do BigQuery.

    :param row: Linha com as colunas de ``nf_batch_jobs``.
    :returns: Evento.
    """
    raw_context = row.get("contexto")
    return JobEvent(
        session_id=row["session_id"],
        phase=row["phase"],
        batch_id=row["bifrost_batch_id"],
        state=row["state"],
        input_file_id=row.get("input_file_id"),
        row_count=row.get("row_count"),
        error=row.get("error"),
        context=SessionContext.from_json(raw_context) if raw_context else None,
    )


def append_event(table: str, event: JobEvent) -> None:
    """Grava um evento.

    :param table: Tabela ``nf_batch_jobs`` totalmente qualificada.
    :param event: Evento a gravar.
    :raises RuntimeError: Se o insert falhar.
    """
    insert_row(
        table,
        {
            "session_id": event.session_id,
            "phase": event.phase,
            "bifrost_batch_id": event.batch_id,
            "state": event.state,
            "input_file_id": event.input_file_id,
            "output_file_id": None,
            "row_count": event.row_count,
            "created_at": datetime.now(UTC).isoformat(),
            "error": event.error,
            "contexto": event.context.to_json() if event.context else None,
        },
    )


def active_sessions(table: str) -> list[JobEvent]:
    """Lista o evento mais recente de cada sessão ainda não terminada.

    :param table: Tabela ``nf_batch_jobs``.
    :returns: Eventos das sessões ativas.
    """
    return [event_from_row(row) for row in run_query(__file__, "active_sessions", table)]


def session_start(table: str, session_id: str) -> JobEvent:
    """Busca o evento de submissão da classificação de uma sessão.

    :param table: Tabela ``nf_batch_jobs``.
    :param session_id: Sessão.
    :returns: Evento inicial (``context`` é ``None`` em sessões do código antigo).
    :raises RuntimeError: Se a sessão não tiver evento de classificação.
    """
    params = [bigquery.ScalarQueryParameter("session_id", "STRING", session_id)]
    rows = run_query(__file__, "session_start", table, params)
    if not rows:
        raise RuntimeError(f"Sessão {session_id} sem evento de classificação em {table}.")
    return event_from_row(rows[0])


def in_flight_pdf_names(table: str) -> set[str]:
    """Lista os PDFs que estão em sessões ativas.

    :param table: Tabela ``nf_batch_jobs``.
    :returns: Nomes dos PDFs em voo.
    """
    return {row["nome_arquivo"] for row in run_query(__file__, "in_flight_pdfs", table)}
