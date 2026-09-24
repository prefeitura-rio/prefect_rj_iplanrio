"""Acompanhamento das sessões ativas: só consulta o Vertex e avança o que terminou.

Nunca lista nem baixa PDFs: a extração é montada a partir do output da
classificação, que ecoa o ``request`` com a página em base64.
"""

import traceback
from collections.abc import Sequence
from dataclasses import dataclass, field

from openai import OpenAI

from prefect_rj_iplanrio.logging import get_logger

from .bifrost import read_batch_output, retrieve_batch, submit_jsonl
from .categories import NF_CATEGORIES
from .llm_requests import encode_custom_id, jsonl_line, page_b64_from_vertex_row
from .output import RunMetadata, build_extracao_pagina_rows, build_versao_pipeline, utc_now_naive
from .pdf import PdfPages
from .prompts import extraction_prompt_with_hint, load_prompts
from .responses import (
    ClassificationResult,
    ExtractionResult,
    output_from_vertex_row,
    parse_classification,
    parse_extraction,
)
from .results import build_pdf_results
from .settings import Settings
from .storage import write_ndjson
from .tracking import (
    PHASE_CLASSIFICATION,
    PHASE_EXTRACTION,
    STATE_DONE,
    STATE_FAILED,
    STATE_SUBMITTED,
    JobEvent,
    SessionContext,
    active_sessions,
    append_event,
    session_start,
)
from .versioning import compute_processing_version

logger = get_logger(__name__)

IN_PROGRESS_STATES = frozenset({"validating", "in_progress", "finalizing", "cancelling"})
FAILURE_STATES = frozenset({"failed", "expired", "canceled", "cancelled"})
SUCCESS_STATE = "completed"


@dataclass
class PollSummary:
    """Sessões por desfecho neste acompanhamento."""

    advanced: list[str] = field(default_factory=list)
    finished: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    waiting: list[str] = field(default_factory=list)


def legacy_context(session_id: str, classifications: Sequence[ClassificationResult]) -> SessionContext:
    """Reconstrói o contexto de uma sessão submetida antes de o contexto ser gravado.

    :param session_id: Sessão.
    :param classifications: Classificações da sessão (fonte do inventário de PDFs).
    :returns: Contexto com prompts atuais e total de páginas pela maior página vista.
    """
    prompts = load_prompts(None, None)
    pages: dict[str, int] = {}
    for item in classifications:
        pages[item.page.pdf_name] = max(pages.get(item.page.pdf_name, 0), item.page.page_number)
    return SessionContext(
        run_id=f"legado-{session_id}",
        input_uri="",
        processing_version=compute_processing_version(prompts),
        classification_prompt_version=prompts.classification_version,
        extraction_prompt_version=prompts.extraction_version,
        pdfs=tuple(PdfPages(name, total) for name, total in sorted(pages.items())),
    )


def finish_session(
    settings: Settings,
    start: JobEvent,
    classifications: Sequence[ClassificationResult],
    extractions: Sequence[ExtractionResult],
) -> None:
    """Grava o NDJSON final da sessão e marca a sessão como concluída.

    :param settings: Configuração de runtime.
    :param start: Evento de submissão da classificação.
    :param classifications: Classificações da sessão.
    :param extractions: Extrações da sessão.
    """
    context = start.context or legacy_context(start.session_id, classifications)
    metadata = RunMetadata(
        versao_pipeline=build_versao_pipeline(
            context.processing_version,
            context.classification_prompt_version,
            context.extraction_prompt_version,
            context.run_id,
            start.session_id,
        ),
        generated_at=utc_now_naive(),
    )
    rows = build_extracao_pagina_rows(build_pdf_results(context.pdfs, classifications, extractions), metadata)
    uri = write_ndjson(
        settings.output_bucket,
        settings.output_base_path,
        rows,
        f"extracao_pagina_{start.session_id}",
        metadata.generated_at,
    )
    append_event(settings.nf_batch_jobs_table, JobEvent(start.session_id, PHASE_EXTRACTION, None, STATE_DONE))
    logger.info("Sessão %s concluída: %d linhas em %s", start.session_id, len(rows), uri)


def advance_classification(client: OpenAI, settings: Settings, event: JobEvent, batch) -> bool:
    """Trata uma classificação concluída: submete a extração ou finaliza se não houver NF.

    :param client: Cliente do Bifrost.
    :param settings: Configuração de runtime.
    :param event: Evento mais recente da sessão.
    :param batch: Batch de classificação concluído.
    :returns: ``True`` se a extração foi submetida; ``False`` se a sessão foi finalizada.
    """
    raw_rows = read_batch_output(batch.output_file_id)
    classifications = [parse_classification(output_from_vertex_row(row)) for row in raw_rows]
    start = session_start(settings.nf_batch_jobs_table, event.session_id)
    nf_pages = [item for item in classifications if item.category in NF_CATEGORIES]
    if not nf_pages:
        finish_session(settings, start, classifications, [])
        return False

    context = start.context
    prompts = load_prompts(
        context.classification_prompt_version if context else None,
        context.extraction_prompt_version if context else None,
    )
    page_by_id = {row["custom_id"]: page_b64_from_vertex_row(row) for row in raw_rows}
    lines = []
    for item in nf_pages:
        custom_id = encode_custom_id(item.page.pdf_name, item.page.page_number)
        prompt = extraction_prompt_with_hint(prompts.extraction_text, item.category)
        lines.append(jsonl_line(custom_id, prompt, page_by_id[custom_id]))
    submitted = submit_jsonl(
        client, b"".join(lines), f"nf-extraction-{event.session_id}.jsonl", settings.bifrost_bucket
    )
    append_event(
        settings.nf_batch_jobs_table,
        JobEvent(
            session_id=event.session_id,
            phase=PHASE_EXTRACTION,
            batch_id=submitted.batch_id,
            state=STATE_SUBMITTED,
            input_file_id=submitted.input_file_id,
            row_count=len(lines),
        ),
    )
    logger.info("Sessão %s: extração submetida para %d páginas", event.session_id, len(lines))
    return True


def finish_extraction(client: OpenAI, settings: Settings, event: JobEvent, batch) -> None:
    """Trata uma extração concluída lendo as duas saídas direto do Vertex.

    :param client: Cliente do Bifrost.
    :param settings: Configuração de runtime.
    :param event: Evento mais recente da sessão.
    :param batch: Batch de extração concluído.
    :raises RuntimeError: Se o evento inicial não tiver o ID do batch de classificação.
    """
    start = session_start(settings.nf_batch_jobs_table, event.session_id)
    if not start.batch_id:
        raise RuntimeError(f"Sessão {event.session_id} sem ID do batch de classificação.")
    classification_batch = retrieve_batch(client, start.batch_id)
    classifications = [
        parse_classification(output_from_vertex_row(row))
        for row in read_batch_output(classification_batch.output_file_id)
    ]
    extractions = [parse_extraction(output_from_vertex_row(row)) for row in read_batch_output(batch.output_file_id)]
    finish_session(settings, start, classifications, extractions)


def poll_sessions(client: OpenAI, settings: Settings) -> PollSummary:
    """Consulta cada sessão ativa uma vez e avança as que terminaram.

    Erros de consulta de status são tratados como transitórios. Erros ao avançar
    uma sessão concluída são acumulados e levantados juntos no fim, depois de
    todas as sessões serem tentadas; a sessão continua ativa para o próximo run.

    :param client: Cliente do Bifrost.
    :param settings: Configuração de runtime.
    :returns: Sessões por desfecho.
    :raises RuntimeError: Se alguma sessão concluída não puder ser avançada.
    """
    summary = PollSummary()
    errors: list[str] = []
    for event in active_sessions(settings.nf_batch_jobs_table):
        if not event.batch_id:
            errors.append(f"sessão {event.session_id} ({event.phase}): evento ativo sem batch_id")
            continue
        try:
            batch = retrieve_batch(client, event.batch_id)
        except Exception as exc:
            logger.warning("Sessão %s: falha ao consultar status: %s", event.session_id, exc)
            summary.waiting.append(event.session_id)
            continue

        if batch.status in FAILURE_STATES:
            append_event(
                settings.nf_batch_jobs_table,
                JobEvent(event.session_id, event.phase, event.batch_id, STATE_FAILED,
                         error=str(getattr(batch, "errors", None) or batch.status)),
            )
            summary.failed.append(event.session_id)
            continue
        if batch.status != SUCCESS_STATE:
            if batch.status not in IN_PROGRESS_STATES:
                logger.warning("Sessão %s: status desconhecido %r, aguardando", event.session_id, batch.status)
            summary.waiting.append(event.session_id)
            continue

        try:
            if not batch.output_file_id:
                raise RuntimeError(f"Batch {event.batch_id} concluído sem output_file_id.")
            if event.phase == PHASE_CLASSIFICATION:
                advanced = advance_classification(client, settings, event, batch)
                (summary.advanced if advanced else summary.finished).append(event.session_id)
            else:
                finish_extraction(client, settings, event, batch)
                summary.finished.append(event.session_id)
        except Exception as exc:
            errors.append(f"sessão {event.session_id} ({event.phase}): {exc}\n{traceback.format_exc()}")

    if errors:
        raise RuntimeError(
            f"Falha ao avançar {len(errors)} sessão(ões); continuam ativas para o próximo run:\n"
            + "\n".join(f"- {error}" for error in errors)
        )
    return summary
