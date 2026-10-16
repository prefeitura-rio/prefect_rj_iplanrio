"""Acompanhamento das sessões ativas: só consulta o Vertex e avança o que terminou.

Nunca lista nem baixa PDFs: a extração é montada a partir do output da
classificação, que ecoa o ``request`` com a página em base64.
"""

import traceback
from collections.abc import Sequence
from dataclasses import dataclass, field

from .categories import NF_CATEGORIES
from .llm_requests import encode_custom_id, jsonl_line, page_b64_from_vertex_row
from .observability import get_logger
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
from .vertex import BatchState, BatchStatus, VertexBatchClient

logger = get_logger(__name__)


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
        settings.gcs_bucket,
        settings.output_base_path,
        rows,
        f"extracao_pagina_{start.session_id}",
        metadata.generated_at,
    )
    append_event(settings.nf_batch_jobs_table, JobEvent(start.session_id, PHASE_EXTRACTION, None, STATE_DONE))
    logger.info("Sessão %s concluída: %d linhas em %s", start.session_id, len(rows), uri)


def advance_classification(client: VertexBatchClient, settings: Settings, event: JobEvent, batch: BatchStatus) -> bool:
    """Trata uma classificação concluída: submete a extração ou finaliza se não houver NF.

    A saída é lida em fluxo: guarda só a classificação de cada página e as linhas de extração
    das páginas fiscais, nunca o output inteiro com as páginas em base64.

    :param client: Cliente do Vertex.
    :param settings: Configuração de runtime.
    :param event: Evento mais recente da sessão.
    :param batch: Batch de classificação concluído.
    :returns: ``True`` se a extração foi submetida; ``False`` se a sessão foi finalizada.
    """
    start = session_start(settings.nf_batch_jobs_table, event.session_id)
    context = start.context
    prompts = load_prompts(
        context.classification_prompt_version if context else None,
        context.extraction_prompt_version if context else None,
    )
    classifications: list[ClassificationResult] = []
    lines: list[bytes] = []
    for row in client.iter_output(batch.output_dir):
        item = parse_classification(output_from_vertex_row(row))
        classifications.append(item)
        if item.category in NF_CATEGORIES:
            custom_id = encode_custom_id(item.page.pdf_name, item.page.page_number)
            prompt = extraction_prompt_with_hint(prompts.extraction_text, item.category)
            lines.append(jsonl_line(custom_id, prompt, page_b64_from_vertex_row(row)))
    if not lines:
        finish_session(settings, start, classifications, [])
        return False

    submitted = client.submit_jsonl(b"".join(lines), f"nf-extraction-{event.session_id}")
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


def finish_extraction(client: VertexBatchClient, settings: Settings, event: JobEvent, batch: BatchStatus) -> None:
    """Trata uma extração concluída lendo as duas saídas direto do Vertex.

    :param client: Cliente do Vertex.
    :param settings: Configuração de runtime.
    :param event: Evento mais recente da sessão.
    :param batch: Batch de extração concluído.
    :raises RuntimeError: Se o evento inicial não tiver o ID do batch de classificação.
    """
    start = session_start(settings.nf_batch_jobs_table, event.session_id)
    if not start.batch_id:
        raise RuntimeError(f"Sessão {event.session_id} sem ID do batch de classificação.")
    classification_batch = client.retrieve(start.batch_id)
    classifications = [
        parse_classification(output_from_vertex_row(row)) for row in client.iter_output(classification_batch.output_dir)
    ]
    extractions = [parse_extraction(output_from_vertex_row(row)) for row in client.iter_output(batch.output_dir)]
    finish_session(settings, start, classifications, extractions)


def fail_if_status_unreachable(session_count: int, status_errors: Sequence[str]) -> None:
    """Falha o run se o status de nenhuma sessão ativa pôde ser consultado.

    Quando todas as consultas falham, o problema não é de uma sessão (chave inativa,
    spend cap, permissão negada, Vertex fora do ar). Levantar aqui faz a causa aparecer no estado do run
    no Prefect, em vez de ficar só em avisos de log.

    :param session_count: Sessões ativas consultadas.
    :param status_errors: Mensagens das consultas que falharam.
    :raises RuntimeError: Se havia sessões ativas e todas as consultas falharam.
    """
    if session_count and len(status_errors) == session_count:
        raise RuntimeError(
            f"Não foi possível consultar o status de nenhuma das {session_count} sessões ativas "
            f"(spend cap, permissão negada ou Vertex fora do ar?). Primeira falha: {status_errors[0][:500]}"
        )


def poll_sessions(client: VertexBatchClient, settings: Settings) -> PollSummary:
    """Consulta cada sessão ativa uma vez e avança as que terminaram.

    Erros de consulta de status são tratados como transitórios, a menos que atinjam
    todas as sessões ativas: aí o problema não é de uma sessão (chave inativa, spend
    cap, permissão negada, Vertex fora do ar) e o run falha com a primeira mensagem, para aparecer no
    estado do run no Prefect. Erros ao avançar uma sessão concluída são acumulados e
    levantados juntos no fim, depois de todas as sessões serem tentadas; a sessão
    continua ativa para o próximo run.

    :param client: Cliente do Vertex.
    :param settings: Configuração de runtime.
    :returns: Sessões por desfecho.
    :raises RuntimeError: Se o status de nenhuma sessão ativa puder ser consultado, ou
        se alguma sessão concluída não puder ser avançada.
    """
    summary = PollSummary()
    errors: list[str] = []
    status_errors: list[str] = []
    sessions = active_sessions(settings.nf_batch_jobs_table)
    for event in sessions:
        if not event.batch_id:
            errors.append(f"sessão {event.session_id} ({event.phase}): evento ativo sem batch_id")
            continue
        try:
            batch = client.retrieve(event.batch_id)
        except Exception as exc:
            logger.warning("Sessão %s: falha ao consultar status: %s", event.session_id, exc)
            status_errors.append(f"sessão {event.session_id} ({event.phase}): {exc}")
            summary.waiting.append(event.session_id)
            continue

        if batch.state is BatchState.FAILED:
            try:
                append_event(
                    settings.nf_batch_jobs_table,
                    JobEvent(
                        event.session_id,
                        event.phase,
                        event.batch_id,
                        STATE_FAILED,
                        error=batch.error or "Job do Vertex terminou sem sucesso.",
                    ),
                )
            except Exception as exc:
                errors.append(f"sessão {event.session_id} ({event.phase}): {exc}\n{traceback.format_exc()}")
                continue
            summary.failed.append(event.session_id)
            continue
        if batch.state is BatchState.IN_PROGRESS:
            summary.waiting.append(event.session_id)
            continue

        try:
            if not batch.output_dir:
                raise RuntimeError(f"Batch {event.batch_id} concluído sem pasta de saída.")
            if event.phase == PHASE_CLASSIFICATION:
                advanced = advance_classification(client, settings, event, batch)
                (summary.advanced if advanced else summary.finished).append(event.session_id)
            else:
                finish_extraction(client, settings, event, batch)
                summary.finished.append(event.session_id)
        except Exception as exc:
            errors.append(f"sessão {event.session_id} ({event.phase}): {exc}\n{traceback.format_exc()}")

    fail_if_status_unreachable(len(sessions), status_errors)
    if errors:
        raise RuntimeError(
            f"Falha ao avançar {len(errors)} sessão(ões); continuam ativas para o próximo run:\n"
            + "\n".join(f"- {error}" for error in errors)
        )
    return summary
