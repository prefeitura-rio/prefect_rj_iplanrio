"""Acompanhamento das sessões ativas: só consulta o Vertex e avança o que terminou.

Nunca lista nem baixa PDFs: a extração é montada a partir do output da
classificação, que ecoa o ``request`` com a página (URI do GCS ou base64).

Páginas que falham num lote são reenviadas uma vez, só elas (fases ``*_retry``). O resultado
da sessão junta os sucessos do lote original com o resultado das novas tentativas.
"""

import traceback
from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import TypeVar

from .. import constants
from .categories import NF_CATEGORIES
from .llm_requests import encode_custom_id, jsonl_line, page_part_from_vertex_row, retry_line
from .observability import get_logger
from .output import RunMetadata, build_extracao_pagina_rows, build_versao_pipeline, utc_now_naive
from .pdf import PdfPages
from .prompts import extraction_prompt_with_hint, load_prompts
from .responses import (
    ClassificationResult,
    ExtractionResult,
    ModelOutput,
    output_from_vertex_row,
    parse_classification,
    parse_extraction,
)
from .results import build_pdf_results
from .settings import Settings
from .storage import write_ndjson
from .tracking import (
    PHASE_CLASSIFICATION,
    PHASE_CLASSIFICATION_RETRY,
    PHASE_EXTRACTION,
    PHASE_EXTRACTION_RETRY,
    STATE_DONE,
    STATE_FAILED,
    STATE_SUBMITTED,
    JobEvent,
    SessionContext,
    active_sessions,
    append_event,
    session_batches,
    session_start,
)
from .versioning import compute_processing_version
from .vertex import BatchState, BatchStatus, VertexBatchClient

logger = get_logger(__name__)

RETRY_PHASES = frozenset({PHASE_CLASSIFICATION_RETRY, PHASE_EXTRACTION_RETRY})
CLASSIFICATION_PHASES = frozenset({PHASE_CLASSIFICATION, PHASE_CLASSIFICATION_RETRY})
ResultT = TypeVar("ResultT", ClassificationResult, ExtractionResult)


class Outcome(StrEnum):
    """O que aconteceu com uma sessão neste acompanhamento."""

    ADVANCED = "advanced"
    FINISHED = "finished"
    FAILED = "failed"


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


def iter_merged_rows(
    client: VertexBatchClient,
    original: BatchStatus,
    retry: BatchStatus | None,
    parse: Callable[[ModelOutput], ResultT],
) -> Iterator[tuple[dict, ResultT]]:
    """Lê a saída do lote original trocando as páginas que falharam pelo resultado da nova tentativa.

    :param client: Cliente do Vertex.
    :param original: Lote original, concluído.
    :param retry: Lote da nova tentativa, se houve; só vale se tiver concluído.
    :param parse: Interpretação da resposta (classificação ou extração).
    :returns: ``(linha, resultado)`` de cada página, na ordem do lote original.
    """
    retried: dict[str, dict] = {}
    if retry is not None and retry.state is BatchState.SUCCEEDED:
        retried = {row["custom_id"]: row for row in client.iter_output(retry.output_dir)}
    for row in client.iter_output(original.output_dir):
        final = row
        item = parse(output_from_vertex_row(final))
        if item.error is not None and row["custom_id"] in retried:
            final = retried[row["custom_id"]]
            item = parse(output_from_vertex_row(final))
        yield final, item


def too_many_errors(errors: int, total: int) -> bool:
    """Diz se as páginas com erro passam do limite aceitável de uma sessão.

    :param errors: Páginas que continuam com erro depois da nova tentativa.
    :param total: Páginas da fase.
    :returns: ``True`` se a fração passar de ``MAX_ERROR_RATIO``.
    """
    return total > 0 and errors / total > constants.MAX_ERROR_RATIO


def fail_session(settings: Settings, event: JobEvent, message: str) -> Outcome:
    """Marca a sessão como falha, sem gravar resultados; os PDFs voltam a ficar pendentes.

    :param settings: Configuração de runtime.
    :param event: Evento mais recente da sessão.
    :param message: Motivo.
    :returns: :attr:`Outcome.FAILED`.
    """
    append_event(
        settings.nf_batch_jobs_table,
        JobEvent(event.session_id, event.phase, event.batch_id, STATE_FAILED, error=message),
    )
    logger.warning("Sessão %s falhou: %s", event.session_id, message)
    return Outcome.FAILED


def submit_retry(
    client: VertexBatchClient, settings: Settings, event: JobEvent, phase: str, failed_rows: Sequence[dict]
) -> Outcome:
    """Reenvia só as páginas que falharam, num novo lote da mesma sessão.

    :param client: Cliente do Vertex.
    :param settings: Configuração de runtime.
    :param event: Evento mais recente da sessão.
    :param phase: Fase da nova tentativa (``classification_retry`` ou ``extraction_retry``).
    :param failed_rows: Linhas da saída que falharam, com o ``request`` ecoado.
    :returns: :attr:`Outcome.ADVANCED`.
    """
    lines = [retry_line(row) for row in failed_rows]
    submitted = client.submit_jsonl(b"".join(lines), f"nf-{phase.replace('_', '-')}-{event.session_id}")
    append_event(
        settings.nf_batch_jobs_table,
        JobEvent(
            session_id=event.session_id,
            phase=phase,
            batch_id=submitted.batch_id,
            state=STATE_SUBMITTED,
            input_file_id=submitted.input_file_id,
            row_count=len(lines),
        ),
    )
    logger.info("Sessão %s: %d páginas com erro reenviadas (%s)", event.session_id, len(lines), phase)
    return Outcome.ADVANCED


def advance_classification(
    client: VertexBatchClient, settings: Settings, event: JobEvent, batch: BatchStatus
) -> Outcome:
    """Trata uma classificação concluída: reenvia o que falhou, submete a extração ou finaliza.

    A saída é lida em fluxo: guarda só a classificação de cada página, as linhas de extração das
    páginas fiscais e as linhas que falharam, nunca o output inteiro.

    :param client: Cliente do Vertex.
    :param settings: Configuração de runtime.
    :param event: Evento mais recente da sessão (classificação ou sua nova tentativa).
    :param batch: Batch concluído desse evento.
    :returns: O desfecho da sessão neste acompanhamento.
    """
    start = session_start(settings.nf_batch_jobs_table, event.session_id)
    is_retry = event.phase == PHASE_CLASSIFICATION_RETRY
    original, retry = (client.retrieve(start.batch_id), batch) if is_retry else (batch, None)
    context = start.context
    prompts = load_prompts(
        context.classification_prompt_version if context else None,
        context.extraction_prompt_version if context else None,
    )
    classifications: list[ClassificationResult] = []
    lines: list[bytes] = []
    failed_rows: list[dict] = []
    for row, item in iter_merged_rows(client, original, retry, parse_classification):
        classifications.append(item)
        if item.error is not None:
            failed_rows.append(row)
        elif item.category in NF_CATEGORIES:
            custom_id = encode_custom_id(item.page.pdf_name, item.page.page_number)
            prompt = extraction_prompt_with_hint(prompts.extraction_text, item.category)
            lines.append(jsonl_line(custom_id, prompt, page_part_from_vertex_row(row)))
    logger.info(
        "Sessão %s: classificação com %d páginas, %d com erro (job: %s ok, %s falhas)",
        event.session_id,
        len(classifications),
        len(failed_rows),
        batch.successful,
        batch.failed,
    )
    if failed_rows and not is_retry:
        return submit_retry(client, settings, event, PHASE_CLASSIFICATION_RETRY, failed_rows)
    if too_many_errors(len(failed_rows), len(classifications)):
        return fail_session(
            settings, event, f"{len(failed_rows)} de {len(classifications)} páginas falharam na classificação."
        )
    if not lines:
        finish_session(settings, start, classifications, [])
        return Outcome.FINISHED

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
    return Outcome.ADVANCED


def finish_extraction(client: VertexBatchClient, settings: Settings, event: JobEvent, batch: BatchStatus) -> Outcome:
    """Trata uma extração concluída: reenvia o que falhou ou grava o resultado da sessão.

    :param client: Cliente do Vertex.
    :param settings: Configuração de runtime.
    :param event: Evento mais recente da sessão (extração ou sua nova tentativa).
    :param batch: Batch concluído desse evento.
    :returns: O desfecho da sessão neste acompanhamento.
    :raises RuntimeError: Se o evento inicial não tiver o ID do batch de classificação.
    """
    start = session_start(settings.nf_batch_jobs_table, event.session_id)
    if not start.batch_id:
        raise RuntimeError(f"Sessão {event.session_id} sem ID do batch de classificação.")
    batches = session_batches(settings.nf_batch_jobs_table, event.session_id)
    classification_retry = batches.get(PHASE_CLASSIFICATION_RETRY)
    classifications = [
        item
        for _, item in iter_merged_rows(
            client,
            client.retrieve(start.batch_id),
            client.retrieve(classification_retry) if classification_retry else None,
            parse_classification,
        )
    ]
    is_retry = event.phase == PHASE_EXTRACTION_RETRY
    original, retry = (client.retrieve(batches[PHASE_EXTRACTION]), batch) if is_retry else (batch, None)
    extractions: list[ExtractionResult] = []
    failed_rows: list[dict] = []
    for row, item in iter_merged_rows(client, original, retry, parse_extraction):
        extractions.append(item)
        if item.error is not None:
            failed_rows.append(row)
    logger.info(
        "Sessão %s: extração com %d páginas, %d com erro (job: %s ok, %s falhas)",
        event.session_id,
        len(extractions),
        len(failed_rows),
        batch.successful,
        batch.failed,
    )
    if failed_rows and not is_retry:
        return submit_retry(client, settings, event, PHASE_EXTRACTION_RETRY, failed_rows)
    if too_many_errors(len(failed_rows), len(extractions)):
        return fail_session(settings, event, f"{len(failed_rows)} de {len(extractions)} páginas falharam na extração.")
    finish_session(settings, start, classifications, extractions)
    return Outcome.FINISHED


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


def poll_sessions(client: VertexBatchClient, settings: Settings, max_sessions: int | None = None) -> PollSummary:
    """Consulta cada sessão ativa uma vez e avança as que terminaram.

    Erros de consulta de status são tratados como transitórios, a menos que atinjam
    todas as sessões ativas: aí o problema não é de uma sessão (chave inativa, spend
    cap, permissão negada, Vertex fora do ar) e o run falha com a primeira mensagem, para aparecer no
    estado do run no Prefect. Erros ao avançar uma sessão concluída são acumulados e
    levantados juntos no fim, depois de todas as sessões serem tentadas; a sessão
    continua ativa para o próximo run.

    :param client: Cliente do Vertex.
    :param settings: Configuração de runtime.
    :param max_sessions: Se informado, acompanha só essas primeiras sessões ativas (para liberar
        um backlog aos poucos e limitar quantos jobs novos uma passada cria).
    :returns: Sessões por desfecho.
    :raises RuntimeError: Se o status de nenhuma sessão ativa puder ser consultado, ou
        se alguma sessão concluída não puder ser avançada.
    """
    summary = PollSummary()
    errors: list[str] = []
    status_errors: list[str] = []
    sessions = active_sessions(settings.nf_batch_jobs_table)
    if max_sessions is not None:
        sessions = sessions[:max_sessions]
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

        if batch.state is BatchState.FAILED and event.phase not in RETRY_PHASES:
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
            if batch.state is BatchState.SUCCEEDED and not batch.output_dir:
                raise RuntimeError(f"Batch {event.batch_id} concluído sem pasta de saída.")
            handler = advance_classification if event.phase in CLASSIFICATION_PHASES else finish_extraction
            outcome = handler(client, settings, event, batch)
            {Outcome.ADVANCED: summary.advanced, Outcome.FINISHED: summary.finished, Outcome.FAILED: summary.failed}[
                outcome
            ].append(event.session_id)
        except Exception as exc:
            errors.append(f"sessão {event.session_id} ({event.phase}): {exc}\n{traceback.format_exc()}")

    fail_if_status_unreachable(len(sessions), status_errors)
    if errors:
        raise RuntimeError(
            f"Falha ao avançar {len(errors)} sessão(ões); continuam ativas para o próximo run:\n"
            + "\n".join(f"- {error}" for error in errors)
        )
    return summary
