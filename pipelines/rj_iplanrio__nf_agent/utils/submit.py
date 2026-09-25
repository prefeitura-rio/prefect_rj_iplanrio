"""Submissão de todos os PDFs pendentes de uma origem em lotes de classificação."""

import dataclasses
import json
import uuid
from dataclasses import dataclass, field

from openai import OpenAI

from .. import constants
from .bifrost import submit_jsonl
from .categories import PAGE_CATEGORIES
from .llm_requests import encode_custom_id, jsonl_line
from .observability import get_logger
from .pdf import PdfPages, split_pdf_pages
from .pending import find_done_pdfs
from .prompts import PromptSet, extraction_prompt_with_hint, load_prompts
from .settings import Settings
from .storage import download_bytes, list_pdfs
from .tracking import PHASE_CLASSIFICATION, STATE_SUBMITTED, JobEvent, SessionContext, append_event, in_flight_pdf_names
from .versioning import compute_processing_version

logger = get_logger(__name__)


@dataclass(frozen=True)
class SubmitRequest:
    """Parâmetros de uma submissão."""

    input_uri: str | None
    max_pages: int | None = None
    processing_version: str | None = None


@dataclass(frozen=True)
class SubmitSummary:
    """Resultado de uma submissão."""

    run_id: str
    processing_version: str
    session_ids: list[str]
    pdf_count: int
    page_count: int
    skipped: list[str]


@dataclass
class SessionDraft:
    """Linhas acumuladas para a próxima sessão."""

    lines: list[bytes] = field(default_factory=list)
    pdfs: list[PdfPages] = field(default_factory=list)
    budget_used: int = 0


def extraction_overhead_per_row(prompts: PromptSet) -> int:
    """Calcula quantos bytes a linha de extração pode ter a mais que a de classificação.

    Reservar essa folga na classificação garante que a extração da mesma sessão
    também caiba no limite de upload.

    :param prompts: Prompts da submissão.
    :returns: Bytes extras por linha (zero se a extração for menor).
    """
    longest_hint = max(PAGE_CATEGORIES, key=len)
    extraction = json.dumps(extraction_prompt_with_hint(prompts.extraction_text, longest_hint), ensure_ascii=False)
    classification = json.dumps(prompts.classification_text, ensure_ascii=False)
    return max(0, len(extraction.encode("utf-8")) - len(classification.encode("utf-8")))


def submit_session(client: OpenAI, settings: Settings, draft: SessionDraft, base_context: SessionContext) -> str:
    """Cria o batch de classificação de uma sessão e grava o evento com o contexto.

    :param client: Cliente do Bifrost.
    :param settings: Configuração de runtime.
    :param draft: Linhas e PDFs da sessão.
    :param base_context: Contexto comum à submissão (sem a lista de PDFs).
    :returns: ID da sessão criada.
    """
    session_id = str(uuid.uuid4())
    submitted = submit_jsonl(
        client, b"".join(draft.lines), f"nf-classification-{session_id}.jsonl", settings.bifrost_bucket
    )
    append_event(
        settings.nf_batch_jobs_table,
        JobEvent(
            session_id=session_id,
            phase=PHASE_CLASSIFICATION,
            batch_id=submitted.batch_id,
            state=STATE_SUBMITTED,
            input_file_id=submitted.input_file_id,
            row_count=len(draft.lines),
            context=dataclasses.replace(base_context, pdfs=tuple(draft.pdfs)),
        ),
    )
    logger.info("Sessão %s submetida: %d PDFs, %d páginas", session_id, len(draft.pdfs), len(draft.lines))
    return session_id


def submit_pending(client: OpenAI, settings: Settings, request: SubmitRequest) -> SubmitSummary:
    """Submete todos os PDFs pendentes da origem, em quantas sessões forem necessárias.

    Pendente = não processado na mesma ``versao_processamento`` e fora de sessões ativas.
    Cada sessão é fechada quando a próxima linha passaria de ``BATCH_MAX_BYTES``.

    :param client: Cliente do Bifrost.
    :param settings: Configuração de runtime.
    :param request: Origem, limite de páginas e versão.
    :returns: Resumo da submissão.
    :raises ValueError: Se a origem não for informada ou ``max_pages`` não for positivo.
    """
    if not request.input_uri:
        raise ValueError("Informe a origem (gs://...) para acao='submeter'.")
    if request.max_pages is not None and request.max_pages <= 0:
        raise ValueError("max_paginas deve ser positivo.")

    prompts = load_prompts()
    version = compute_processing_version(prompts, request.processing_version)
    refs = list_pdfs(request.input_uri)
    done = find_done_pdfs(settings.extracao_pagina_table, [ref.name for ref in refs], version)
    in_flight = in_flight_pdf_names(settings.nf_batch_jobs_table)
    pending = [ref for ref in refs if ref.name not in done and ref.name not in in_flight]
    logger.info(
        "Origem %s: %d PDFs, %d já processados na versão %s, %d em voo, %d pendentes",
        request.input_uri, len(refs), len(done), version, len(in_flight & {ref.name for ref in refs}), len(pending),
    )

    run_id = str(uuid.uuid4())
    base_context = SessionContext(
        run_id=run_id,
        input_uri=request.input_uri,
        processing_version=version,
        classification_prompt_version=prompts.classification_version,
        extraction_prompt_version=prompts.extraction_version,
        pdfs=(),
    )
    overhead = extraction_overhead_per_row(prompts)
    draft = SessionDraft()
    session_ids: list[str] = []
    skipped: list[str] = []
    pdf_count = 0
    page_count = 0

    for ref in pending:
        try:
            pages = split_pdf_pages(download_bytes(ref.uri))
            lines = [
                jsonl_line(encode_custom_id(ref.name, number), prompts.classification_text, page)
                for number, page in enumerate(pages, start=1)
            ]
        except ValueError as exc:
            logger.warning("PDF %s ignorado: %s", ref.name, exc)
            skipped.append(ref.name)
            continue
        if request.max_pages is not None and page_count + len(pages) > request.max_pages:
            break
        cost = sum(len(line) for line in lines) + overhead * len(lines)
        if cost > constants.BATCH_MAX_BYTES:
            logger.warning("PDF %s ignorado: %d bytes não cabem num lote", ref.name, cost)
            skipped.append(ref.name)
            continue
        if draft.lines and draft.budget_used + cost > constants.BATCH_MAX_BYTES:
            session_ids.append(submit_session(client, settings, draft, base_context))
            draft = SessionDraft()
        draft.lines.extend(lines)
        draft.pdfs.append(PdfPages(ref.name, len(pages)))
        draft.budget_used += cost
        pdf_count += 1
        page_count += len(pages)

    if draft.lines:
        session_ids.append(submit_session(client, settings, draft, base_context))

    return SubmitSummary(run_id, version, session_ids, pdf_count, page_count, skipped)
