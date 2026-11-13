"""Submissão de todos os PDFs pendentes de uma origem em lotes de classificação."""

import dataclasses
import json
import re
import uuid
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from .. import constants
from .categories import PAGE_CATEGORIES
from .llm_requests import encode_custom_id, file_part, jsonl_line
from .observability import get_logger
from .pdf import PdfPages, split_pdf_page_bytes
from .pending import find_done_pdfs, list_downloaded_pdfs
from .prompts import PromptSet, extraction_prompt_with_hint, load_prompts
from .settings import Settings
from .storage import PdfRef, download_bytes, list_pdfs
from .tracking import (
    PHASE_CLASSIFICATION,
    STATE_SUBMITTED,
    JobEvent,
    SessionContext,
    active_sessions,
    append_event,
    in_flight_pdf_names,
)
from .versioning import compute_processing_version
from .vertex import VertexBatchClient

logger = get_logger(__name__)

PREPARE_WORKERS = 12
PREPARE_CHUNK = 48


@dataclass(frozen=True)
class SubmitRequest:
    """Parâmetros de uma submissão."""

    input_uri: str | None
    max_pages: int | None = None
    processing_version: str | None = None
    run_id: str | None = None


# Igual ao nome das subpastas já existentes no bucket (``mes_envio=2021-11-01``):
# uma data completa, não um mês — o nome ficou de um formato antigo.
MES_ENVIO_PATTERN = re.compile(r"\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])")


def resolve_origem(origem: str | None, mes_envio: str | None, settings: Settings) -> str:
    """Resolve de onde vêm os PDFs da submissão.

    Com ``origem`` explícita (``gs://...``), lista essa pasta do GCS. Sem ela, lê os PDFs
    baixados do mês ``mes_envio`` na view de download, onde quer que estejam no bucket:
    devolve ``view://<view>?mes_envio=<data>``.

    :param origem: URI completa (``gs://...``) informada explicitamente, ou ``None``.
    :param mes_envio: Data ``YYYY-MM-DD`` do mês de envio na view; ignorada se houver ``origem``.
    :param settings: Configuração de runtime (``pdfs_view_table``).
    :returns: Origem final, para :func:`find_pending`.
    :raises ValueError: Se não houver ``origem`` nem ``mes_envio``, ou se ``mes_envio`` não
        estiver no formato ``YYYY-MM-DD``.
    """
    if origem:
        return origem
    if mes_envio is None:
        raise ValueError("Informe 'mes_envio' (lê os PDFs baixados da view) ou 'origem' (uma pasta gs://).")
    if not MES_ENVIO_PATTERN.fullmatch(mes_envio):
        raise ValueError(f"mes_envio inválido: {mes_envio!r}. Use o formato YYYY-MM-DD.")
    return f"{constants.VIEW_ORIGIN_PREFIX}{settings.pdfs_view_table}?mes_envio={mes_envio}"


@dataclass(frozen=True)
class SubmitSummary:
    """Resultado de uma submissão."""

    run_id: str
    processing_version: str
    session_ids: list[str]
    pdf_count: int
    page_count: int
    skipped: list[str]
    remaining: int = 0


@dataclass(frozen=True)
class PreparedPdf:
    """Um PDF dividido em páginas, com as linhas de classificação já montadas."""

    ref: PdfRef
    pages: list[bytes]
    lines: list[bytes]
    cost: int


@dataclass
class SessionDraft:
    """Linhas acumuladas para a próxima sessão."""

    lines: list[bytes] = field(default_factory=list)
    pdfs: list[PdfPages] = field(default_factory=list)
    budget_used: int = 0

    def is_full_with(self, prepared: PreparedPdf) -> bool:
        """Diz se acrescentar o PDF passaria dos limites de bytes ou de linhas da sessão.

        :param prepared: PDF candidato.
        :returns: ``True`` se a sessão precisa ser fechada antes de recebê-lo.
        """
        return (
            self.budget_used + prepared.cost > constants.BATCH_MAX_BYTES
            or len(self.lines) + len(prepared.lines) > constants.BATCH_MAX_REQUESTS
        )

    def add(self, prepared: PreparedPdf) -> None:
        """Acrescenta um PDF à sessão.

        :param prepared: PDF a acrescentar.
        """
        self.lines.extend(prepared.lines)
        self.pdfs.append(PdfPages(prepared.ref.name, len(prepared.pages)))
        self.budget_used += prepared.cost


def prepare_pdf(client: VertexBatchClient, ref: PdfRef, prompt: str, overhead: int) -> PreparedPdf | None:
    """Baixa um PDF, divide em páginas e monta as linhas de classificação (ainda sem gravar nada).

    :param client: Cliente do Vertex (só para calcular a URI de cada página).
    :param ref: PDF de origem.
    :param prompt: Texto do prompt de classificação.
    :param overhead: Bytes extras por linha que a extração pode ter a mais (ver
        :func:`extraction_overhead_per_row`).
    :returns: PDF preparado, ou ``None`` se não puder ser lido ou não couber numa sessão.
    """
    try:
        pages = split_pdf_page_bytes(download_bytes(ref.uri))
    except Exception as exc:  # um PDF ruim (baixar ou ler) nunca pode derrubar o lote inteiro
        logger.warning("PDF %s ignorado: falha ao baixar/ler: %s", ref.name, exc)
        return None
    try:
        lines = [
            jsonl_line(encode_custom_id(ref.name, number), prompt, file_part(client.page_uri(ref.name, number)))
            for number in range(1, len(pages) + 1)
        ]
    except ValueError as exc:  # nome longo demais para o custom_id: pula só este PDF
        logger.warning("PDF %s ignorado: %s", ref.name, exc)
        return None
    cost = sum(len(line) for line in lines) + overhead * len(lines)
    if cost > constants.BATCH_MAX_BYTES or len(lines) > constants.BATCH_MAX_REQUESTS:
        logger.warning("PDF %s ignorado: %d páginas (%d bytes) não cabem numa sessão", ref.name, len(lines), cost)
        return None
    return PreparedPdf(ref, pages, lines, cost)


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


def session_id_for(run_id: str, pdfs: Sequence[PdfPages]) -> str:
    """Deriva o ID da sessão do run e dos PDFs, para que repetir a submissão o reproduza.

    Se um job foi criado mas o evento não chegou a ser gravado, a nova tentativa monta a
    mesma sessão, chega ao mesmo ID e reaproveita o job em vez de criar (e pagar) outro.

    :param run_id: ID do run de submissão (estável entre as tentativas da task).
    :param pdfs: PDFs da sessão, na ordem em que entram.
    :returns: UUID determinístico.
    """
    key = "|".join([run_id, *(f"{pdf.name}:{pdf.pages}" for pdf in pdfs)])
    return str(uuid.uuid5(uuid.NAMESPACE_URL, key))


def submit_session(
    client: VertexBatchClient, settings: Settings, draft: SessionDraft, base_context: SessionContext
) -> str:
    """Cria o batch de classificação de uma sessão e grava o evento com o contexto.

    :param client: Cliente do Vertex.
    :param settings: Configuração de runtime.
    :param draft: Linhas e PDFs da sessão.
    :param base_context: Contexto comum à submissão (sem a lista de PDFs).
    :returns: ID da sessão criada.
    """
    session_id = session_id_for(base_context.run_id, draft.pdfs)
    submitted = client.submit_jsonl(b"".join(draft.lines), f"nf-classification-{session_id}")
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


def prepare_and_upload(client: VertexBatchClient, ref: PdfRef, prompt: str, overhead: int) -> PreparedPdf | None:
    """Prepara um PDF e grava as páginas no bucket; devolve ``None`` se ele precisar ser pulado.

    :param client: Cliente do Vertex.
    :param ref: PDF de origem.
    :param prompt: Texto do prompt de classificação.
    :param overhead: Ver :func:`prepare_pdf`.
    :returns: PDF pronto e já gravado, ou ``None``.
    """
    prepared = prepare_pdf(client, ref, prompt, overhead)
    if prepared is None:
        return None
    try:
        client.upload_pages(ref.name, prepared.pages)
    except Exception as exc:
        logger.warning("PDF %s ignorado: falha ao gravar as páginas: %s", ref.name, exc)
        return None
    return prepared


def iter_prepared(
    client: VertexBatchClient, pending: Sequence[PdfRef], prompt: str, overhead: int
) -> Iterator[tuple[PdfRef, PreparedPdf | None]]:
    """Prepara os PDFs em paralelo, devolvendo-os na ordem original.

    Trabalha em blocos para limitar a memória e poder parar cedo (teto de sessões, limite de páginas).
    Uma página gravada de um PDF que acabe não entrando na sessão é inofensiva: o caminho depende só
    do nome do PDF, e a próxima rodada a regrava.

    :param client: Cliente do Vertex.
    :param pending: PDFs a preparar.
    :param prompt: Texto do prompt de classificação.
    :param overhead: Ver :func:`prepare_pdf`.
    :returns: Pares ``(PDF, preparado ou None)``.
    """
    with ThreadPoolExecutor(max_workers=PREPARE_WORKERS) as pool:
        for start in range(0, len(pending), PREPARE_CHUNK):
            chunk = pending[start : start + PREPARE_CHUNK]
            yield from zip(
                chunk, pool.map(lambda ref: prepare_and_upload(client, ref, prompt, overhead), chunk), strict=True
            )


def find_pending(settings: Settings, input_uri: str, version: str) -> list[PdfRef]:
    """Lista os PDFs da origem que ainda precisam ser processados.

    Pendente = não processado na mesma ``versao_processamento`` e fora de sessões ativas.

    :param settings: Configuração de runtime.
    :param input_uri: Origem: pasta ``gs://`` ou ``view://<view>?mes_envio=<data>``.
    :param version: ``versao_processamento`` da submissão.
    :returns: PDFs pendentes, ordenados por nome.
    """
    if input_uri.startswith(constants.VIEW_ORIGIN_PREFIX):
        refs = list_downloaded_pdfs(settings.pdfs_view_table, input_uri.rsplit("mes_envio=", 1)[1])
    else:
        refs = list_pdfs(input_uri)
    done = find_done_pdfs(settings.extracao_pagina_table, [ref.name for ref in refs], version)
    in_flight = in_flight_pdf_names(settings.nf_batch_jobs_table)
    pending = [ref for ref in refs if ref.name not in done and ref.name not in in_flight]
    logger.info(
        "Origem %s: %d PDFs, %d já processados na versão %s, %d em voo, %d pendentes",
        input_uri,
        len(refs),
        len(done),
        version,
        len(in_flight & {ref.name for ref in refs}),
        len(pending),
    )
    return pending


def submit_pending(client: VertexBatchClient, settings: Settings, request: SubmitRequest) -> SubmitSummary:
    """Submete todos os PDFs pendentes da origem, em quantas sessões forem necessárias.

    Pendente = não processado na mesma ``versao_processamento`` e fora de sessões ativas.
    As páginas de cada PDF vão para o bucket como objetos e o JSONL as referencia por URI.
    Cada sessão é fechada ao atingir ``BATCH_MAX_REQUESTS`` linhas ou ``BATCH_MAX_BYTES``, e a
    submissão para quando as sessões ativas chegam a ``max_active_sessions``: o que sobrar
    fica pendente para a próxima rodada (``remaining``). Um PDF que falhe ao baixar, ler ou
    gravar é pulado (fica em ``skipped``) e não interrompe os demais — com centenas de PDFs,
    uma falha pontual de rede é esperada.

    :param client: Cliente do Vertex.
    :param settings: Configuração de runtime.
    :param request: Origem, limite de páginas, versão e ID do run.
    :returns: Resumo da submissão.
    :raises ValueError: Se a origem não for informada ou ``max_pages`` não for positivo.
    """
    if not request.input_uri:
        raise ValueError("Informe a origem (gs://...) para acao='submeter'.")
    if request.max_pages is not None and request.max_pages <= 0:
        raise ValueError("max_paginas deve ser positivo.")

    prompts = load_prompts()
    version = compute_processing_version(prompts, request.processing_version)
    pending = find_pending(settings, request.input_uri, version)
    run_id = request.run_id or str(uuid.uuid4())
    base_context = SessionContext(
        run_id=run_id,
        input_uri=request.input_uri,
        processing_version=version,
        classification_prompt_version=prompts.classification_version,
        extraction_prompt_version=prompts.extraction_version,
        pdfs=(),
    )
    slots = settings.max_active_sessions - len(active_sessions(settings.nf_batch_jobs_table))
    if slots <= 0:
        logger.warning(
            "Já há %d sessões ativas (teto %d); nada submetido.",
            settings.max_active_sessions - slots,
            settings.max_active_sessions,
        )
        return SubmitSummary(run_id, version, [], 0, 0, [], remaining=len(pending))

    overhead = extraction_overhead_per_row(prompts)
    draft = SessionDraft()
    session_ids: list[str] = []
    skipped: list[str] = []
    pdf_count = 0
    page_count = 0

    for ref, prepared in iter_prepared(client, pending, prompts.classification_text, overhead):
        if slots <= 0:
            break
        if prepared is None:
            skipped.append(ref.name)
            continue
        if request.max_pages is not None and page_count + len(prepared.pages) > request.max_pages:
            break
        if draft.lines and draft.is_full_with(prepared):
            session_ids.append(submit_session(client, settings, draft, base_context))
            draft = SessionDraft()
            slots -= 1
            if slots <= 0:
                break
        draft.add(prepared)
        pdf_count += 1
        page_count += len(prepared.pages)

    if draft.lines:
        session_ids.append(submit_session(client, settings, draft, base_context))

    remaining = len(pending) - pdf_count - len(skipped)
    if remaining > 0:
        logger.info("Ficaram %d PDFs para a próxima rodada (limite de sessões ativas ou de páginas).", remaining)
    return SubmitSummary(run_id, version, session_ids, pdf_count, page_count, skipped, remaining)
