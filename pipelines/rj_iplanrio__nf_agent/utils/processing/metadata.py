"""Metadata and JSON output builders for ``POCProcessor``."""

import subprocess
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from prefect_rj_iplanrio.logging import get_logger

from ..classification.config import DEFAULT_GENERATION_CONFIG as CLASSIFICATION_GENERATION_CONFIG
from ..classification.config import DEFAULT_MODEL_NAME as CLASSIFICATION_MODEL_NAME
from ..extraction.config import FALLBACK_MODEL_NAME, GEMINI_CONFIG

if TYPE_CHECKING:
    from .processor import POCProcessor

logger = get_logger(__name__)


def utc_now_naive() -> datetime:
    """Current UTC time as a naive ``datetime`` (no tzinfo).

    ``datetime.utcnow()`` is deprecated (it's naive but silently assumes UTC);
    this reads the clock via the timezone-aware ``datetime.now(UTC)`` and
    strips the tzinfo back off. Naive-on-purpose: every ``timestamp_geracao``
    in this module is built as ``dt.isoformat() + "Z"`` — an aware datetime
    here would double up the offset (``...+00:00Z``), corrupting the field
    written to BigQuery.
    """
    return datetime.now(UTC).replace(tzinfo=None)


def build_classification_detail(
    page_categories: dict[int, str], page_justifications: dict[int, str], nf_pages: list[int]
) -> dict:
    """
    Constrói detalhe estruturado da classificação por página.

    :param page_categories: Dicionário {page_num: category}.
    :param page_justifications: Dicionário {page_num: justification}.
    :param nf_pages: Lista de páginas consideradas documentos fiscais válidos.
    :returns: Dicionário estruturado com detalhes da classificação.
    """
    pages_detail = []
    for page_num in sorted(page_categories.keys()):
        category = page_categories[page_num]
        justification = page_justifications.get(page_num, "")
        is_valid = page_num in nf_pages

        pages_detail.append(
            {"page": page_num, "category": category, "justification": justification, "is_valid_document": is_valid}
        )

    return {
        "total_pages": len(page_categories),
        "classified_pages": len(page_categories),
        "valid_document_pages": sorted(nf_pages),
        "pages": pages_detail,
    }


def build_versao_pipeline(
    processor: "POCProcessor",
    workers: int,
    requests_per_minute: int,
    max_concurrent: int,
) -> dict[str, Any]:
    """
    Monta o JSON de rastreabilidade de configuração da execução.

    Unifica em um único dict tudo que é necessário para comparar/filtrar
    resultados entre execuções: parâmetros operacionais, os modelos Gemini
    configurados, as versões de prompt usadas e informações do repositório
    git. Historicamente ``versao_pipeline`` (config operacional) e
    ``versao_prompt`` (versões de prompt) eram dois campos separados no
    output — foram unificados aqui para simplificar o schema.

    Os nomes de modelo e os parâmetros de geração vêm diretamente dos módulos
    de config (não de ``processor.classifier``/``processor.extractor``) para
    não forçar a instanciação lazy do classifier/extractor apenas para montar
    este campo. Isso reflete a configuração *estática* — os mesmos valores
    para toda a run, inclusive quando o fallback de extração é acionado (o
    fallback troca apenas o modelo, não os parâmetros de geração). O modelo
    efetivamente usado em cada página (primário ou fallback) é registrado por
    página em ``uso.extracao.modelo``/``uso.classificacao.modelo`` (ver
    ``build_uso_field``).

    :param processor: The ``POCProcessor`` instance (supplies prompt versions).
    """
    info: dict[str, Any] = {
        "extraction_batch_size": 1,
        "workers": workers,
        "requests_per_minute": requests_per_minute,
        "max_concurrent": max_concurrent,
        "modelo_classificacao": CLASSIFICATION_MODEL_NAME,
        "modelo_extracao": GEMINI_CONFIG["model_name"],
        "modelo_extracao_fallback": FALLBACK_MODEL_NAME,
        "parametros_classificacao": {
            "temperature": CLASSIFICATION_GENERATION_CONFIG["temperature"],
            "top_p": CLASSIFICATION_GENERATION_CONFIG["top_p"],
            "top_k": CLASSIFICATION_GENERATION_CONFIG["top_k"],
            "max_output_tokens": CLASSIFICATION_GENERATION_CONFIG["max_output_tokens"],
        },
        "parametros_extracao": {
            "temperature": GEMINI_CONFIG["temperature"],
            "top_p": GEMINI_CONFIG["top_p"],
            "top_k": GEMINI_CONFIG["top_k"],
            "max_output_tokens": GEMINI_CONFIG["max_output_tokens"],
        },
        "versao_prompt_classificacao": processor.prompt_versions.get("classification"),
        "versao_prompt_extracao": processor.prompt_versions.get("extraction"),
    }
    info.update(get_git_info())
    return info


def get_git_info() -> dict[str, Any]:
    """
    Get Git repository information (commit, branch, dirty status).

    :returns: Dictionary with git info, or empty dict if not in a git repo.
    """
    try:
        # Get current commit hash
        commit = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL, text=True
        ).strip()

        # Get current branch
        branch = subprocess.check_output(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL, text=True
        ).strip()

        # Check if working directory is dirty (uncommitted changes)
        dirty_check = subprocess.check_output(
            ["git", "status", "--porcelain"], stderr=subprocess.DEVNULL, text=True
        ).strip()
        dirty = len(dirty_check) > 0

        return {"commit": commit, "branch": branch, "dirty": dirty}
    except (subprocess.CalledProcessError, FileNotFoundError):
        # Not in a git repo or git not available
        return {}


# ------------------------------------------------------------------
# JSON output helpers
# ------------------------------------------------------------------


def _empty_uso_component() -> dict[str, int | str | None]:
    return {"modelo": None, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}


def build_uso_field(
    page_num: int | None,
    page_classification_usage: dict[int, dict[str, Any]],
    page_extraction_usage: dict[int, dict[str, Any]],
) -> dict[str, dict[str, int | str | None]]:
    """
    Build the per-page ``uso`` (token usage + modelo utilizado) output field.

    :param page_num: The page number this row is about, or None for the
        whole-PDF sentinel row (download failed before any page was processed).
    :param page_classification_usage: ``process_pdf`` result's
        ``page_classification_usage`` — page_num -> {"input_tokens",
        "output_tokens", "total_tokens", "model_name"} for that page's
        classification call.
    :param page_extraction_usage: ``process_pdf`` result's ``page_extraction_usage``
        — page_num -> {"prompt_tokens", "completion_tokens", "total_tokens",
        "model_name"} for that page's extraction call (only present for pages
        with an extracted document). ``model_name`` reflects the model
        *actually used* for that page — the fallback model when the fallback
        was triggered, the primary model otherwise.
    :returns: ``{"classificacao": {...}, "extracao": {...}}``, each a dict with
        ``modelo``/``prompt_tokens``/``completion_tokens``/``total_tokens`` —
        zeroed/``None`` when no usage data is available for that page (page
        not reached, cache predates usage tracking, or no document was
        extracted from that page).
    """
    classificacao = _empty_uso_component()
    if page_num is not None:
        raw = page_classification_usage.get(page_num)
        if raw:
            classificacao = {
                "modelo": raw.get("model_name"),
                "prompt_tokens": raw.get("input_tokens", 0) or 0,
                "completion_tokens": raw.get("output_tokens", 0) or 0,
                "total_tokens": raw.get("total_tokens", 0) or 0,
            }

    extracao = _empty_uso_component()
    if page_num is not None:
        raw = page_extraction_usage.get(page_num)
        if raw:
            extracao = {
                "modelo": raw.get("model_name"),
                "prompt_tokens": raw.get("prompt_tokens", 0) or 0,
                "completion_tokens": raw.get("completion_tokens", 0) or 0,
                "total_tokens": raw.get("total_tokens", 0) or 0,
            }

    return {"classificacao": classificacao, "extracao": extracao}


def empty_json_item(  # noqa: PLR0913, PLR0917
    # 5 call sites, all within this module (build_extracao_pagina_rows) —
    # each param maps 1:1 to an independent output column, not a reusable
    # config; a dataclass here would just rename "params" to "fields".
    pdf_name: str,
    page_num: int | None,
    pipeline_status: str,
    pipeline_erro: str | None = None,
    tipo_classificacao: str | None = None,
    justificativa_classif: str | None = None,
) -> dict:
    """Build a JSON output item with all document fields set to null."""
    return {
        "nome_arquivo": pdf_name,
        "pagina": page_num,
        "pipeline_status": pipeline_status,
        "pipeline_erro": pipeline_erro,
        "tipo_documento_classificacao": tipo_classificacao,
        "justificativa_classificacao": justificativa_classif,
        "tipo_documento_extracao": None,
        "numero_documento": None,
        "data_emissao_documento": None,
        "cnpj_emitente": None,
        "valor_documento": None,
        "cnpj_destinatario": None,
        "data_competencia_documento": None,
        "data_servico_documento": None,
        "numero_rps": None,
        "valores_encontrados": None,
        "cnpjs_encontrados": None,
        "observacao_extracao": None,
        "uso": {"classificacao": _empty_uso_component(), "extracao": _empty_uso_component()},
    }


def build_extracao_pagina_rows(
    pdf_tasks: list[dict],
    pdf_results: dict[str, dict],
    timestamp_geracao: datetime | None = None,
    versao_pipeline: dict | None = None,
) -> list[dict]:
    """
    Build the per-page ``extracao_pagina`` row list.

    Every page of every processed PDF gets exactly one item in the output
    list — regardless of whether a fiscal document was found on it or
    whether processing failed. This is what lets a consumer distinguish
    "processed successfully, nothing here" from "never got processed".

    Per PDF, pages are classified into one of three states:
    - Page was classified and (optionally) had a document extracted:
      pipeline_status="ok", with extraction fields populated if a
      document was found on that page, or null if not.
    - Page was never reached because processing aborted partway through
      (e.g. a Gemini API/credential error on an earlier page):
      pipeline_status="erro_processamento".
    - The PDF's page count itself is unknown (e.g. download failed
      before the file could even be opened): a single sentinel item
      with pagina=None is emitted, since there's nothing to enumerate.

    Erros silenciosos tratados aqui:
    - Páginas em page_categories cuja justificativa contém "Erro ao extrair
      página" (falha de bytes/PDF corrompido) são emitidas com
      pipeline_status="erro_processamento", não "ok". Isso evita que uma
      classificação fake ("Nenhuma das Opções" forçada por exceção) seja
      indistinguível de uma classificação legítima.

    Schema per item:
    {
        "nome_arquivo":              str,
        "pagina":                    int | null,
        "pipeline_status":           "ok" | "erro_processamento",
        "pipeline_erro":             str | null,
        "tipo_documento_classificacao": str | null,
        "justificativa_classificacao":  str | null,
        "tipo_documento_extracao":   str | null,
        "numero_documento":          str | null,
        "data_emissao_documento":    str | null,
        "cnpj_emitente":             str | null,
        "valor_documento":           float | null,
        "cnpj_destinatario":         str | null,
        "data_competencia_documento": str | null,
        "data_servico_documento":    str | null,
        "numero_rps":                str | null,
        "valores_encontrados":       dict | null,
        "cnpjs_encontrados":         dict | null,
        "observacao_extracao":       str | null,
        "uso": {
            "classificacao": {"modelo": str | null, "prompt_tokens": int, "completion_tokens": int, "total_tokens": int},
            "extracao":      {"modelo": str | null, "prompt_tokens": int, "completion_tokens": int, "total_tokens": int},
        },
        "timestamp_geracao":         str (ISO-8601 UTC),
        "versao_pipeline":           dict | null,
    }

    :param pdf_tasks: list of task dicts produced in process_database.
    :param pdf_results: mapping pdf_name -> result dict from process_pdf.
    :param timestamp_geracao: UTC timestamp of this pipeline run (auto-generated if None).
    :param versao_pipeline: dict with pipeline config params, model names, and prompt
        versions for traceability (see ``build_versao_pipeline``).
    :returns: List of per-page dicts ready for json.dump / NDJSON write.
    """
    # Garante que timestamp_geracao é sempre gerado automaticamente pela pipeline.
    # O campo NUNCA deve depender de input manual — gerado aqui uma única vez
    # por run e injetado em todos os itens de saída.
    if timestamp_geracao is None:
        timestamp_geracao = utc_now_naive()
    ts_iso = timestamp_geracao.isoformat() + "Z"

    output_items: list[dict] = []

    for task in pdf_tasks:
        pdf_name = task["pdf_name"]
        result = pdf_results.get(pdf_name, {})

        total_pages = result.get("total_pages")
        page_categories = result.get("page_categories") or {}
        page_justifications = result.get("page_justifications") or {}
        page_classification_usage = result.get("page_classification_usage") or {}
        page_extraction_usage = result.get("page_extraction_usage") or {}
        extracted_nfs = result.get("extracted_nfs") or []
        pipeline_ok = result.get("success", True)
        error_msg = result.get("error") if not pipeline_ok else None

        # A page can yield at most one extracted document in the current schema.
        extracted_by_page = {nf.get("pagina"): nf for nf in extracted_nfs if nf.get("pagina") is not None}

        if not total_pages:
            # Page count itself is unknown (e.g. download failed before the
            # PDF could be opened) — nothing to enumerate, emit one sentinel.
            item = empty_json_item(
                pdf_name,
                None,
                "ok" if pipeline_ok else "erro_processamento",
                error_msg,
            )
            item["timestamp_geracao"] = ts_iso
            item["versao_pipeline"] = versao_pipeline
            item["uso"] = build_uso_field(None, page_classification_usage, page_extraction_usage)
            output_items.append(item)
            continue

        for page_num in range(1, total_pages + 1):
            if page_num not in page_categories:
                # Never classified: either the whole PDF failed before
                # reaching this page, or processing aborted partway through.
                item = empty_json_item(
                    pdf_name,
                    page_num,
                    "erro_processamento",
                    error_msg or "Página não processada",
                )
                item["timestamp_geracao"] = ts_iso
                item["versao_pipeline"] = versao_pipeline
                item["uso"] = build_uso_field(page_num, page_classification_usage, page_extraction_usage)
                output_items.append(item)
                continue

            tipo_classificacao = page_categories.get(page_num)
            justificativa_classif = page_justifications.get(page_num, "")

            # Erro silencioso 1.1: página foi "classificada" apenas porque
            # extract_page_as_bytes falhou (PDF corrompido) e a exceção foi
            # capturada, salvando "Nenhuma das Opções" no cache.
            # Nesses casos a justificativa contém o prefixo "Erro ao extrair
            # página:" — emitimos como erro_processamento, não como ok.
            _is_byte_extraction_error = justificativa_classif is not None and justificativa_classif.startswith(
                "Erro ao extrair página:"
            )
            if _is_byte_extraction_error:
                item = empty_json_item(
                    pdf_name,
                    page_num,
                    "erro_processamento",
                    justificativa_classif,
                    tipo_classificacao,
                    justificativa_classif,
                )
                item["timestamp_geracao"] = ts_iso
                item["versao_pipeline"] = versao_pipeline
                item["uso"] = build_uso_field(page_num, page_classification_usage, page_extraction_usage)
                output_items.append(item)
                continue

            nf = extracted_by_page.get(page_num)

            if nf is None:
                item = empty_json_item(
                    pdf_name,
                    page_num,
                    "ok",
                    None,
                    tipo_classificacao,
                    justificativa_classif,
                )
                item["timestamp_geracao"] = ts_iso
                item["versao_pipeline"] = versao_pipeline
                item["uso"] = build_uso_field(page_num, page_classification_usage, page_extraction_usage)
                output_items.append(item)
                continue

            item = empty_json_item(
                pdf_name,
                page_num,
                "ok",
                None,
                tipo_classificacao,
                justificativa_classif,
            )
            item.update(
                {
                    "tipo_documento_extracao": nf.get("tipo_documento"),
                    "numero_documento": nf.get("numero_nf"),
                    "data_emissao_documento": nf.get("data_emissao"),
                    "cnpj_emitente": nf.get("cnpj_emitente"),
                    "valor_documento": nf.get("valor_total"),
                    "cnpj_destinatario": nf.get("cnpj_destinatario"),
                    "data_competencia_documento": nf.get("data_competencia"),
                    "data_servico_documento": nf.get("data_servico"),
                    "numero_rps": nf.get("numero_rps"),
                    "valores_encontrados": nf.get("campos_de_valor_encontrados"),
                    "cnpjs_encontrados": nf.get("campos_de_cnpj_encontrados"),
                    "observacao_extracao": nf.get("observacao"),
                    "timestamp_geracao": ts_iso,
                    "versao_pipeline": versao_pipeline,
                    "uso": build_uso_field(page_num, page_classification_usage, page_extraction_usage),
                }
            )
            output_items.append(item)

    return output_items
