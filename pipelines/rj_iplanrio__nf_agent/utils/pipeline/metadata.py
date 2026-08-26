"""Metadata and JSON output builders for ``POCProcessor``."""

import logging
import subprocess
from datetime import datetime
from typing import TYPE_CHECKING, Any

import pandas as pd

from ..compliance.utils import normalize_cnpj, normalize_number
from .modes import ExecutionMode

if TYPE_CHECKING:
    from .processor import POCProcessor

logger = logging.getLogger(".".join(__name__.split(".")[:-1] + ["processor"]))


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


def build_extraction_detail(extracted_nfs: list[dict], result: dict) -> dict:
    """
    Constrói detalhe estruturado da extração por documento.
    Inclui metadados completos da resposta do modelo (possui_nota_fiscal, quantidade, etc).

    :param extracted_nfs: Lista de NFs extraídas.
    :param result: Resultado completo da extração.
    :returns: Dicionário estruturado com detalhes da extração.
    """
    # Capturar resposta completa do modelo (se disponível)
    possui_nota_fiscal = result.get("possui_nota_fiscal", len(extracted_nfs) > 0)
    quantidade = result.get("quantidade_notas_fiscais", len(extracted_nfs))

    documents_detail = []
    for nf in extracted_nfs:
        doc = {
            "original_page": nf.get("pagina"),
            "tipo_documento": nf.get("tipo_documento"),
            "cnpj_emitente": nf.get("cnpj_emitente"),
            "cnpj_destinatario": nf.get("cnpj_destinatario"),
            "numero_nf": nf.get("numero_nf"),
            "valor_total": nf.get("valor_total"),
            "data_emissao": nf.get("data_emissao"),
        }

        # Adicionar batch_info se presente (novo campo de rastreabilidade)
        if "_page_mapping" in nf:
            doc["batch_info"] = nf["_page_mapping"]

        documents_detail.append(doc)

    extraction_method = "batch" if result.get("batching_used", False) else "single"

    extraction_detail = {
        "possui_nota_fiscal": possui_nota_fiscal,
        "quantidade_notas_fiscais": quantidade,
        "documents_extracted": len(extracted_nfs),
        "extraction_method": extraction_method,
        "documents": documents_detail,
    }

    # Adicionar batch_details se batching foi usado (inclui raw responses)
    if result.get("batching_used"):
        extraction_detail["batch_details"] = result.get("batch_details", [])

    return extraction_detail


def build_versao_pipeline(
    processor: "POCProcessor",
    mode: "ExecutionMode",
    workers: int,
    requests_per_minute: int,
    max_concurrent: int,
) -> dict[str, Any]:
    """
    Monta o JSON de rastreabilidade de configuração da execução.

    Inclui parâmetros operacionais e informações do repositório git para
    permitir comparar resultados apenas entre execuções com a mesma config.

    :param processor: The ``POCProcessor`` instance (supplies config fields).
    """
    info: dict[str, Any] = {
        "mode": mode.value,
        "extraction_batch_size": processor.extraction_batch_size,
        "min_match_score": processor.min_match_score,
        "match_requires_pdf_name": processor.match_requires_pdf_name,
        "workers": workers,
        "requests_per_minute": requests_per_minute,
        "max_concurrent": max_concurrent,
    }
    info.update(get_git_info())
    return info


def build_versao_prompt(processor: "POCProcessor") -> dict[str, Any]:
    """
    Monta o JSON de rastreabilidade de versões de prompt.

    Permite filtrar/comparar resultados apenas entre execuções que usaram
    exatamente os mesmos prompts e batch_size de extração.

    :param processor: The ``POCProcessor`` instance (supplies prompt versions).
    """
    return {
        "versao_prompt_classificacao": processor.prompt_versions.get("classification"),
        "versao_prompt_extracao": processor.prompt_versions.get("extraction"),
        "batch_size_extracao": processor.extraction_batch_size,
    }


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


def empty_json_item(
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
        "match_id_documento": [],
        "valor_documento": None,
        "cnpj_destinatario": None,
        "data_competencia_documento": None,
        "data_servico_documento": None,
        "numero_rps": None,
        "valores_encontrados": None,
        "cnpjs_encontrados": None,
        "observacao_extracao": None,
    }


def build_json_output(
    pdf_tasks: list[dict],
    pdf_results: dict[str, dict],
    input_df: "pd.DataFrame",
    min_match_score: int,
    match_requires_pdf_name: bool = False,
    timestamp_geracao: datetime | None = None,
    versao_pipeline: dict | None = None,
    versao_prompt: dict | None = None,
) -> list[dict]:
    """
    Build the per-page JSON output list.

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
        "match_id_documento":        list[str],
        "valor_documento":           float | null,
        "cnpj_destinatario":         str | null,
        "data_competencia_documento": str | null,
        "data_servico_documento":    str | null,
        "numero_rps":                str | null,
        "valores_encontrados":       dict | null,
        "cnpjs_encontrados":         dict | null,
        "observacao_extracao":       str | null,
        "timestamp_geracao":         str (ISO-8601 UTC),
        "versao_pipeline":           dict | null,
        "versao_prompt":             dict | null,
    }

    :param pdf_tasks: list of task dicts produced in process_database.
    :param pdf_results: mapping pdf_name -> result dict from process_pdf.
    :param input_df: the full input DataFrame (used to build id_documento lookup).
    :param min_match_score: minimum match_score_3_fields threshold for match_id_documento.
    :param match_requires_pdf_name: when True, only declarations whose pdf_name matches the
        current PDF are candidates for match_id_documento (legacy
        behaviour). When False (default), every declaration in
        input_df is a candidate regardless of pdf_name, enabling
        cross-PDF match analysis in BigQuery.
    :param timestamp_geracao: UTC timestamp of this pipeline run (auto-generated if None).
    :param versao_pipeline: dict with pipeline config params for traceability.
    :param versao_prompt: dict with prompt versions and batch_size for traceability.
    :returns: List of per-page dicts ready for json.dump / NDJSON write.
    """
    from ..compliance.utils import (
        DocumentFields,
        match_score_3_fields,
    )

    # Garante que timestamp_geracao é sempre gerado automaticamente pela pipeline.
    # O campo NUNCA deve depender de input manual — gerado aqui uma única vez
    # por run e injetado em todos os itens de saída.
    if timestamp_geracao is None:
        timestamp_geracao = datetime.utcnow()
    ts_iso = timestamp_geracao.isoformat() + "Z"

    # Pre-build a lookup from pdf_name -> list of declaration dicts,
    # used to resolve match_id_documento for each extracted NF page.
    declaration_lookup: dict[str, list] = {}
    for _, row in input_df.iterrows():
        pdf_name = str(row.get("descricao_limpa", ""))
        declaration_lookup.setdefault(pdf_name, []).append(
            {
                "id_documento": str(row.get("id_documento", "")),
                "cnpj_norm": normalize_cnpj(str(row.get("cnpj_cpf", ""))),
                "numero_norm": normalize_number(str(row.get("num_documento", ""))),
                "data_emissao": str(row.get("data_emissao", "")),
            }
        )

    output_items: list[dict] = []

    for task in pdf_tasks:
        pdf_name = task["pdf_name"]
        result = pdf_results.get(pdf_name, {})

        total_pages = result.get("total_pages")
        page_categories = result.get("page_categories") or {}
        page_justifications = result.get("page_justifications") or {}
        extracted_nfs = result.get("extracted_nfs") or []
        pipeline_ok = result.get("success", True)
        error_msg = result.get("error") if not pipeline_ok else None

        # A page can yield at most one extracted document in the current schema.
        extracted_by_page = {nf.get("pagina"): nf for nf in extracted_nfs if nf.get("pagina") is not None}
        if match_requires_pdf_name:
            # Legacy behaviour: only declarations that explicitly point to this PDF.
            declarations = declaration_lookup.get(pdf_name, [])
        else:
            # Cross-PDF mode: all declarations in the input are candidates.
            # Useful for BigQuery analysis of documents declared in one PDF
            # that match content extracted from a different PDF.
            declarations = [d for dlist in declaration_lookup.values() for d in dlist]

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
            item["versao_prompt"] = versao_prompt
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
                item["versao_prompt"] = versao_prompt
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
                item["versao_prompt"] = versao_prompt
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
                item["versao_prompt"] = versao_prompt
                output_items.append(item)
                continue

            # Find which declarations match this NF with the configured threshold.
            # match_id_documento is a list (possibly multiple declarations match same NF).
            matched_ids: list[str] = []
            ext_cnpj = nf.get("cnpj_emitente", "") or ""
            ext_numero = nf.get("numero_nf", "") or ""
            ext_data = nf.get("data_emissao") or ""

            for decl in declarations:
                score = match_score_3_fields(
                    expected=DocumentFields(
                        cnpj=decl["cnpj_norm"],
                        numero=decl["numero_norm"],
                        data=decl["data_emissao"],
                    ),
                    extracted=DocumentFields(
                        cnpj=ext_cnpj,
                        numero=ext_numero,
                        data=ext_data,
                    ),
                )
                if score >= min_match_score:
                    matched_ids.append(decl["id_documento"])

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
                    "match_id_documento": matched_ids,
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
                    "versao_prompt": versao_prompt,
                }
            )
            output_items.append(item)

    return output_items
