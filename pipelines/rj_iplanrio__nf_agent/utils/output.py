"""Linhas da tabela ``extracao_pagina`` (uma por página de cada PDF)."""

import os
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime

from .. import constants
from .results import PdfResult

NF_FIELDS = {
    "tipo_documento_extracao": "tipo_documento",
    "numero_documento": "numero_nf",
    "data_emissao_documento": "data_emissao",
    "cnpj_emitente": "cnpj_emitente",
    "valor_documento": "valor_total",
    "cnpj_destinatario": "cnpj_destinatario",
    "data_competencia_documento": "data_competencia",
    "data_servico_documento": "data_servico",
    "numero_rps": "numero_rps",
    "valores_encontrados": "campos_de_valor_encontrados",
    "cnpjs_encontrados": "campos_de_cnpj_encontrados",
    "observacao_extracao": "observacao",
}


@dataclass(frozen=True)
class RunMetadata:
    """Campos de rastreabilidade repetidos em todas as linhas de uma sessão."""

    versao_pipeline: dict
    generated_at: datetime


def utc_now_naive() -> datetime:
    """Retorna o horário UTC atual sem ``tzinfo`` (serializado com sufixo ``Z``).

    :returns: Horário atual em UTC.
    """
    return datetime.now(UTC).replace(tzinfo=None)


def current_commit() -> str | None:
    """Retorna o commit do código em execução.

    A imagem não tem ``.git``; o deploy injeta ``GIT_COMMIT_SHA``. Localmente usa ``git``.

    :returns: Hash curto do commit, ou ``None`` se indisponível.
    """
    commit = os.environ.get("GIT_COMMIT_SHA")
    if commit:
        return commit
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL, text=True
        ).strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def build_versao_pipeline(
    processing_version: str,
    classification_prompt_version: str,
    extraction_prompt_version: str,
    run_id: str,
    session_id: str | None,
) -> dict:
    """Monta o campo ``versao_pipeline``; ``versao_processamento`` é a chave de reprocessamento.

    :param processing_version: ``versao_processamento`` da submissão.
    :param classification_prompt_version: Ex. ``"v8"``.
    :param extraction_prompt_version: Ex. ``"v9"``.
    :param run_id: Submissão que originou a sessão.
    :param session_id: Sessão (``None`` na execução local).
    :returns: Dicionário de rastreabilidade.
    """
    return {
        "versao_processamento": processing_version,
        "commit": current_commit(),
        "modelo": constants.MODEL_NAME,
        "versao_prompt_classificacao": classification_prompt_version,
        "versao_prompt_extracao": extraction_prompt_version,
        "run_id": run_id,
        "session_id": session_id,
    }


def usage_field(usage: dict[str, int] | None) -> dict:
    """Converte o uso de tokens de uma chamada para o formato de saída.

    :param usage: Tokens da chamada, ou ``None`` se a chamada não ocorreu.
    :returns: ``{"modelo", "prompt_tokens", "completion_tokens", "total_tokens"}``.
    """
    if not usage:
        return {"modelo": None, "prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    return {"modelo": constants.MODEL_NAME, **usage}


def page_row(result: PdfResult, page: int, metadata: RunMetadata) -> dict:
    """Monta a linha de uma página sem campos de NF.

    :param result: Resultado do PDF.
    :param page: Página, a partir de 1.
    :param metadata: Rastreabilidade da sessão.
    :returns: Linha com status e classificação preenchidos.
    """
    error = result.classification_errors.get(page) or result.extraction_errors.get(page)
    if error is None and page not in result.categories:
        error = "Página não processada"
    row = {
        "nome_arquivo": result.pdf_name,
        "pagina": page,
        "pipeline_status": "ok" if error is None else "erro_processamento",
        "pipeline_erro": error,
        "tipo_documento_classificacao": result.categories.get(page),
        "justificativa_classificacao": result.justifications.get(page),
    }
    row.update(dict.fromkeys(NF_FIELDS))
    row["uso"] = {
        "classificacao": usage_field(result.classification_usage.get(page)),
        "extracao": usage_field(result.extraction_usage.get(page)),
    }
    row["timestamp_geracao"] = metadata.generated_at.isoformat() + "Z"
    row["versao_pipeline"] = metadata.versao_pipeline
    return row


def build_extracao_pagina_rows(pdf_results: dict[str, PdfResult], metadata: RunMetadata) -> list[dict]:
    """Gera uma linha por página de cada PDF, com os campos da NF quando houver.

    :param pdf_results: Resultado de :func:`results.build_pdf_results`.
    :param metadata: Rastreabilidade da sessão.
    :returns: Linhas prontas para NDJSON.
    """
    rows = []
    for result in pdf_results.values():
        nf_by_page = {nf["pagina"]: nf for nf in result.extracted_nfs if nf.get("pagina") is not None}
        for page in range(1, result.total_pages + 1):
            row = page_row(result, page, metadata)
            nf = nf_by_page.get(page)
            if nf is not None:
                row.update({column: nf.get(source) for column, source in NF_FIELDS.items()})
            rows.append(row)
    return rows
