"""Interpretação das respostas do modelo, comum ao batch e às chamadas diretas."""

import json
from dataclasses import dataclass
from typing import Any

from .categories import normalize_category
from .llm_requests import PageId, decode_custom_id

EMPTY_USAGE = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}


@dataclass(frozen=True)
class ModelOutput:
    """Resposta crua de uma página: texto ou erro, mais o uso de tokens."""

    page: PageId
    text: str | None
    usage: dict[str, int]
    error: str | None


@dataclass(frozen=True)
class ClassificationResult:
    """Classificação de uma página."""

    page: PageId
    category: str | None
    justification: str
    usage: dict[str, int]
    error: str | None


@dataclass(frozen=True)
class ExtractionResult:
    """Extração de uma página classificada como documento fiscal."""

    page: PageId
    extracted: dict | None
    usage: dict[str, int]
    error: str | None


def output_from_vertex_row(raw_row: dict) -> ModelOutput:
    """Converte uma linha de ``predictions.jsonl``.

    :param raw_row: Linha já decodificada.
    :returns: Resposta da página.
    :raises ValueError: Se o ``custom_id`` for inválido.
    """
    page = decode_custom_id(raw_row["custom_id"])
    status = raw_row.get("status")
    if status:
        return ModelOutput(page, None, dict(EMPTY_USAGE), f"Linha do batch falhou no Vertex: {status}")
    response = raw_row.get("response") or {}
    usage_raw = response.get("usageMetadata") or {}
    usage = {
        "prompt_tokens": usage_raw.get("promptTokenCount") or 0,
        "completion_tokens": usage_raw.get("candidatesTokenCount") or 0,
        "total_tokens": usage_raw.get("totalTokenCount") or 0,
    }
    candidates = response.get("candidates") or [{}]
    parts = (candidates[0].get("content") or {}).get("parts") or [{}]
    text = parts[0].get("text")
    if text is None:
        return ModelOutput(page, None, usage, "Resposta do Vertex sem texto.")
    return ModelOutput(page, text, usage, None)


def parse_json_response(text: str) -> Any:
    """Faz o parse de JSON removendo cercas de Markdown, se houver.

    :param text: Texto retornado pelo modelo.
    :returns: Valor JSON.
    :raises json.JSONDecodeError: Se o texto não for JSON válido.
    """
    cleaned = text.strip()
    fence = cleaned.find("```")
    if fence != -1:
        cleaned = cleaned[fence:].removeprefix("```json").removeprefix("```").removesuffix("```")
    return json.loads(cleaned.strip())


def parse_classification(output: ModelOutput) -> ClassificationResult:
    """Interpreta a resposta de classificação.

    :param output: Resposta crua.
    :returns: Categoria normalizada, ou erro.
    """
    if output.error is not None:
        return ClassificationResult(output.page, None, "", output.usage, output.error)
    try:
        parsed = parse_json_response(output.text)
    except json.JSONDecodeError as exc:
        return ClassificationResult(output.page, None, "", output.usage, f"Resposta de classificação não é JSON: {exc}")
    if not isinstance(parsed, dict):
        return ClassificationResult(
            output.page, None, "", output.usage, "Resposta de classificação não é um objeto JSON."
        )
    category = normalize_category(parsed.get("categoria") or "")
    return ClassificationResult(output.page, category, parsed.get("justificativa") or "", output.usage, None)


def parse_extraction(output: ModelOutput) -> ExtractionResult:
    """Interpreta a resposta de extração.

    :param output: Resposta crua.
    :returns: JSON extraído, ou erro.
    """
    if output.error is not None:
        return ExtractionResult(output.page, None, output.usage, output.error)
    try:
        parsed = parse_json_response(output.text)
    except json.JSONDecodeError as exc:
        return ExtractionResult(output.page, None, output.usage, f"Resposta de extração não é JSON: {exc}")
    if not isinstance(parsed, dict):
        return ExtractionResult(output.page, None, output.usage, "Resposta de extração não é um objeto JSON.")
    return ExtractionResult(output.page, parsed, output.usage, None)
