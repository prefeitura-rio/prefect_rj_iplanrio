"""Linhas de JSONL no formato nativo do Vertex AI Batch Prediction."""

import json
from dataclasses import dataclass

from .. import constants

# IDs de 128 caracteres são rejeitados na validação do Vertex; 96 passam.
MAX_CUSTOM_ID_LENGTH = 96


@dataclass(frozen=True)
class PageId:
    """Página de um PDF (numeração a partir de 1)."""

    pdf_name: str
    page_number: int


def encode_custom_id(pdf_name: str, page_number: int) -> str:
    """Monta o ``custom_id`` de uma linha.

    :param pdf_name: Nome do PDF sem extensão.
    :param page_number: Página, a partir de 1.
    :returns: ``"<pdf_name>:<page_number>"``.
    :raises ValueError: Se passar de :data:`MAX_CUSTOM_ID_LENGTH`.
    """
    custom_id = f"{pdf_name}:{page_number}"
    if len(custom_id) > MAX_CUSTOM_ID_LENGTH:
        raise ValueError(f"custom_id com {len(custom_id)} caracteres (limite {MAX_CUSTOM_ID_LENGTH}): {custom_id!r}")
    return custom_id


def decode_custom_id(custom_id: str) -> PageId:
    """Recupera a página de um ``custom_id``.

    :param custom_id: Valor ecoado pelo Vertex.
    :returns: Página identificada.
    :raises ValueError: Se não estiver no formato ``nome:pagina``.
    """
    name, sep, page = custom_id.rpartition(":")
    if not sep or not name or not page.isdigit():
        raise ValueError(f"custom_id malformado: {custom_id!r}")
    return PageId(pdf_name=name, page_number=int(page))


def inline_part(page_b64: str) -> dict:
    """Parte do request com a página embutida em base64.

    :param page_b64: PDF de uma página em base64.
    :returns: Parte ``inlineData``.
    """
    return {"inlineData": {"mimeType": "application/pdf", "data": page_b64}}


def file_part(uri: str) -> dict:
    """Parte do request que referencia a página por URI do GCS.

    :param uri: URI ``gs://`` do PDF de uma página.
    :returns: Parte ``fileData``.
    """
    return {"fileData": {"fileUri": uri, "mimeType": "application/pdf"}}


def vertex_request(prompt: str, page_part: dict) -> dict:
    """Monta o ``request`` de uma página no formato ``generateContent``.

    :param prompt: Texto do prompt.
    :param page_part: Parte com a página (:func:`file_part` ou :func:`inline_part`).
    :returns: Dicionário serializável.
    """
    return {
        "contents": [{"role": "user", "parts": [{"text": prompt}, page_part]}],
        "generationConfig": dict(constants.GENERATION_CONFIG),
    }


def serialize_row(row: dict) -> bytes:
    """Serializa uma linha do JSONL, já com a quebra de linha.

    :param row: Linha com ``custom_id`` e ``request``.
    :returns: Linha em UTF-8 terminada em ``\\n``.
    """
    return (json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")


def jsonl_line(custom_id: str, prompt: str, page_part: dict) -> bytes:
    """Serializa uma linha do JSONL de entrada, já com a quebra de linha.

    :param custom_id: Ver :func:`encode_custom_id`.
    :param prompt: Texto do prompt.
    :param page_part: Parte com a página (:func:`file_part` ou :func:`inline_part`).
    :returns: Linha em UTF-8 terminada em ``\\n``.
    """
    return serialize_row({"custom_id": custom_id, "request": vertex_request(prompt, page_part)})


def retry_line(raw_row: dict) -> bytes:
    """Refaz a linha de entrada de uma página a partir do ``request`` que o Vertex ecoa.

    :param raw_row: Linha de ``predictions.jsonl`` que falhou.
    :returns: Linha de entrada para uma nova tentativa.
    :raises ValueError: Se a linha não trouxer o ``request`` ecoado.
    """
    if "request" not in raw_row:
        raise ValueError(f"Linha {raw_row.get('custom_id')!r} sem o request ecoado.")
    return serialize_row({"custom_id": raw_row["custom_id"], "request": raw_row["request"]})


def page_part_from_vertex_row(raw_row: dict) -> dict:
    """Extrai a parte da página (URI ou base64) do ``request`` que o Vertex ecoa no output.

    :param raw_row: Linha de ``predictions.jsonl``.
    :returns: Parte ``fileData`` ou ``inlineData``, como foi enviada.
    :raises ValueError: Se a linha não trouxer o ``request`` com a página.
    """
    try:
        part = raw_row["request"]["contents"][0]["parts"][1]
    except (KeyError, IndexError, TypeError) as exc:
        raise ValueError(f"Linha {raw_row.get('custom_id')!r} sem a página no request ecoado.") from exc
    if not isinstance(part, dict) or not ({"fileData", "inlineData"} & part.keys()):
        raise ValueError(f"Linha {raw_row.get('custom_id')!r} sem a página no request ecoado.")
    return part
