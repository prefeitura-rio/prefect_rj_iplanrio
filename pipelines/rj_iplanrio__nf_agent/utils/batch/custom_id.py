"""Encode/decode the ``custom_id`` carried on every Bifrost batch JSONL row.

Replaces the passthrough ``pdf_name``/``page_number``/``session_id`` columns
the old direct-Vertex-via-BigQuery implementation relied on to map a batch
output row back to the page that produced it (see ``result_adapter.py``'s
module docstring) — Bifrost's Batch API has no equivalent extra-columns
mechanism, only the one opaque ``custom_id`` string every provider's batch
format supports (mirrors OpenAI's own Batch API). Base64-encoding a small
JSON payload into it avoids picking a delimiter character that might appear
in a real ``pdf_name`` (spaces, parentheses, etc. all show up in the wild —
see the filenames in ``pdfs_ground_truth/``).
"""

import base64
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class CustomIdPayload:
    """Decoded identity of one batch JSONL row."""

    phase: str
    session_id: str
    pdf_name: str
    page_number: int


def encode_custom_id(phase: str, session_id: str, pdf_name: str, page_number: int) -> str:
    """Build the opaque ``custom_id`` string for one batch input row.

    :param phase: ``"classification"`` or ``"extraction"``.
    :param session_id: Current batch session UUID.
    :param pdf_name: Source PDF filename.
    :param page_number: 1-indexed page number within the source PDF.
    :returns: URL-safe base64 string, short enough to stay well under any
        provider's ``custom_id`` length limit.
    """
    payload = {"phase": phase, "session_id": session_id, "pdf_name": pdf_name, "page_number": page_number}
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_custom_id(custom_id: str) -> CustomIdPayload:
    """Recover the page identity encoded by :func:`encode_custom_id`.

    :param custom_id: The ``custom_id`` echoed back on a batch result row.
    :returns: The decoded :class:`CustomIdPayload`.
    :raises ValueError: If ``custom_id`` isn't validly-encoded JSON (should
        never happen for a row this pipeline itself submitted).
    """
    try:
        raw = base64.urlsafe_b64decode(custom_id.encode("ascii"))
        payload = json.loads(raw)
        return CustomIdPayload(
            phase=payload["phase"],
            session_id=payload["session_id"],
            pdf_name=payload["pdf_name"],
            page_number=int(payload["page_number"]),
        )
    except Exception as exc:
        raise ValueError(f"Malformed custom_id: {custom_id!r}") from exc
