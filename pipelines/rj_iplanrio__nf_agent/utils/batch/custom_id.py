"""Encode/decode the ``custom_id`` carried on every Bifrost batch JSONL row.

Replaces the passthrough ``pdf_name``/``page_number``/``session_id`` columns
the old direct-Vertex-via-BigQuery implementation relied on to map a batch
output row back to the page that produced it (see ``result_adapter.py``'s
module docstring) — Bifrost's Batch API has no equivalent extra-columns
mechanism, only the one opaque ``custom_id`` string every provider's batch
format supports (mirrors OpenAI's own Batch API).

The encoding is deliberately minimal — ``f"{pdf_name}:{page_number}"``,
nothing else. An earlier version base64-encoded a JSON payload carrying
``phase``/``session_id``/``pdf_name``/``page_number`` (~216 chars), which
Vertex rejects at validation time: bisected directly against staging,
96-char IDs complete while 128-char IDs fail with 'custom_id ... of
unsupported type' (2026-09-23). Two things make the short form sufficient:

- ``phase`` is known by *which* parser runs (classification vs extraction
  output parsing are separate functions) — it never needed to travel.
- ``session_id`` is known from the job context (which batch the row came
  from) — same, never needed per row.

Only ``(pdf_name, page_number)`` actually needs to round-trip, and that is
all the encoding carries. Decoding splits on the LAST colon, so a
``pdf_name`` containing ``:`` would still survive (no such names exist in
this bucket — verified 2026-09-23 — but the parsing doesn't assume that).
"""

from dataclasses import dataclass

# Validated-safe ceiling for custom_id length: 96-char IDs complete,
# 128-char IDs fail Vertex-side validation (bisected against staging on
# 2026-09-23; the true limit is somewhere in between, mechanism unknown).
# encode_custom_id raises past this rather than submitting a job Vertex is
# known to reject ~40 minutes later. Real pdf_names in this bucket run
# <=49 chars (measured 2026-09-23), so name:page IDs land ~55 chars max —
# comfortable margin, and any future growth fails loudly here instead of
# silently at validation time.
MAX_CUSTOM_ID_LENGTH = 96


@dataclass(frozen=True)
class CustomIdPayload:
    """Decoded identity of one batch JSONL row."""

    pdf_name: str
    page_number: int


def encode_custom_id(pdf_name: str, page_number: int) -> str:
    """Build the opaque ``custom_id`` string for one batch input row.

    :param pdf_name: Source PDF filename (bare, no bucket prefix).
    :param page_number: 1-indexed page number within the source PDF.
    :returns: ``f"{pdf_name}:{page_number}"`` — plain, short, reversible.
    :raises ValueError: If the result would exceed
        :data:`MAX_CUSTOM_ID_LENGTH` (fail fast at submit-planning time
        instead of submitting a job Vertex will reject at validation).
    """
    custom_id = f"{pdf_name}:{page_number}"
    if len(custom_id) > MAX_CUSTOM_ID_LENGTH:
        raise ValueError(
            f"custom_id too long ({len(custom_id)} chars, limit {MAX_CUSTOM_ID_LENGTH}): {custom_id!r}. "
            "Vertex rejects oversized custom_ids at batch validation time."
        )
    return custom_id


def decode_custom_id(custom_id: str) -> CustomIdPayload:
    """Recover the page identity encoded by :func:`encode_custom_id`.

    :param custom_id: The ``custom_id`` echoed back on a batch result row.
    :returns: The decoded :class:`CustomIdPayload`.
    :raises ValueError: If ``custom_id`` isn't in ``name:page`` shape
        (should never happen for a row this pipeline itself submitted —
        notably, the retired base64-JSON encoding contains no colon and is
        therefore rejected here loudly rather than misparsed).
    """
    try:
        name, sep, page = custom_id.rpartition(":")
        if not sep or not name or not page:
            raise ValueError("not in 'pdf_name:page_number' shape")
        return CustomIdPayload(pdf_name=name, page_number=int(page))
    except ValueError as exc:
        raise ValueError(f"Malformed custom_id: {custom_id!r}") from exc
