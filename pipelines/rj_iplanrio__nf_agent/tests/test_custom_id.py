"""Tests for ``utils/batch/custom_id.py``'s compact ``name:page`` encoding.

The encoding must stay short: Vertex rejects batch rows whose custom_id is
too long (bisected against staging on 2026-09-23 — 96-char IDs complete,
128-char IDs fail validation with 'unsupported type'). The previous
base64-JSON encoding (~216 chars) was what killed every staging session.
"""

import pytest

from pipelines.rj_iplanrio__nf_agent.utils.batch.custom_id import (
    MAX_CUSTOM_ID_LENGTH,
    decode_custom_id,
    encode_custom_id,
)


def test_round_trip():
    custom_id = encode_custom_id("0001_9615_AP21_16727386000178_75435265_01_2024", 12)
    assert custom_id == "0001_9615_AP21_16727386000178_75435265_01_2024:12"

    payload = decode_custom_id(custom_id)
    assert payload.pdf_name == "0001_9615_AP21_16727386000178_75435265_01_2024"
    assert payload.page_number == 12


def test_realistic_production_id_stays_well_under_the_validated_ceiling():
    # Longest pdf_name measured in the bucket (49 chars, 2026-09-23) plus a
    # 4-digit page number must stay comfortably under the 96-char IDs that
    # were proven to pass Vertex validation.
    custom_id = encode_custom_id("02.12.01_AP2.1_BR_CIRURGICAOLIMPIO_127028_10_2021", 1234)
    assert len(custom_id) < MAX_CUSTOM_ID_LENGTH
    # And specifically under the empirically validated 96-char bound too
    # (MAX_CUSTOM_ID_LENGTH equals it today, but the assertion documents
    # the intent even if the constant ever moves).
    assert len(custom_id) < 96


def test_name_containing_colons_survives_via_last_colon_split():
    # No such names exist in this bucket, but the parsing must not assume it.
    payload = decode_custom_id(encode_custom_id("weird:name.pdf", 3))
    assert payload.pdf_name == "weird:name.pdf"
    assert payload.page_number == 3


def test_retired_base64_format_is_rejected_loudly_not_misparsed():
    # The old encoding contains no colon at all, so it must fail here with
    # a clear error — never silently decode to a wrong identity.
    with pytest.raises(ValueError, match="Malformed custom_id"):
        decode_custom_id("eyJwaGFzZSI6ImNsYXNzaWZpY2F0aW9uIn0=")


def test_garbage_is_rejected():
    with pytest.raises(ValueError, match="Malformed custom_id"):
        decode_custom_id("not-an-id")
    with pytest.raises(ValueError, match="Malformed custom_id"):
        decode_custom_id("name:not-a-number")


def test_oversized_id_fails_fast_at_encode_time():
    with pytest.raises(ValueError, match="too long"):
        encode_custom_id("x" * 200, 1)
