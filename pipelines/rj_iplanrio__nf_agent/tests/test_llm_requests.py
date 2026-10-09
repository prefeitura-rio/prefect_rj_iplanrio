"""Tests for Vertex-native request building and custom_id round-trip."""

import json

import pytest

from pipelines.rj_iplanrio__nf_agent import constants
from pipelines.rj_iplanrio__nf_agent.utils import llm_requests
from pipelines.rj_iplanrio__nf_agent.utils.llm_requests import PageId


def test_custom_id_round_trip_splits_on_last_colon():
    custom_id = llm_requests.encode_custom_id("a:b_doc", 3)
    assert custom_id == "a:b_doc:3"
    assert llm_requests.decode_custom_id(custom_id) == PageId("a:b_doc", 3)


def test_custom_id_rejects_long_names_and_bad_shapes():
    with pytest.raises(ValueError, match="custom_id"):
        llm_requests.encode_custom_id("x" * 100, 1)
    with pytest.raises(ValueError, match="custom_id"):
        llm_requests.decode_custom_id("semdoispontos")


def test_jsonl_line_shape():
    line = llm_requests.jsonl_line("doc:1", "prompt ç", "QUJD")
    assert line.endswith(b"\n")
    row = json.loads(line)
    assert row["custom_id"] == "doc:1"
    parts = row["request"]["contents"][0]["parts"]
    assert parts[0] == {"text": "prompt ç"}
    assert parts[1] == {"inlineData": {"mimeType": "application/pdf", "data": "QUJD"}}
    assert row["request"]["generationConfig"] == constants.GENERATION_CONFIG


def test_page_b64_from_vertex_row(vertex_row):
    assert llm_requests.page_b64_from_vertex_row(vertex_row("doc:1", "{}", page_b64="Wlo=")) == "Wlo="
    with pytest.raises(ValueError, match="doc:2"):
        llm_requests.page_b64_from_vertex_row({"custom_id": "doc:2", "request": {}})
