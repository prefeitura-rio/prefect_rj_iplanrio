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


def test_jsonl_line_shape_with_an_inline_page():
    line = llm_requests.jsonl_line("doc:1", "prompt ç", llm_requests.inline_part("QUJD"))
    assert line.endswith(b"\n")
    row = json.loads(line)
    assert row["custom_id"] == "doc:1"
    parts = row["request"]["contents"][0]["parts"]
    assert parts[0] == {"text": "prompt ç"}
    assert parts[1] == {"inlineData": {"mimeType": "application/pdf", "data": "QUJD"}}
    assert row["request"]["generationConfig"] == constants.GENERATION_CONFIG


def test_jsonl_line_references_the_page_by_uri():
    line = llm_requests.jsonl_line("doc:1", "prompt", llm_requests.file_part("gs://b/p/0001.pdf"))
    parts = json.loads(line)["request"]["contents"][0]["parts"]
    assert parts[1] == {"fileData": {"fileUri": "gs://b/p/0001.pdf", "mimeType": "application/pdf"}}
    assert len(line) < 1000  # noqa: PLR2004 -- a linha por URI não carrega a página


def test_page_part_from_vertex_row_returns_inline_or_file_part(vertex_row):
    assert llm_requests.page_part_from_vertex_row(vertex_row("doc:1", "{}", page_b64="Wlo=")) == {
        "inlineData": {"mimeType": "application/pdf", "data": "Wlo="}
    }
    row = {
        "custom_id": "doc:2",
        "request": {"contents": [{"parts": [{"text": "p"}, llm_requests.file_part("gs://b/x")]}]},
    }
    assert llm_requests.page_part_from_vertex_row(row) == llm_requests.file_part("gs://b/x")


def test_page_part_from_vertex_row_rejects_rows_without_the_page():
    with pytest.raises(ValueError, match="doc:2"):
        llm_requests.page_part_from_vertex_row({"custom_id": "doc:2", "request": {}})
    with pytest.raises(ValueError, match="doc:3"):
        llm_requests.page_part_from_vertex_row({"custom_id": "doc:3", "request": {"contents": [{"parts": [{}, {}]}]}})


def test_retry_line_reemits_the_echoed_request(vertex_row):
    failed = vertex_row("doc:1", None, status='{"code":13}')
    line = llm_requests.retry_line(failed)
    row = json.loads(line)
    assert row == {"custom_id": "doc:1", "request": failed["request"]}
    assert line.endswith(b"\n")
    with pytest.raises(ValueError, match="doc:9"):
        llm_requests.retry_line({"custom_id": "doc:9"})
