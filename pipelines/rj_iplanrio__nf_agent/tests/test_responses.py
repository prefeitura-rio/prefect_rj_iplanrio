"""Tests for parsing Vertex batch output rows."""

from pipelines.rj_iplanrio__nf_agent.utils import responses
from pipelines.rj_iplanrio__nf_agent.utils.llm_requests import PageId


def test_output_from_ok_row(vertex_row):
    output = responses.output_from_vertex_row(vertex_row("doc:2", '{"categoria": "NFS-e"}'))
    assert output.page == PageId("doc", 2)
    assert output.text == '{"categoria": "NFS-e"}'
    assert output.usage == {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
    assert output.error is None


def test_output_from_failed_row(vertex_row):
    output = responses.output_from_vertex_row(vertex_row("doc:1", None, status="erro interno"))
    assert output.text is None
    assert "erro interno" in output.error


def test_output_without_text_is_an_error(vertex_row):
    output = responses.output_from_vertex_row(vertex_row("doc:1", None))
    assert output.error is not None


def test_parse_json_response_strips_fences():
    assert responses.parse_json_response('Segue:\n```json\n{"a": 1}\n```') == {"a": 1}


def test_parse_classification_normalizes_category(vertex_row):
    output = responses.output_from_vertex_row(vertex_row("doc:1", '{"categoria": "nfse", "justificativa": "j"}'))
    result = responses.parse_classification(output)
    assert result.category == "NFS-e"
    assert result.justification == "j"
    assert result.error is None


def test_parse_classification_bad_json_is_an_error(vertex_row):
    result = responses.parse_classification(responses.output_from_vertex_row(vertex_row("doc:1", "não é json")))
    assert result.category is None
    assert "JSON" in result.error


def test_parse_extraction(vertex_row):
    ok = responses.parse_extraction(
        responses.output_from_vertex_row(vertex_row("doc:1", '{"notas_fiscais": [{"numero_nf": "1"}]}'))
    )
    assert ok.extracted == {"notas_fiscais": [{"numero_nf": "1"}]}
    bad = responses.parse_extraction(responses.output_from_vertex_row(vertex_row("doc:1", "[1, 2]")))
    assert bad.extracted is None
    assert bad.error is not None
