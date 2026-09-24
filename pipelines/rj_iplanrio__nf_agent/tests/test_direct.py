"""Tests for the direct (non-batch) transport used by the local CLI."""

from types import SimpleNamespace
from unittest.mock import MagicMock

from pipelines.rj_iplanrio__nf_agent.utils import direct
from pipelines.rj_iplanrio__nf_agent.utils.llm_requests import PageId
from pipelines.rj_iplanrio__nf_agent.utils.prompts import PromptSet


def chat_response(text: str) -> SimpleNamespace:
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=text))],
        usage=SimpleNamespace(prompt_tokens=3, completion_tokens=2, total_tokens=5),
    )


def test_call_direct_request_shape():
    client = MagicMock()
    client.chat.completions.create.return_value = chat_response('{"categoria": "NF-e"}')
    output = direct.call_direct(client, "prompt", "QUJD", PageId("doc", 1))
    kwargs = client.chat.completions.create.call_args.kwargs
    assert kwargs["model"] == "vertex/gemini-3.1-flash-lite"
    assert kwargs["response_format"] == {"type": "json_object"}
    file_part = kwargs["messages"][0]["content"][1]
    assert file_part["file"]["file_data"] == "data:application/pdf;base64,QUJD"
    assert output.text == '{"categoria": "NF-e"}'
    assert output.usage == {"prompt_tokens": 3, "completion_tokens": 2, "total_tokens": 5}


def test_call_direct_turns_exceptions_into_errors():
    client = MagicMock()
    client.chat.completions.create.side_effect = RuntimeError("503")
    output = direct.call_direct(client, "prompt", "QUJD", PageId("doc", 1))
    assert output.text is None
    assert "503" in output.error


def test_process_pdf_direct_extracts_only_nf_pages(pdf_bytes):
    client = MagicMock()

    def respond(**kwargs):
        prompt = kwargs["messages"][0]["content"][0]["text"]
        if prompt == "classifica":
            page = kwargs["messages"][0]["content"][1]["file"]["filename"]
            return chat_response('{"categoria": "NFS-e"}' if page.endswith("_p2.pdf") else '{"categoria": "Nenhuma das Opções"}')
        return chat_response('{"notas_fiscais": [{"numero_nf": "9"}]}')

    client.chat.completions.create.side_effect = respond
    prompts = PromptSet("v1", "classifica", "v1", "extrai {classification_hint}")
    result = direct.process_pdf_direct(client, "doc", pdf_bytes(3), prompts, max_workers=2)
    assert result.total_pages == 3
    assert [item.category for item in result.classifications] == ["Nenhuma das Opções", "NFS-e", "Nenhuma das Opções"]
    assert [item.page.page_number for item in result.extractions] == [2]
    assert result.extractions[0].extracted == {"notas_fiscais": [{"numero_nf": "9"}]}
