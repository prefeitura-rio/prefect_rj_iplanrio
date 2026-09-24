"""Tests for the Bifrost batch client wrapper."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipelines.rj_iplanrio__nf_agent import constants
from pipelines.rj_iplanrio__nf_agent.utils import bifrost


def test_build_client_requires_env(monkeypatch):
    monkeypatch.delenv("BIFROST_API_KEY", raising=False)
    monkeypatch.setenv("BIFROST_BASE_URL", "https://x")
    with pytest.raises(RuntimeError, match="BIFROST_API_KEY"):
        bifrost.build_client()


def test_submit_jsonl_uses_vertex_contract():
    client = MagicMock()
    client.files.create.return_value = SimpleNamespace(id="file-1")
    client.batches.create.return_value = SimpleNamespace(id="batch-1")
    submitted = bifrost.submit_jsonl(client, b'{"a":1}\n', "nf-classification-s1.jsonl", "bkt")
    assert submitted == bifrost.SubmittedBatch(batch_id="batch-1", input_file_id="file-1")
    files_kwargs = client.files.create.call_args.kwargs
    assert files_kwargs["extra_body"] == {
        "provider": "vertex",
        "storage_config": {"gcs": {"bucket": "bkt", "prefix": "bifrost-batch-io"}},
    }
    batch_kwargs = client.batches.create.call_args.kwargs
    assert batch_kwargs["input_file_id"] == "file-1"
    assert batch_kwargs["extra_body"] == {
        "provider": "vertex",
        "model": "gemini-3.1-flash-lite",
        "output_folder": {"url": "gs://bkt/bifrost-batch-io/output"},
    }


def test_submit_jsonl_rejects_oversized_and_empty(monkeypatch):
    monkeypatch.setattr(constants, "BATCH_MAX_BYTES", 5)
    with pytest.raises(ValueError, match="limite"):
        bifrost.submit_jsonl(MagicMock(), b"123456", "f.jsonl", "bkt")
    with pytest.raises(ValueError, match="vazio"):
        bifrost.submit_jsonl(MagicMock(), b"", "f.jsonl", "bkt")


def test_retrieve_batch_passes_provider_as_query():
    client = MagicMock()
    bifrost.retrieve_batch(client, "batch-1")
    client.batches.retrieve.assert_called_once_with("batch-1", extra_query={"provider": "vertex"})


def test_read_batch_output_reads_predictions_file():
    with patch.object(bifrost, "download_text", return_value='{"a": 1}\n\n{"b": 2}\n') as download:
        assert bifrost.read_batch_output("gs://bkt/out/pred-1/") == [{"a": 1}, {"b": 2}]
    download.assert_called_once_with("gs://bkt/out/pred-1/predictions.jsonl")


def test_read_batch_output_rejects_missing_or_non_gcs():
    with pytest.raises(ValueError, match="output_file_id"):
        bifrost.read_batch_output(None)
    with pytest.raises(ValueError, match="gs://"):
        bifrost.read_batch_output("file-abc")
