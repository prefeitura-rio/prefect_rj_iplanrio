"""Tests for the direct Vertex AI batch client."""

import base64
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from google.genai import errors as genai_errors
from google.genai import types

from pipelines.rj_iplanrio__nf_agent import constants
from pipelines.rj_iplanrio__nf_agent.utils import vertex

JOB_NAME = "projects/123/locations/global/batchPredictionJobs/456"


def make_client() -> vertex.VertexBatchClient:
    return vertex.VertexBatchClient(MagicMock(), MagicMock(), "bkt")


def job(state: str, **extra) -> SimpleNamespace:
    return SimpleNamespace(
        name=JOB_NAME,
        state=types.JobState[state],
        output_info=extra.get("output_info"),
        completion_stats=extra.get("completion_stats"),
        error=extra.get("error"),
    )


def api_error(code: int, message: str) -> genai_errors.APIError:
    return genai_errors.APIError(code, {"error": {"code": code, "message": message, "status": "X"}})


# ---------- ID dos batches (formato novo e o antigo do gateway)


def test_resource_name_keeps_the_new_format():
    assert vertex.resource_name(JOB_NAME) == JOB_NAME


def test_resource_name_decodes_the_old_gateway_id():
    old = base64.urlsafe_b64encode(JOB_NAME.encode()).decode().rstrip("=")
    assert vertex.resource_name(old) == JOB_NAME


@pytest.mark.parametrize("bad", ["abc", base64.urlsafe_b64encode(b"outra-coisa").decode()])
def test_resource_name_rejects_unknown_formats(bad):
    with pytest.raises(ValueError, match="formato desconhecido"):
        vertex.resource_name(bad)


# ---------- credenciais


def test_load_credentials_requires_env(monkeypatch):
    monkeypatch.delenv(vertex.CREDENTIALS_ENV, raising=False)
    with pytest.raises(RuntimeError, match=vertex.CREDENTIALS_ENV):
        vertex.load_credentials()


def test_load_credentials_rejects_invalid_base64_without_leaking_the_value(monkeypatch):
    monkeypatch.setenv(vertex.CREDENTIALS_ENV, "segredo não é base64!")
    with pytest.raises(ValueError, match="base64 válido") as exc_info:
        vertex.load_credentials()
    assert "segredo" not in str(exc_info.value)


def test_load_credentials_uses_project_override_then_the_account_project(monkeypatch):
    info = {"type": "service_account", "project_id": "conta-proj"}
    monkeypatch.setenv(vertex.CREDENTIALS_ENV, base64.b64encode(json.dumps(info).encode()).decode())
    monkeypatch.delenv(vertex.PROJECT_ENV, raising=False)
    with patch.object(vertex.service_account.Credentials, "from_service_account_info", return_value="cred") as build:
        assert vertex.load_credentials() == ("cred", "conta-proj")
        monkeypatch.setenv(vertex.PROJECT_ENV, "outro-proj")
        assert vertex.load_credentials() == ("cred", "outro-proj")
    assert build.call_args.kwargs["scopes"] == list(vertex.SCOPES)


# ---------- submissão


def test_submit_creates_job_with_stable_name_and_uploads_input():
    client = make_client()
    client.genai.batches.list.return_value = []
    client.genai.batches.create.return_value = SimpleNamespace(name=JOB_NAME)
    submitted = client.submit_jsonl(b'{"a":1}\n', "nf-classification-s1")
    assert submitted == vertex.SubmittedBatch(JOB_NAME, "gs://bkt/vertex-batch-io/input/nf-classification-s1.jsonl")
    client.storage.bucket.assert_called_once_with("bkt")
    client.storage.bucket.return_value.blob.assert_called_once_with("vertex-batch-io/input/nf-classification-s1.jsonl")
    kwargs = client.genai.batches.create.call_args.kwargs
    assert kwargs["model"] == constants.MODEL_NAME
    assert kwargs["src"] == "gs://bkt/vertex-batch-io/input/nf-classification-s1.jsonl"
    assert kwargs["config"].display_name == "nf-classification-s1"
    assert kwargs["config"].dest == "gs://bkt/vertex-batch-io/output/nf-classification-s1/"


def test_submit_reuses_an_existing_job_instead_of_creating_another():
    client = make_client()
    client.genai.batches.list.return_value = [SimpleNamespace(name=JOB_NAME)]
    submitted = client.submit_jsonl(b"x\n", "nf-classification-s1")
    assert submitted.batch_id == JOB_NAME
    client.genai.batches.create.assert_not_called()
    client.storage.bucket.assert_not_called()


def test_submit_rejects_empty_and_oversized(monkeypatch):
    client = make_client()
    monkeypatch.setattr(constants, "BATCH_MAX_BYTES", 5)
    with pytest.raises(ValueError, match="limite"):
        client.submit_jsonl(b"123456", "n")
    with pytest.raises(ValueError, match="vazio"):
        client.submit_jsonl(b"", "n")


def test_submit_turns_spend_cap_into_a_fatal_error():
    client = make_client()
    client.genai.batches.list.return_value = []
    client.genai.batches.create.side_effect = api_error(403, "Spend cap breached for project: projects/1")
    with pytest.raises(vertex.SpendCapReachedError, match="Spend cap"):
        client.submit_jsonl(b"x\n", "n")


def test_submit_turns_other_403_into_access_denied():
    client = make_client()
    client.genai.batches.list.side_effect = api_error(403, "Permission denied on resource")
    with pytest.raises(vertex.BatchAccessDeniedError):
        client.submit_jsonl(b"x\n", "n")


def test_submit_does_not_hide_transient_errors():
    client = make_client()
    client.genai.batches.list.return_value = []
    client.genai.batches.create.side_effect = api_error(503, "unavailable")
    with pytest.raises(genai_errors.APIError):
        client.submit_jsonl(b"x\n", "n")


# ---------- consulta


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("JOB_STATE_QUEUED", vertex.BatchState.IN_PROGRESS),
        ("JOB_STATE_PENDING", vertex.BatchState.IN_PROGRESS),
        ("JOB_STATE_RUNNING", vertex.BatchState.IN_PROGRESS),
        ("JOB_STATE_PAUSED", vertex.BatchState.IN_PROGRESS),
        ("JOB_STATE_CANCELLING", vertex.BatchState.IN_PROGRESS),
        ("JOB_STATE_UNSPECIFIED", vertex.BatchState.IN_PROGRESS),
        ("JOB_STATE_SUCCEEDED", vertex.BatchState.SUCCEEDED),
        ("JOB_STATE_PARTIALLY_SUCCEEDED", vertex.BatchState.SUCCEEDED),
        ("JOB_STATE_FAILED", vertex.BatchState.FAILED),
        ("JOB_STATE_CANCELLED", vertex.BatchState.FAILED),
        ("JOB_STATE_EXPIRED", vertex.BatchState.FAILED),
    ],
)
def test_retrieve_maps_vertex_states(raw, expected):
    client = make_client()
    client.genai.batches.get.return_value = job(raw)
    assert client.retrieve(JOB_NAME).state is expected


def test_retrieve_returns_output_dir_counts_and_error():
    client = make_client()
    client.genai.batches.get.return_value = job(
        "JOB_STATE_PARTIALLY_SUCCEEDED",
        output_info=types.BatchJobOutputInfo(gcs_output_directory="gs://bkt/out/prediction-1"),
        completion_stats=types.CompletionStats(successful_count=9, failed_count=1),
        error=types.JobError(message="algumas linhas falharam"),
    )
    status = client.retrieve(JOB_NAME)
    assert (status.output_dir, status.successful, status.failed) == ("gs://bkt/out/prediction-1", 9, 1)
    assert status.error == "algumas linhas falharam"


def test_retrieve_accepts_the_old_gateway_id():
    client = make_client()
    client.genai.batches.get.return_value = job("JOB_STATE_RUNNING")
    old = base64.urlsafe_b64encode(JOB_NAME.encode()).decode().rstrip("=")
    client.retrieve(old)
    client.genai.batches.get.assert_called_once_with(name=JOB_NAME)


def test_retrieve_turns_inactive_or_denied_access_into_a_fatal_error():
    client = make_client()
    client.genai.batches.get.side_effect = api_error(403, "caller does not have permission")
    with pytest.raises(vertex.FatalBatchError):
        client.retrieve(JOB_NAME)


# ---------- saída


def fake_blob(name: str, text: str) -> MagicMock:
    blob = MagicMock()
    blob.name = name
    blob.open.return_value.__enter__.return_value = text.splitlines(keepends=True)
    return blob


def test_iter_output_streams_every_predictions_file_in_order():
    client = make_client()
    client.storage.list_blobs.return_value = [
        fake_blob("out/pred/predictions-2.jsonl", '{"n": 3}\n'),
        fake_blob("out/pred/incomplete.jsonl", '{"n": 99}\n'),
        fake_blob("out/pred/predictions-1.jsonl", '{"n": 1}\n\n{"n": 2}\n'),
    ]
    rows = client.iter_output("gs://bkt/out/pred")
    assert [row["n"] for row in rows] == [1, 2, 3]
    client.storage.list_blobs.assert_called_once_with("bkt", prefix="out/pred/")


def test_iter_output_fails_without_a_predictions_file():
    client = make_client()
    client.storage.list_blobs.return_value = [fake_blob("out/pred/_SUCCESS", "")]
    with pytest.raises(FileNotFoundError):
        list(client.iter_output("gs://bkt/out/pred"))


def test_iter_output_rejects_missing_output_dir():
    with pytest.raises(ValueError, match="pasta de saída"):
        list(make_client().iter_output(None))
