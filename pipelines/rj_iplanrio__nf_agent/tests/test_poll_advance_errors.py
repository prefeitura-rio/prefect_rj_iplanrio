"""Tests for ``poll_once``'s fail-loud advance-error aggregation.

A completed session whose results can't be consumed (download/parse/
follow-up submit error) must fail the run with an aggregated RuntimeError
— visible via task Failed state + traceback, the channels proven to
render — instead of looping silently every 15 minutes. Other sessions
must still be attempted in the same run (isolation preserved).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipelines.rj_iplanrio__nf_agent.utils.batch import poll as poll_mod
from pipelines.rj_iplanrio__nf_agent.utils.batch.job_tracking import BatchJobEvent


def _active_event(session_id: str, phase: str = "classification") -> BatchJobEvent:
    return BatchJobEvent(
        session_id=session_id,
        phase=phase,
        bifrost_batch_id=f"batch-{session_id}",
        state="validating",
        input_file_id="file-in",
        output_file_id=None,
        row_count=10,
        error=None,
    )


def _completed_batch() -> SimpleNamespace:
    return SimpleNamespace(status="completed", output_file_id="gs://bucket/output", errors=None)


def _config() -> poll_mod.PollConfig:
    return poll_mod.PollConfig(
        nf_batch_jobs_table="proj.ds.nf_batch_jobs",
        gcs_bucket="bucket",
        pdfs_base_path="pdfs",
        gcs_output_base_path="out",
        workers=1,
        requests_per_minute=0,
        max_concurrent=0,
    )


def _client_ok() -> MagicMock:
    client = MagicMock()
    client.batches.retrieve.return_value = _completed_batch()
    return client


class TestAdvanceErrorsAreLoud:
    def test_successful_advance_returns_without_raising(self):
        with (
            patch.object(poll_mod, "get_active_sessions", return_value=[_active_event("s1")]),
            patch.object(poll_mod, "_handle_classification_succeeded") as handle,
        ):
            assert poll_mod.poll_once(_client_ok(), _config()) == []
        handle.assert_called_once()

    def test_failed_advance_raises_aggregated_error(self):
        with (
            patch.object(poll_mod, "get_active_sessions", return_value=[_active_event("s1")]),
            patch.object(poll_mod, "_handle_classification_succeeded", side_effect=RuntimeError("403 download")),
        ):
            with pytest.raises(RuntimeError, match=r"s1.*403 download"):
                poll_mod.poll_once(_client_ok(), _config())

    def test_aggregated_error_includes_full_traceback(self):
        # str(exc) alone once cost a full debug cycle guessing at the wrong
        # call site — the traceback must travel with the message.
        with (
            patch.object(poll_mod, "get_active_sessions", return_value=[_active_event("s1")]),
            patch.object(poll_mod, "_handle_classification_succeeded", side_effect=RuntimeError("boom")),
        ):
            with pytest.raises(RuntimeError) as exc_info:
                poll_mod.poll_once(_client_ok(), _config())
        assert "Traceback (most recent call last)" in str(exc_info.value)
        assert "_handle_classification_succeeded" in str(exc_info.value)

    def test_one_failure_does_not_block_other_sessions(self):
        handled: list[str] = []

        def _handle(client, config, event):  # noqa: ARG001
            handled.append(event.session_id)
            if event.session_id == "bad":
                raise RuntimeError("boom")

        with patch.object(poll_mod, "get_active_sessions", return_value=[_active_event("bad"), _active_event("good")]):
            with (
                patch.object(poll_mod, "_handle_classification_succeeded", side_effect=_handle),
                pytest.raises(RuntimeError, match=r"bad.*boom"),
            ):
                poll_mod.poll_once(_client_ok(), _config())

        assert handled == ["bad", "good"]

    def test_transient_status_check_failure_still_does_not_raise(self):
        # retrieve() blowing up is a network/auth blip, retried next run —
        # must NOT fail the run (unchanged pre-existing behavior).
        client = MagicMock()
        client.batches.retrieve.side_effect = RuntimeError("connection reset")
        with patch.object(poll_mod, "get_active_sessions", return_value=[_active_event("s1")]):
            assert poll_mod.poll_once(client, _config()) == []

    def test_extraction_finish_reads_classification_output_from_vertex(self):
        # Regression: the tracked classification row only ever holds the
        # submit-time output_file_id=NULL; reading it crashed every finish.
        batches = {
            "batch-s1-extraction": SimpleNamespace(status="completed", output_file_id="gs://b/extr", errors=None),
            "batch-s1": SimpleNamespace(status="completed", output_file_id="gs://b/class", errors=None),
        }
        client = MagicMock()
        client.batches.retrieve.side_effect = lambda batch_id, **_: batches[batch_id]
        extraction_event = BatchJobEvent(
            session_id="s1", phase="extraction", bifrost_batch_id="batch-s1-extraction", state="validating"
        )
        with (
            patch.object(poll_mod, "get_active_sessions", return_value=[extraction_event]),
            patch.object(poll_mod, "_find_classification_event", return_value=_active_event("s1")),
            patch.object(poll_mod, "_download_batch_results", return_value=[]) as download,
            patch.object(poll_mod, "_finish_session") as finish,
        ):
            assert poll_mod.poll_once(client, _config()) == ["s1"]
        assert [c.args[1] for c in download.call_args_list] == ["gs://b/class", "gs://b/extr"]
        finish.assert_called_once()

    def test_extraction_finish_without_classification_event_raises(self):
        with (
            patch.object(poll_mod, "get_active_sessions", return_value=[_active_event("s1", phase="extraction")]),
            patch.object(poll_mod, "_find_classification_event", return_value=None),
        ):
            with pytest.raises(RuntimeError, match=r"s1.*no classification batch id"):
                poll_mod.poll_once(_client_ok(), _config())

    def test_download_rejects_missing_output_file_id(self):
        with pytest.raises(ValueError, match="no output_file_id"):
            poll_mod._download_batch_results(MagicMock(), None)

    def test_extraction_advance_failure_also_aggregates(self):
        event = _active_event("s1", phase="extraction")
        with (
            patch.object(poll_mod, "get_active_sessions", return_value=[event]),
            patch.object(poll_mod, "_find_classification_event", return_value=_active_event("s1")),
            patch.object(poll_mod, "_download_batch_results", side_effect=RuntimeError("404 predictions.jsonl")),
        ):
            with pytest.raises(RuntimeError, match=r"s1.*404"):
                poll_mod.poll_once(_client_ok(), _config())
