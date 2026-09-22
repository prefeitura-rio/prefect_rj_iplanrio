"""Tests for the batch submit gates: failure gate + global page cap.

Covers ``utils.batch.job_tracking.get_most_recent_event``,
``utils.bigquery.PageStatusReader.count_pages_at_commit``, and
``utils.pipeline.resolve_submit_budget`` — all with a mocked BigQuery
client (no real BQ calls, per this suite's convention).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from pipelines.rj_iplanrio__nf_agent.utils import pipeline as pipeline_mod
from pipelines.rj_iplanrio__nf_agent.utils.batch import job_tracking
from pipelines.rj_iplanrio__nf_agent.utils.batch.job_tracking import STATE_DONE, STATE_FAILED
from pipelines.rj_iplanrio__nf_agent.utils.batch.row_counting import SessionBudget
from pipelines.rj_iplanrio__nf_agent.utils.bigquery import PageStatusReader


def _df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def _mock_bq_client(df: pd.DataFrame) -> MagicMock:
    job = MagicMock()
    job.to_dataframe.return_value = df
    client = MagicMock()
    client.query.return_value = job
    return client


class TestGetMostRecentEvent:
    def test_returns_latest_event_by_created_at(self):
        df = _df(
            [
                {
                    "session_id": "sess-1",
                    "phase": "classification",
                    "bifrost_batch_id": "batch-1",
                    "state": STATE_FAILED,
                    "input_file_id": "file-1",
                    "output_file_id": None,
                    "row_count": 10,
                    "error": "boom",
                }
            ]
        )
        with patch.object(job_tracking.bigquery, "Client", return_value=_mock_bq_client(df)):
            event = job_tracking.get_most_recent_event("proj.ds.nf_batch_jobs")

        assert event is not None
        assert event.session_id == "sess-1"
        assert event.state == STATE_FAILED
        assert event.error == "boom"
        assert event.row_count == 10

    def test_empty_table_returns_none(self):
        df = _df(
            [
                {
                    "session_id": "x",
                    "phase": "classification",
                    "bifrost_batch_id": None,
                    "state": STATE_DONE,
                    "input_file_id": None,
                    "output_file_id": None,
                    "row_count": None,
                    "error": None,
                }
            ]
        ).iloc[0:0]
        with patch.object(job_tracking.bigquery, "Client", return_value=_mock_bq_client(df)):
            assert job_tracking.get_most_recent_event("proj.ds.nf_batch_jobs") is None


class TestCountPagesAtCommit:
    def test_returns_count(self):
        reader = PageStatusReader.__new__(PageStatusReader)
        reader.client = _mock_bq_client(_df([{"total_pages": 251}]))
        assert reader.count_pages_at_commit("proj.ds.extracao_pagina", "abc123") == 251

    def test_empty_result_returns_zero(self):
        reader = PageStatusReader.__new__(PageStatusReader)
        reader.client = _mock_bq_client(_df([{"total_pages": 0}]).iloc[0:0])
        assert reader.count_pages_at_commit("proj.ds.extracao_pagina", "abc123") == 0


def _event(state: str) -> MagicMock:
    event = MagicMock()
    event.state = state
    event.session_id = "sess-1"
    event.error = "boom" if state == STATE_FAILED else None
    return event


class TestResolveSubmitBudget:
    def _resolve(self, latest_state: str | None, processed: int, **kwargs) -> object:
        params = {
            "nf_batch_jobs_table": "proj.ds.nf_batch_jobs",
            "bq_extracao_pagina_table": "proj.ds.extracao_pagina",
            "budget": SessionBudget(max_rows=1000, max_bytes=60_000_000),
            "max_total_pages": None,
            "force_submit": False,
        }
        params.update(kwargs)
        latest = None if latest_state is None else _event(latest_state)
        with (
            patch.object(pipeline_mod, "get_most_recent_event", return_value=latest),
            patch.object(PageStatusReader, "count_pages_at_commit", return_value=processed),
            patch.object(pipeline_mod, "get_git_info", return_value={"commit": "abc123"}),
        ):
            return pipeline_mod.resolve_submit_budget(**params)

    def test_no_history_submits_full_budget(self):
        assert self._resolve(None, 0) == (SessionBudget(max_rows=1000, max_bytes=60_000_000), None)

    def test_latest_failed_blocks_without_force(self):
        assert self._resolve(STATE_FAILED, 0) == (None, None)

    def test_latest_failed_with_force_submits(self):
        assert self._resolve(STATE_FAILED, 0, force_submit=True) == (
            SessionBudget(max_rows=1000, max_bytes=60_000_000),
            None,
        )

    def test_latest_done_submits(self):
        assert self._resolve(STATE_DONE, 0) == (SessionBudget(max_rows=1000, max_bytes=60_000_000), None)

    def test_cap_reached_returns_none(self):
        assert self._resolve(STATE_DONE, 1000, max_total_pages=1000) == (None, None)
        assert self._resolve(STATE_DONE, 1500, max_total_pages=1000) == (None, None)

    def test_cap_shrinks_session_budget_to_remainder(self):
        assert self._resolve(STATE_DONE, 800, max_total_pages=1000) == (
            SessionBudget(max_rows=200, max_bytes=60_000_000),
            200,
        )

    def test_cap_not_yet_binding_keeps_full_budget(self):
        assert self._resolve(STATE_DONE, 0, max_total_pages=1000) == (
            SessionBudget(max_rows=1000, max_bytes=60_000_000),
            1000,
        )
