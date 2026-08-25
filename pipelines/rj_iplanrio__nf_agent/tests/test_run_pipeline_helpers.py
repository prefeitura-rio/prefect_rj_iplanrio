"""Tests for the one pure, easily-isolable helper in ``utils/run_poc/run_pipeline.py``.

``nf_processing_flow`` itself (~520 lines / 28 params) and
``pipeline/database.py::process_database`` (~365 lines) are batch-orchestration
entry points that depend on BigQuery, GCS, and a thread pool end-to-end — per
the task brief, PR 1 deliberately does not attempt full mocked end-to-end
tests of either (that's flagged for a dedicated PR; see the final report).
``prepare_output_for_bq`` is the only function in ``run_pipeline.py`` that is
a pure DataFrame transform with no I/O, so it gets full coverage here.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pandas as pd
import pytest

from pipelines.rj_iplanrio__nf_agent.utils.run_poc.run_pipeline import prepare_output_for_bq


class TestPrepareOutputForBq:
    def test_merges_debug_columns_into_debug_info_json(self):
        df = pd.DataFrame(
            {
                "pdf_name": ["a.pdf"],
                "pipeline_classification_detail": ['{"foo": "bar"}'],
                "pipeline_extraction_detail": ['{"baz": 1}'],
                "pipeline_error": [None],
            }
        )
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

        out = prepare_output_for_bq(df, ts)

        assert "pipeline_classification_detail" not in out.columns
        assert "pipeline_extraction_detail" not in out.columns
        assert "pipeline_error" not in out.columns
        assert "debug_info" in out.columns
        debug = json.loads(out.loc[0, "debug_info"])
        assert debug["pipeline_classification_detail"] == {"foo": "bar"}
        assert debug["pipeline_extraction_detail"] == {"baz": 1}
        assert debug["pipeline_error"] is None

    def test_renames_nf_extraida_column(self):
        df = pd.DataFrame({"nf_extraida_pdf_modelo": [True, False]})
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

        out = prepare_output_for_bq(df, ts)

        assert "nf_extraida_pdf_modelo" not in out.columns
        assert list(out["indicador_nf_encontrada_modelo"]) == [True, False]

    def test_rename_drops_preexisting_indicador_column(self):
        df = pd.DataFrame(
            {
                "nf_extraida_pdf_modelo": [True],
                "indicador_nf_encontrada_modelo": ["stale_value"],
            }
        )
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

        out = prepare_output_for_bq(df, ts)

        assert list(out["indicador_nf_encontrada_modelo"]) == [True]

    def test_adds_timestamp_geracao(self):
        df = pd.DataFrame({"pdf_name": ["a.pdf", "b.pdf"]})
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

        out = prepare_output_for_bq(df, ts)

        assert (out["timestamp_geracao"] == ts).all()

    def test_does_not_mutate_input_dataframe(self):
        df = pd.DataFrame({"pdf_name": ["a.pdf"], "pipeline_error": ["oops"]})
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

        prepare_output_for_bq(df, ts)

        assert "pipeline_error" in df.columns
        assert "timestamp_geracao" not in df.columns

    def test_handles_malformed_json_in_debug_column_gracefully(self):
        df = pd.DataFrame({"pdf_name": ["a.pdf"], "pipeline_error": ["not-json{"]})
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

        out = prepare_output_for_bq(df, ts)

        debug = json.loads(out.loc[0, "debug_info"])
        assert debug["pipeline_error"] == "not-json{"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
