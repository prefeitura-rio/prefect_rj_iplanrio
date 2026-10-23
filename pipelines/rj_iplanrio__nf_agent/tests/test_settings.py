"""Tests for runtime settings and credential injection."""

import base64
import os

import pytest

from pipelines.rj_iplanrio__nf_agent import constants
from pipelines.rj_iplanrio__nf_agent.utils import settings

ENV = {
    "VERTEX_BATCH_BUCKET": "batch-bucket",
    "GCS_BUCKET": "out-bucket",
    "GCS_OUTPUT_BASE_PATH": "staging/extracao_pagina",
    "BQ_EXTRACAO_PAGINA_TABLE": "p.d.extracao_pagina",
    "NF_BATCH_JOBS_TABLE": "p.d.nf_batch_jobs",
}


def test_load_settings_reads_env(monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    loaded = settings.load_settings()
    assert loaded.batch_bucket == "batch-bucket"
    assert loaded.gcs_bucket == "out-bucket"
    assert loaded.nf_batch_jobs_table == "p.d.nf_batch_jobs"
    assert loaded.pdfs_view_table == constants.PDFS_VIEW_TABLE


def test_load_settings_accepts_the_old_bifrost_bucket_variable(monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("VERTEX_BATCH_BUCKET")
    monkeypatch.setenv("BIFROST_GCS_BUCKET", "old-bucket")
    assert settings.load_settings().batch_bucket == "old-bucket"


def test_load_settings_prefers_the_new_bucket_variable(monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("BIFROST_GCS_BUCKET", "old-bucket")
    assert settings.load_settings().batch_bucket == "batch-bucket"


def test_load_settings_reads_optional_pdfs_view_table(monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("PDFS_VIEW_TABLE", "p.d.minha_view")
    assert settings.load_settings().pdfs_view_table == "p.d.minha_view"


def test_load_settings_lists_every_missing_var(monkeypatch):
    for key in (*ENV, "BIFROST_GCS_BUCKET"):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(
        RuntimeError, match=r"BQ_EXTRACAO_PAGINA_TABLE.*NF_BATCH_JOBS_TABLE.*VERTEX_BATCH_BUCKET ou BIFROST_GCS_BUCKET"
    ):
        settings.load_settings()


def test_inject_gcp_credentials_writes_file(monkeypatch, tmp_path):
    target = tmp_path / "creds.json"
    monkeypatch.setattr(settings, "CREDENTIALS_PATH", target)
    monkeypatch.setenv("RJ_NF_AGENT_CREDENTIALS", base64.b64encode(b'{"type": "sa"}').decode())
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    settings.inject_gcp_credentials()
    assert target.read_bytes() == b'{"type": "sa"}'
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == str(target)


def test_inject_gcp_credentials_without_env_keeps_adc(monkeypatch):
    monkeypatch.delenv("RJ_NF_AGENT_CREDENTIALS", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    settings.inject_gcp_credentials()
    assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ


def test_inject_gcp_credentials_rejects_invalid_base64(monkeypatch):
    monkeypatch.setenv("RJ_NF_AGENT_CREDENTIALS", "não é base64!")
    with pytest.raises(ValueError, match="RJ_NF_AGENT_CREDENTIALS"):
        settings.inject_gcp_credentials()


def test_max_active_sessions_defaults_and_can_be_overridden(monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("MAX_ACTIVE_SESSIONS", raising=False)
    assert settings.load_settings().max_active_sessions == constants.DEFAULT_MAX_ACTIVE_SESSIONS
    monkeypatch.setenv("MAX_ACTIVE_SESSIONS", "7")
    assert settings.load_settings().max_active_sessions == 7  # noqa: PLR2004 -- valor definido no próprio teste
