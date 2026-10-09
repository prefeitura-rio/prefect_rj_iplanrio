"""Tests for runtime settings and credential injection."""

import base64
import os

import pytest

from pipelines.rj_iplanrio__nf_agent.utils import settings

ENV = {
    "BIFROST_GCS_BUCKET": "bifrost-bucket",
    "GCS_BUCKET": "out-bucket",
    "GCS_OUTPUT_BASE_PATH": "staging/extracao_pagina",
    "BQ_EXTRACAO_PAGINA_TABLE": "p.d.extracao_pagina",
    "NF_BATCH_JOBS_TABLE": "p.d.nf_batch_jobs",
}


def test_load_settings_reads_env(monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    loaded = settings.load_settings()
    assert loaded.bifrost_bucket == "bifrost-bucket"
    assert loaded.gcs_bucket == "out-bucket"
    assert loaded.nf_batch_jobs_table == "p.d.nf_batch_jobs"
    assert loaded.pdfs_base_path is None


def test_load_settings_reads_optional_pdfs_base_path(monkeypatch):
    for key, value in ENV.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("PDFS_BASE_PATH", "staging/pdfs")
    assert settings.load_settings().pdfs_base_path == "staging/pdfs"


def test_load_settings_lists_every_missing_var(monkeypatch):
    for key in ENV:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(RuntimeError, match=r"BIFROST_GCS_BUCKET.*NF_BATCH_JOBS_TABLE"):
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
