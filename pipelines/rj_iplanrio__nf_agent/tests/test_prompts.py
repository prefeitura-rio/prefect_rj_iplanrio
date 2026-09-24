"""Tests for prompt loading and hint injection."""

import os

import pytest

from pipelines.rj_iplanrio__nf_agent.utils import prompts


@pytest.fixture(autouse=True)
def clean_prompt_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("PROMPT_"):
            monkeypatch.delenv(key)


def test_list_versions_sorts_numerically(monkeypatch):
    for version in ("V9", "V10", "V8"):
        monkeypatch.setenv(f"PROMPT_CLASSIFICATION_{version}", "texto")
    assert prompts.list_versions("classification") == ["v8", "v9", "v10"]


def test_load_prompts_defaults_to_latest(monkeypatch):
    monkeypatch.setenv("PROMPT_CLASSIFICATION_V9", "classifica v9")
    monkeypatch.setenv("PROMPT_CLASSIFICATION_V10", "classifica v10")
    monkeypatch.setenv("PROMPT_EXTRACTION_V2", "extrai {classification_hint}")
    loaded = prompts.load_prompts()
    assert loaded.classification_version == "v10"
    assert loaded.classification_text == "classifica v10"
    assert loaded.extraction_version == "v2"


def test_load_prompts_honors_explicit_versions(monkeypatch):
    monkeypatch.setenv("PROMPT_CLASSIFICATION_V1", "c1")
    monkeypatch.setenv("PROMPT_CLASSIFICATION_V2", "c2")
    monkeypatch.setenv("PROMPT_EXTRACTION_V1", "e1")
    loaded = prompts.load_prompts(classification_version="v1", extraction_version="v1")
    assert loaded.classification_text == "c1"


def test_load_prompts_rejects_missing_or_empty(monkeypatch):
    monkeypatch.setenv("PROMPT_CLASSIFICATION_V1", "   ")
    monkeypatch.setenv("PROMPT_EXTRACTION_V1", "e1")
    with pytest.raises(RuntimeError, match="PROMPT_CLASSIFICATION_V1"):
        prompts.load_prompts()
    monkeypatch.setenv("PROMPT_CLASSIFICATION_V1", "c1")
    with pytest.raises(RuntimeError, match="PROMPT_EXTRACTION_V7"):
        prompts.load_prompts(classification_version="v1", extraction_version="v7")


def test_load_prompts_without_any_version_fails(monkeypatch):
    with pytest.raises(RuntimeError, match="classification"):
        prompts.load_prompts()


def test_extraction_prompt_with_hint():
    with_hint = prompts.extraction_prompt_with_hint("A {classification_hint}B", "NFS-e")
    assert "**NFS-e**" in with_hint
    assert with_hint.startswith("A <<<")
    assert prompts.extraction_prompt_with_hint("A {classification_hint}B", None) == "A B"
