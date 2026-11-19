"""Tests for the processing-version fingerprint."""

import pytest

from pipelines.rj_iplanrio__nf_agent import constants
from pipelines.rj_iplanrio__nf_agent.utils.prompts import PromptSet
from pipelines.rj_iplanrio__nf_agent.utils.versioning import compute_processing_version

PROMPTS = PromptSet("v1", "classifica", "v1", "extrai")


def test_auto_version_is_stable_and_prefixed():
    first = compute_processing_version(PROMPTS)
    assert first == compute_processing_version(PROMPTS)
    assert first.startswith("auto-")
    assert len(first) == len("auto-") + 12


def test_auto_version_ignores_version_labels():
    renamed = PromptSet("v7", "classifica", "v3", "extrai")
    assert compute_processing_version(renamed) == compute_processing_version(PROMPTS)


def test_auto_version_changes_with_prompt_text_and_config(monkeypatch):
    base = compute_processing_version(PROMPTS)
    assert compute_processing_version(PromptSet("v1", "classifica!", "v1", "extrai")) != base
    monkeypatch.setitem(constants.GENERATION_CONFIG, "temperature", 0.5)
    assert compute_processing_version(PROMPTS) != base


def test_override_wins_and_is_trimmed():
    assert compute_processing_version(PROMPTS, "  merge-fatura-v2 ") == "merge-fatura-v2"


def test_blank_override_is_rejected():
    with pytest.raises(ValueError, match="versao_processamento"):
        compute_processing_version(PROMPTS, "   ")
