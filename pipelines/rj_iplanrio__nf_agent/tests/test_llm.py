"""Tests for ``utils.llm.build_gemini_model`` — the shared Bifrost-routed client.

No network: ``google.generativeai`` is replaced with a fake module so we can
assert exactly how ``genai.configure`` / ``GenerativeModel`` are called.
"""

from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pipelines.rj_iplanrio__nf_agent import constants
from pipelines.rj_iplanrio__nf_agent.utils import llm


@pytest.fixture
def fake_genai(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Install a fake ``google.generativeai`` module and return it."""
    fake = MagicMock(name="google.generativeai")
    fake.GenerativeModel.return_value = SimpleNamespace(kind="fake-model")
    monkeypatch.setitem(sys.modules, "google.generativeai", fake)
    return fake


def test_raises_when_bifrost_key_missing(monkeypatch: pytest.MonkeyPatch, fake_genai: MagicMock) -> None:
    monkeypatch.delenv(constants.BIFROST_API_KEY_ENV, raising=False)

    with pytest.raises(RuntimeError, match=constants.BIFROST_API_KEY_ENV):
        llm.build_gemini_model("gemini-3.1-flash-lite")

    fake_genai.configure.assert_not_called()


def test_configures_genai_against_bifrost_endpoint(
    monkeypatch: pytest.MonkeyPatch, fake_genai: MagicMock
) -> None:
    monkeypatch.setenv(constants.BIFROST_API_KEY_ENV, "bifrost-virtual-key")

    model = llm.build_gemini_model("gemini-3.1-flash-lite", {"temperature": 0.1})

    fake_genai.configure.assert_called_once_with(
        api_key="bifrost-virtual-key",
        transport="rest",
        client_options={"api_endpoint": constants.BIFROST_BASE_URL},
    )
    fake_genai.GenerativeModel.assert_called_once_with(
        model_name="gemini-3.1-flash-lite", generation_config={"temperature": 0.1}
    )
    assert model.kind == "fake-model"


def test_base_url_follows_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BIFROST_BASE_URL", "https://gateway.example/genai")

    reloaded = importlib.reload(constants)
    try:
        assert reloaded.BIFROST_BASE_URL == "https://gateway.example/genai"
    finally:
        monkeypatch.delenv("BIFROST_BASE_URL", raising=False)
        importlib.reload(constants)
