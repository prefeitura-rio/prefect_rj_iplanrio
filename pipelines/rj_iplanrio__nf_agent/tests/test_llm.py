"""Tests for ``utils.llm.build_gemini_model`` — the shared Bifrost-routed client.

No network: ``google.generativeai`` is replaced with a fake module so we can
assert exactly how ``genai.configure`` / ``GenerativeModel`` are called.
"""

from __future__ import annotations

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
    monkeypatch.setenv(constants.BIFROST_BASE_URL_ENV, "https://gateway.example/genai")

    model = llm.build_gemini_model("gemini-3.1-flash-lite", {"temperature": 0.1})

    fake_genai.configure.assert_called_once_with(
        api_key="bifrost-virtual-key",
        transport="rest",
        client_options={"api_endpoint": "https://gateway.example/genai"},
    )
    fake_genai.GenerativeModel.assert_called_once_with(
        model_name="gemini-3.1-flash-lite", generation_config={"temperature": 0.1}
    )
    assert model.kind == "fake-model"


def test_raises_when_bifrost_base_url_missing(monkeypatch: pytest.MonkeyPatch, fake_genai: MagicMock) -> None:
    monkeypatch.setenv(constants.BIFROST_API_KEY_ENV, "bifrost-virtual-key")
    monkeypatch.delenv(constants.BIFROST_BASE_URL_ENV, raising=False)

    with pytest.raises(RuntimeError, match=constants.BIFROST_BASE_URL_ENV):
        llm.build_gemini_model("gemini-3.1-flash-lite")

    fake_genai.configure.assert_not_called()
