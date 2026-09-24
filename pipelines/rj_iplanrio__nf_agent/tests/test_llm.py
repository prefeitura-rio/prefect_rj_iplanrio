"""Tests for ``utils.llm.build_llm_client`` — the shared Bifrost-routed client.

No network: constructing an ``openai.OpenAI`` client doesn't make any request
by itself, so these just assert how it's configured (api_key/base_url), same
as the client's own constructor validates its arguments without a live call.
"""

from __future__ import annotations

import pytest

from pipelines.rj_iplanrio__nf_agent import constants
from pipelines.rj_iplanrio__nf_agent.utils import llm


def test_raises_when_bifrost_key_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(constants.BIFROST_API_KEY_ENV, raising=False)
    monkeypatch.setenv(constants.BIFROST_BASE_URL_ENV, "https://gateway.example/openai/v1")

    with pytest.raises(RuntimeError, match=constants.BIFROST_API_KEY_ENV):
        llm.build_llm_client()


def test_configures_client_against_bifrost_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(constants.BIFROST_API_KEY_ENV, "bifrost-virtual-key")
    monkeypatch.setenv(constants.BIFROST_BASE_URL_ENV, "https://gateway.example/openai/v1")

    client = llm.build_llm_client()

    assert client.api_key == "bifrost-virtual-key"
    assert str(client.base_url) == "https://gateway.example/openai/v1/"


def test_raises_when_bifrost_base_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(constants.BIFROST_API_KEY_ENV, "bifrost-virtual-key")
    monkeypatch.delenv(constants.BIFROST_BASE_URL_ENV, raising=False)

    with pytest.raises(RuntimeError, match=constants.BIFROST_BASE_URL_ENV):
        llm.build_llm_client()
