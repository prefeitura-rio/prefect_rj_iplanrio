"""Marcador ``oracle_leitura``: testes que precisam de amostras exportadas do Oracle (somente leitura) ficam desligados."""

import os

import pytest


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "oracle_leitura: compara com amostras do Oracle; desligado no CI")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if os.environ.get("PARIDADE_HASH_AMOSTRAS"):
        return
    pular = pytest.mark.skip(reason="defina PARIDADE_HASH_AMOSTRAS com o arquivo de amostras do Oracle (somente leitura)")
    for item in items:
        if "oracle_leitura" in item.keywords:
            item.add_marker(pular)
