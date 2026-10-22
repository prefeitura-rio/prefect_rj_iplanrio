"""Fixtures of the parity tests: the engines that are up."""

import pytest
from parity_lab import available


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    """Parametrize `engine` over the engines that are up."""
    if "engine" in metafunc.fixturenames:
        metafunc.parametrize("engine", available())
