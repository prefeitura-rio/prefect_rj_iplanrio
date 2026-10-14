"""Register the fixtures module, so its fixtures reach every test."""

import fixtures
import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register `fixtures.py` under a name of its own, because pytest keeps `fixtures` for its plugin."""
    config.pluginmanager.register(fixtures, "project-fixtures")
