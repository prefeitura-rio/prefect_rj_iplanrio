"""Register the fixtures module, so its fixtures reach every test."""

import fixtures
import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register `fixtures.py` under its own name, because pytest keeps `fixtures`.

    The name is for its plugin.
    """
    config.pluginmanager.register(fixtures, "project-fixtures")
