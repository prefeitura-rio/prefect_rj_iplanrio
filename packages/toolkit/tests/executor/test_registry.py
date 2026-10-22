from pathlib import Path
from sys import modules

import pytest

from toolkit.executor.sources import backends
from toolkit.executor.sources.base import Registered
from toolkit.executor.sources.registry import get_source, load_sources


def test_the_provided_sources_are_registered_by_their_names():
    """Register the provided sources by their names."""
    assert {"postgres", "mysql", "mssql", "oracle"} <= set(load_sources())


def test_a_new_module_in_backends_is_found(tmp_path: Path, monkeypatch):
    """Find a new module in `backends/`."""
    (tmp_path / "newone.py").write_text(
        "from typing import ClassVar\n\n"
        "from pydantic.dataclasses import dataclass\n\n"
        "from toolkit.executor.sources.base import Registered\n\n\n"
        "@dataclass(frozen=True)\n"
        "class NewoneSource(Registered):\n"
        '    name: ClassVar[str] = "newone"\n'
    )
    monkeypatch.setattr(backends, "__path__", [str(tmp_path), *backends.__path__])
    monkeypatch.delitem(modules, f"{backends.__name__}.newone", raising=False)
    try:
        assert "newone" in load_sources()
    finally:
        Registered.sources.pop("newone", None)


def test_a_backend_whose_extra_is_missing_is_left_out(
    monkeypatch, fail_to_import_backend
):
    """List the other sources, and not the one whose extra is not installed."""
    fail_to_import_backend(monkeypatch, "mongodb", "pymongoarrow")

    sources = load_sources()

    assert "mongodb" not in set(sources)
    assert {"postgres", "couchdb", "oracle"} <= set(sources)


def test_a_missing_extra_gives_the_command_that_installs_it(
    monkeypatch, fail_to_import_backend
):
    """Name the extra in the error, instead of a bare `KeyError`."""
    fail_to_import_backend(monkeypatch, "mongodb", "pymongo")

    with pytest.raises(ValueError, match=r"install toolkit\[mongodb\]"):
        get_source("mongodb")


def test_an_import_error_of_another_module_is_not_forgiven(
    monkeypatch, fail_to_import_backend
):
    """Raise a missing module that does not belong to the extra of the backend."""
    fail_to_import_backend(monkeypatch, "mongodb", "not_the_extra")

    with pytest.raises(ModuleNotFoundError, match="not_the_extra"):
        load_sources()


def test_an_unknown_source_is_still_a_key_error():
    """Keep `KeyError` for a name that no backend has."""
    with pytest.raises(KeyError):
        get_source("nothing")
