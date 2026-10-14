from pathlib import Path
from sys import modules

from toolkit.executor.sources import backends
from toolkit.executor.sources.base import SourceBase
from toolkit.executor.sources.registry import load_sources


def test_the_provided_sources_are_registered_by_their_names():
    """Register the provided sources by their names."""
    assert {"postgres", "mysql", "mssql", "oracle"} <= set(load_sources())


def test_a_new_module_in_backends_is_found(tmp_path: Path, monkeypatch):
    """Find a new module in `backends/`."""
    (tmp_path / "newone.py").write_text(
        "from toolkit.executor.sources.base import SourceBase\n\n\n"
        "class NewoneSource(SourceBase):\n"
        "    pass\n"
    )
    monkeypatch.setattr(backends, "__path__", [str(tmp_path), *backends.__path__])
    monkeypatch.delitem(modules, f"{backends.__name__}.newone", raising=False)
    try:
        assert "newone" in load_sources()
    finally:
        SourceBase.__registry__.pop("newone", None)
