import sys
from pathlib import Path

from toolkit.executor.sources import backends
from toolkit.executor.sources.base import SourceBase
from toolkit.executor.sources.registry import load_sources


def test_every_provided_source_is_registered_by_its_name():
    assert set(load_sources()) == {
        "postgres",
        "mysql",
        "mssql",
        "oracle",
        "bigquery",
        "gsheets",
        "arcgis",
    }


def test_a_new_module_in_backends_is_found(tmp_path: Path, monkeypatch):
    (tmp_path / "newone.py").write_text(
        "from toolkit.executor.sources.base import SourceBase\n\n\n"
        "class NewoneSource(SourceBase):\n"
        "    pass\n"
    )
    monkeypatch.setattr(backends, "__path__", [str(tmp_path), *backends.__path__])
    monkeypatch.delitem(sys.modules, f"{backends.__name__}.newone", raising=False)
    try:
        assert "newone" in load_sources()
    finally:
        SourceBase.__registry__.pop("newone", None)
