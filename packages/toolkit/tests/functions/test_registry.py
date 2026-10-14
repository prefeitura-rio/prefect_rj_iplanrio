import sys
from pathlib import Path

import duckdb
import pytest

from toolkit import functions
from toolkit.functions import registry

EXTRA = """\
import polars as pl
from duckdb.sqltypes import VARCHAR
from toolkit.functions.decorators import arrow, native


@native([], VARCHAR)
def found() -> str:
    return "found"


@arrow(VARCHAR, name="whisper")
def lower(column: pl.Expr) -> pl.Expr:
    return column.str.to_lowercase()
"""


@pytest.fixture
def extra(tmp_path: Path, monkeypatch) -> None:
    """Add a module with one function of each kind to the package."""
    (tmp_path / "extra.py").write_text(EXTRA)
    monkeypatch.setattr(functions, "__path__", [str(tmp_path), *functions.__path__])
    monkeypatch.delitem(sys.modules, f"{functions.__name__}.extra", raising=False)


@pytest.mark.usefixtures("extra")
def test_a_new_module_in_the_package_is_found_and_registered():
    """Import every module of the package, so a new one needs no other change."""
    with duckdb.connect() as connection:
        registry.register_functions(connection)
        rows = connection.execute("SELECT found(), whisper('A'), whisper(NULL)")

        assert rows.fetchall() == [("found", "a", None)]


@pytest.mark.usefixtures("extra")
def test_get_functions_puts_each_function_in_the_list_of_its_kind():
    """Keep an Arrow function out of the native list, and the other way round."""
    arrow, native = registry.get_functions()

    assert "whisper" in {function.name for function in arrow}
    assert "found" not in {function.name for function in arrow}
    assert "found" in {function.name for function in native}
    assert "whisper" not in {function.name for function in native}


def test_get_functions_returns_the_functions_that_the_package_declares():
    """Return `(arrow, native)`, with the functions of `arrow.py` and `native.py`."""
    arrow, native = registry.get_functions()

    assert "clean_text" in {function.name for function in arrow}
    assert "normalize" in {function.name for function in native}
