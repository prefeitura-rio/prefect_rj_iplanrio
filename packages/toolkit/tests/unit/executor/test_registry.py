import sys
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, override

import pytest
from autoregistry.exceptions import InvalidNameError, KeyCollisionError
from psycopg.sql import SQL, Composable

from toolkit.executor.engines.duckdb import Extension
from toolkit.executor.sources import backends
from toolkit.executor.sources.backends.bigquery import BigQuerySource
from toolkit.executor.sources.base import SourceBase
from toolkit.executor.sources.duckdb import DuckDBSource
from toolkit.executor.sources.registry import get_source, load_sources
from toolkit.partitions import Selection

PROVIDED_NAMES = {"postgres", "mysql", "mssql", "oracle", "bigquery"}


def test_the_registry_has_the_provided_sources_and_no_base_class():
    assert set(load_sources()) == PROVIDED_NAMES


@pytest.mark.parametrize("name", sorted(PROVIDED_NAMES))
def test_each_source_is_found_by_name_and_builds_a_source(name, make_context):
    source = get_source(name).from_context(make_context())
    assert source.name == name


@pytest.mark.parametrize("name", ["Postgres", "POSTGRES", "postgres"])
def test_lookup_ignores_case(name):
    assert get_source(name).name == "postgres"


def test_the_intermediate_class_is_not_registered_but_its_children_are():
    assert "duckdb" not in load_sources()
    assert {"postgres", "mysql", "mssql"} <= set(DuckDBSource)


def test_an_unknown_name_raises_key_error():
    with pytest.raises(KeyError):
        get_source("db2")


def test_two_classes_with_the_same_name_are_rejected():
    with pytest.raises(KeyCollisionError):

        class PostgresSource(SourceBase):
            pass


def test_a_class_name_must_end_with_source():
    with pytest.raises(InvalidNameError, match="must end with"):

        class Postgres(SourceBase):
            pass


def test_a_new_module_in_backends_is_found(tmp_path: Path, monkeypatch):
    (tmp_path / "newone.py").write_text(
        "from toolkit.executor.sources.base import SourceBase\n\n\n"
        "class NewoneSource(SourceBase):\n"
        "    @classmethod\n"
        "    def from_context(cls, context):\n"
        "        return cls()\n"
    )
    monkeypatch.setattr(backends, "__path__", [str(tmp_path), *backends.__path__])
    monkeypatch.delitem(sys.modules, f"{backends.__name__}.newone", raising=False)
    try:
        assert "newone" in load_sources()
    finally:
        SourceBase.__registry__.pop("newone", None)


def test_from_context_defaults_to_building_the_source_with_no_arguments(make_context):
    source = BigQuerySource.from_context(make_context(conn=None))
    assert isinstance(source, BigQuerySource)
    assert source.project is None


def test_a_source_with_required_fields_needs_its_own_from_context(make_context):
    @dataclass(frozen=True)
    class NeedsConnSource(SourceBase, skip=True):
        conn: str
        extensions: ClassVar[tuple[Extension, ...]] = ()

        @override
        def window(self, query: str, selection: Selection) -> str:
            return query

        @override
        def setup(self) -> str | None:
            return None

        @override
        def scan(self, query: str) -> Composable:
            return SQL("(SELECT 1)")

    with pytest.raises(TypeError, match="conn"):
        NeedsConnSource.from_context(make_context())
