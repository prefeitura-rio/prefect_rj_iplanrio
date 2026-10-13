from pathlib import Path

import duckdb
import pytest
from jinja2 import UndefinedError
from psycopg.sql import SQL, Identifier, Literal

from toolkit.executor.engines.duckdb import DuckDBEngine
from toolkit.executor.executor import Executor, Step

SCAN = SQL("(SELECT * FROM (VALUES ('a', 1), ('b', 2), (' ', 3)) AS t(name, n))")


@pytest.fixture
def templates(tmp_path: Path) -> Path:
    (tmp_path / "upper.sql").write_text(
        "SELECT upper(name) AS name, n FROM {{ source }}\n"
    )
    (tmp_path / "drop_blank.sql").write_text(
        "SELECT * FROM {{ source }} WHERE trim(name) <> ''\n"
    )
    (tmp_path / "bigger_than.sql").write_text(
        "SELECT * FROM {{ source }} WHERE {{ column }} > {{ limit }}\n"
    )
    (tmp_path / "needs_variable.sql").write_text(
        "SELECT * FROM {{ source }} WHERE n > {{ missing }}\n"
    )
    return tmp_path


async def rows(engine: DuckDBEngine, scan) -> list[tuple[object, ...]]:
    sql = SQL("SELECT * FROM {} ORDER BY n").format(scan).as_string(None)
    return await engine.query(sql)


async def rows_without_order(engine: DuckDBEngine, scan) -> list[tuple[object, ...]]:
    return await engine.query(SQL("SELECT * FROM {}").format(scan).as_string(None))


@pytest.mark.parametrize(
    ("steps", "expected"),
    [
        pytest.param([], [("a", 1), ("b", 2), (" ", 3)], id="no_steps_keeps_the_scan"),
        pytest.param([Step("upper")], [("A", 1), ("B", 2), (" ", 3)], id="one_step"),
        pytest.param(
            [Step("upper"), Step("drop_blank")],
            [("A", 1), ("B", 2)],
            id="two_steps_run_in_order",
        ),
        pytest.param(
            [
                Step(
                    "bigger_than",
                    {"column": Identifier("n"), "limit": Literal(1)},
                )
            ],
            [("b", 2), (" ", 3)],
            id="values_are_identifiers_and_literals",
        ),
    ],
)
async def test_clean_applies_the_steps(steps, expected, templates, duckdb_memory):
    executor = Executor(duckdb_memory, templates=templates)
    cleaned = await executor.clean(SCAN, steps)
    assert await rows(duckdb_memory, cleaned) == expected


async def test_a_step_with_a_missing_variable_fails(templates, duckdb_memory):
    executor = Executor(duckdb_memory, templates=templates)
    with pytest.raises(UndefinedError):
        await executor.clean(SCAN, [Step("needs_variable")])


async def test_two_clean_calls_do_not_overwrite_each_other(templates, duckdb_memory):
    """Regression guard: a reused view name would make the first result read the second."""
    executor = Executor(duckdb_memory, templates=templates)
    first = await executor.clean(SCAN, [Step("upper")])
    second = await executor.clean(SCAN, [Step("drop_blank")])
    assert await rows(duckdb_memory, first) == [("A", 1), ("B", 2), (" ", 3)]
    assert await rows(duckdb_memory, second) == [("a", 1), ("b", 2)]


async def test_the_clean_step_can_use_a_toolkit_macro(tmp_path, duckdb_memory):
    (tmp_path / "plain.sql").write_text(
        '{% from "duckdb/macros.sql" import text %}\n'
        "SELECT {{ text('name') }} AS name FROM {{ source }}\n"
    )
    scan = SQL("(SELECT 'a' || chr(10) || 'b' AS name)")
    cleaned = await Executor(duckdb_memory, templates=tmp_path).clean(
        scan, [Step("plain")]
    )
    assert await rows_without_order(duckdb_memory, cleaned) == [("a b",)]


@pytest.mark.parametrize(
    "values",
    [
        pytest.param({"key": Identifier("id")}, id="identifier"),
        pytest.param({"key": Literal("a")}, id="literal"),
        pytest.param({"key": SQL("1 + 1")}, id="sql"),
        pytest.param({"key": True}, id="bool"),
        pytest.param({"key": 3}, id="int"),
        pytest.param({"key": [Literal("a"), Literal("b")]}, id="list_of_literals"),
        pytest.param({"key": {"inner": Identifier("x")}}, id="mapping_of_identifiers"),
    ],
)
def test_a_step_accepts_safe_values(values):
    assert Step("x", values).values == values


@pytest.mark.parametrize(
    "values",
    [
        pytest.param({"key": "id"}, id="plain_string"),
        pytest.param({"key": ["a", Literal("b")]}, id="string_in_list"),
        pytest.param({"key": ("a",)}, id="string_in_tuple"),
        pytest.param({"key": {"inner": "a"}}, id="string_in_mapping"),
    ],
)
def test_a_step_rejects_plain_text_and_names_the_key(values):
    with pytest.raises(TypeError, match="'key' must not be a plain string"):
        Step("x", values)


async def test_an_error_in_a_step_names_the_step(templates, duckdb_memory):
    executor = Executor(duckdb_memory, templates=templates)
    with pytest.raises(UndefinedError) as error:
        await executor.clean(SCAN, [Step("upper"), Step("needs_variable")])
    assert error.value.__notes__ == ["cleaning step 2: needs_variable"]


async def test_a_sql_error_in_a_step_names_the_step_without_the_sql(
    templates, duckdb_memory
):
    (templates / "bad_column.sql").write_text(
        "SELECT no_such_column FROM {{ source }}\n"
    )
    executor = Executor(duckdb_memory, templates=templates)
    with pytest.raises(duckdb.Error) as error:
        await executor.clean(SCAN, [Step("bad_column")])
    assert error.value.__notes__ == ["cleaning step 1: bad_column"]
