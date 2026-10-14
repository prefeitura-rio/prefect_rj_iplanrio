"""Helpers of the toolkit that no other module owns: the columns of a dump."""

from collections.abc import Collection, Mapping, Sequence

from psycopg.sql import SQL, Composable, Identifier, Literal

from .constants import FILTER_OPERATORS
from .functions.native import normalize
from .functions.registry import get_functions
from .types import Filter


def normalize_column(name: str, separator: str = "_", lowercase: bool = True) -> str:
    """Return a column name that BigQuery accepts.

    It is a slug, with a leading `_` if the slug is empty or starts with a digit,
    because BigQuery needs a letter or an underscore first. BigQuery also rejects a
    separator other than `_`.
    """
    text = normalize(name, separator, lowercase)
    return f"_{text}" if not text or text[0].isdigit() else text


def column_aliases(
    names: Sequence[str], separator: str = "_", lowercase: bool = True
) -> list[dict[str, Identifier]]:
    """Return each column name with its normalized name, as SQL identifiers.

    They are the values of the `normalized_columns` macro of `duckdb/macros.sql`.
    """
    return [
        {
            "source": Identifier(name),
            "target": Identifier(normalize_column(name, separator, lowercase)),
        }
        for name in names
    ]


def select_columns(
    columns: Mapping[str, str],
    *,
    normalize_names: bool = True,
    normalize_values: bool = False,
    transforms: Mapping[str, str] | None = None,
    select: Sequence[str] | None = None,
) -> list[dict[str, Composable]]:
    """Return the select list of a dump, as SQL fragments.

    `columns` maps each column of the result to its DuckDB type. `normalize_names`
    renames each column to a slug. `normalize_values` applies `clean_text` to the text
    columns. `transforms` maps a column to the Arrow function that gives its value.
    `select` keeps only those columns, in that order. A column is one that the result
    has, by its own name. A function is one of the Arrow functions, and it reads the
    column as text: a date or a timestamp is cast first, in the time zone of São Paulo.
    Anything else raises a `ValueError`.

    They are the values of the `select_columns` macro of `duckdb/macros.sql`.

    ```python
    select_columns({"Nome": "VARCHAR"}, normalize_values=True)
    # [{"source": Identifier("Nome"), "target": Identifier("nome"),
    #   "function": Identifier("clean_text")}]
    ```
    """
    transforms = transforms or {}
    functions = {function.name for function in get_functions()[0]}
    targets = {
        name: normalize_column(name) if normalize_names else name for name in columns
    }

    unknown = {*transforms, *(select or ())} - set(columns)
    if unknown:
        raise ValueError(f"Unknown columns: {sorted(unknown)}")
    wrong = set(transforms.values()) - functions
    if wrong:
        raise ValueError(f"Not Arrow functions: {sorted(wrong)}")

    return [
        select_column(
            name,
            columns[name],
            targets[name],
            transforms.get(name)
            or (
                "clean_text"
                if normalize_values and columns[name] == "VARCHAR"
                else None
            ),
        )
        for name in (columns if select is None else select)
    ]


def select_column(
    source: str, kind: str, target: str, function: str | None
) -> dict[str, Composable]:
    """Return one column of the select list, with `function` if there is one."""
    if function is None:
        return {"source": Identifier(source), "target": Identifier(target)}
    return {
        "source": as_text(source, kind),
        "target": Identifier(target),
        "function": Identifier(function),
    }


def as_text(name: str, kind: str) -> Composable:
    """Return a column as text, because the Arrow functions read text."""
    match kind:
        case "VARCHAR":
            return Identifier(name)
        case "TIMESTAMP WITH TIME ZONE":
            return SQL("CAST(to_sao_paulo({}) AS VARCHAR)").format(Identifier(name))
        case _:
            return SQL("CAST({} AS VARCHAR)").format(Identifier(name))


def filter_predicates(
    filters: Sequence[Filter], columns: Collection[str]
) -> list[dict[str, Composable | str]]:
    """Return the filters of a dump as typed predicates, ready for `WHERE`.

    Every column must be one of `columns` and every operator one of the list. A value
    goes through `Literal`, so it never reaches the SQL as text. Anything else raises a
    `ValueError`.

    ```python
    filter_predicates([{"column": "id", "op": ">=", "value": 10}], {"id"})
    # [{"column": Identifier("id"), "op": ">=", "value": Literal(10)}]
    ```
    """
    unknown = {item["column"] for item in filters} - set(columns)
    if unknown:
        raise ValueError(f"Unknown columns: {sorted(unknown)}")

    return [predicate(item) for item in filters]


def predicate(item: Filter) -> dict[str, Composable | str]:
    """Return one filter as `column`, `op` and, if the operator takes one, `value`."""
    op = item["op"]
    if op not in FILTER_OPERATORS:
        raise ValueError(f"Not a filter operator: {op!r}")

    column = Identifier(item["column"])
    match op, item.get("value"):
        case "is null" | "is not null", None:
            return {"column": column, "op": op}
        case "is null" | "is not null", _:
            raise ValueError(f"The operator {op!r} takes no value")
        case "in" | "not in", [_, *_] as values:
            joined = SQL(", ").join(Literal(value) for value in values)
            return {"column": column, "op": op, "value": SQL("({})").format(joined)}
        case "in" | "not in", _:
            raise ValueError(f"The operator {op!r} needs a list of values")
        case _, list():
            raise ValueError(f"The operator {op!r} needs one value, not a list")
        case _, None:
            raise ValueError(f"The operator {op!r} needs a value")
        case _, value:
            return {"column": column, "op": op, "value": Literal(value)}
