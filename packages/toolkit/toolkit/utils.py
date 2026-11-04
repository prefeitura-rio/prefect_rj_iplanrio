"""Helpers of the toolkit that no other module owns: the columns of a dump."""

from collections.abc import Mapping, Sequence

from psycopg.sql import SQL, Composable, Identifier
from pydantic import JsonValue, TypeAdapter, ValidationError

from .constants import DELTA_CASTS, FILTER_FORBIDDEN, TEXT_TYPE
from .functions.native import normalize
from .functions.registry import get_functions


def normalize_column(name: str, separator: str = "_", lowercase: bool = True) -> str:
    """Return a column name that BigQuery accepts.

    It is a slug, with a leading `_` if the slug is empty or starts with a digit,
    because BigQuery needs a letter or an underscore first. BigQuery also rejects a
    separator other than `_`.

    ```python
    normalize_column("Código do Órgão")  # "codigo_do_orgao"
    normalize_column("1a")  # "_1a"
    ```
    """
    text = normalize(name, separator, lowercase)
    return f"_{text}" if not text or text[0].isdigit() else text


def build_column_aliases(
    names: Sequence[str], separator: str = "_", lowercase: bool = True
) -> list[dict[str, Identifier]]:
    """Return each column name with its normalized name, as SQL identifiers.

    They are the values of the `normalized_columns` macro of `duckdb/macros.sql`.

    ```python
    build_column_aliases(["Código", "id"])
    # [{"source": Identifier("Código"), "target": Identifier("codigo")},
    #  {"source": Identifier("id"), "target": Identifier("id")}]
    ```
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
    """Return the select list of a dump, as the values of the `select_columns` macro.

    `columns` maps each result column to its DuckDB type, and `select` keeps some of
    them, in order. `normalize_names` makes each name a slug, `normalize_values` cleans
    the text, and `transforms` gives a column the value of an Arrow function, which
    reads text (a time stamp goes through São Paulo first). A type that Delta cannot
    hold (`DELTA_CASTS`) is cast. Raise `ValueError` for an unknown column or function.

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
                if normalize_values and TEXT_TYPE.fullmatch(columns[name])
                else None
            ),
        )
        for name in (columns if select is None else select)
    ]


def select_column(
    source: str, kind: str, target: str, function: str | None
) -> dict[str, Composable]:
    """Return one column of the select list, with `function` if there is one."""
    if function is not None:
        return {
            "source": convert_to_text(source, kind),
            "target": Identifier(target),
            "function": Identifier(function),
        }
    column: dict[str, Composable] = {
        "source": Identifier(source),
        "target": Identifier(target),
    }
    return {**column, "cast": SQL(DELTA_CASTS[kind])} if kind in DELTA_CASTS else column


def convert_to_text(name: str, kind: str) -> Composable:
    """Return a column as text, because the Arrow functions read text."""
    match kind:
        case "VARCHAR":
            return Identifier(name)
        case "TIMESTAMP WITH TIME ZONE":
            return SQL("CAST(to_sao_paulo({}) AS VARCHAR)").format(Identifier(name))
        case _:
            return SQL("CAST({} AS VARCHAR)").format(Identifier(name))


def filter_conditions(filters: Sequence[str]) -> list[str]:
    """Return the filters of a dump as conditions for `WHERE`, each in parentheses.

    A filter is one SQL predicate in the dialect of the source, so `a = 1 OR b = 2`
    stays one condition. Raise `ValueError` for an empty filter, for a `;`, or for a
    comment, so that a filter cannot hold more than one predicate.

    ```python
    filter_conditions(["a = 1", "b > 2 OR c IS NULL"])
    # ["(a = 1)", "(b > 2 OR c IS NULL)"]
    ```
    """
    conditions: list[str] = []

    for item in filters:
        text = item.strip()

        if not text:
            raise ValueError("A filter is empty")
        if any(mark in text for mark in FILTER_FORBIDDEN):
            raise ValueError(
                f"A filter is one predicate, without ';' or a comment: {text!r}"
            )

        conditions.append(f"({text})")

    return conditions


def filter_selectors(filters: Sequence[str]) -> list[dict[str, JsonValue]]:
    """Return the filters of a dump as JSON selectors, for a document store.

    A filter is the text of one selector, a JSON object: a Mango selector for CouchDB
    and a query document for MongoDB. Raise `ValueError` for a filter that is not a JSON
    object.

    ```python
    filter_selectors(['{"situacao": "ativo"}'])  # [{"situacao": "ativo"}]
    ```
    """
    adapter = TypeAdapter(dict[str, JsonValue])

    try:
        return [adapter.validate_json(text) for text in filters]
    except ValidationError as error:
        raise ValueError(f"A filter is not a JSON selector: {error}") from error
