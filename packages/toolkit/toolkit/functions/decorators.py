"""The decorators that declare a function of the toolkit for DuckDB."""

from collections.abc import Callable

from duckdb.sqltypes import DuckDBPyType
from polars import Expr

from ..types import NativeResult
from .registry import ArrowFunction, NativeFunction


def native[**P, R: NativeResult](
    parameters: list[DuckDBPyType],
    return_type: DuckDBPyType,
    *,
    name: str | None = None,
) -> Callable[[Callable[P, R]], NativeFunction[P, R]]:
    """Declare a Python function that DuckDB calls once for each row.

    The SQL name is `name`, or the name of the function. The result still runs as the
    Python function.

    ```python
    @native([VARCHAR], VARCHAR)
    def shout(value: str) -> str:
        return value.upper()  # SELECT shout('a')  -- 'A'
    ```
    """

    def declare(function: Callable[P, R]) -> NativeFunction[P, R]:
        """Wrap the function with its name and types."""
        return NativeFunction(
            name or function.__name__, function, parameters, return_type
        )

    return declare


def arrow(
    return_type: DuckDBPyType, *, name: str | None = None
) -> Callable[[Callable[[Expr], Expr]], ArrowFunction]:
    """Declare a Polars expression of one text column as a function of whole columns.

    The SQL name is `name`, or the name of the function. The column is `VARCHAR`. The
    result still applies the expression to a column.

    ```python
    @arrow(VARCHAR)
    def shout(column: Expr) -> Expr:
        return column.str.to_uppercase()  # SELECT shout('a')  -- 'A'
    ```
    """

    def declare(expression: Callable[[Expr], Expr]) -> ArrowFunction:
        """Wrap the expression with its name and type."""
        return ArrowFunction(name or expression.__name__, expression, return_type)

    return declare
