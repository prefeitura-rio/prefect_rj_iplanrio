"""Register the functions of the toolkit on a DuckDB connection.

A function declares itself where it is defined, with the decorators of `decorators.py`,
and `get_functions` finds every one in the modules of this package. A new module with
decorated functions needs no other change.
"""

from collections.abc import Callable
from dataclasses import dataclass
from importlib import import_module
from itertools import chain
from pkgutil import iter_modules
from typing import cast

from duckdb import DuckDBPyConnection
from duckdb.func import ARROW, SPECIAL
from duckdb.sqltypes import VARCHAR, DuckDBPyType
from polars import Expr, Series, lit, select

from ..types import ArrowColumn, NativeResult, TextColumn


@dataclass(frozen=True, slots=True)
class NativeFunction[**P, R: NativeResult]:
    """A function that DuckDB calls once for each row.

    Calling it runs the Python function.
    """

    name: str
    function: Callable[P, R]
    parameters: list[DuckDBPyType]
    return_type: DuckDBPyType

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R:
        """Run the Python function."""
        return self.function(*args, **kwargs)

    def register(self, conn: DuckDBPyConnection) -> None:
        """Register the function on a connection. A NULL argument gives NULL."""
        conn.create_function(
            self.name, self.function, [*self.parameters], self.return_type
        )


@dataclass(frozen=True, slots=True)
class ArrowFunction:
    """A function that DuckDB calls once for each batch of rows.

    Calling it applies the Polars expression to a column.
    """

    name: str
    expression: Callable[[Expr], Expr]
    return_type: DuckDBPyType

    def __call__(self, column: Expr) -> Expr:
        """Apply the expression to a column."""
        return self.expression(column)

    def evaluate_batch(self, values: TextColumn) -> ArrowColumn:
        """Evaluate the expression on one batch of the column, for DuckDB."""
        series = select(self.expression(lit(Series(values)))).to_series()
        return cast("ArrowColumn", series.to_arrow())

    def register(self, conn: DuckDBPyConnection) -> None:
        """Register the function, with the NULL handling in `vectorized`.

        A text that the expression cannot read gives NULL, so DuckDB must not expect a
        value for every row.
        """
        conn.create_function(
            self.name,
            self.evaluate_batch,
            [VARCHAR],
            self.return_type,
            type=ARROW,
            null_handling=SPECIAL,
        )


def get_functions() -> tuple[
    list[ArrowFunction], list[NativeFunction[..., NativeResult]]
]:
    """Return the Arrow functions and the native functions of this package.

    It imports every module of the package and collects what the decorators made.

    ```python
    arrow, native = get_functions()
    ```
    """
    package = __name__.rpartition(".")[0]
    arrow: list[ArrowFunction] = []
    native: list[NativeFunction[..., NativeResult]] = []

    for info in iter_modules(import_module(package).__path__):
        members: list[object] = list(
            vars(import_module(f"{package}.{info.name}")).values()
        )

        for member in members:
            match member:
                case ArrowFunction():
                    arrow.append(member)
                case NativeFunction():
                    native.append(member)
                case _:
                    pass

    return arrow, native


def register_functions(conn: DuckDBPyConnection) -> None:
    """Register every function that `get_functions` returns on a connection."""
    for function in chain(*get_functions()):
        function.register(conn)
