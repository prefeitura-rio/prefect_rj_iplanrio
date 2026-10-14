import polars as pl
from duckdb.sqltypes import VARCHAR

from toolkit.functions.decorators import arrow, native


def test_a_declared_function_keeps_its_name_unless_another_is_given():
    """Declare under the function name, unless `name` says another."""

    @native([VARCHAR], VARCHAR)
    def shout(value: str) -> str:
        """Return the text in upper case."""
        return value.upper()

    @native([VARCHAR], VARCHAR, name="yell")
    def other(value: str) -> str:
        """Return the text in upper case."""
        return value.upper()

    @arrow(VARCHAR)
    def whisper(column: pl.Expr) -> pl.Expr:
        """Return the column in lower case."""
        return column.str.to_lowercase()

    assert (shout.name, other.name, whisper.name) == ("shout", "yell", "whisper")


def test_a_declared_function_still_runs_as_the_function_it_wraps():
    """Call a native function as Python, and an Arrow function on an expression."""

    @native([VARCHAR], VARCHAR)
    def shout(value: str) -> str:
        """Return the text in upper case."""
        return value.upper()

    @arrow(VARCHAR)
    def whisper(column: pl.Expr) -> pl.Expr:
        """Return the column in lower case."""
        return column.str.to_lowercase()

    assert shout("a") == "A"
    assert pl.select(whisper(pl.lit("A"))).item() == "a"
