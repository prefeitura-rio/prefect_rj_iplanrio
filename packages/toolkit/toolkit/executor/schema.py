"""Schema classes and the check of a Polars frame against one.

A schema class is a ``TypedDict``, a dataclass, a ``NamedTuple`` or any class with
annotated fields. Each field is a column, and ``X | None`` lets it hold nulls::

    class Order(TypedDict):
        id: int
        note: str | None
"""

from dataclasses import dataclass
from inspect import isclass
from types import NoneType, UnionType
from typing import ClassVar, Literal, cast, get_args, get_origin, get_type_hints

import polars as pl

from .constants import SCALAR_TYPES


class SchemaError(ValueError):
    """The result of a query does not fit the schema class."""


@dataclass(frozen=True, slots=True)
class Column:
    """What a schema class expects of one column."""

    dtype: pl.DataType | type[pl.DataType]
    nullable: bool


def schema_of(schema: type) -> dict[str, Column]:
    """Return the expected columns of a schema class, in declaration order.

    :raises TypeError: If an annotation has no Polars type. The message names the field.
    """
    hints = cast("dict[str, object]", get_type_hints(schema))
    return {
        name: column_of(name, annotation)
        for name, annotation in hints.items()
        if get_origin(annotation) is not ClassVar
    }


def column_of(name: str, annotation: object) -> Column:
    """Return the column that one annotated field describes."""
    try:
        annotation, nullable = split_nullable(annotation)
        return Column(dtype_of(annotation), nullable)
    except TypeError as error:
        raise TypeError(f"{name}: {error}") from error


def split_nullable(annotation: object) -> tuple[object, bool]:
    """Return the annotation without ``| None``, and whether it had ``| None``."""
    if get_origin(annotation) is not UnionType:
        return annotation, False

    members = cast("tuple[object, ...]", get_args(annotation))
    others = [member for member in members if member is not NoneType]

    if len(others) != 1 or len(members) != 2:
        raise TypeError(f"only `X | None` unions are supported: {annotation!r}")

    return others[0], True


def dtype_of(annotation: object) -> pl.DataType | type[pl.DataType]:
    """Return the Polars type of an annotation without ``| None``."""
    if get_origin(annotation) is list:
        (item,) = cast("tuple[object]", get_args(annotation))
        return pl.List(dtype_of(item))

    if get_origin(annotation) is Literal:
        return pl.String

    if isclass(annotation):
        if annotation in SCALAR_TYPES:
            return SCALAR_TYPES[annotation]

        if get_type_hints(annotation):
            fields = schema_of(annotation)
            return pl.Struct({name: column.dtype for name, column in fields.items()})

    raise TypeError(f"unsupported annotation: {annotation!r}")


def validate_frame(frame: pl.DataFrame, columns: dict[str, Column]) -> pl.DataFrame:
    """Check a frame against the columns and return it with the schema's order.

    A frame with no rows and no columns is an empty result from a client that cannot
    tell the columns of an empty result: it becomes an empty frame with the schema.

    :raises SchemaError: With every problem found, naming each column.
    """
    if frame.shape == (0, 0):
        return pl.DataFrame(schema={name: c.dtype for name, c in columns.items()})

    problems = name_problems(frame, columns)
    fitted: list[pl.Series] = []

    for name in columns:
        if name not in frame.columns:
            continue

        try:
            fitted.append(fit(frame[name], columns[name]))
        except SchemaError as error:
            problems.append(str(error))

    if problems:
        raise SchemaError("; ".join(problems))

    return pl.DataFrame(fitted)


def name_problems(frame: pl.DataFrame, columns: dict[str, Column]) -> list[str]:
    """Return one message for the missing columns and one for the unexpected ones."""
    missing = [name for name in columns if name not in frame.columns]
    unexpected = [name for name in frame.columns if name not in columns]

    return [
        f"{label} columns: {', '.join(names)}"
        for label, names in [("missing", missing), ("unexpected", unexpected)]
        if names
    ]


def fit(series: pl.Series, column: Column) -> pl.Series:
    """Return the series with the type that the column expects.

    :raises SchemaError: If the type, the nulls or a value do not fit.
    """
    if series.dtype == pl.Null:
        series = series.cast(column.dtype)

    dtype = widen(series.dtype)

    if not same_type(dtype, column.dtype):
        raise SchemaError(
            f"{series.name}: {label(series.dtype)} is not {label(column.dtype)}"
        )

    if series.null_count() and not column.nullable:
        raise SchemaError(
            f"{series.name}: {series.null_count()} null values in a column that is not nullable"
        )

    try:
        return series.cast(dtype, strict=True)
    except pl.exceptions.PolarsError as error:
        raise SchemaError(f"{series.name}: {error}") from error


def widen(dtype: pl.DataType) -> pl.DataType:
    """Return the type with every integer as ``Int64`` and every float as ``Float64``.

    This holds inside lists and structs too. A decimal or a timestamp keeps its own
    precision, and a string is never read as a number.
    """
    if dtype.is_integer():
        return pl.Int64()

    if dtype.is_float():
        return pl.Float64()

    match dtype:
        case pl.List():
            return pl.List(widen(cast("pl.DataType", dtype.inner)))
        case pl.Struct():
            return pl.Struct(
                {
                    field.name: widen(cast("pl.DataType", field.dtype))
                    for field in dtype.fields
                }
            )
        case _:
            return dtype


def same_type(actual: pl.DataType, expected: pl.DataType | type[pl.DataType]) -> bool:
    """Return whether two types are equal. Extension types compare by name only.

    An extension type carries its metadata, such as the CRS of a geometry, and that
    does not make it another type.
    """
    match actual, expected:
        case pl.Extension(), pl.Extension():
            return actual.ext_name() == expected.ext_name()
        case _:
            return actual == expected


def label(dtype: pl.DataType | type[pl.DataType]) -> str:
    """Return the type as text for an error message, without extension metadata."""
    match dtype:
        case pl.Extension():
            return f"Extension({dtype.ext_name()})"
        case _:
            return str(dtype)
