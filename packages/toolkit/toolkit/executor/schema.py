"""Schema classes, and the check of a Polars dataframe against one.

A schema class is a `TypedDict`, a dataclass, a `NamedTuple` or any class with annotated
fields. `X | None` lets a column hold nulls.

```python
class Order(TypedDict):
    id: int
    note: str | None
```
"""

from dataclasses import dataclass
from inspect import isclass
from types import NoneType, UnionType
from typing import ClassVar, Literal, cast, get_args, get_origin, get_type_hints

from polars import (
    DataFrame,
    DataType,
    Extension,
    Float64,
    Int64,
    List,
    Null,
    Series,
    String,
    Struct,
    exceptions,
)

from .constants import SCALAR_TYPES


class SchemaError(ValueError):
    """The result of a query does not fit the schema class."""


@dataclass(frozen=True, slots=True)
class Column:
    """What a schema class expects of one column."""

    dtype: DataType | type[DataType]
    nullable: bool


def build_columns(schema: type) -> dict[str, Column]:
    """Return the expected columns of a schema class, in declaration order.

    Raise `TypeError` if an annotation has no Polars type.

    ```python
    class Order(TypedDict):
        id: int
        note: str | None

    build_columns(Order)
    # {"id": Column(dtype=Int64, nullable=False),
    #  "note": Column(dtype=String, nullable=True)}
    ```
    """
    hints = cast("dict[str, object]", get_type_hints(schema))
    return {
        name: build_column(name, annotation)
        for name, annotation in hints.items()
        if get_origin(annotation) is not ClassVar
    }


def read_columns(dataframe: DataFrame) -> dict[str, Column]:
    """Return the columns of a dataframe: all nullable, integers and floats widened.

    It is the schema of a read whose columns are known only at run time. The first
    dataframe of a read gives it, and the next dataframes are checked against it.

    ```python
    read_columns(DataFrame({"id": [1], "nome": ["a"]}))
    # {"id": Column(dtype=Int64, nullable=True),
    #  "nome": Column(dtype=String, nullable=True)}
    ```
    """
    return {
        name: Column(widen(dtype), nullable=True)
        for name, dtype in dataframe.schema.items()
    }


def build_column(name: str, annotation: object) -> Column:
    """Return the column that one annotated field describes."""
    try:
        annotation, nullable = split_nullable(annotation)
        return Column(resolve_dtype(annotation), nullable)
    except TypeError as error:
        raise TypeError(f"{name}: {error}") from error


def split_nullable(annotation: object) -> tuple[object, bool]:
    """Return the annotation without `| None`, and whether it had `| None`.

    ```python
    split_nullable(str | None)  # (str, True)
    split_nullable(int)  # (int, False)
    ```
    """
    if get_origin(annotation) is not UnionType:
        return annotation, False

    members = cast("tuple[object, ...]", get_args(annotation))
    others = [member for member in members if member is not NoneType]

    if len(others) != 1 or len(members) != 2:
        raise TypeError(f"only `X | None` unions are supported: {annotation!r}")

    return others[0], True


def resolve_dtype(annotation: object) -> DataType | type[DataType]:
    """Return the Polars type of an annotation without `| None`."""
    if get_origin(annotation) is list:
        (item,) = cast("tuple[object]", get_args(annotation))
        return List(resolve_dtype(item))

    if get_origin(annotation) is Literal:
        return String

    if isclass(annotation):
        if annotation in SCALAR_TYPES:
            return SCALAR_TYPES[annotation]

        if get_type_hints(annotation):
            fields = build_columns(annotation)
            return Struct({name: column.dtype for name, column in fields.items()})

    raise TypeError(f"unsupported annotation: {annotation!r}")


def validate_dataframe(dataframe: DataFrame, columns: dict[str, Column]) -> DataFrame:
    """Check a dataframe against the columns and return it in schema order.

    Raise `SchemaError` with every problem found. A dataframe with no rows and no
    columns becomes an empty dataframe with the schema.

    ```python
    validate_dataframe(DataFrame({"a": ["x"]}), {"a": Column(Int64, nullable=False)})
    # SchemaError: a: String is not Int64
    ```
    """
    if dataframe.shape == (0, 0):
        return DataFrame(schema={name: c.dtype for name, c in columns.items()})

    problems = find_name_problems(dataframe, columns)
    fitted: list[Series] = []

    for name in columns:
        if name not in dataframe.columns:
            continue

        try:
            fitted.append(fit(dataframe[name], columns[name]))
        except SchemaError as error:
            problems.append(str(error))

    if problems:
        raise SchemaError("; ".join(problems))

    return DataFrame(fitted)


def find_name_problems(dataframe: DataFrame, columns: dict[str, Column]) -> list[str]:
    """Return one message for the missing columns and one for the unexpected ones."""
    missing = [name for name in columns if name not in dataframe.columns]
    unexpected = [name for name in dataframe.columns if name not in columns]

    return [
        f"{describe_type} columns: {', '.join(names)}"
        for describe_type, names in [("missing", missing), ("unexpected", unexpected)]
        if names
    ]


def fit(series: Series, column: Column) -> Series:
    """Return the series with the type that the column expects.

    Raise `SchemaError` if the type, the nulls or a value do not fit.
    """
    if series.dtype == Null:
        series = series.cast(column.dtype)

    dtype = widen(series.dtype)

    if not is_same_type(dtype, column.dtype):
        raise SchemaError(
            f"{series.name}: {describe_type(series.dtype)} is not {describe_type(column.dtype)}"
        )

    if series.null_count() and not column.nullable:
        raise SchemaError(
            f"{series.name}: {series.null_count()} null values in a column that is not nullable"
        )

    try:
        return series.cast(dtype, strict=True)
    except exceptions.PolarsError as error:
        raise SchemaError(f"{series.name}: {error}") from error


def widen(dtype: DataType) -> DataType:
    """Return the type with integers as `Int64` and floats as `Float64`.

    This holds inside lists and structs.

    ```python
    widen(Int8)  # Int64
    widen(List(Float32))  # List(Float64)
    widen(Decimal(10, 2))  # Decimal(10, 2)
    ```
    """
    if dtype.is_integer():
        return Int64()

    if dtype.is_float():
        return Float64()

    match dtype:
        case List():
            return List(widen(cast("DataType", dtype.inner)))
        case Struct():
            return Struct(
                {
                    field.name: widen(cast("DataType", field.dtype))
                    for field in dtype.fields
                }
            )
        case _:
            return dtype


def is_same_type(actual: DataType, expected: DataType | type[DataType]) -> bool:
    """Return whether two types are equal.

    Extension types compare by name only. Their metadata, such as a geometry CRS, does
    not make them another type.
    """
    match actual, expected:
        case Extension(), Extension():
            return actual.ext_name() == expected.ext_name()
        case _:
            return actual == expected


def describe_type(dtype: DataType | type[DataType]) -> str:
    """Return the type as text for an error message, without extension metadata."""
    match dtype:
        case Extension():
            return f"Extension({dtype.ext_name()})"
        case _:
            return str(dtype)
