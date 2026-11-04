from dataclasses import dataclass, make_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal, NamedTuple, TypedDict

import pytest
from hypothesis import given
from hypothesis import strategies as st
from polars import (
    Binary,
    Boolean,
    DataFrame,
    DataType,
    Date,
    Datetime,
    Extension,
    Float32,
    Float64,
    Int8,
    Int16,
    Int32,
    Int64,
    List,
    Schema,
    Series,
    String,
    Struct,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    lit,
)
from polars import Decimal as polars_Decimal

from toolkit.executor.constants import GEOMETRY_EXTENSION
from toolkit.executor.dataset import convert_columns
from toolkit.executor.schema import (
    Column,
    SchemaError,
    build_columns,
    validate_dataframe,
)
from toolkit.types import Geometry


class Order(TypedDict):
    id: int
    name: str | None
    day: date


class Point(NamedTuple):
    x: float
    y: float


@dataclass
class Address:
    street: str
    number: int | None


class Customer(TypedDict):
    tags: list[str]
    address: Address
    kind: Literal["a", "b"]
    at: datetime
    price: Decimal
    raw: bytes
    active: bool


class Place(TypedDict):
    id: int
    geom: Geometry


class Reading(TypedDict):
    mm: float


class Nested(TypedDict):
    ids: list[int]
    point: Point


class Price(TypedDict):
    price: Decimal
    at: datetime


GEOMETRY = Extension(GEOMETRY_EXTENSION, Binary)
WITH_CRS = Extension(GEOMETRY_EXTENSION, Binary, '{"crs": "EPSG:4326"}')
DAY = date(2024, 1, 1)


def order(**columns: object) -> DataFrame:
    """Build a dataframe of one order, with extra columns."""
    return DataFrame({"id": [1], "name": ["a"], "day": [DAY], **columns})


def place(dtype: DataType) -> DataFrame:
    """Build a dataframe of one place whose geometry has the type."""
    wkb = Series("geom", [b"\x01\x01\x00\x00\x00" + bytes(16)], dtype=Binary)
    return DataFrame({"id": [1], "geom": wkb.cast(dtype)})


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        pytest.param(
            Order,
            {
                "id": Column(Int64, nullable=False),
                "name": Column(String, nullable=True),
                "day": Column(Date, nullable=False),
            },
            id="typed_dict_with_optional",
        ),
        pytest.param(
            Point,
            {"x": Column(Float64, False), "y": Column(Float64, False)},
            id="named_tuple",
        ),
        pytest.param(
            Customer,
            {
                "tags": Column(List(String), nullable=False),
                "address": Column(Struct({"street": String, "number": Int64}), False),
                "kind": Column(String, False),
                "at": Column(Datetime, False),
                "price": Column(polars_Decimal, False),
                "raw": Column(Binary, False),
                "active": Column(Boolean, False),
            },
            id="nested_and_scalar_types",
        ),
        pytest.param(
            Place,
            {"id": Column(Int64, False), "geom": Column(GEOMETRY, False)},
            id="geometry",
        ),
    ],
)
def test_schema_of_reads_the_annotations(schema, expected) -> None:
    """Read the columns from the annotations."""
    assert build_columns(schema) == expected


@pytest.mark.parametrize(
    ("annotation", "message"),
    [
        pytest.param(set[int], "value: unsupported annotation", id="set"),
        pytest.param(list[set[int]], "value: unsupported annotation", id="inner_set"),
        pytest.param(int | str, "value: only `X | None`", id="union_of_two_types"),
    ],
)
def test_an_annotation_without_a_polars_type_is_rejected(annotation, message) -> None:
    """Reject an annotation without a Polars type."""
    bad = make_dataclass("Bad", [("value", annotation)])

    with pytest.raises(TypeError, match=message):
        build_columns(bad)


@pytest.mark.parametrize(
    ("schema", "dataframe", "expected"),
    [
        pytest.param(
            Order,
            order().select("day", "id", "name"),
            Schema({"id": Int64, "name": String, "day": Date}),
            id="columns_follow_the_schema_order",
        ),
        pytest.param(
            Reading,
            DataFrame({"mm": Series([1.5], dtype=Float32)}),
            Schema({"mm": Float64}),
            id="float32_widens",
        ),
        pytest.param(
            Price,
            DataFrame(
                {
                    "price": Series([Decimal("1.250")], dtype=polars_Decimal(18, 3)),
                    "at": Series([DAY]).cast(Datetime("ms")),
                }
            ),
            Schema({"price": polars_Decimal(18, 3), "at": Datetime("ms")}),
            id="decimal_and_timestamp_keep_their_precision",
        ),
        pytest.param(
            Order,
            order(name=[None]),
            Schema({"id": Int64, "name": String, "day": Date}),
            id="a_column_of_nulls_takes_the_declared_type",
        ),
        pytest.param(
            Nested,
            DataFrame(
                {
                    "ids": Series([[1]], dtype=List(Int32)),
                    "point": Series(
                        [{"x": 1.5, "y": 2.5}],
                        dtype=Struct({"x": Float32, "y": Float32}),
                    ),
                }
            ),
            Schema(
                {
                    "ids": List(Int64),
                    "point": Struct({"x": Float64, "y": Float64}),
                }
            ),
            id="widening_reaches_lists_and_structs",
        ),
        pytest.param(
            Place,
            place(WITH_CRS),
            Schema({"id": Int64, "geom": WITH_CRS}),
            id="geometry_keeps_the_crs",
        ),
        pytest.param(
            Order,
            DataFrame(),
            Schema({"id": Int64, "name": String, "day": Date}),
            id="no_rows_and_no_columns_is_an_empty_result",
        ),
    ],
)
def test_a_dataframe_that_fits_is_returned_with_the_schema_types(
    schema, dataframe, expected
) -> None:
    """Return a dataframe that fits, with the schema types."""
    assert validate_dataframe(dataframe, build_columns(schema)).schema == expected


@pytest.mark.parametrize(
    ("schema", "dataframe", "message"),
    [
        pytest.param(
            Order, order().drop("name"), "missing columns: name", id="missing"
        ),
        pytest.param(Order, order(extra=[1]), "unexpected columns: extra", id="extra"),
        pytest.param(
            Order, order(id=["1"]), "id: String is not Int64", id="string_for_an_int"
        ),
        pytest.param(
            Order, order(id=[1.5]), "id: Float64 is not Int64", id="float_for_an_int"
        ),
        pytest.param(
            Order, order(id=[None]), "id: 1 null", id="null_in_a_required_column"
        ),
        pytest.param(
            Order,
            order(id=Series([2**63], dtype=UInt64)),
            "id",
            id="an_unsigned_value_beyond_int64",
        ),
        pytest.param(
            Nested,
            DataFrame({"ids": [["x"]], "point": [{"x": 1.5, "y": 2.5}]}),
            "ids",
            id="a_list_of_the_wrong_type",
        ),
        pytest.param(
            Place,
            DataFrame({"id": [1], "geom": [b"x"]}),
            "geom: Binary is not Extension",
            id="plain_binary_for_a_geometry",
        ),
        pytest.param(
            Order,
            place(GEOMETRY).rename({"geom": "name"}).with_columns(day=lit(DAY)),
            "name: Extension",
            id="geometry_for_a_string",
        ),
    ],
)
def test_a_dataframe_that_does_not_fit_is_rejected_naming_the_column(
    schema, dataframe, message
) -> None:
    """Reject a dataframe that does not fit, naming the column."""
    with pytest.raises(SchemaError, match=message):
        validate_dataframe(dataframe, build_columns(schema))


def test_every_problem_is_reported_at_once() -> None:
    """Report every problem at once."""
    with pytest.raises(SchemaError) as error:
        validate_dataframe(DataFrame({"id": ["1"], "extra": [1]}), build_columns(Order))

    assert str(error.value).split("; ")[:2] == [
        "missing columns: name, day",
        "unexpected columns: extra",
    ]


INTEGERS = [
    (Int8, -(2**7), 2**7 - 1),
    (Int16, -(2**15), 2**15 - 1),
    (Int32, -(2**31), 2**31 - 1),
    (Int64, -(2**63), 2**63 - 1),
    (UInt8, 0, 2**8 - 1),
    (UInt16, 0, 2**16 - 1),
    (UInt32, 0, 2**32 - 1),
]


class OnlyId(TypedDict):
    id: int


@given(st.data())
def test_any_integer_type_widens_to_int64_and_keeps_its_values(data) -> None:
    """Widen any integer type to `Int64` and keep the values."""
    dtype, low, high = data.draw(st.sampled_from(INTEGERS))
    values = data.draw(st.lists(st.integers(low, high), min_size=1))

    result = validate_dataframe(
        DataFrame({"id": Series(values, dtype=dtype)}), build_columns(OnlyId)
    )

    assert result.schema == Schema({"id": Int64})
    assert result["id"].to_list() == values


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        pytest.param(
            Schema({"id": Int64, "nome": String}),
            {
                "id": Column(Int64, nullable=False),
                "nome": Column(String, nullable=False),
            },
            id="a_polars_schema_has_no_nulls",
        ),
        pytest.param(
            {"id": Int64, "nota": Column(String, nullable=True)},
            {
                "id": Column(Int64, nullable=False),
                "nota": Column(String, nullable=True),
            },
            id="a_column_keeps_its_nullability",
        ),
        pytest.param(
            {"id": Int64()},
            {"id": Column(Int64(), nullable=False)},
            id="a_type_instance_has_no_nulls",
        ),
        pytest.param({}, {}, id="an_empty_map_expects_nothing"),
    ],
)
def test_a_map_of_polars_types_is_a_map_of_columns(given, expected) -> None:
    """Make a bare type a column with no nulls, and keep a `Column` as it is."""
    assert convert_columns(given) == expected


def test_a_polars_schema_refuses_a_null() -> None:
    """Raise `SchemaError` for a null in a column of a Polars schema."""
    dataframe = DataFrame({"id": [1, None]})

    with pytest.raises(SchemaError, match="id: 1 null values"):
        validate_dataframe(dataframe, convert_columns(Schema({"id": Int64})))


def test_a_column_that_says_nullable_accepts_the_null() -> None:
    """Return the dataframe when the schema says that the column holds nulls."""
    dataframe = DataFrame({"id": [1, None]})

    assert validate_dataframe(
        dataframe, convert_columns({"id": Column(Int64, nullable=True)})
    ).equals(dataframe)
