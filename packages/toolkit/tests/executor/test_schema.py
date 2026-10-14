from dataclasses import dataclass, make_dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal, NamedTuple, TypedDict

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st

from toolkit.executor.constants import GEOMETRY_EXTENSION
from toolkit.executor.schema import Column, SchemaError, schema_of, validate_frame
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


GEOMETRY = pl.Extension(GEOMETRY_EXTENSION, pl.Binary)
WITH_CRS = pl.Extension(GEOMETRY_EXTENSION, pl.Binary, '{"crs": "EPSG:4326"}')
DAY = date(2024, 1, 1)


def order(**columns: object) -> pl.DataFrame:
    """Build a frame of one order, with extra columns."""
    return pl.DataFrame({"id": [1], "name": ["a"], "day": [DAY], **columns})


def place(dtype: pl.DataType) -> pl.DataFrame:
    """Build a frame of one place whose geometry has the type."""
    wkb = pl.Series("geom", [b"\x01\x01\x00\x00\x00" + bytes(16)], dtype=pl.Binary)
    return pl.DataFrame({"id": [1], "geom": wkb.cast(dtype)})


@pytest.mark.parametrize(
    ("schema", "expected"),
    [
        pytest.param(
            Order,
            {
                "id": Column(pl.Int64, nullable=False),
                "name": Column(pl.String, nullable=True),
                "day": Column(pl.Date, nullable=False),
            },
            id="typed_dict_with_optional",
        ),
        pytest.param(
            Point,
            {"x": Column(pl.Float64, False), "y": Column(pl.Float64, False)},
            id="named_tuple",
        ),
        pytest.param(
            Customer,
            {
                "tags": Column(pl.List(pl.String), nullable=False),
                "address": Column(
                    pl.Struct({"street": pl.String, "number": pl.Int64}), False
                ),
                "kind": Column(pl.String, False),
                "at": Column(pl.Datetime, False),
                "price": Column(pl.Decimal, False),
                "raw": Column(pl.Binary, False),
                "active": Column(pl.Boolean, False),
            },
            id="nested_and_scalar_types",
        ),
        pytest.param(
            Place,
            {"id": Column(pl.Int64, False), "geom": Column(GEOMETRY, False)},
            id="geometry",
        ),
    ],
)
def test_schema_of_reads_the_annotations(schema, expected):
    """Read the columns from the annotations."""
    assert schema_of(schema) == expected


@pytest.mark.parametrize(
    ("annotation", "message"),
    [
        pytest.param(set[int], "value: unsupported annotation", id="set"),
        pytest.param(list[set[int]], "value: unsupported annotation", id="inner_set"),
        pytest.param(int | str, "value: only `X | None`", id="union_of_two_types"),
    ],
)
def test_an_annotation_without_a_polars_type_is_rejected(annotation, message):
    """Reject an annotation without a Polars type."""
    bad = make_dataclass("Bad", [("value", annotation)])

    with pytest.raises(TypeError, match=message):
        schema_of(bad)


@pytest.mark.parametrize(
    ("schema", "frame", "expected"),
    [
        pytest.param(
            Order,
            order().select("day", "id", "name"),
            pl.Schema({"id": pl.Int64, "name": pl.String, "day": pl.Date}),
            id="columns_follow_the_schema_order",
        ),
        pytest.param(
            Reading,
            pl.DataFrame({"mm": pl.Series([1.5], dtype=pl.Float32)}),
            pl.Schema({"mm": pl.Float64}),
            id="float32_widens",
        ),
        pytest.param(
            Price,
            pl.DataFrame(
                {
                    "price": pl.Series([Decimal("1.250")], dtype=pl.Decimal(18, 3)),
                    "at": pl.Series([DAY]).cast(pl.Datetime("ms")),
                }
            ),
            pl.Schema({"price": pl.Decimal(18, 3), "at": pl.Datetime("ms")}),
            id="decimal_and_timestamp_keep_their_precision",
        ),
        pytest.param(
            Order,
            order(name=[None]),
            pl.Schema({"id": pl.Int64, "name": pl.String, "day": pl.Date}),
            id="a_column_of_nulls_takes_the_declared_type",
        ),
        pytest.param(
            Nested,
            pl.DataFrame(
                {
                    "ids": pl.Series([[1]], dtype=pl.List(pl.Int32)),
                    "point": pl.Series(
                        [{"x": 1.5, "y": 2.5}],
                        dtype=pl.Struct({"x": pl.Float32, "y": pl.Float32}),
                    ),
                }
            ),
            pl.Schema(
                {
                    "ids": pl.List(pl.Int64),
                    "point": pl.Struct({"x": pl.Float64, "y": pl.Float64}),
                }
            ),
            id="widening_reaches_lists_and_structs",
        ),
        pytest.param(
            Place,
            place(WITH_CRS),
            pl.Schema({"id": pl.Int64, "geom": WITH_CRS}),
            id="geometry_keeps_the_crs",
        ),
        pytest.param(
            Order,
            pl.DataFrame(),
            pl.Schema({"id": pl.Int64, "name": pl.String, "day": pl.Date}),
            id="no_rows_and_no_columns_is_an_empty_result",
        ),
    ],
)
def test_a_frame_that_fits_is_returned_with_the_schema_types(schema, frame, expected):
    """Return a frame that fits, with the schema types."""
    assert validate_frame(frame, schema_of(schema)).schema == expected


@pytest.mark.parametrize(
    ("schema", "frame", "message"),
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
            order(id=pl.Series([2**63], dtype=pl.UInt64)),
            "id",
            id="an_unsigned_value_beyond_int64",
        ),
        pytest.param(
            Nested,
            pl.DataFrame({"ids": [["x"]], "point": [{"x": 1.5, "y": 2.5}]}),
            "ids",
            id="a_list_of_the_wrong_type",
        ),
        pytest.param(
            Place,
            pl.DataFrame({"id": [1], "geom": [b"x"]}),
            "geom: Binary is not Extension",
            id="plain_binary_for_a_geometry",
        ),
        pytest.param(
            Order,
            place(GEOMETRY).rename({"geom": "name"}).with_columns(day=pl.lit(DAY)),
            "name: Extension",
            id="geometry_for_a_string",
        ),
    ],
)
def test_a_frame_that_does_not_fit_is_rejected_naming_the_column(
    schema, frame, message
):
    """Reject a frame that does not fit, naming the column."""
    with pytest.raises(SchemaError, match=message):
        validate_frame(frame, schema_of(schema))


def test_every_problem_is_reported_at_once():
    """Report every problem at once."""
    with pytest.raises(SchemaError) as error:
        validate_frame(pl.DataFrame({"id": ["1"], "extra": [1]}), schema_of(Order))

    assert str(error.value).split("; ")[:2] == [
        "missing columns: name, day",
        "unexpected columns: extra",
    ]


INTEGERS = [
    (pl.Int8, -(2**7), 2**7 - 1),
    (pl.Int16, -(2**15), 2**15 - 1),
    (pl.Int32, -(2**31), 2**31 - 1),
    (pl.Int64, -(2**63), 2**63 - 1),
    (pl.UInt8, 0, 2**8 - 1),
    (pl.UInt16, 0, 2**16 - 1),
    (pl.UInt32, 0, 2**32 - 1),
]


class OnlyId(TypedDict):
    id: int


@given(st.data())
def test_any_integer_type_widens_to_int64_and_keeps_its_values(data):
    """Widen any integer type to `Int64` and keep the values."""
    dtype, low, high = data.draw(st.sampled_from(INTEGERS))
    values = data.draw(st.lists(st.integers(low, high), min_size=1))

    result = validate_frame(
        pl.DataFrame({"id": pl.Series(values, dtype=dtype)}), schema_of(OnlyId)
    )

    assert result.schema == pl.Schema({"id": pl.Int64})
    assert result["id"].to_list() == values
