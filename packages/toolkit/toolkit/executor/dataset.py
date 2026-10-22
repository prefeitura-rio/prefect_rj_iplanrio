"""A typed query result: Polars dataframes checked against a schema class."""

from collections.abc import AsyncGenerator, AsyncIterator, Generator, Mapping
from dataclasses import dataclass
from typing import overload

from polars import DataFrame, concat

from ..constants import QUERY_SPAN, TEMPLATE_ATTRIBUTE
from ..telemetry import QUERY_ROWS, logger, tracer
from .schema import Column, build_columns, validate_dataframe
from .types import Expected


@dataclass(frozen=True, slots=True)
class Dataset[Schema]:
    """A Polars dataframe that fits a schema class, the type parameter."""

    schema: type[Schema]
    dataframe: DataFrame

    def __len__(self) -> int:
        """Return the number of rows."""
        return self.dataframe.height


async def observe(
    template: str, dataframes: AsyncIterator[DataFrame]
) -> AsyncGenerator[DataFrame]:
    """Yield the dataframes of a query, with a span, a row counter and a log line.

    `template` labels all three.
    """
    rows = 0

    with tracer.start_as_current_span(
        QUERY_SPAN, attributes={TEMPLATE_ATTRIBUTE: template}
    ) as span:
        async for dataframe in dataframes:
            rows += dataframe.height
            span.set_attribute("toolkit.rows", rows)
            QUERY_ROWS.add(dataframe.height, {"template": template})
            yield dataframe

    logger.info("Query finished: template=%s rows=%d", template, rows)


@dataclass(frozen=True, slots=True)
class Result[Schema]:
    """The result of a query, read once.

    `await` it for one dataset, or iterate it for one dataset per batch.
    """

    schema: type[Schema]
    columns: dict[str, Column] | None
    template: str
    dataframes: AsyncIterator[DataFrame]

    async def __aiter__(self) -> AsyncGenerator[Dataset[Schema]]:
        """Yield one checked dataset per batch."""
        async for dataframe in observe(self.template, self.dataframes):
            yield Dataset(self.schema, self.check_dataframe(dataframe))

    def check_dataframe(self, dataframe: DataFrame) -> DataFrame:
        """Return the dataframe checked against the columns, or as it is with none."""
        return (
            dataframe
            if self.columns is None
            else validate_dataframe(dataframe, self.columns)
        )

    async def collect(self) -> Dataset[Schema]:
        """Read every batch and join them into one dataset."""
        dataframes = [batch.dataframe async for batch in self]
        dataframe = (
            concat(dataframes) if dataframes else self.check_dataframe(DataFrame())
        )
        return Dataset(self.schema, dataframe)

    def __await__(self) -> Generator[object, None, Dataset[Schema]]:
        """Return the awaitable that collects every batch."""
        return self.collect().__await__()


def convert_columns(columns: Expected) -> dict[str, Column]:
    """Return the expected columns of a map of Polars types.

    A `Column` stays as it is. A bare Polars type is a column with no nulls, so a
    `polars.Schema`, which has no nullability, is a schema with no nulls too.

    ```python
    convert_columns(Schema({"id": Int64}))
    # {"id": Column(dtype=Int64, nullable=False)}
    convert_columns({"id": Int64, "note": Column(String, nullable=True)})
    # {"id": Column(dtype=Int64, nullable=False),
    #  "note": Column(dtype=String, nullable=True)}
    ```
    """
    return {
        name: value if isinstance(value, Column) else Column(value, nullable=False)
        for name, value in columns.items()
    }


@overload
def build_result[Schema](
    dataframes: AsyncIterator[DataFrame], expect: type[Schema], template: str
) -> Result[Schema]: ...


@overload
def build_result(
    dataframes: AsyncIterator[DataFrame], expect: Expected | None, template: str
) -> Result[dict[str, object]]: ...


def build_result[Schema](
    dataframes: AsyncIterator[DataFrame],
    expect: type[Schema] | Expected | None,
    template: str,
) -> Result[Schema] | Result[dict[str, object]]:
    """Return the dataframes as a result that is checked against `expect`.

    `expect` is a schema class, a map of columns or Polars types, or nothing to check
    nothing. `template` labels the span, the counter and the log line.
    """
    match expect:
        case None:
            return Result(dict, None, template, dataframes)
        case Mapping():
            return Result(dict, convert_columns(expect), template, dataframes)
        case _:
            return Result(expect, build_columns(expect), template, dataframes)
