"""A typed query result: Polars frames checked against a schema class."""

from collections.abc import AsyncGenerator, AsyncIterator, Generator
from dataclasses import dataclass

from polars import DataFrame, concat

from ..constants import QUERY_SPAN, TEMPLATE_ATTRIBUTE
from ..telemetry import QUERY_ROWS, logger, tracer
from .schema import Column, validate_frame


@dataclass(frozen=True, slots=True)
class Dataset[Schema]:
    """A Polars frame that fits a schema class, the type parameter."""

    schema: type[Schema]
    frame: DataFrame

    def __len__(self) -> int:
        """Return the number of rows."""
        return self.frame.height


async def observe(
    template: str, frames: AsyncIterator[DataFrame]
) -> AsyncGenerator[DataFrame]:
    """Yield the frames of a query, with a span, a row counter and a log line.

    `template` labels all three.
    """
    rows = 0

    with tracer.start_as_current_span(
        QUERY_SPAN, attributes={TEMPLATE_ATTRIBUTE: template}
    ) as span:
        async for frame in frames:
            rows += frame.height
            span.set_attribute("toolkit.rows", rows)
            QUERY_ROWS.add(frame.height, {"template": template})
            yield frame

    logger.info("Query finished: template=%s rows=%d", template, rows)


@dataclass(frozen=True, slots=True)
class Result[Schema]:
    """The result of a query, read once.

    `await` it for one dataset, or iterate it for one dataset per batch.
    """

    schema: type[Schema]
    columns: dict[str, Column] | None
    template: str
    frames: AsyncIterator[DataFrame]

    async def __aiter__(self) -> AsyncGenerator[Dataset[Schema]]:
        """Yield one checked dataset per batch."""
        async for frame in observe(self.template, self.frames):
            yield Dataset(self.schema, self.checked(frame))

    def checked(self, frame: DataFrame) -> DataFrame:
        """Return the frame checked against the columns, or as it is with none."""
        return frame if self.columns is None else validate_frame(frame, self.columns)

    async def collect(self) -> Dataset[Schema]:
        """Read every batch and join them into one dataset."""
        frames = [batch.frame async for batch in self]
        frame = concat(frames) if frames else self.checked(DataFrame())
        return Dataset(self.schema, frame)

    def __await__(self) -> Generator[object, None, Dataset[Schema]]:
        """Return the awaitable that collects every batch."""
        return self.collect().__await__()
