"""A typed query result: Polars frames checked against a schema class."""

from collections.abc import AsyncGenerator, AsyncIterator, Generator
from dataclasses import dataclass

import polars as pl

from ..constants import QUERY_SPAN, TEMPLATE_ATTRIBUTE
from ..telemetry import QUERY_ROWS, logger, tracer
from .schema import Column, validate_frame


@dataclass(frozen=True, slots=True)
class Dataset[Schema]:
    """A Polars frame that fits a schema class. The type parameter is that class."""

    schema: type[Schema]
    frame: pl.DataFrame

    def __len__(self) -> int:
        return self.frame.height


@dataclass(frozen=True, slots=True)
class Result[Schema]:
    """The result of a query: ``await`` it for one dataset, or iterate it in batches.

    Each batch is checked against the schema when it arrives. A result is read once.
    """

    schema: type[Schema]
    columns: dict[str, Column]
    template: str
    """The template that made the query. It labels the metrics and the log."""
    frames: AsyncIterator[pl.DataFrame]

    async def __aiter__(self) -> AsyncGenerator[Dataset[Schema]]:
        """Yield one checked dataset per batch."""
        rows = 0

        with tracer.start_as_current_span(
            QUERY_SPAN, attributes={TEMPLATE_ATTRIBUTE: self.template}
        ) as span:
            async for frame in self.frames:
                dataset = Dataset(self.schema, validate_frame(frame, self.columns))
                rows += len(dataset)
                span.set_attribute("toolkit.rows", rows)
                QUERY_ROWS.add(len(dataset), {"template": self.template})
                yield dataset

        logger.info("Query finished: template=%s rows=%d", self.template, rows)

    async def collect(self) -> Dataset[Schema]:
        """Read every batch and join them into one dataset."""
        frames = [batch.frame async for batch in self]
        frame = (
            pl.concat(frames)
            if frames
            else validate_frame(pl.DataFrame(), self.columns)
        )
        return Dataset(self.schema, frame)

    def __await__(self) -> Generator[object, None, Dataset[Schema]]:
        return self.collect().__await__()
