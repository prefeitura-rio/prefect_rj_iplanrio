"""The contract of an engine: the thing that runs statements."""

from collections.abc import AsyncIterator
from typing import Protocol

import polars as pl

from ...types import QueryParams
from ..constants import DEFAULT_BATCH_SIZE


class Engine(Protocol):
    """Runs rendered SQL. ``DuckDBEngine`` is the one the toolkit provides."""

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one statement."""
        ...

    def query(
        self,
        sql: str,
        *,
        params: QueryParams | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> AsyncIterator[pl.DataFrame]:
        """Run one query and yield the result as Polars frames of up to ``batch_size`` rows.

        A result with no rows still yields one frame, so the columns are known. A
        client that cannot tell the columns of an empty result yields a frame with
        no columns. Read the batches before running another statement on the client.
        """
        ...
