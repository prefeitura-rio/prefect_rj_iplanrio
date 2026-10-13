"""The contract of an engine: the thing that runs statements."""

from typing import Protocol

from ...types import DatabaseRecord, QueryParams


class Engine(Protocol):
    """Runs rendered SQL. ``DuckDBEngine`` is the one the toolkit provides."""

    async def execute(self, sql: str, *, params: QueryParams | None = None) -> None:
        """Run one statement."""
        ...

    async def records(
        self, sql: str, *, params: QueryParams | None = None
    ) -> list[DatabaseRecord]:
        """Run one query and return every row as a dict keyed by column name."""
        ...
